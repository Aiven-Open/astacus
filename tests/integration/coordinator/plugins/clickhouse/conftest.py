"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from astacus.client import create_client_parsers
from astacus.common.ipc import Plugin
from astacus.common.rohmustorage import (
    RohmuCompression, RohmuCompressionType, RohmuConfig, RohmuEncryptionKey, RohmuLocalStorageConfig, RohmuStorageType
)
from astacus.config import GlobalConfig, UvicornConfig
from astacus.coordinator.config import CoordinatorConfig, CoordinatorNode
from astacus.coordinator.plugins import ClickHousePlugin
from astacus.coordinator.plugins.clickhouse.client import HttpClickHouseClient
from astacus.coordinator.plugins.clickhouse.config import ClickHouseConfiguration, ClickHouseNode, ReplicatedDatabaseSettings
from astacus.coordinator.plugins.zookeeper_config import ZooKeeperConfiguration, ZooKeeperNode
from astacus.node.config import NodeConfig
from pathlib import Path
from tests.system.conftest import background_process, wait_url_up
from tests.utils import CONSTANT_TEST_RSA_PRIVATE_KEY, CONSTANT_TEST_RSA_PUBLIC_KEY
from typing import AsyncIterator, Awaitable, Dict, Iterator, List, Optional, Union

import argparse
import asyncio
import contextlib
import dataclasses
import logging
import pytest
import subprocess
import sys
import tempfile
import threading

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.clickhouse]

USER_CONFIG = """
    <yandex>
        <users>
            <default>
                <password>secret</password>
                <access_management>true</access_management>
            </default>
        </users>
        <profiles>
            <default>
                <allow_experimental_database_replicated>true</allow_experimental_database_replicated>
            </default>
        </profiles>
        <quotas><default></default></quotas>
    </yandex>
"""


@pytest.fixture(scope="module", name="event_loop")
def fixture_event_loop() -> Iterator[asyncio.AbstractEventLoop]:
    # This is the same as the original `event_loop` fixture from `pytest_asyncio`
    # but with a module scope, re-declaring this fixture is their suggested way
    # of locally increasing the scope of this fixture.
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@dataclasses.dataclass
class Service:
    process: asyncio.subprocess.Process
    data_dir: Path
    port: int
    host: str = "localhost"
    username: Optional[str] = None
    password: Optional[str] = None


@dataclasses.dataclass
class ServiceCluster:
    services: List[Service]


class Ports:
    def __init__(self, start: int = 50000):
        self.start = start

    def allocate(self) -> int:
        allocated = self.start
        self.start += 1
        return allocated


@pytest.fixture(scope="session", name="ports")
def fixture_ports() -> Ports:
    return Ports()


@pytest.fixture(scope="module", name="clickhouse")
async def fixture_clickhouse(ports: Ports) -> AsyncIterator[Service]:
    async with create_clickhouse_service(ports) as service:
        yield service


@contextlib.asynccontextmanager
async def create_clickhouse_service(ports: Ports) -> AsyncIterator[Service]:
    command = await get_clickhouse_command()
    if command is None:
        pytest.skip("clickhouse installation not found")
    http_port = ports.allocate()
    with tempfile.TemporaryDirectory(prefix=f"clickhouse_{http_port}_") as data_dir_str:
        data_dir = Path(data_dir_str)
        command += ["--", f"--http_port={http_port}"]
        async with run_process_and_wait_for_pattern(args=command, cwd=data_dir, pattern="Ready for connections.") as process:
            yield Service(process=process, port=http_port, data_dir=data_dir)


@pytest.fixture(scope="module", name="zookeeper")
async def fixture_zookeeper(ports: Ports) -> AsyncIterator[Service]:
    async with create_zookeeper(ports) as zookeeper:
        yield zookeeper


@contextlib.asynccontextmanager
async def create_zookeeper(ports: Ports) -> AsyncIterator[Service]:
    java_path = await get_command_path("java")
    if java_path is None:
        pytest.skip("java installation not found")
    port = ports.allocate()
    with tempfile.TemporaryDirectory(prefix=f"zookeeper_{port}_") as data_dir_str:
        data_dir = Path(data_dir_str)
        log4j_properties = data_dir / "log4j.properties"
        log4j_properties.write_text(
            """
log4j.rootLogger=INFO,default
log4j.appender.default=org.apache.log4j.ConsoleAppender
log4j.appender.default.Target=System.err
log4j.appender.default.layout=org.apache.log4j.PatternLayout
log4j.appender.default.layout.ConversionPattern=[%-5p] %m%n
"""
        )
        command = get_zookeeper_command(java_path=java_path, data_dir=data_dir, port=port)
        if command is None:
            pytest.skip("zookeeper installation not found")
        async with run_process_and_wait_for_pattern(
            args=command, cwd=data_dir, pattern="PrepRequestProcessor (sid:0) started"
        ) as process:
            yield Service(process=process, port=port, data_dir=data_dir)


@contextlib.asynccontextmanager
async def create_clickhouse_cluster(
    zookeeper: Service,
    ports: Ports,
    cluster_size: int = 2,
) -> AsyncIterator[ServiceCluster]:
    command = await get_clickhouse_command()
    if command is None:
        pytest.skip("clickhouse installation not found")
    tcp_ports = [ports.allocate() for _ in range(cluster_size)]
    http_ports = [ports.allocate() for _ in range(cluster_size)]
    interserver_http_ports = [ports.allocate() for _ in range(cluster_size)]
    joined_http_ports = "-".join(str(port) for port in http_ports)
    with tempfile.TemporaryDirectory(prefix=f"clickhouse_{joined_http_ports}_") as base_data_dir:
        data_dirs = [Path(base_data_dir) / f"clickhouse_{http_port}" for http_port in http_ports]
        configs = create_clickhouse_configs(zookeeper, data_dirs, tcp_ports, http_ports, interserver_http_ports)
        for config, data_dir in zip(configs, data_dirs):
            data_dir.mkdir()
            (data_dir / "config.xml").write_text(config)
            (data_dir / "users.xml").write_text(USER_CONFIG)
        async with contextlib.AsyncExitStack() as stack:
            processes = [
                await stack.enter_async_context(
                    run_process_and_wait_for_pattern(args=command, cwd=data_dir, pattern="Ready for connections.")
                ) for data_dir in data_dirs
            ]
            yield ServiceCluster(
                services=[
                    Service(process=process, port=http_port, username="default", password="secret", data_dir=data_dir)
                    for process, http_port, data_dir in zip(processes, http_ports, data_dirs)
                ]
            )


@contextlib.asynccontextmanager
async def create_astacus_cluster(storage_path: Path, zookeeper: Service, clickhouse_cluster: ServiceCluster,
                                 ports: Ports) -> AsyncIterator[ServiceCluster]:
    configs = create_astacus_configs(zookeeper, clickhouse_cluster, ports, Path(storage_path))
    async with contextlib.AsyncExitStack() as stack:
        astacus_services_coro: List[Awaitable] = [stack.enter_async_context(_astacus(config=config)) for config in configs]
        astacus_services = list(await asyncio.gather(*astacus_services_coro))
        yield ServiceCluster(services=astacus_services)


def create_clickhouse_configs(
    zookeeper: Service, data_dirs: List[Path], tcp_ports: List[int], http_ports: List[int], interserver_http_ports: List[int]
):
    replicas = "\n".join(
        f"""
        <replica>
            <host>localhost</host>
            <port>{tcp_port}</port>
            <secure>false</secure>
        </replica>
        """ for tcp_port in tcp_ports
    )
    return [
        f"""
                <yandex>
                    <path>{str(data_dir)}</path>
                    <logger>
                        <level>debug</level>
                        <console>true</console>
                    </logger>
                    <tcp_port>{tcp_port}</tcp_port>
                    <http_port>{http_port}</http_port>
                    <interserver_http_host>localhost</interserver_http_host>
                    <interserver_http_port>{interserver_http_port}</interserver_http_port>
                    <zookeeper>
                        <node>
                            <host>{zookeeper.host}</host>
                            <port>{zookeeper.port}</port>
                        </node>
                    </zookeeper>
                    <mark_cache_size>5368709120</mark_cache_size>
                    <max_server_memory_usage_to_ram_ratio>0.5</max_server_memory_usage_to_ram_ratio>
                    <user_directories>
                        <users_xml>
                            <path>{str(data_dir / "users.xml")}</path>
                        </users_xml>
                        <replicated>
                            <zookeeper_path>/clickhouse/access/</zookeeper_path>
                        </replicated>
                    </user_directories>
                    <remote_servers>
                        <defaultcluster>
                            <shard>
                                <internal_replication>true</internal_replication>
                                {replicas}
                            </shard>
                        </defaultcluster>
                    </remote_servers>
                    <macros>
                        <shard>s01</shard>
                        <replica>r{http_port}</replica>
                    </macros>
                </yandex>
                """ for data_dir, tcp_port, http_port, interserver_http_port in
        zip(data_dirs, tcp_ports, http_ports, interserver_http_ports)
    ]


async def get_clickhouse_command() -> Optional[List[Union[str, Path]]]:
    for command_name in "clickhouse", "clickhouse-server":
        path = await get_command_path(command_name)
        if path:
            return [path] if path.name.endswith("-server") else [path, "server"]
    return None


def get_zookeeper_command(*, java_path: Path, data_dir: Path, port: int) -> Optional[List[Union[str, Path]]]:
    zookeeper_jars = list(Path("/usr/share/zookeeper").glob("*.jar"))
    if zookeeper_jars:
        class_paths = [data_dir, *zookeeper_jars]
        class_path_option = ":".join(str(path) for path in class_paths)
        zookeeper_path = "org.apache.zookeeper.server.quorum.QuorumPeerMain"
        java_options = ["-Dzookeeper.admin.enableServer=false"]
        return [java_path, "-cp", class_path_option, *java_options, zookeeper_path, str(port), data_dir]
    return None


async def get_command_path(name: str) -> Optional[Path]:
    process = await asyncio.create_subprocess_exec(
        "which", name, stderr=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE
    )
    stdout, stderr = await process.communicate()
    decoded_stderr = stderr.decode()
    if decoded_stderr:
        logger.debug("which %s: %s", name, decoded_stderr)
    if process.returncode == 0:
        return Path(stdout.decode().rstrip("\n"))
    return None


@contextlib.asynccontextmanager
async def run_process_and_wait_for_pattern(
    *,
    args: List[Union[str, Path]],
    cwd: Path,
    pattern: str,
    timeout: float = 10.0,
) -> AsyncIterator[asyncio.subprocess.Process]:
    # This stringification is a workaround for a bug in pydev (pydev_monkey.py:111)
    str_args = [str(arg) for arg in args]
    env: Dict[str, str] = {}
    pattern_found = asyncio.Event()
    loop = asyncio.get_running_loop()
    with subprocess.Popen(str_args, cwd=cwd, env=env, stderr=subprocess.PIPE) as process:

        def read_logs() -> None:
            assert process.stderr is not None
            while process.poll() is None:
                for line in process.stderr:
                    line = line.rstrip(b"\n")
                    decoded_line = line.rstrip(b"\n").decode(encoding="utf-8", errors="replace")
                    logger.debug("%d: %s", process.pid, decoded_line)
                    if pattern in decoded_line:
                        loop.call_soon_threadsafe(pattern_found.set)

        thread = threading.Thread(target=read_logs)
        thread.start()
        try:
            try:
                await asyncio.wait_for(pattern_found.wait(), timeout=timeout)
            except asyncio.TimeoutError as e:
                raise Exception(f"Pattern {pattern!r} not found after {timeout:.3f}s in output of {str_args}") from e
            yield process
        finally:
            process.kill()
            thread.join()


@contextlib.asynccontextmanager
async def _astacus(*, config: GlobalConfig) -> AsyncIterator[Service]:
    astacus_source_root = Path(__file__).parent.parent.parent
    assert config.object_storage is not None
    config_path = Path(config.object_storage.temporary_directory) / f"astacus_{config.uvicorn.port}.json"
    config_path.write_text(config.json())
    cmd = [sys.executable, "-m", "astacus.main", "server", "-c", str(config_path)]
    async with background_process(*cmd, env={"PYTHONPATH": astacus_source_root}) as process:
        await wait_url_up(f"http://localhost:{config.uvicorn.port}")
        storage = config.object_storage.storages[config.object_storage.default_storage]
        assert isinstance(storage, RohmuLocalStorageConfig)
        data_dir = storage.directory
        yield Service(process=process, port=config.uvicorn.port, data_dir=data_dir)


def run_astacus_command(astacus_cluster: ServiceCluster, *args: str) -> None:
    first_astacus = astacus_cluster.services[0]
    astacus_url = f"http://localhost:{first_astacus.port}"
    all_args = ["--url", astacus_url, "--wait-completion", "20000000"] + list(args)
    parser = argparse.ArgumentParser()
    create_client_parsers(parser, parser.add_subparsers())
    parsed_args = parser.parse_args(all_args)
    if not parsed_args.func(parsed_args):
        raise Exception(f"Command {all_args} on {astacus_url} failed")


def create_astacus_configs(
    zookeeper: Service,
    clickhouse_cluster: ServiceCluster,
    ports: Ports,
    storage_path: Path,
) -> List[GlobalConfig]:
    storage_tmp_path = storage_path / "tmp"
    storage_tmp_path.mkdir(exist_ok=True)
    node_ports = [ports.allocate() for _ in clickhouse_cluster.services]
    return [
        GlobalConfig(
            coordinator=CoordinatorConfig(
                nodes=[CoordinatorNode(url=f"http://localhost:{node_port}/node") for node_port in node_ports],
                plugin=Plugin.clickhouse,
                backup_attempts=1,
                restore_attempts=1,
                plugin_config=ClickHousePlugin(
                    zookeeper=ZooKeeperConfiguration(nodes=[ZooKeeperNode(host=zookeeper.host, port=zookeeper.port)]),
                    clickhouse=ClickHouseConfiguration(
                        username=clickhouse_cluster.services[0].username,
                        password=clickhouse_cluster.services[0].password,
                        nodes=[
                            ClickHouseNode(
                                host=service.host,
                                port=service.port,
                            ) for service in clickhouse_cluster.services
                        ]
                    ),
                    replicated_databases_settings=ReplicatedDatabaseSettings(
                        cluster_username=clickhouse_cluster.services[0].username,
                        cluster_password=clickhouse_cluster.services[0].password,
                    ),
                    sync_timeout=30.0,
                ).jsondict()
            ),
            node=NodeConfig(root=clickhouse_service.data_dir, ),
            object_storage=RohmuConfig(
                temporary_directory=str(storage_tmp_path),
                default_storage="test",
                storages={"test": RohmuLocalStorageConfig(
                    storage_type=RohmuStorageType.local,
                    directory=storage_path,
                )},
                compression=RohmuCompression(algorithm=RohmuCompressionType.zstd),
                encryption_key_id="test",
                encryption_keys={
                    "test": RohmuEncryptionKey(public=CONSTANT_TEST_RSA_PUBLIC_KEY, private=CONSTANT_TEST_RSA_PRIVATE_KEY)
                }
            ),
            uvicorn=UvicornConfig(
                port=node_port,
                log_level="debug",
            ),
        ) for node_port, clickhouse_service in zip(node_ports, clickhouse_cluster.services)
    ]


def get_clickhouse_client(clickhouse: Service, timeout: float = 10.0) -> HttpClickHouseClient:
    return HttpClickHouseClient(
        host=clickhouse.host,
        port=clickhouse.port,
        username=clickhouse.username,
        password=clickhouse.password,
        timeout=timeout
    )
