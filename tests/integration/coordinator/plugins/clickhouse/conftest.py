"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from _pytest.fixtures import FixtureRequest
from astacus.client import create_client_parsers
from astacus.common.ipc import Plugin
from astacus.common.rohmustorage import (
    RohmuCompression,
    RohmuCompressionType,
    RohmuConfig,
    RohmuEncryptionKey,
    RohmuLocalStorageConfig,
    RohmuStorageType,
)
from astacus.config import GlobalConfig, UvicornConfig
from astacus.coordinator.config import CoordinatorConfig, CoordinatorNode
from astacus.coordinator.plugins.clickhouse.client import HttpClickHouseClient
from astacus.coordinator.plugins.clickhouse.config import ClickHouseConfiguration, ClickHouseNode, ReplicatedDatabaseSettings
from astacus.coordinator.plugins.clickhouse.plugin import ClickHousePlugin
from astacus.coordinator.plugins.zookeeper_config import ZooKeeperConfiguration, ZooKeeperNode
from astacus.node.config import NodeConfig
from pathlib import Path
from tests.conftest import CLICKHOUSE_PATH_OPTION, CLICKHOUSE_RESTORE_PATH_OPTION
from tests.integration.conftest import get_command_path, Ports, run_process_and_wait_for_pattern, Service, ServiceCluster
from tests.system.conftest import background_process, wait_url_up
from tests.utils import CONSTANT_TEST_RSA_PRIVATE_KEY, CONSTANT_TEST_RSA_PUBLIC_KEY, get_clickhouse_version
from typing import AsyncIterator, Awaitable, List, Sequence, Union

import argparse
import asyncio
import contextlib
import dataclasses
import logging
import pytest
import sys
import tempfile

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.clickhouse, pytest.mark.x86_64]


@dataclasses.dataclass
class ClickHouseServiceCluster(ServiceCluster):
    use_named_collections: bool
    expands_uuid_in_zookeeper_path: bool


USER_CONFIG = """
    <clickhouse>
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
    </clickhouse>
"""

ClickHouseCommand = List[Union[str, Path]]


@pytest.fixture(scope="module", name="clickhouse_command")
async def fixture_clickhouse_command(request: FixtureRequest) -> ClickHouseCommand:
    clickhouse_path = request.config.getoption(CLICKHOUSE_PATH_OPTION)
    if clickhouse_path is None:
        clickhouse_path = await get_command_path("clickhouse")
    if clickhouse_path is None:
        clickhouse_path = await get_command_path("clickhouse-server")
    if clickhouse_path is None:
        pytest.skip("clickhouse installation not found")
    return get_clickhouse_command(clickhouse_path)


@pytest.fixture(scope="module", name="clickhouse_restore_command")
def fixture_clickhouse_restore_command(request: FixtureRequest, clickhouse_command: ClickHouseCommand) -> ClickHouseCommand:
    clickhouse_restore_path = request.config.getoption(CLICKHOUSE_RESTORE_PATH_OPTION)
    if clickhouse_restore_path is None:
        return clickhouse_command
    return get_clickhouse_command(clickhouse_restore_path)


def get_clickhouse_command(clickhouse_path: Path) -> ClickHouseCommand:
    return [clickhouse_path] if clickhouse_path.name.endswith("-server") else [clickhouse_path, "server"]


@pytest.fixture(scope="module", name="clickhouse")
async def fixture_clickhouse(ports: Ports, clickhouse_command: ClickHouseCommand) -> AsyncIterator[Service]:
    async with create_clickhouse_service(ports, clickhouse_command) as service:
        yield service


@contextlib.asynccontextmanager
async def create_clickhouse_service(ports: Ports, command: ClickHouseCommand) -> AsyncIterator[Service]:
    http_port = ports.allocate()
    with tempfile.TemporaryDirectory(prefix=f"clickhouse_{http_port}_") as data_dir_str:
        data_dir = Path(data_dir_str)
        command += ["--", f"--http_port={http_port}"]
        async with run_process_and_wait_for_pattern(args=command, cwd=data_dir, pattern="Ready for connections.") as process:
            yield Service(process=process, port=http_port, data_dir=data_dir)


@contextlib.asynccontextmanager
async def create_clickhouse_cluster(
    zookeeper: Service,
    ports: Ports,
    cluster_shards: Sequence[str],
    command: ClickHouseCommand,
) -> AsyncIterator[ClickHouseServiceCluster]:
    cluster_size = len(cluster_shards)
    clickhouse_version = get_clickhouse_version(command)
    use_named_collections = clickhouse_version >= (22, 4)
    expands_uuid_in_zookeeper_path = clickhouse_version < (22, 4)
    tcp_ports = [ports.allocate() for _ in range(cluster_size)]
    http_ports = [ports.allocate() for _ in range(cluster_size)]
    interserver_http_ports = [ports.allocate() for _ in range(cluster_size)]
    joined_http_ports = "-".join(str(port) for port in http_ports)
    with tempfile.TemporaryDirectory(prefix=f"clickhouse_{joined_http_ports}_") as base_data_dir:
        data_dirs = [Path(base_data_dir) / f"clickhouse_{http_port}" for http_port in http_ports]
        configs = create_clickhouse_configs(
            cluster_shards, zookeeper, data_dirs, tcp_ports, http_ports, interserver_http_ports, use_named_collections
        )
        for config, data_dir in zip(configs, data_dirs):
            data_dir.mkdir()
            (data_dir / "config.xml").write_text(config)
            (data_dir / "users.xml").write_text(USER_CONFIG)
        async with contextlib.AsyncExitStack() as stack:
            processes = [
                await stack.enter_async_context(
                    run_process_and_wait_for_pattern(args=command, cwd=data_dir, pattern="Ready for connections.")
                )
                for data_dir in data_dirs
            ]
            yield ClickHouseServiceCluster(
                services=[
                    Service(process=process, port=http_port, username="default", password="secret", data_dir=data_dir)
                    for process, http_port, data_dir in zip(processes, http_ports, data_dirs)
                ],
                use_named_collections=use_named_collections,
                expands_uuid_in_zookeeper_path=expands_uuid_in_zookeeper_path,
            )


@contextlib.asynccontextmanager
async def create_astacus_cluster(
    storage_path: Path,
    zookeeper: Service,
    clickhouse_cluster: ClickHouseServiceCluster,
    ports: Ports,
    *,
    use_system_unfreeze: bool,
) -> AsyncIterator[ServiceCluster]:
    configs = create_astacus_configs(zookeeper, clickhouse_cluster, ports, Path(storage_path), use_system_unfreeze)
    async with contextlib.AsyncExitStack() as stack:
        astacus_services_coro: List[Awaitable] = [stack.enter_async_context(_astacus(config=config)) for config in configs]
        astacus_services = list(await asyncio.gather(*astacus_services_coro))
        yield ServiceCluster(services=astacus_services)


def create_clickhouse_configs(
    cluster_shards: Sequence[str],
    zookeeper: Service,
    data_dirs: List[Path],
    tcp_ports: List[int],
    http_ports: List[int],
    interserver_http_ports: List[int],
    use_named_collections: bool,
):
    replicas = "\n".join(
        f"""
        <replica>
            <host>localhost</host>
            <port>{tcp_port}</port>
            <secure>false</secure>
        </replica>
        """
        for tcp_port in tcp_ports
    )
    named_collections = (
        """
        <named_collections>
            <default_cluster>
                <cluster_secret>secret</cluster_secret>
            </default_cluster>
        </named_collections>
        """
        if use_named_collections
        else ""
    )
    return [
        f"""
                <clickhouse>
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
                    <enable_system_unfreeze>true</enable_system_unfreeze>
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
                    {named_collections}
                    <default_replica_path>/clickhouse/tables/{{uuid}}/{{my_shard}}</default_replica_path>
                    <default_replica_name>{{my_replica}}</default_replica_name>
                    <macros>
                        <my_shard>{cluster_shard}</my_shard>
                        <my_replica>r{http_port}</my_replica>
                    </macros>
                </clickhouse>
                """
        for cluster_shard, data_dir, tcp_port, http_port, interserver_http_port in zip(
            cluster_shards, data_dirs, tcp_ports, http_ports, interserver_http_ports
        )
    ]


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
    clickhouse_cluster: ClickHouseServiceCluster,
    ports: Ports,
    storage_path: Path,
    use_system_unfreeze: bool,
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
                            )
                            for service in clickhouse_cluster.services
                        ],
                    ),
                    replicated_databases_settings=ReplicatedDatabaseSettings(
                        collection_name="default_cluster",
                    )
                    if clickhouse_cluster.use_named_collections
                    else ReplicatedDatabaseSettings(
                        cluster_username=clickhouse_cluster.services[0].username,
                        cluster_password=clickhouse_cluster.services[0].password,
                    ),
                    sync_databases_timeout=10.0,
                    sync_tables_timeout=30.0,
                    use_system_unfreeze=use_system_unfreeze,
                ).jsondict(),
            ),
            node=NodeConfig(
                root=clickhouse_service.data_dir,
            ),
            object_storage=RohmuConfig(
                temporary_directory=str(storage_tmp_path),
                default_storage="test",
                storages={
                    "test": RohmuLocalStorageConfig(
                        storage_type=RohmuStorageType.local,
                        directory=storage_path,
                    )
                },
                compression=RohmuCompression(algorithm=RohmuCompressionType.zstd),
                encryption_key_id="test",
                encryption_keys={
                    "test": RohmuEncryptionKey(public=CONSTANT_TEST_RSA_PUBLIC_KEY, private=CONSTANT_TEST_RSA_PRIVATE_KEY)
                },
            ),
            uvicorn=UvicornConfig(
                port=node_port,
                log_level="debug",
            ),
        )
        for node_port, clickhouse_service in zip(node_ports, clickhouse_cluster.services)
    ]


def get_clickhouse_client(clickhouse: Service, timeout: float = 10.0) -> HttpClickHouseClient:
    return HttpClickHouseClient(
        host=clickhouse.host,
        port=clickhouse.port,
        username=clickhouse.username,
        password=clickhouse.password,
        timeout=timeout,
    )
