"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
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
from tests.integration.conftest import get_command_path, Ports, run_process_and_wait_for_pattern, Service, ServiceCluster
from tests.system.conftest import background_process, wait_url_up
from tests.utils import CONSTANT_TEST_RSA_PRIVATE_KEY, CONSTANT_TEST_RSA_PUBLIC_KEY
from typing import AsyncIterator, Awaitable, List, Optional, Sequence, Union

import argparse
import asyncio
import contextlib
import dataclasses
import logging
import pytest
import subprocess
import sys
import tempfile

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.clickhouse, pytest.mark.x86_64]


@dataclasses.dataclass
class ClickHouseServiceCluster(ServiceCluster):
    use_named_collections: bool
    expands_uuid_in_zookeeper_path: bool


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


@contextlib.asynccontextmanager
async def create_clickhouse_cluster(
    zookeeper: Service,
    ports: Ports,
    cluster_shards: Sequence[str],
) -> AsyncIterator[ClickHouseServiceCluster]:
    cluster_size = len(cluster_shards)
    command = await get_clickhouse_command()
    if command is None:
        pytest.skip("clickhouse installation not found")
    raw_clickhouse_version = subprocess.check_output([*command, "--version"]).strip().partition(b"version")[2]
    clickhouse_version = tuple(int(part) for part in raw_clickhouse_version.split(b".") if part)
    use_named_collections = clickhouse_version >= (22, 4)
    expands_uuid_in_zookeeper_path = clickhouse_version < (22, 4)
    tcp_ports = [ports.allocate() for _ in range(cluster_size)]
    http_ports = [ports.allocate() for _ in range(cluster_size)]
    interserver_http_ports = [ports.allocate() for _ in range(cluster_size)]
    joined_http_ports = "-".join(str(port) for port in http_ports)
    with tempfile.TemporaryDirectory(prefix=f"clickhouse_{joined_http_ports}_") as base_data_dir:
        data_dirs = [Path(base_data_dir) / f"clickhouse_{http_port}" for http_port in http_ports]
        configs = create_clickhouse_configs(
            cluster_shards, zookeeper, data_dirs, tcp_ports, http_ports, interserver_http_ports
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
    storage_path: Path, zookeeper: Service, clickhouse_cluster: ServiceCluster, ports: Ports
) -> AsyncIterator[ServiceCluster]:
    configs = create_astacus_configs(zookeeper, clickhouse_cluster, ports, Path(storage_path))
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
                    <default_replica_path>/clickhouse/tables/{{uuid}}/{{my_shard}}</default_replica_path>
                    <default_replica_name>{{my_replica}}</default_replica_name>
                    <macros>
                        <my_shard>{cluster_shard}</my_shard>
                        <my_replica>r{http_port}</my_replica>
                    </macros>
                </yandex>
                """
        for cluster_shard, data_dir, tcp_port, http_port, interserver_http_port in zip(
            cluster_shards, data_dirs, tcp_ports, http_ports, interserver_http_ports
        )
    ]


async def get_clickhouse_command() -> Optional[List[Union[str, Path]]]:
    for command_name in "clickhouse", "clickhouse-server":
        path = await get_command_path(command_name)
        if path:
            return [path] if path.name.endswith("-server") else [path, "server"]
    return None


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
                            )
                            for service in clickhouse_cluster.services
                        ],
                    ),
                    replicated_databases_settings=ReplicatedDatabaseSettings(
                        cluster_username=clickhouse_cluster.services[0].username,
                        cluster_password=clickhouse_cluster.services[0].password,
                    ),
                    sync_databases_timeout=10.0,
                    sync_tables_timeout=30.0,
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
