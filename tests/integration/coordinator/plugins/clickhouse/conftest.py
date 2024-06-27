"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""

from _pytest.fixtures import FixtureRequest
from astacus.client import create_client_parsers
from astacus.common.ipc import Plugin
from astacus.common.rohmustorage import RohmuCompression, RohmuCompressionType, RohmuConfig, RohmuEncryptionKey
from astacus.common.utils import build_netloc
from astacus.config import GlobalConfig, UvicornConfig
from astacus.coordinator.config import CoordinatorConfig, CoordinatorNode
from astacus.coordinator.plugins.clickhouse.client import HttpClickHouseClient
from astacus.coordinator.plugins.clickhouse.config import (
    ClickHouseConfiguration,
    ClickHouseNode,
    DiskConfiguration,
    DiskObjectStorageConfiguration,
    DiskType,
    ReplicatedDatabaseSettings,
)
from astacus.coordinator.plugins.clickhouse.plugin import ClickHousePlugin
from astacus.coordinator.plugins.zookeeper_config import ZooKeeperConfiguration, ZooKeeperNode
from astacus.node.config import NodeConfig
from collections.abc import AsyncIterator, Awaitable, Iterator, Sequence
from pathlib import Path
from tests.conftest import CLICKHOUSE_PATH_OPTION, CLICKHOUSE_RESTORE_PATH_OPTION
from tests.integration.conftest import get_command_path, Ports, run_process_and_wait_for_pattern, Service, ServiceCluster
from tests.system.conftest import background_process, wait_url_up
from tests.utils import (
    CONSTANT_TEST_RSA_PRIVATE_KEY,
    CONSTANT_TEST_RSA_PUBLIC_KEY,
    format_astacus_command,
    get_clickhouse_version,
)

import argparse
import asyncio
import botocore.client
import botocore.session
import contextlib
import dataclasses
import logging
import pytest
import rohmu
import secrets
import subprocess
import tempfile
import urllib.parse

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.clickhouse, pytest.mark.x86_64]


@dataclasses.dataclass
class ClickHouseServiceCluster(ServiceCluster):
    use_named_collections: bool
    expands_uuid_in_zookeeper_path: bool
    object_storage_prefix: str


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

ClickHouseCommand = Sequence[str | Path]


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
        command = [*command, "--", f"--http_port={http_port}"]
        async with run_process_and_wait_for_pattern(args=command, cwd=data_dir, pattern="Ready for connections.") as process:
            yield Service(process=process, port=http_port, data_dir=data_dir)


@dataclasses.dataclass(frozen=True)
class MinioBucket:
    host: str
    port: int
    name: str
    endpoint_url: str
    access_key_id: str
    secret_access_key: str


@dataclasses.dataclass(frozen=True)
class MinioService:
    process: subprocess.Popen[bytes]
    data_dir: Path
    host: str
    server_port: int
    console_port: int
    root_user: str
    root_password: str = dataclasses.field(repr=False)

    @property
    def netloc(self) -> str:
        return build_netloc(self.host, self.server_port)

    @property
    def endpoint_url(self) -> str:
        return urllib.parse.urlunsplit(("http", self.netloc, "", "", ""))

    @contextlib.contextmanager
    def get_client(self, access_key: str, secret_key: str) -> Iterator[botocore.client.BaseClient]:
        boto_config = botocore.client.Config(
            s3={"addressing_style": "path"},
            signature_version="s3v4",
            connect_timeout=30,
            read_timeout=30,
            retries={"max_attempts": 2},
        )
        botocore_session = botocore.session.get_session()
        s3_client = botocore_session.create_client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=boto_config,
            endpoint_url=self.endpoint_url,
            use_ssl=False,
            verify=False,
        )
        try:
            yield s3_client
        finally:
            s3_client.close()

    @contextlib.contextmanager
    def bucket(self, *, bucket_name: str) -> Iterator[MinioBucket]:
        with self.get_client(access_key=self.root_user, secret_key=self.root_password) as s3_client:
            s3_client.create_bucket(Bucket=bucket_name, ACL="private")  # type: ignore[attr-defined]
            yield MinioBucket(
                host=self.host,
                port=self.server_port,
                name=bucket_name,
                endpoint_url=f"{self.endpoint_url}/{bucket_name}/",
                access_key_id=self.root_user,
                secret_access_key=self.root_password,
            )
            response = s3_client.list_objects_v2(Bucket=bucket_name)  # type: ignore[attr-defined]
            if response["KeyCount"] > 0:
                s3_client.delete_objects(  # type: ignore[attr-defined]
                    Bucket=bucket_name,
                    Delete={"Objects": [{"Key": content["Key"]} for content in response["Contents"]]},
                )
            s3_client.delete_bucket(Bucket=bucket_name)  # type: ignore[attr-defined]


@pytest.fixture(scope="module", name="minio")
async def fixture_minio(ports: Ports) -> AsyncIterator[MinioService]:
    async with create_minio_service(ports) as service:
        yield service


@pytest.fixture(scope="module", name="minio_bucket")
async def fixture_minio_bucket(minio: MinioService) -> AsyncIterator[MinioBucket]:
    with minio.bucket(bucket_name="clickhouse-bucket") as bucket:
        yield bucket


@pytest.fixture(scope="function", name="function_minio_bucket")
async def fixture_function_minio_bucket(minio: MinioService) -> AsyncIterator[MinioBucket]:
    with minio.bucket(bucket_name="function-clickhouse-bucket") as bucket:
        yield bucket


@contextlib.asynccontextmanager
async def create_minio_service(ports: Ports) -> AsyncIterator[MinioService]:
    server_port = ports.allocate()
    console_port = ports.allocate()
    # It would be nice to verify IPv6 works well but rohmu does not use build_netloc or equivalent,
    # the endpoint url is wrong when the host contains ":".
    host = "localhost"
    server_netloc = build_netloc(host, server_port)
    console_netloc = build_netloc(host, console_port)
    root_user = "minio_root"
    root_password = secrets.token_hex()
    with tempfile.TemporaryDirectory(prefix=f"minio_{server_port}_") as data_dir_str:
        data_dir = Path(data_dir_str)
        env = {
            "HOME": data_dir_str,
            "MINIO_ROOT_USER": root_user,
            "MINIO_ROOT_PASSWORD": root_password,
        }
        command: Sequence[str | Path] = [
            "/usr/bin/minio",
            "server",
            data_dir,
            "--address",
            server_netloc,
            "--console-address",
            console_netloc,
        ]
        async with run_process_and_wait_for_pattern(
            args=command,
            cwd=data_dir,
            pattern="1 Online",
            env=env,
        ) as process:
            yield MinioService(
                process=process,
                data_dir=data_dir,
                host=host,
                server_port=server_port,
                console_port=console_port,
                root_user=root_user,
                root_password=root_password,
            )


@contextlib.asynccontextmanager
async def create_clickhouse_cluster(
    zookeeper: Service,
    minio_bucket: MinioBucket,
    ports: Ports,
    cluster_shards: Sequence[str],
    command: ClickHouseCommand,
    object_storage_prefix: str = "prefix/",
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
            cluster_shards,
            zookeeper,
            data_dirs,
            tcp_ports,
            http_ports,
            interserver_http_ports,
            use_named_collections,
            minio_bucket=minio_bucket,
            object_storage_prefix=object_storage_prefix,
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
                object_storage_prefix=object_storage_prefix,
            )


@dataclasses.dataclass(frozen=True)
class RestorableSource:
    astacus_storage_path: Path
    clickhouse_object_storage_prefix: str


@contextlib.asynccontextmanager
async def create_astacus_cluster(
    storage_path: Path,
    zookeeper: Service,
    clickhouse_cluster: ClickHouseServiceCluster,
    ports: Ports,
    minio_bucket: MinioBucket,
    restorable_source: RestorableSource | None = None,
) -> AsyncIterator[ServiceCluster]:
    configs = create_astacus_configs(
        zookeeper, clickhouse_cluster, ports, Path(storage_path), minio_bucket, restorable_source
    )
    async with contextlib.AsyncExitStack() as stack:
        astacus_services_coro: Sequence[Awaitable] = [
            stack.enter_async_context(_astacus(config=config)) for config in configs
        ]
        astacus_services = list(await asyncio.gather(*astacus_services_coro))
        yield ServiceCluster(services=astacus_services)


def create_clickhouse_configs(
    cluster_shards: Sequence[str],
    zookeeper: Service,
    data_dirs: Sequence[Path],
    tcp_ports: Sequence[int],
    http_ports: Sequence[int],
    interserver_http_ports: Sequence[int],
    use_named_collections: bool,
    minio_bucket: MinioBucket | None = None,
    object_storage_prefix: str = "/",
):
    # Helper for emitting XML configuration: avoid very long lines and deduplicate
    def setting(name: str, value: int | float | str):
        return f"<{name}>{value}</{name}>"

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
    storage_configuration = (
        f"""
        <storage_configuration>
            <disks>
                <default>
                    <type>local</type>
                </default>
                <remote>
                    <type>s3</type>
                    <endpoint>{minio_bucket.endpoint_url}{object_storage_prefix}</endpoint>
                    <access_key_id>{minio_bucket.access_key_id}</access_key_id>
                    <secret_access_key>{minio_bucket.secret_access_key}</secret_access_key>
                    <support_batch_delete>true</support_batch_delete>
                    <skip_access_check>true</skip_access_check>
                </remote>
            </disks>
            <policies>
                <default>
                    <volumes>
                        <default><disk>default</disk></default>
                    </volumes>
                </default>
                <remote>
                    <volumes>
                        <remote><disk>remote</disk></remote>
                    </volumes>
                </remote>
            </policies>
        </storage_configuration>
        """
        if minio_bucket is not None
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
                    <user_defined_zookeeper_path>/clickhouse/user_defined_functions/</user_defined_zookeeper_path>
                    <zookeeper>
                        <node>
                            <host>{zookeeper.host}</host>
                            <port>{zookeeper.port}</port>
                        </node>
                    </zookeeper>
                    <merge_tree>
                        {setting("number_of_free_entries_in_pool_to_execute_mutation", 2)}
                        {setting("number_of_free_entries_in_pool_to_execute_optimize_entire_partition", 2)}
                    </merge_tree>
                    {setting("background_pool_size", 4)}
                    {setting("background_move_pool_size", 2)}
                    {setting("background_fetches_pool_size", 2)}
                    {setting("background_common_pool_size", 4)}
                    {setting("background_buffer_flush_schedule_pool_size", 2)}
                    {setting("background_schedule_pool_size", 2)}
                    {setting("background_message_broker_schedule_pool_size", 2)}
                    {setting("background_distributed_schedule_pool_size", 2)}
                    {setting("tables_loader_foreground_pool_size", 2)}
                    {setting("tables_loader_background_pool_size", 2)}
                    {setting("restore_threads", 2)}
                    {setting("backup_threads", 2)}
                    {setting("backups_io_thread_pool_queue_size", 2)}
                    {setting("max_parts_cleaning_thread_pool_size", 2)}
                    {setting("max_active_parts_loading_thread_pool_size", 2)}
                    {setting("max_outdated_parts_loading_thread_pool_size", 2)}
                    {setting("mark_cache_size", 5368709120)}
                    {setting("max_server_memory_usage_to_ram_ratio", 0.5)}
                    {setting("enable_system_unfreeze", "true")}
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
                    {storage_configuration}
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
    cmd = format_astacus_command("server", "-c", str(config_path))
    async with background_process(*cmd, env={"PYTHONPATH": astacus_source_root}) as process:
        await wait_url_up(f"http://localhost:{config.uvicorn.port}")
        storage = config.object_storage.storages[config.object_storage.default_storage]
        assert isinstance(storage, rohmu.LocalObjectStorageConfig)
        data_dir = storage.directory
        yield Service(process=process, port=config.uvicorn.port, data_dir=data_dir)


class AstacusCommandError(Exception):
    pass


def run_astacus_command(astacus_cluster: ServiceCluster, *args: str) -> None:
    first_astacus = astacus_cluster.services[0]
    astacus_url = f"http://localhost:{first_astacus.port}"
    all_args = ["--url", astacus_url, "--wait-completion", "20000000"] + list(args)
    parser = argparse.ArgumentParser()
    create_client_parsers(parser, parser.add_subparsers())
    parsed_args = parser.parse_args(all_args)
    if not parsed_args.func(parsed_args):
        raise AstacusCommandError(f"Command {all_args} on {astacus_url} failed")


def create_astacus_configs(
    zookeeper: Service,
    clickhouse_cluster: ClickHouseServiceCluster,
    ports: Ports,
    storage_path: Path,
    minio_bucket: MinioBucket,
    restorable_source: RestorableSource | None = None,
) -> Sequence[GlobalConfig]:
    storage_tmp_path = storage_path / "tmp"
    storage_tmp_path.mkdir(exist_ok=True)
    astacus_backup_storage_path = storage_path / "astacus_backup"
    astacus_backup_storage_path.mkdir(exist_ok=True)
    node_ports = [ports.allocate() for _ in clickhouse_cluster.services]
    snapshotter_db_paths = [astacus_backup_storage_path / f"{s.host}_{s.port}" for s in clickhouse_cluster.services]
    for p in snapshotter_db_paths:
        p.mkdir(exist_ok=True)
    disk_storages = {
        "default": rohmu.S3ObjectStorageConfig(
            storage_type=rohmu.StorageDriver.s3,
            region="fake",
            host=minio_bucket.host,
            port=str(minio_bucket.port),
            aws_access_key_id=minio_bucket.access_key_id,
            aws_secret_access_key=minio_bucket.secret_access_key,
            bucket_name=minio_bucket.name,
            prefix=clickhouse_cluster.object_storage_prefix,
        )
    }
    backup_storages = {
        "default": rohmu.LocalObjectStorageConfig(
            storage_type=rohmu.StorageDriver.local,
            directory=astacus_backup_storage_path,
        )
    }
    if restorable_source:
        backup_storages["restorable"] = rohmu.LocalObjectStorageConfig(
            storage_type=rohmu.StorageDriver.local,
            directory=restorable_source.astacus_storage_path,
        )
        disk_storages["restorable"] = rohmu.S3ObjectStorageConfig(
            storage_type=rohmu.StorageDriver.s3,
            region="fake",
            host=minio_bucket.host,
            port=str(minio_bucket.port),
            aws_access_key_id=minio_bucket.access_key_id,
            aws_secret_access_key=minio_bucket.secret_access_key,
            bucket_name=minio_bucket.name,
            prefix=restorable_source.clickhouse_object_storage_prefix,
        )
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
                    replicated_databases_settings=(
                        ReplicatedDatabaseSettings(
                            collection_name="default_cluster",
                        )
                        if clickhouse_cluster.use_named_collections
                        else ReplicatedDatabaseSettings(
                            cluster_username=clickhouse_cluster.services[0].username,
                            cluster_password=clickhouse_cluster.services[0].password,
                        )
                    ),
                    replicated_user_defined_zookeeper_path="/clickhouse/user_defined_functions/",
                    disks=[
                        DiskConfiguration(
                            type=DiskType.local,
                            path=Path(""),
                            name="default",
                        ),
                        DiskConfiguration(
                            type=DiskType.object_storage,
                            path=Path("disks/remote"),
                            name="remote",
                            object_storage=DiskObjectStorageConfiguration(
                                default_storage="default",
                                storages=disk_storages,
                            ),
                        ),
                    ],
                    sync_databases_timeout=10.0,
                    sync_tables_timeout=30.0,
                ).jsondict(),
            ),
            node=NodeConfig(
                root=clickhouse_service.data_dir,
                db_path=db_path,
            ),
            object_storage=RohmuConfig(
                temporary_directory=str(storage_tmp_path),
                default_storage="default",
                storages=backup_storages,
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
        for node_port, db_path, clickhouse_service in zip(node_ports, snapshotter_db_paths, clickhouse_cluster.services)
    ]


def get_clickhouse_client(clickhouse: Service, timeout: float = 10.0) -> HttpClickHouseClient:
    return HttpClickHouseClient(
        host=clickhouse.host,
        port=clickhouse.port,
        username=clickhouse.username,
        password=clickhouse.password,
        timeout=timeout,
    )
