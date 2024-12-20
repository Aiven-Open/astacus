"""Copyright (c) 2021 Aiven Ltd
See LICENSE for details.
"""

from _pytest.fixtures import SubRequest
from astacus.common.ipc import RestoreRequest
from astacus.coordinator.plugins.base import OperationContext
from astacus.coordinator.plugins.clickhouse.client import (
    ClickHouseClient,
    ClickHouseClientQueryError,
    escape_sql_identifier,
    escape_sql_string,
    HttpClickHouseClient,
)
from astacus.coordinator.plugins.clickhouse.engines import TableEngine
from astacus.coordinator.plugins.clickhouse.plugin import ClickHousePlugin
from astacus.coordinator.plugins.clickhouse.steps import wait_for_condition_on_every_node
from collections.abc import AsyncIterable, AsyncIterator, Sequence
from contextlib import asynccontextmanager
from pathlib import Path
from tests.integration.conftest import create_zookeeper, Ports
from tests.integration.coordinator.plugins.clickhouse.conftest import (
    ClickHouseCommand,
    create_astacus_cluster,
    create_clickhouse_cluster,
    get_clickhouse_client,
    MinioBucket,
    RestorableSource,
    run_astacus_command,
)
from tests.utils import get_clickhouse_version
from typing import Final
from unittest import mock

import base64
import pytest
import tempfile

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.order("last"),
]


SAMPLE_URL_ENGINE_DDL: Final[str] = (
    "CREATE TABLE default.url_engine_table (`thekey` UInt32, `thedata` String) "
    "ENGINE = URL('https://127.0.0.1:12345/', 'CSV')"
)


def _b64_str(b: bytes) -> str:
    return base64.b64encode(b).decode()


def get_restore_steps_names() -> Sequence[str]:
    plugin = ClickHousePlugin()
    steps = plugin.get_restore_steps(
        context=OperationContext(storage_name="", json_storage=mock.Mock(), hexdigest_storage=mock.Mock()),
        req=RestoreRequest(storage="", name=""),
    )
    return [step.__class__.__name__ for step in steps]


async def is_engine_available(client: ClickHouseClient, engine: TableEngine) -> bool:
    query = f"SELECT TRUE FROM system.table_engines WHERE name = {escape_sql_string(engine.value.encode())}".encode()
    return len(await client.execute(query)) == 1


@asynccontextmanager
async def restorable_cluster_manager(
    ports: Ports,
    clickhouse_command: ClickHouseCommand,
    minio_bucket: MinioBucket,
) -> AsyncIterator[Path]:
    with tempfile.TemporaryDirectory(prefix="storage_") as storage_path_str:
        storage_path = Path(storage_path_str)
        async with create_zookeeper(ports) as zookeeper:
            async with create_clickhouse_cluster(
                zookeeper=zookeeper,
                minio_bucket=minio_bucket,
                ports=ports,
                cluster_shards=("s1", "s1", "s2"),
                command=clickhouse_command,
                object_storage_prefix="restorable/",
            ) as clickhouse_cluster:
                async with create_astacus_cluster(
                    storage_path, zookeeper, clickhouse_cluster, ports, minio_bucket
                ) as astacus_cluster:
                    clients = [get_clickhouse_client(service) for service in clickhouse_cluster.services]
                    await setup_cluster_content(
                        clients,
                        get_clickhouse_version(clickhouse_command),
                    )
                    await setup_cluster_users(clients)
                    run_astacus_command(astacus_cluster, "backup")
        # We have destroyed everything except the backup storage dir
        yield storage_path


@pytest.fixture(scope="module", name="restorable_cluster")
async def fixture_restorable_cluster(
    ports: Ports,
    clickhouse_command: ClickHouseCommand,
    minio_bucket: MinioBucket,
) -> AsyncIterator[Path]:
    async with restorable_cluster_manager(ports, clickhouse_command, minio_bucket) as restorable_cluster:
        yield restorable_cluster


@asynccontextmanager
async def restored_cluster_manager(
    restorable_cluster: Path,
    ports: Ports,
    stop_after_step: str | None,
    clickhouse_restore_command: ClickHouseCommand,
    minio_bucket: MinioBucket,
) -> AsyncIterator[Sequence[ClickHouseClient]]:
    restorable_source = RestorableSource(
        astacus_storage_path=restorable_cluster / "astacus_backup", clickhouse_object_storage_prefix="restorable/"
    )
    async with create_zookeeper(ports) as zookeeper:
        async with create_clickhouse_cluster(
            zookeeper=zookeeper,
            minio_bucket=minio_bucket,
            ports=ports,
            cluster_shards=("s1", "s1", "s2"),
            command=clickhouse_restore_command,
            object_storage_prefix="restored/",
        ) as clickhouse_cluster:
            clients = [get_clickhouse_client(service) for service in clickhouse_cluster.services]
            with tempfile.TemporaryDirectory(prefix="storage_") as storage_path_str:
                storage_path = Path(storage_path_str)
                async with create_astacus_cluster(
                    storage_path, zookeeper, clickhouse_cluster, ports, minio_bucket, restorable_source
                ) as astacus_cluster:
                    # To test if we can survive transient failures during an entire restore operation,
                    # we first run a partial restore that stops after one of the restore steps,
                    # then we run the full restore, on the same ClickHouse cluster,
                    # then we check if the final restored data is as expected.
                    # This sequence is repeated with a different partial restore each time, stopping at a different step.
                    # We also need to test failure in the middle of a step, this is covered in the unit tests of each step.
                    if stop_after_step is not None:
                        run_astacus_command(
                            astacus_cluster,
                            "restore",
                            "--stop-after-step",
                            stop_after_step,
                            "--storage",
                            "restorable",
                        )
                    run_astacus_command(astacus_cluster, "restore", "--storage", "restorable")
                    yield clients


@pytest.fixture(scope="module", name="restored_cluster", params=[*get_restore_steps_names(), None])
async def fixture_restored_cluster(
    ports: Ports,
    request: SubRequest,
    restorable_cluster: Path,
    clickhouse_restore_command: ClickHouseCommand,
    minio_bucket: MinioBucket,
) -> AsyncIterable[Sequence[ClickHouseClient]]:
    stop_after_step: str | None = request.param
    async with restored_cluster_manager(
        restorable_cluster, ports, stop_after_step, clickhouse_restore_command, minio_bucket
    ) as clients:
        yield clients


@pytest.fixture(name="function_restored_cluster")
async def fixture_function_restored_cluster(
    ports: Ports,
    clickhouse_command: ClickHouseCommand,
    clickhouse_restore_command: ClickHouseCommand,
    function_minio_bucket: MinioBucket,
) -> AsyncIterable[Sequence[ClickHouseClient]]:
    async with restorable_cluster_manager(ports, clickhouse_command, function_minio_bucket) as restorable_cluster:
        async with restored_cluster_manager(
            restorable_cluster, ports, None, clickhouse_restore_command, function_minio_bucket
        ) as clients:
            yield clients


async def sync_replicated_table(clients: Sequence[ClickHouseClient], table_name: str) -> None:
    for client in clients:
        await client.execute(f"SYSTEM SYNC REPLICA default.{escape_sql_identifier(table_name.encode())} STRICT".encode())


async def setup_cluster_content(clients: Sequence[HttpClickHouseClient], clickhouse_version: tuple[int, ...]) -> None:
    for client in clients:
        await client.execute(b"DROP DATABASE default SYNC")
        await client.execute(
            b"CREATE DATABASE default ENGINE = Replicated('/clickhouse/databases/thebase', '{my_shard}', '{my_replica}') "
            b"SETTINGS collection_name='default_cluster'"
        )
    # table creation is auto-replicated so we only do it once :
    await clients[0].execute(
        b"CREATE TABLE default.replicated_merge_tree  (thekey UInt32, thedata String)  "
        b"ENGINE = ReplicatedMergeTree ORDER BY (thekey)"
    )
    # test that we can re-create a table requiring custom global settings.
    await clients[0].execute(
        b"CREATE TABLE default.with_experimental_types (thekey UInt32, thedata JSON) "
        b"ENGINE = ReplicatedMergeTree ORDER BY (thekey) "
        b"SETTINGS index_granularity=8192 "
        b"SETTINGS allow_experimental_object_type=1"
    )
    # test that we can re-create a table requiring custom merge tree settings.
    # Unlike other cases, we don't have to SET a global settings because it's merge tree setting.
    await clients[0].execute(
        b"CREATE TABLE default.with_nullable_key (thekey Nullable(UInt32), thedata String) "
        b"ENGINE = ReplicatedMergeTree ORDER BY (thekey) "
        b"SETTINGS allow_nullable_key=true"
    )
    # test that we correctly restore nested fields
    await clients[0].execute(
        b"CREATE TABLE default.nested_not_flatten (thekey UInt32, thedata Nested(a UInt32, b UInt32)) "
        b"ENGINE = ReplicatedMergeTree ORDER BY (thekey) "
        b"SETTINGS index_granularity=8192 "
        b"SETTINGS flatten_nested=0"
    )
    await clients[0].execute(
        b"CREATE TABLE default.nested_flatten (thekey UInt32, thedata Nested(a UInt32, b UInt32)) "
        b"ENGINE = ReplicatedMergeTree ORDER BY (thekey) "
        b"SETTINGS index_granularity=8192 "
        b"SETTINGS flatten_nested=1"
    )
    await clients[0].execute(
        b"CREATE TABLE default.array_tuple_not_flatten (thekey UInt32, thedata Array(Tuple(a UInt32, b UInt32))) "
        b"ENGINE = ReplicatedMergeTree ORDER BY (thekey) "
        b"SETTINGS index_granularity=8192 "
        b"SETTINGS flatten_nested=0"
    )
    if clickhouse_version < (24, 3):
        await clients[0].execute(
            b"CREATE TABLE default.array_tuple_flatten (thekey UInt32, thedata Array(Tuple(a UInt32, b UInt32))) "
            b"ENGINE = ReplicatedMergeTree ORDER BY (thekey) "
            b"SETTINGS index_granularity=8192 "
            b"SETTINGS flatten_nested=1"
        )
    # integrations - note most of these never actually attempt to connect to the remote server.
    if await is_engine_available(clients[0], TableEngine.PostgreSQL):
        await clients[0].execute(
            b"CREATE TABLE default.postgresql (a Int) "
            b"ENGINE = PostgreSQL('https://host:1234', 'database', 'table', 'user', 'password')"
        )
    if await is_engine_available(clients[0], TableEngine.MySQL):
        await clients[0].execute(
            b"CREATE TABLE default.mysql (a Int) "
            b"ENGINE = MySQL('https://host:1234', 'database', 'table', 'user', 'password')"
        )
    if await is_engine_available(clients[0], TableEngine.S3):
        await clients[0].execute(b"CREATE TABLE default.s3 (a Int) ENGINE = S3('http://bucket.s3.amazonaws.com/key.json')")
    # add a function table
    await clients[0].execute(b"CREATE TABLE default.from_function_table AS numbers(3)")
    # add a table with data in object storage
    await clients[0].execute(
        b"CREATE TABLE default.in_object_storage (thekey UInt32, thedata String) "
        b"ENGINE = ReplicatedMergeTree ORDER BY (thekey) "
        b"SETTINGS storage_policy='remote'"
    )
    await clients[0].execute(SAMPLE_URL_ENGINE_DDL.encode())
    await clients[0].execute(
        b"CREATE VIEW default.simple_view AS SELECT toInt32(thekey * 2) as thekey2 FROM default.replicated_merge_tree"
    )
    await clients[0].execute(
        b"CREATE TABLE default.source_table_for_view_deleted (thekey UInt32, thedata String) "
        b"ENGINE = ReplicatedMergeTree ORDER BY (thekey)"
    )
    await clients[0].execute(
        b"CREATE VIEW default.view_deleted_source AS SELECT toInt32(thekey * 2) as thekey2 "
        b"FROM default.source_table_for_view_deleted"
    )
    await clients[0].execute(
        b"CREATE MATERIALIZED VIEW default.materialized_view "
        b"ENGINE = MergeTree ORDER BY (thekey3) "
        b"AS SELECT toInt32(thekey * 3) as thekey3 FROM default.replicated_merge_tree"
    )
    await clients[0].execute(
        b"CREATE TABLE default.data_table_for_mv (thekey3 UInt32) ENGINE = ReplicatedMergeTree ORDER BY (thekey3)"
    )
    await clients[0].execute(
        b"CREATE MATERIALIZED VIEW default.materialized_view_deleted_source to default.data_table_for_mv "
        b"AS SELECT toInt32(thekey * 3) as thekey3 FROM default.source_table_for_view_deleted"
    )
    await clients[0].execute(
        b"CREATE DICTIONARY default.dictionary (thekey UInt32, thedata String) "
        b"PRIMARY KEY thekey "
        b"SOURCE(CLICKHOUSE(DB 'default' TABLE 'replicated_merge_tree')) "
        b"LAYOUT(FLAT()) "
        b"LIFETIME(0)"
    )
    await clients[0].execute(b"INSERT INTO default.source_table_for_view_deleted VALUES (7, '7')")
    await clients[2].execute(b"INSERT INTO default.source_table_for_view_deleted VALUES (9, '9')")
    await clients[0].execute(b"DROP TABLE default.source_table_for_view_deleted")

    await clients[0].execute(b"CREATE TABLE default.memory  (thekey UInt32, thedata String)  ENGINE = Memory")
    # This will be replicated between nodes of the same shard (servers 0 and 1, but not 2)
    await clients[0].execute(b"INSERT INTO default.replicated_merge_tree VALUES (123, 'foo')")
    await clients[1].execute(b"INSERT INTO default.replicated_merge_tree VALUES (456, 'bar')")
    await clients[2].execute(b"INSERT INTO default.replicated_merge_tree VALUES (789, 'baz')")
    # Same with the table with the experimental data type
    await clients[0].execute(b"INSERT INTO default.with_experimental_types VALUES (123, '{\"a\":1}')")
    await clients[1].execute(b"INSERT INTO default.with_experimental_types VALUES (456, '{\"b\":2}')")
    await clients[2].execute(b"INSERT INTO default.with_experimental_types VALUES (789, '{\"c\":3}')")
    # Sample data for nested fields
    await clients[0].execute(b"INSERT INTO default.nested_not_flatten VALUES (123, [(4, 5)])")
    await clients[0].execute(b"INSERT INTO default.nested_flatten VALUES (123, [4], [5])")
    await clients[0].execute(b"INSERT INTO default.array_tuple_not_flatten VALUES (123, [(4, 5)])")
    if clickhouse_version < (24, 3):
        await clients[0].execute(b"INSERT INTO default.array_tuple_flatten VALUES (123, [4], [5])")
    # And some object storage data
    await clients[0].execute(b"INSERT INTO default.in_object_storage VALUES (123, 'foo')")
    await clients[1].execute(b"INSERT INTO default.in_object_storage VALUES (456, 'bar')")
    await clients[2].execute(b"INSERT INTO default.in_object_storage VALUES (789, 'baz')")
    # This won't be backed up
    await clients[0].execute(b"INSERT INTO default.memory VALUES (123, 'foo')")
    await clients[0].execute(b"CREATE FUNCTION `linear_equation_\x80` AS (x, k, b) -> k*x + b")
    if await is_engine_available(clients[0], TableEngine.KeeperMap):
        await add_keeper_map_tables(clients)


async def add_keeper_map_tables(clients: Sequence[ClickHouseClient]) -> None:
    # add and drop a table with KeeperMap engine.  This leaves some metadata in ZooKeeper that we want to ignore.
    await clients[0].execute(
        b"CREATE TABLE default.keeper_map_dropped(key UInt32, value String) "
        b"ENGINE = KeeperMap('keeper_map_dropped') PRIMARY KEY key"
    )
    await clients[0].execute(b"DROP TABLE default.keeper_map_dropped")
    # empty table to backup
    await clients[0].execute(
        b"CREATE TABLE default.keeper_map_empty (key UInt32, value String) "
        b"ENGINE = KeeperMap('keeper_map_empty') PRIMARY KEY key"
    )
    # add a table to backup
    await clients[0].execute(
        b"CREATE TABLE default.keeper_map (key UInt32, value String) ENGINE = KeeperMap('keeper_map') PRIMARY KEY key"
    )
    await clients[0].execute(b"INSERT INTO default.keeper_map VALUES (1, 'one'), (2, 'two')")

    # wait for every zookeeper node to receive the update
    async def keeper_map_has_replicated(client: ClickHouseClient) -> bool:
        return len(await client.execute(b"SELECT * FROM default.keeper_map")) == 2

    await wait_for_condition_on_every_node(
        clients,
        keeper_map_has_replicated,
        "waiting for keeper_map to be replicated",
        timeout_seconds=5,
    )


async def setup_cluster_users(clients: Sequence[HttpClickHouseClient]) -> None:
    await clients[0].execute(b"CREATE USER alice")
    await clients[0].execute(b"GRANT SELECT on default.* TO alice")
    await clients[0].execute(b"CREATE ROLE bob")
    await clients[0].execute(b"GRANT INSERT on default.* TO bob")
    await clients[0].execute(b"GRANT bob TO alice")
    await clients[0].execute(b"CREATE ROW POLICY charlie ON the_table TO alice, bob")
    # These use special characters to check our escaping roundtrip (yes, dash is special)
    await clients[0].execute(b"CREATE QUOTA `dan-dave-david` TO alice, bob")
    await clients[0].execute("CREATE SETTINGS PROFILE `érin` TO alice, bob".encode())
    await clients[0].execute(b"CREATE USER `z_\\x80_enjoyer`")


async def test_restores_access_entities(restored_cluster: Sequence[ClickHouseClient]) -> None:
    for client in restored_cluster:
        assert await client.execute(
            b"SELECT base64Encode(name) FROM system.users WHERE storage = 'replicated' ORDER BY name"
        ) == [[_b64_str(b"alice")], [_b64_str(b"z_\x80_enjoyer")]]
        assert await client.execute(b"SELECT name FROM system.roles WHERE storage = 'replicated' ORDER BY name") == [["bob"]]
        assert await client.execute(
            b"SELECT DISTINCT user_name,role_name FROM system.grants ORDER BY user_name,role_name"
        ) == [
            ["alice", None],
            ["default", None],
            [None, "bob"],
        ]
        assert await client.execute(
            b"SELECT user_name,granted_role_name FROM system.role_grants ORDER BY user_name,granted_role_name"
        ) == [["alice", "bob"]]
        assert await client.execute(b"SELECT short_name FROM system.row_policies") == [["charlie"]]
        assert await client.execute(b"SELECT name FROM system.quotas WHERE storage = 'replicated'") == [["dan-dave-david"]]
        assert await client.execute(b"SELECT name FROM system.settings_profiles WHERE storage = 'replicated'") == [["érin"]]


async def test_restores_replicated_merge_tree_tables_data(restored_cluster: Sequence[ClickHouseClient]) -> None:
    s1_data = [[123, "foo"], [456, "bar"]]
    s2_data = [[789, "baz"]]
    cluster_data = [s1_data, s1_data, s2_data]
    for client, expected_data in zip(restored_cluster, cluster_data):
        response = await client.execute(b"SELECT thekey, thedata FROM default.replicated_merge_tree ORDER BY thekey")
        assert response == expected_data


async def test_restores_table_with_experimental_types(restored_cluster: Sequence[ClickHouseClient]) -> None:
    # The JSON type merges the keys in the response,
    # hence the extra zero-valued entries we don't see in the insert queries.
    s1_data = [[123, {"a": 1, "b": 0}], [456, {"a": 0, "b": 2}]]
    s2_data = [[789, {"c": 3}]]
    cluster_data = [s1_data, s1_data, s2_data]
    for client, expected_data in zip(restored_cluster, cluster_data):
        response = await client.execute(b"SELECT thekey, thedata FROM default.with_experimental_types ORDER BY thekey")
        assert response == expected_data


async def test_restores_table_with_nullable_key(restored_cluster: Sequence[ClickHouseClient]) -> None:
    for client in restored_cluster:
        response = await client.execute(b"SELECT thekey, thedata FROM default.with_nullable_key ORDER BY thekey")
        assert response == []


async def test_restores_table_with_nested_fields(
    restored_cluster: Sequence[ClickHouseClient], clickhouse_command: ClickHouseCommand
) -> None:
    client = restored_cluster[0]
    response = await client.execute(b"SELECT thekey, thedata FROM default.nested_not_flatten ORDER BY thekey")
    assert response == [[123, [{"a": 4, "b": 5}]]]
    response = await client.execute(b"SELECT thekey, thedata.a, thedata.b FROM default.nested_flatten ORDER BY thekey")
    assert response == [[123, [4], [5]]]
    response = await client.execute(b"SELECT thekey, thedata FROM default.array_tuple_not_flatten ORDER BY thekey")
    assert response == [[123, [{"a": 4, "b": 5}]]]
    if get_clickhouse_version(clickhouse_command) < (24, 3):
        response = await client.execute(
            b"SELECT thekey, thedata.a, thedata.b FROM default.array_tuple_flatten ORDER BY thekey"
        )
        assert response == [[123, [4], [5]]]


async def test_restores_function_table(restored_cluster: Sequence[ClickHouseClient]) -> None:
    client = restored_cluster[0]
    response = await client.execute(b"SELECT * FROM default.from_function_table")
    assert response == [["0"], ["1"], ["2"]]


async def check_object_storage_data(cluster: Sequence[ClickHouseClient]) -> None:
    s1_data = [[123, "foo"], [456, "bar"]]
    s2_data = [[789, "baz"]]
    cluster_data = [s1_data, s1_data, s2_data]
    for client, expected_data in zip(cluster, cluster_data):
        response = await client.execute(b"SELECT thekey, thedata FROM default.in_object_storage ORDER BY thekey")
        assert response == expected_data


async def test_restores_url_engine_table(restored_cluster: Sequence[ClickHouseClient]) -> None:
    for client in restored_cluster:
        response = await client.execute(b"SELECT create_table_query FROM system.tables WHERE name = 'url_engine_table'")
        assert response[0][0] == SAMPLE_URL_ENGINE_DDL


async def test_restores_object_storage_data(restored_cluster: Sequence[ClickHouseClient]) -> None:
    await check_object_storage_data(restored_cluster)


async def test_restores_simple_view(restored_cluster: Sequence[ClickHouseClient]) -> None:
    s1_data = [[123 * 2], [456 * 2]]
    s2_data = [[789 * 2]]
    cluster_data = [s1_data, s1_data, s2_data]
    for client, expected_data in zip(restored_cluster, cluster_data):
        response = await client.execute(b"SELECT thekey2 FROM default.simple_view ORDER BY thekey2")
        assert response == expected_data


async def test_restores_materialized_view_data(restored_cluster: Sequence[ClickHouseClient]) -> None:
    s1_data = [[123 * 3], [456 * 3]]
    s2_data = [[789 * 3]]
    cluster_data = [s1_data, s1_data, s2_data]
    for client, expected_data in zip(restored_cluster, cluster_data):
        response = await client.execute(b"SELECT thekey3 FROM default.materialized_view ORDER BY thekey3")
        assert response == expected_data


async def test_restores_materialized_view_deleted_source_table(restored_cluster: Sequence[ClickHouseClient]) -> None:
    s1_data = [[7 * 3]]
    s2_data = [[9 * 3]]
    cluster_data = [s1_data, s1_data, s2_data]
    for client, expected_data in zip(restored_cluster, cluster_data):
        response = await client.execute(b"SELECT thekey3 FROM default.materialized_view_deleted_source ORDER BY thekey3")
        assert response == expected_data


async def test_restores_materialized_view_with_undeleted_source_table(
    function_restored_cluster: Sequence[ClickHouseClient],
) -> None:
    await function_restored_cluster[0].execute(
        b"CREATE TABLE default.source_table_for_view_deleted (thekey UInt32, thedata String) "
        b"ENGINE = ReplicatedMergeTree ORDER BY (thekey)"
    )
    await function_restored_cluster[0].execute(b"INSERT INTO default.source_table_for_view_deleted VALUES (8, '8')")
    await sync_replicated_table(function_restored_cluster, "source_table_for_view_deleted")
    await sync_replicated_table(function_restored_cluster, "data_table_for_mv")
    s1_data = [[7 * 3], [8 * 3]]
    s2_data = [[9 * 3]]
    cluster_data = [s1_data, s1_data, s2_data]
    # Recreated deleted table works again
    for client, expected_data in zip(function_restored_cluster, cluster_data):
        response = await client.execute(b"SELECT thekey3 FROM default.materialized_view_deleted_source ORDER BY thekey3")
        assert response == expected_data


async def test_restores_view_with_deleted_source_table(restored_cluster: Sequence[ClickHouseClient]) -> None:
    unknown_table_exception_code = 60
    for client in restored_cluster:
        with pytest.raises(ClickHouseClientQueryError) as raised:
            await client.execute(b"SELECT thekey2 FROM default.view_deleted_source ORDER BY thekey2")
        assert raised.value.status_code == 404
        assert raised.value.exception_code == unknown_table_exception_code


async def test_restores_view_undeleted_source_table(function_restored_cluster: Sequence[ClickHouseClient]) -> None:
    await function_restored_cluster[0].execute(
        b"CREATE TABLE default.source_table_for_view_deleted (thekey UInt32, thedata String) "
        b"ENGINE = ReplicatedMergeTree ORDER BY (thekey)"
    )
    await function_restored_cluster[0].execute(b"INSERT INTO default.source_table_for_view_deleted VALUES (11, '11')")
    await function_restored_cluster[2].execute(b"INSERT INTO default.source_table_for_view_deleted VALUES (17, '17')")
    await sync_replicated_table(function_restored_cluster, "source_table_for_view_deleted")
    s1_data = [[11 * 2]]
    s2_data = [[17 * 2]]
    cluster_data = [s1_data, s1_data, s2_data]
    # Recreated deleted table works again
    for client, expected_data in zip(function_restored_cluster, cluster_data):
        response = await client.execute(b"SELECT thekey2 FROM default.view_deleted_source ORDER BY thekey2")
        assert response == expected_data


async def test_restores_connectivity_between_distributed_servers(restored_cluster: Sequence[ClickHouseClient]) -> None:
    # This only works if each node can connect to all nodes of the cluster named after the Distributed database
    for client in restored_cluster:
        assert await client.execute(b"SELECT * FROM clusterAllReplicas('default', system.one) ") == [[0], [0], [0]]


async def test_does_not_restore_log_tables_data(restored_cluster: Sequence[ClickHouseClient]) -> None:
    # We restored the table structure but not the data
    for client in restored_cluster:
        assert await client.execute(b"SELECT thekey, thedata FROM default.memory") == []


async def test_cleanup_does_not_break_object_storage_disk_files(
    ports: Ports,
    clickhouse_command: ClickHouseCommand,
    minio_bucket: MinioBucket,
) -> None:
    with tempfile.TemporaryDirectory(prefix="storage_") as storage_path_str:
        storage_path = Path(storage_path_str)
        async with create_zookeeper(ports) as zookeeper:
            async with create_clickhouse_cluster(
                zookeeper, minio_bucket, ports, ("s1", "s1", "s2"), clickhouse_command
            ) as clickhouse_cluster:
                async with create_astacus_cluster(
                    storage_path, zookeeper, clickhouse_cluster, ports, minio_bucket
                ) as astacus_cluster:
                    clients = [get_clickhouse_client(service) for service in clickhouse_cluster.services]
                    await setup_cluster_content(clients, get_clickhouse_version(clickhouse_command))
                    await setup_cluster_users(clients)
                    run_astacus_command(astacus_cluster, "backup")
                    run_astacus_command(astacus_cluster, "backup")
                    run_astacus_command(astacus_cluster, "cleanup", "--maximum-backups", "1")
                    # First check after the cleanup
                    await check_object_storage_data(clients)
        # We have destroyed everything except the backup storage dir and whatever is in minio
        async with create_zookeeper(ports) as zookeeper:
            async with create_clickhouse_cluster(
                zookeeper, minio_bucket, ports, ("s1", "s1", "s2"), clickhouse_command
            ) as clickhouse_cluster:
                clients = [get_clickhouse_client(service) for service in clickhouse_cluster.services]
                async with create_astacus_cluster(
                    storage_path, zookeeper, clickhouse_cluster, ports, minio_bucket
                ) as astacus_cluster:
                    run_astacus_command(astacus_cluster, "restore")
                # Then check again after a full restore
                await check_object_storage_data(clients)


@pytest.mark.parametrize(
    ("table_name", "table_engine"),
    [
        ("default.postgresql", TableEngine.PostgreSQL),
        ("default.mysql", TableEngine.MySQL),
        ("default.s3", TableEngine.S3),
    ],
)
async def test_restores_integration_tables(
    restored_cluster: Sequence[ClickHouseClient], table_name: str, table_engine: TableEngine
) -> None:
    for client in restored_cluster:
        if not await is_engine_available(client, table_engine):
            pytest.skip(f"Table engine {table_engine.value} not available")
        assert bool(await client.execute(f"EXISTS TABLE {table_name}".encode()))


async def test_restores_dictionaries(restored_cluster: Sequence[ClickHouseClient]) -> None:
    s1_data = [["123", "foo"], ["456", "bar"]]
    s2_data = [["789", "baz"]]
    cluster_data = [s1_data, s1_data, s2_data]
    for client, expected in zip(restored_cluster, cluster_data):
        await client.execute(b"SYSTEM RELOAD DICTIONARY default.dictionary")
        assert await client.execute(b"SELECT * FROM default.dictionary") == expected


async def test_restores_user_defined_functions(restored_cluster: Sequence[ClickHouseClient]) -> None:
    for client in restored_cluster:
        functions = await client.execute(b"SELECT base64Encode(name) FROM system.functions WHERE origin = 'SQLUserDefined'")
        assert functions == [[_b64_str(b"linear_equation_\x80")]]
        result = await client.execute(b"SELECT `linear_equation_\x80`(1, 2, 3)")
        assert result == [[5]]


async def test_restores_keeper_map_tables(restored_cluster: Sequence[ClickHouseClient]) -> None:
    if not await is_engine_available(restored_cluster[0], TableEngine.KeeperMap):
        pytest.skip("KeeperMap engine not available")
    for client in restored_cluster:
        result = await client.execute(b"SELECT key, value FROM default.keeper_map ORDER BY key")
        assert result == [[1, "one"], [2, "two"]]
        result = await client.execute(b"SELECT key, value FROM default.keeper_map_empty ORDER BY key")
        assert result == []
        with pytest.raises(ClickHouseClientQueryError) as e:
            await client.execute(b"SELECT * FROM default.keeper_map_dropped")
        assert "UNKNOWN_TABLE" in e.value.args[0]
