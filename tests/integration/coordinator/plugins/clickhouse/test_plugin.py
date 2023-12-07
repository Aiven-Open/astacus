"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from _pytest.fixtures import SubRequest
from astacus.common.ipc import RestoreRequest
from astacus.coordinator.plugins.base import OperationContext
from astacus.coordinator.plugins.clickhouse.client import ClickHouseClient, HttpClickHouseClient
from astacus.coordinator.plugins.clickhouse.plugin import ClickHousePlugin
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
from typing import AsyncIterable, AsyncIterator, Final, List, Sequence
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
    "ENGINE = URL('http://127.0.0.1:12345/', 'CSV')"
)


def _b64_str(b: bytes) -> str:
    return base64.b64encode(b).decode()


def get_restore_steps_names() -> List[str]:
    plugin = ClickHousePlugin()
    steps = plugin.get_restore_steps(
        context=OperationContext(storage_name="", json_storage=mock.Mock(), hexdigest_storage=mock.Mock()),
        req=RestoreRequest(storage="", name=""),
    )
    return [step.__class__.__name__ for step in steps]


@pytest.fixture(scope="module", name="restorable_cluster")
async def fixture_restorable_cluster(
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
                    await setup_cluster_content(clients, clickhouse_cluster.use_named_collections)
                    await setup_cluster_users(clients)
                    run_astacus_command(astacus_cluster, "backup")
        # We have destroyed everything except the backup storage dir
        yield storage_path


@pytest.fixture(scope="module", name="restored_cluster", params=[*get_restore_steps_names(), None])
async def fixture_restored_cluster(
    restorable_cluster: Path,
    ports: Ports,
    request: SubRequest,
    clickhouse_restore_command: ClickHouseCommand,
    minio_bucket: MinioBucket,
) -> AsyncIterable[Sequence[ClickHouseClient]]:
    stop_after_step: str = request.param
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


async def setup_cluster_content(clients: List[HttpClickHouseClient], use_named_collections: bool) -> None:
    for client in clients:
        await client.execute(b"DROP DATABASE default SYNC")
        await client.execute(
            b"CREATE DATABASE default ENGINE = Replicated('/clickhouse/databases/thebase', '{my_shard}', '{my_replica}') "
            + (
                b"SETTINGS collection_name='default_cluster'"
                if use_named_collections
                else b"SETTINGS cluster_username='default', cluster_password='secret'"
            )
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
    await clients[0].execute(
        b"CREATE TABLE default.array_tuple_flatten (thekey UInt32, thedata Array(Tuple(a UInt32, b UInt32))) "
        b"ENGINE = ReplicatedMergeTree ORDER BY (thekey) "
        b"SETTINGS index_granularity=8192 "
        b"SETTINGS flatten_nested=1"
    )
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
        b"CREATE MATERIALIZED VIEW default.materialized_view "
        b"ENGINE = MergeTree ORDER BY (thekey3) "
        b"AS SELECT toInt32(thekey * 3) as thekey3 FROM default.replicated_merge_tree"
    )
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
    await clients[0].execute(b"INSERT INTO default.array_tuple_flatten VALUES (123, [4], [5])")
    # And some object storage data
    await clients[0].execute(b"INSERT INTO default.in_object_storage VALUES (123, 'foo')")
    await clients[1].execute(b"INSERT INTO default.in_object_storage VALUES (456, 'bar')")
    await clients[2].execute(b"INSERT INTO default.in_object_storage VALUES (789, 'baz')")
    # This won't be backed up
    await clients[0].execute(b"INSERT INTO default.memory VALUES (123, 'foo')")


async def setup_cluster_users(clients: List[HttpClickHouseClient]) -> None:
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


@pytest.mark.asyncio
async def test_restores_access_entities(restored_cluster: List[ClickHouseClient]) -> None:
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


@pytest.mark.asyncio
async def test_restores_replicated_merge_tree_tables_data(restored_cluster: List[ClickHouseClient]) -> None:
    s1_data = [[123, "foo"], [456, "bar"]]
    s2_data = [[789, "baz"]]
    cluster_data = [s1_data, s1_data, s2_data]
    for client, expected_data in zip(restored_cluster, cluster_data):
        response = await client.execute(b"SELECT thekey, thedata FROM default.replicated_merge_tree ORDER BY thekey")
        assert response == expected_data


@pytest.mark.asyncio
async def test_restores_table_with_experimental_types(restored_cluster: List[ClickHouseClient]) -> None:
    # The JSON type merges the keys in the response,
    # hence the extra zero-valued entries we don't see in the insert queries.
    s1_data = [[123, {"a": 1, "b": 0}], [456, {"a": 0, "b": 2}]]
    s2_data = [[789, {"c": 3}]]
    cluster_data = [s1_data, s1_data, s2_data]
    for client, expected_data in zip(restored_cluster, cluster_data):
        response = await client.execute(b"SELECT thekey, thedata FROM default.with_experimental_types ORDER BY thekey")
        assert response == expected_data


@pytest.mark.asyncio
async def test_restores_table_with_nested_fields(restored_cluster: List[ClickHouseClient]) -> None:
    client = restored_cluster[0]
    response = await client.execute(b"SELECT thekey, thedata FROM default.nested_not_flatten ORDER BY thekey")
    assert response == [[123, [{"a": 4, "b": 5}]]]
    response = await client.execute(b"SELECT thekey, thedata.a, thedata.b FROM default.nested_flatten ORDER BY thekey")
    assert response == [[123, [4], [5]]]
    response = await client.execute(b"SELECT thekey, thedata FROM default.array_tuple_not_flatten ORDER BY thekey")
    assert response == [[123, [{"a": 4, "b": 5}]]]
    response = await client.execute(b"SELECT thekey, thedata.a, thedata.b FROM default.array_tuple_flatten ORDER BY thekey")
    assert response == [[123, [4], [5]]]


@pytest.mark.asyncio
async def test_restores_function_table(restored_cluster: List[ClickHouseClient]) -> None:
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


@pytest.mark.asyncio
async def test_restores_url_engine_table(restored_cluster: List[ClickHouseClient]) -> None:
    for client in restored_cluster:
        response = await client.execute(b"SELECT create_table_query FROM system.tables WHERE name = 'url_engine_table'")
        assert response[0][0] == SAMPLE_URL_ENGINE_DDL


@pytest.mark.asyncio
async def test_restores_object_storage_data(restored_cluster: List[ClickHouseClient]) -> None:
    await check_object_storage_data(restored_cluster)


@pytest.mark.asyncio
async def test_restores_simple_view(restored_cluster: List[ClickHouseClient]) -> None:
    s1_data = [[123 * 2], [456 * 2]]
    s2_data = [[789 * 2]]
    cluster_data = [s1_data, s1_data, s2_data]
    for client, expected_data in zip(restored_cluster, cluster_data):
        response = await client.execute(b"SELECT thekey2 FROM default.simple_view ORDER BY thekey2")
        assert response == expected_data


@pytest.mark.asyncio
async def test_restores_materialized_view_data(restored_cluster: List[ClickHouseClient]) -> None:
    s1_data = [[123 * 3], [456 * 3]]
    s2_data = [[789 * 3]]
    cluster_data = [s1_data, s1_data, s2_data]
    for client, expected_data in zip(restored_cluster, cluster_data):
        response = await client.execute(b"SELECT thekey3 FROM default.materialized_view ORDER BY thekey3")
        assert response == expected_data


@pytest.mark.asyncio
async def test_restores_connectivity_between_distributed_servers(restored_cluster: List[ClickHouseClient]) -> None:
    # This only works if each node can connect to all nodes of the cluster named after the Distributed database
    for client in restored_cluster:
        assert await client.execute(b"SELECT * FROM clusterAllReplicas('default', system.one) ") == [[0], [0], [0]]


@pytest.mark.asyncio
async def test_does_not_restore_log_tables_data(restored_cluster: List[ClickHouseClient]) -> None:
    # We restored the table structure but not the data
    for client in restored_cluster:
        assert await client.execute(b"SELECT thekey, thedata FROM default.memory") == []


@pytest.mark.asyncio
async def test_cleanup_does_not_break_object_storage_disk_files(
    ports: Ports,
    clickhouse_command: ClickHouseCommand,
    minio_bucket: MinioBucket,
):
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
                    await setup_cluster_content(clients, clickhouse_cluster.use_named_collections)
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
