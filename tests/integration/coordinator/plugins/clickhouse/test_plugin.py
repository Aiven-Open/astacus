"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from astacus.coordinator.plugins.clickhouse.client import ClickHouseClient, HttpClickHouseClient
from tests.integration.coordinator.plugins.clickhouse.conftest import (
    create_astacus_cluster, create_clickhouse_cluster, create_zookeeper, get_clickhouse_client, Ports, run_astacus_command
)
from typing import AsyncIterable, List

import pytest
import tempfile

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.order("last"),
]


@pytest.fixture(scope="module", name="restored_cluster")
async def fixture_restored_cluster(ports: Ports) -> AsyncIterable[List[ClickHouseClient]]:
    with tempfile.TemporaryDirectory(prefix="storage_") as storage_path:
        async with create_zookeeper(ports) as zookeeper:
            async with create_clickhouse_cluster(zookeeper, ports) as clickhouse_cluster:
                async with create_astacus_cluster(storage_path, zookeeper, clickhouse_cluster, ports) as astacus_cluster:
                    clients = [get_clickhouse_client(service) for service in clickhouse_cluster.services]
                    await setup_cluster_content(clients)
                    await setup_cluster_users(clients)
                    run_astacus_command(astacus_cluster, "backup")
        # We've destroying everything except the backup storage dir, now we restore
        async with create_zookeeper(ports) as zookeeper:
            async with create_clickhouse_cluster(zookeeper, ports) as clickhouse_cluster:
                clients = [get_clickhouse_client(service) for service in clickhouse_cluster.services]
                for client in clients:
                    await client.execute("DROP DATABASE default")
                async with create_astacus_cluster(storage_path, zookeeper, clickhouse_cluster, ports) as astacus_cluster:
                    run_astacus_command(astacus_cluster, "restore")
                    yield clients


async def setup_cluster_content(clients: List[HttpClickHouseClient]):
    for client in clients:
        await client.execute("DROP DATABASE default")
        await client.execute(
            "CREATE DATABASE default ENGINE = Replicated('/clickhouse/databases/thebase', '{shard}', '{replica}')"
        )
    # table creation is auto-replicated so we only do it once :
    await clients[0].execute(
        "CREATE TABLE default.replicated_merge_tree  (thekey UInt32, thedata String)  "
        "ENGINE = ReplicatedMergeTree ORDER BY (thekey)"
    )
    await clients[0].execute(
        "CREATE TABLE default.merge_tree  (thekey UInt32, thedata String)  "
        "ENGINE = MergeTree ORDER BY (thekey)"
    )
    await clients[0].execute(
        "CREATE VIEW default.simple_view AS SELECT toInt32(thekey * 2) as thekey2 FROM default.merge_tree"
    )
    await clients[0].execute(
        "CREATE MATERIALIZED VIEW default.materialized_view "
        "ENGINE = MergeTree ORDER BY (thekey3) "
        "AS SELECT toInt32(thekey * 3) as thekey3 FROM default.merge_tree"
    )
    await clients[0].execute("CREATE TABLE default.log  (thekey UInt32, thedata String)  ENGINE = Log")
    # This will be replicated between nodes
    await clients[0].execute("INSERT INTO default.replicated_merge_tree VALUES (123, 'foo')")
    await clients[1].execute("INSERT INTO default.replicated_merge_tree VALUES (456, 'bar')")
    # The won't be replicated but can still be backed up
    await clients[0].execute("INSERT INTO default.merge_tree VALUES (123, 'foo')")
    await clients[1].execute("INSERT INTO default.merge_tree VALUES (456, 'bar')")
    # This won't be backed up
    await clients[0].execute("INSERT INTO default.log VALUES (123, 'foo')")


async def setup_cluster_users(clients: List[HttpClickHouseClient]):
    await clients[0].execute("CREATE USER alice")
    await clients[0].execute("GRANT SELECT on default.* TO alice")
    await clients[0].execute("CREATE ROLE bob")
    await clients[0].execute("GRANT INSERT on default.* TO bob")
    await clients[0].execute("GRANT bob TO alice")
    await clients[0].execute("CREATE ROW POLICY charlie ON the_table TO alice, bob")
    # These use special characters to check our escaping roundtrip (yes, dash is special)
    await clients[0].execute("CREATE QUOTA `dan-dave-david` TO alice, bob")
    await clients[0].execute("CREATE SETTINGS PROFILE `érin` TO alice, bob")


@pytest.mark.asyncio
async def test_restores_access_entities(restored_cluster: List[ClickHouseClient]):
    for client in restored_cluster:
        assert await client.execute("SELECT name FROM system.users WHERE storage = 'replicated' ORDER BY name") == [[
            "alice"
        ]]
        assert await client.execute("SELECT name FROM system.roles WHERE storage = 'replicated' ORDER BY name") == [["bob"]]
        assert await client.execute("SELECT user_name,role_name FROM system.grants ORDER BY user_name,role_name") == [[
            "alice", None
        ], ["default", None], [None, "bob"]]
        assert await client.execute(
            "SELECT user_name,granted_role_name FROM system.role_grants ORDER BY user_name,granted_role_name"
        ) == [["alice", "bob"]]
        assert await client.execute("SELECT short_name FROM system.row_policies") == [["charlie"]]
        assert await client.execute("SELECT name FROM system.quotas WHERE storage = 'replicated'") == [["dan-dave-david"]]
        assert await client.execute("SELECT name FROM system.settings_profiles WHERE storage = 'replicated'") == [["érin"]]


@pytest.mark.asyncio
async def test_restores_replicated_merge_tree_tables_data(restored_cluster: List[ClickHouseClient]):
    # In replicated table, all servers of the cluster have the same data
    for client in restored_cluster:
        assert await client.execute("SELECT thekey, thedata FROM default.replicated_merge_tree ORDER BY thekey") == [
            [123, "foo"],
            [456, "bar"],
        ]


@pytest.mark.asyncio
async def test_restores_unreplicated_merge_tree_tables_data(restored_cluster: List[ClickHouseClient]):
    # In an unreplicated table, each server of the cluster has different data
    first_client, second_client = restored_cluster
    assert await first_client.execute("SELECT thekey, thedata FROM default.merge_tree ") == [
        [123, "foo"],
    ]
    assert await second_client.execute("SELECT thekey, thedata FROM default.merge_tree ") == [
        [456, "bar"],
    ]


@pytest.mark.asyncio
async def test_restores_unreplicated_simple_view(restored_cluster: List[ClickHouseClient]):
    first_client, second_client = restored_cluster
    assert await first_client.execute("SELECT thekey2 FROM default.simple_view ") == [
        [123 * 2],
    ]
    assert await second_client.execute("SELECT thekey2 FROM default.simple_view ") == [
        [456 * 2],
    ]


@pytest.mark.asyncio
async def test_restores_unreplicated_materialized_view_data(restored_cluster: List[ClickHouseClient]):
    first_client, second_client = restored_cluster
    assert await first_client.execute("SELECT thekey3 FROM default.materialized_view ") == [
        [123 * 3],
    ]
    assert await second_client.execute("SELECT thekey3 FROM default.materialized_view ") == [
        [456 * 3],
    ]


@pytest.mark.asyncio
async def test_does_not_restore_log_tables_data(restored_cluster: List[ClickHouseClient]):
    # We restored the table structure but not the data
    for client in restored_cluster:
        assert await client.execute("SELECT thekey, thedata FROM default.log") == []
