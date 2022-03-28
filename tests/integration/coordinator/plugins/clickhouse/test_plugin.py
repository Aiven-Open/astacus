"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from _pytest.fixtures import SubRequest
from astacus.common.ipc import RestoreRequest
from astacus.coordinator.plugins import ClickHousePlugin
from astacus.coordinator.plugins.base import OperationContext
from astacus.coordinator.plugins.clickhouse.client import ClickHouseClient, HttpClickHouseClient
from pathlib import Path
from tests.integration.conftest import create_zookeeper, Ports
from tests.integration.coordinator.plugins.clickhouse.conftest import (
    create_astacus_cluster, create_clickhouse_cluster, get_clickhouse_client, run_astacus_command
)
from typing import AsyncIterable, AsyncIterator, List, Sequence
from unittest import mock

import base64
import pytest
import tempfile

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.order("last"),
]


def _b64_str(b: bytes) -> str:
    return base64.b64encode(b).decode()


def get_restore_steps_names() -> List[str]:
    plugin = ClickHousePlugin()
    steps = plugin.get_restore_steps(
        context=OperationContext(storage_name="", json_storage=mock.Mock(), hexdigest_storage=mock.Mock()),
        req=RestoreRequest(storage="", name="")
    )
    return [step.__class__.__name__ for step in steps]


@pytest.fixture(scope="module", name="restorable_cluster")
async def fixture_restorable_cluster(ports: Ports) -> AsyncIterator[Path]:
    with tempfile.TemporaryDirectory(prefix="storage_") as storage_path_str:
        storage_path = Path(storage_path_str)
        async with create_zookeeper(ports) as zookeeper:
            async with create_clickhouse_cluster(zookeeper, ports) as clickhouse_cluster:
                async with create_astacus_cluster(storage_path, zookeeper, clickhouse_cluster, ports) as astacus_cluster:
                    clients = [get_clickhouse_client(service) for service in clickhouse_cluster.services]
                    await setup_cluster_content(clients)
                    await setup_cluster_users(clients)
                    run_astacus_command(astacus_cluster, "backup")
        # We have destroyed everything except the backup storage dir
        yield storage_path


@pytest.fixture(scope="module", name="restored_cluster", params=[*get_restore_steps_names(), None])
async def fixture_restored_cluster(
    restorable_cluster: Path,
    ports: Ports,
    request: SubRequest,
) -> AsyncIterable[Sequence[ClickHouseClient]]:
    stop_after_step: str = request.param
    async with create_zookeeper(ports) as zookeeper:
        async with create_clickhouse_cluster(zookeeper, ports) as clickhouse_cluster:
            clients = [get_clickhouse_client(service) for service in clickhouse_cluster.services]
            async with create_astacus_cluster(restorable_cluster, zookeeper, clickhouse_cluster, ports) as astacus_cluster:
                # To test if we can survive transient failures during an entire restore operation,
                # we first run a partial restore that stops after one of the restore steps,
                # then we run the full restore, on the same ClickHouse cluster,
                # then we check if the final restored data is as expected.
                # This sequence is repeated with a different partial restore each time, stopping at a different step.
                # We also need to test failure in the middle of a step, this is covered in the unit tests of each step.
                if stop_after_step is not None:
                    run_astacus_command(astacus_cluster, "restore", "--stop-after-step", stop_after_step)
                run_astacus_command(astacus_cluster, "restore")
                yield clients


async def setup_cluster_content(clients: List[HttpClickHouseClient]) -> None:
    for client in clients:
        await client.execute(b"DROP DATABASE default SYNC")
        await client.execute(
            b"CREATE DATABASE default ENGINE = Replicated('/clickhouse/databases/thebase', '{shard}', '{replica}') "
            b"SETTINGS cluster_username='default', cluster_password='secret'"
        )
    # table creation is auto-replicated so we only do it once :
    await clients[0].execute(
        b"CREATE TABLE default.replicated_merge_tree  (thekey UInt32, thedata String)  "
        b"ENGINE = ReplicatedMergeTree ORDER BY (thekey)"
    )
    await clients[0].execute(
        b"CREATE VIEW default.simple_view AS SELECT toInt32(thekey * 2) as thekey2 FROM default.replicated_merge_tree"
    )
    await clients[0].execute(
        b"CREATE MATERIALIZED VIEW default.materialized_view "
        b"ENGINE = MergeTree ORDER BY (thekey3) "
        b"AS SELECT toInt32(thekey * 3) as thekey3 FROM default.replicated_merge_tree"
    )
    await clients[0].execute(b"CREATE TABLE default.memory  (thekey UInt32, thedata String)  ENGINE = Memory")
    # This will be replicated between nodes
    await clients[0].execute(b"INSERT INTO default.replicated_merge_tree VALUES (123, 'foo')")
    await clients[1].execute(b"INSERT INTO default.replicated_merge_tree VALUES (456, 'bar')")
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
        assert await client.execute(b"SELECT user_name,role_name FROM system.grants ORDER BY user_name,role_name") == [[
            "alice", None
        ], ["default", None], [None, "bob"]]
        assert await client.execute(
            b"SELECT user_name,granted_role_name FROM system.role_grants ORDER BY user_name,granted_role_name"
        ) == [["alice", "bob"]]
        assert await client.execute(b"SELECT short_name FROM system.row_policies") == [["charlie"]]
        assert await client.execute(b"SELECT name FROM system.quotas WHERE storage = 'replicated'") == [["dan-dave-david"]]
        assert await client.execute(b"SELECT name FROM system.settings_profiles WHERE storage = 'replicated'") == [["érin"]]


@pytest.mark.asyncio
async def test_restores_replicated_merge_tree_tables_data(restored_cluster: List[ClickHouseClient]) -> None:
    for client in restored_cluster:
        assert await client.execute(b"SELECT thekey, thedata FROM default.replicated_merge_tree ORDER BY thekey") == [
            [123, "foo"],
            [456, "bar"],
        ]


@pytest.mark.asyncio
async def test_restores_simple_view(restored_cluster: List[ClickHouseClient]) -> None:
    first_client, second_client = restored_cluster
    assert await first_client.execute(b"SELECT thekey2 FROM default.simple_view ORDER BY thekey2") == [
        [123 * 2],
        [456 * 2],
    ]
    assert await second_client.execute(b"SELECT thekey2 FROM default.simple_view ORDER BY thekey2") == [
        [123 * 2],
        [456 * 2],
    ]


@pytest.mark.asyncio
async def test_restores_materialized_view_data(restored_cluster: List[ClickHouseClient]) -> None:
    first_client, second_client = restored_cluster
    assert await first_client.execute(b"SELECT thekey3 FROM default.materialized_view ORDER BY thekey3") == [
        [123 * 3],
        [456 * 3],
    ]
    assert await second_client.execute(b"SELECT thekey3 FROM default.materialized_view ORDER BY thekey3") == [
        [123 * 3],
        [456 * 3],
    ]


@pytest.mark.asyncio
async def test_restores_connectivity_between_distributed_servers(restored_cluster: List[ClickHouseClient]) -> None:
    # This only works if each node can connect to all nodes of the cluster named after the Distributed database
    for client in restored_cluster:
        assert await client.execute(b"SELECT * FROM clusterAllReplicas('default', system.one) ") == [[0], [0]]


@pytest.mark.asyncio
async def test_does_not_restore_log_tables_data(restored_cluster: List[ClickHouseClient]) -> None:
    # We restored the table structure but not the data
    for client in restored_cluster:
        assert await client.execute(b"SELECT thekey, thedata FROM default.memory") == []
