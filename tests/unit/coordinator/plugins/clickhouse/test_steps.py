"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from astacus.common.exceptions import TransientException
from astacus.common.ipc import BackupManifest, NodeResult, Plugin, SnapshotFile, SnapshotResult, SnapshotState
from astacus.common.op import Op
from astacus.common.progress import Progress
from astacus.coordinator.cluster import Cluster
from astacus.coordinator.config import CoordinatorNode
from astacus.coordinator.plugins.base import BackupManifestStep, SnapshotStep, StepFailedError, StepsContext
from astacus.coordinator.plugins.clickhouse.client import ClickHouseClient, StubClickHouseClient
from astacus.coordinator.plugins.clickhouse.config import ClickHouseConfiguration, ClickHouseNode, ReplicatedDatabaseSettings
from astacus.coordinator.plugins.clickhouse.manifest import AccessEntity, ClickHouseManifest, ReplicatedDatabase, Table
from astacus.coordinator.plugins.clickhouse.steps import (
    AttachMergeTreePartsStep, ClickHouseManifestStep, CreateClickHouseManifestStep, DistributeReplicatedPartsStep,
    FreezeTablesStep, RemoveFrozenTablesStep, RestoreAccessEntitiesStep, RestoreReplicatedDatabasesStep,
    RetrieveAccessEntitiesStep, RetrieveDatabasesAndTablesStep, SyncReplicasStep, TABLES_LIST_QUERY, UnfreezeTablesStep,
    ValidateConfigStep
)
from astacus.coordinator.plugins.clickhouse.zookeeper import FakeZooKeeperClient, ZooKeeperClient
from pathlib import Path
from typing import Optional, Sequence, Type, Union
from unittest import mock
from unittest.mock import _Call as MockCall  # pylint: disable=protected-access

import asyncio
import datetime
import httpx
import json
import pytest
import respx
import sys
import uuid

pytestmark = [pytest.mark.clickhouse]

SAMPLE_ENTITIES = [
    AccessEntity(type="P", uuid=uuid.UUID(int=1), name="a_policy", attach_query="ATTACH ROW POLICY ..."),
    AccessEntity(type="Q", uuid=uuid.UUID(int=2), name="a_quota", attach_query="ATTACH QUOTA ..."),
    AccessEntity(type="R", uuid=uuid.UUID(int=3), name="a_role", attach_query="ATTACH ROLE ..."),
    AccessEntity(type="S", uuid=uuid.UUID(int=4), name="a_settings_profile", attach_query="ATTACH SETTINGS PROFILE ..."),
    AccessEntity(type="U", uuid=uuid.UUID(int=5), name="josé", attach_query="ATTACH USER ..."),
]
SAMPLE_DATABASES = [
    ReplicatedDatabase(name="db-one"),
    ReplicatedDatabase(name="db-two"),
]
SAMPLE_TABLES = [
    Table(
        database="db-one",
        name="table-uno",
        uuid=uuid.UUID("00000000-0000-0000-0000-100000000001"),
        engine="ReplicatedMergeTree",
        create_query="CREATE TABLE db-one.table-uno ...",
        dependencies=[("db-one", "table-dos"), ("db-two", "table-eins")]
    ),
    Table(
        database="db-one",
        name="table-dos",
        uuid=uuid.UUID("00000000-0000-0000-0000-100000000002"),
        engine="MergeTree",
        create_query="CREATE TABLE db-one.table-dos ...",
    ),
    Table(
        database="db-two",
        name="table-eins",
        uuid=uuid.UUID("00000000-0000-0000-0000-200000000001"),
        engine="ReplicatedMergeTree",
        create_query="CREATE TABLE db-two.table-eins ...",
    )
]
SAMPLE_MANIFEST = ClickHouseManifest(
    access_entities=SAMPLE_ENTITIES,
    replicated_databases=SAMPLE_DATABASES,
    tables=SAMPLE_TABLES,
)


def mock_clickhouse_client() -> mock.Mock:
    mock_client = mock.Mock(spec_set=ClickHouseClient)
    if sys.version_info < (3, 8):
        awaitable = asyncio.Future()
        awaitable.set_result(mock.Mock(spec_set=list))
        mock_client.execute.return_value = awaitable
    return mock_client


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "clickhouse_count,coordinator_count,success", [
        (3, 3, True),
        (0, 0, True),
        (1, 2, False),
        (0, 1, False),
        (2, 1, False),
        (1, 0, False),
    ]
)
async def test_validate_step_require_equal_nodes_count(clickhouse_count: int, coordinator_count: int, success: bool) -> None:
    clickhouse_configuration = ClickHouseConfiguration(
        nodes=[ClickHouseNode(host="::1", port=9000) for _ in range(clickhouse_count)]
    )
    step = ValidateConfigStep(clickhouse=clickhouse_configuration)

    coordinator_nodes = [CoordinatorNode(url=f"node{i}") for i in range(coordinator_count)]
    cluster = Cluster(nodes=coordinator_nodes)
    if success:
        await step.run_step(cluster, StepsContext())
    else:
        with pytest.raises(StepFailedError):
            await step.run_step(cluster, StepsContext())


async def create_zookeper_access_entities(zookeeper_client: ZooKeeperClient) -> None:
    async with zookeeper_client.connect() as connection:
        await asyncio.gather(
            connection.create("/clickhouse/access/P/a_policy",
                              str(uuid.UUID(int=1)).encode()),
            connection.create(f"/clickhouse/access/uuid/{str(uuid.UUID(int=1))}", b"ATTACH ROW POLICY ..."),
            connection.create("/clickhouse/access/Q/a_quota",
                              str(uuid.UUID(int=2)).encode()),
            connection.create(f"/clickhouse/access/uuid/{str(uuid.UUID(int=2))}", b"ATTACH QUOTA ..."),
            connection.create("/clickhouse/access/R/a_role",
                              str(uuid.UUID(int=3)).encode()),
            connection.create(f"/clickhouse/access/uuid/{str(uuid.UUID(int=3))}", b"ATTACH ROLE ..."),
            connection.create("/clickhouse/access/S/a_settings_profile",
                              str(uuid.UUID(int=4)).encode()),
            connection.create(f"/clickhouse/access/uuid/{str(uuid.UUID(int=4))}", b"ATTACH SETTINGS PROFILE ..."),
            connection.create("/clickhouse/access/U/jos%C3%A9",
                              str(uuid.UUID(int=5)).encode()),
            connection.create(f"/clickhouse/access/uuid/{str(uuid.UUID(int=5))}", b"ATTACH USER ..."),
        )


@pytest.mark.asyncio
async def test_retrieve_access_entities() -> None:
    zookeeper_client = FakeZooKeeperClient()
    await create_zookeper_access_entities(zookeeper_client)
    step = RetrieveAccessEntitiesStep(zookeeper_client=zookeeper_client, access_entities_path="/clickhouse/access")
    access_entities = await step.run_step(Cluster(nodes=[]), StepsContext())
    assert access_entities == SAMPLE_ENTITIES


class TrappedZooKeeperClient(FakeZooKeeperClient):
    """
    A fake ZooKeeper client with a trap: it will inject a concurrent write after a few reads.
    """
    def __init__(self) -> None:
        super().__init__()
        self.calls_until_failure: Optional[int] = None

    async def inject_fault(self) -> None:
        if self.calls_until_failure == 0:
            self.calls_until_failure = None
            # This is our "failure": a concurrent modification
            async with self.connect() as new_connection:
                new_uuid = str(uuid.UUID(int=5))
                await new_connection.create("/clickhouse/access/R/a_new_role", new_uuid.encode())
                await new_connection.create("/clickhouse/access/uuid/{new_uuid}", b"ATTACH ROLE a_new_role ...")
        elif self.calls_until_failure is not None:
            self.calls_until_failure -= 1


@pytest.mark.asyncio
async def test_retrieve_access_entities_fails_from_concurrent_updates() -> None:
    zookeeper_client = TrappedZooKeeperClient()
    await create_zookeper_access_entities(zookeeper_client)
    # This fixed value is not ideal, we need to wait for a few reads before injecting a concurrent
    # update and see it cause problems, because we must do an update after something was
    # read by the step.
    # This is not a defect of the step, it's OK if something is updated in a part of the ZooKeeper
    # tree we haven't explored yet: the snapshot would have been the same if we had started
    # the snapshot just after this update.
    zookeeper_client.calls_until_failure = 8
    step = RetrieveAccessEntitiesStep(zookeeper_client=zookeeper_client, access_entities_path="/clickhouse/access")
    with pytest.raises(TransientException):
        await step.run_step(Cluster(nodes=[]), StepsContext())


@pytest.mark.asyncio
async def test_retrieve_tables() -> None:
    clients = [StubClickHouseClient(), StubClickHouseClient()]
    clients[0].set_response(
        TABLES_LIST_QUERY,
        [
            # This special row is what we get for a database without tables
            ["db-empty", "", "", "00000000-0000-0000-0000-000000000000", "", []],
            [
                "db-one",
                "table-uno",
                "ReplicatedMergeTree",
                "00000000-0000-0000-0000-100000000001",
                "CREATE TABLE db-one.table-uno ...",
                [("db-one", "table-dos"), ("db-two", "table-eins")],
            ],
            [
                "db-one",
                "table-dos",
                "MergeTree",
                "00000000-0000-0000-0000-100000000002",
                "CREATE TABLE db-one.table-dos ...",
                [],
            ],
            [
                "db-two",
                "table-eins",
                "ReplicatedMergeTree",
                "00000000-0000-0000-0000-200000000001",
                "CREATE TABLE db-two.table-eins ...",
                [],
            ],
        ]
    )
    step = RetrieveDatabasesAndTablesStep(clients=clients)
    context = StepsContext()
    databases, tables = await step.run_step(Cluster(nodes=[]), context)
    assert databases == [ReplicatedDatabase(name="db-empty")] + SAMPLE_DATABASES
    assert tables == SAMPLE_TABLES


@pytest.mark.asyncio
async def test_retrieve_tables_without_any_database_or_table() -> None:
    clients = [StubClickHouseClient(), StubClickHouseClient()]
    clients[0].set_response(TABLES_LIST_QUERY, [])
    step = RetrieveDatabasesAndTablesStep(clients=clients)
    context = StepsContext()
    assert await step.run_step(Cluster(nodes=[]), context) == ([], [])


@pytest.mark.asyncio
async def test_retrieve_tables_without_any_table() -> None:
    clients = [StubClickHouseClient(), StubClickHouseClient()]
    clients[0].set_response(TABLES_LIST_QUERY, [
        ["db-empty", "", "", "00000000-0000-0000-0000-000000000000", "", []],
    ])
    step = RetrieveDatabasesAndTablesStep(clients=clients)
    context = StepsContext()
    databases, tables = await step.run_step(Cluster(nodes=[]), context)
    assert databases == [ReplicatedDatabase(name="db-empty")]
    assert tables == []


@pytest.mark.asyncio
async def test_create_clickhouse_manifest() -> None:
    step = CreateClickHouseManifestStep()
    context = StepsContext()
    context.set_result(RetrieveAccessEntitiesStep, SAMPLE_ENTITIES)
    context.set_result(RetrieveDatabasesAndTablesStep, (SAMPLE_DATABASES, SAMPLE_TABLES))
    assert await step.run_step(Cluster(nodes=[]), context) == SAMPLE_MANIFEST


@pytest.mark.asyncio
async def test_remove_frozen_tables_step() -> None:
    step = RemoveFrozenTablesStep(freeze_name="some-thing+special")
    cluster = Cluster(nodes=[CoordinatorNode(url="http://node1/node"), CoordinatorNode(url="http://node2/node")])
    with respx.mock:
        respx.post(
            "http://node1/node/clear", content=Op.StartResult(op_id=123, status_url="http://node1/clear/123").jsondict()
        )
        respx.post(
            "http://node2/node/clear", content=Op.StartResult(op_id=456, status_url="http://node2/clear/456").jsondict()
        )
        respx.get("http://node1/clear/123", content=NodeResult(progress=Progress(final=True)).jsondict())
        respx.get("http://node2/clear/456", content=NodeResult(progress=Progress(final=True)).jsondict())
        try:
            await step.run_step(cluster, StepsContext())
        finally:
            for call in respx.calls:
                request: httpx.Request = call[0]
                if request.url in {"http://node1/node/clear", "http://node2/node/clear"}:
                    assert json.loads(request.read())["root_globs"] == ["shadow/some%2Dthing%2Bspecial/store/**/*"]


@pytest.mark.asyncio
async def test_freezes_all_mergetree_tables_listed_in_manifest() -> None:
    await _test_freeze_unfreezes_all_mergetree_tables_listed_in_manifest(step_class=FreezeTablesStep, operation="FREEZE")


@pytest.mark.asyncio
async def test_unfreezes_all_mergetree_tables_listed_in_manifest() -> None:
    await _test_freeze_unfreezes_all_mergetree_tables_listed_in_manifest(step_class=UnfreezeTablesStep, operation="UNFREEZE")


async def _test_freeze_unfreezes_all_mergetree_tables_listed_in_manifest(
    *, step_class: Union[Type[FreezeTablesStep], Type[UnfreezeTablesStep]], operation: str
) -> None:
    first_client, second_client = mock_clickhouse_client(), mock_clickhouse_client()
    step = step_class(clients=[first_client, second_client], freeze_name="Äs`t:/.././@c'_'s")

    cluster = Cluster(nodes=[CoordinatorNode(url="node1"), CoordinatorNode(url="node2")])
    context = StepsContext()
    context.set_result(RetrieveDatabasesAndTablesStep, (SAMPLE_DATABASES, SAMPLE_TABLES))
    await step.run_step(cluster, context)
    assert first_client.mock_calls == [
        mock.call.execute(f"ALTER TABLE `db-one`.`table-uno` {operation} WITH NAME 'Äs`t:/.././@c\\'_\\'s'"),
        mock.call.execute(f"ALTER TABLE `db-one`.`table-dos` {operation} WITH NAME 'Äs`t:/.././@c\\'_\\'s'"),
        mock.call.execute(f"ALTER TABLE `db-two`.`table-eins` {operation} WITH NAME 'Äs`t:/.././@c\\'_\\'s'"),
    ]
    # The operation is replicated, so we'll only do it on the first client
    assert second_client.mock_calls == []


@pytest.mark.asyncio
async def test_distribute_parts_of_replicated_tables() -> None:
    step = DistributeReplicatedPartsStep()
    context = StepsContext()
    context.set_result(
        SnapshotStep, [
            SnapshotResult(
                state=SnapshotState(
                    root_globs=[],
                    files=[
                        SnapshotFile(
                            relative_path=Path("store/000/00000000-0000-0000-0000-100000000001/detached/all_0_0_0/data.bin"),
                            file_size=1000,
                            mtime_ns=0,
                            hexdigest="0001"
                        ),
                        SnapshotFile(
                            relative_path=Path("store/000/00000000-0000-0000-0000-100000000001/detached/all_1_1_0/data.bin"),
                            file_size=1000,
                            mtime_ns=0,
                            hexdigest="0002"
                        ),
                        SnapshotFile(
                            relative_path=Path("store/000/00000000-0000-0000-0000-100000000002/detached/all_0_0_0/data.bin"),
                            file_size=1000,
                            mtime_ns=0,
                            hexdigest="0003"
                        ),
                    ]
                ),
            ),
            SnapshotResult(
                state=SnapshotState(
                    root_globs=[],
                    files=[
                        SnapshotFile(
                            relative_path=Path("store/000/00000000-0000-0000-0000-100000000001/detached/all_0_0_0/data.bin"),
                            file_size=1000,
                            mtime_ns=0,
                            hexdigest="0001"
                        ),
                        SnapshotFile(
                            relative_path=Path("store/000/00000000-0000-0000-0000-100000000001/detached/all_1_1_0/data.bin"),
                            file_size=1000,
                            mtime_ns=0,
                            hexdigest="0002"
                        ),
                        SnapshotFile(
                            relative_path=Path("store/000/00000000-0000-0000-0000-100000000002/detached/all_0_0_0/data.bin"),
                            file_size=1000,
                            mtime_ns=0,
                            hexdigest="0004"
                        ),
                    ]
                ),
            ),
        ]
    )
    context.set_result(RetrieveDatabasesAndTablesStep, (SAMPLE_DATABASES, SAMPLE_TABLES))
    await step.run_step(Cluster(nodes=[]), context)
    snapshot_results = context.get_result(SnapshotStep)
    # On the ReplicatedMergeTree table (uuid ending in 0001), each server has only half the parts now
    # On the MergeTree table (uuid ending in 0002), each server has kept its own part (same name but different digest)
    assert sorted(snapshot_results[0].state.files) == [
        SnapshotFile(
            relative_path=Path("store/000/00000000-0000-0000-0000-100000000001/detached/all_0_0_0/data.bin"),
            file_size=1000,
            mtime_ns=0,
            hexdigest="0001"
        ),
        SnapshotFile(
            relative_path=Path("store/000/00000000-0000-0000-0000-100000000002/detached/all_0_0_0/data.bin"),
            file_size=1000,
            mtime_ns=0,
            hexdigest="0003"
        ),
    ]
    assert sorted(snapshot_results[1].state.files) == [
        SnapshotFile(
            relative_path=Path("store/000/00000000-0000-0000-0000-100000000001/detached/all_1_1_0/data.bin"),
            file_size=1000,
            mtime_ns=0,
            hexdigest="0002"
        ),
        SnapshotFile(
            relative_path=Path("store/000/00000000-0000-0000-0000-100000000002/detached/all_0_0_0/data.bin"),
            file_size=1000,
            mtime_ns=0,
            hexdigest="0004"
        ),
    ]


@pytest.mark.asyncio
async def test_parse_clickhouse_manifest() -> None:
    step = ClickHouseManifestStep()
    context = StepsContext()
    context.set_result(
        BackupManifestStep,
        BackupManifest(
            start=datetime.datetime(2020, 1, 2, 3, 4, 5, 678, tzinfo=datetime.timezone.utc),
            end=datetime.datetime(2020, 1, 2, 5, 6, 7, 891, tzinfo=datetime.timezone.utc),
            attempt=1,
            snapshot_results=[],
            upload_results=[],
            plugin=Plugin.clickhouse,
            plugin_data={
                "access_entities": [{
                    "name": "default",
                    "uuid": "00000000-0000-0000-0000-000000000002",
                    "type": "U",
                    "attach_query": "ATTACH USER ..."
                }],
                "replicated_databases": [{
                    "name": "db-one"
                }],
                "tables": [{
                    "database": "db-one",
                    "name": "t1",
                    "engine": "MergeTree",
                    "uuid": "00000000-0000-0000-0000-000000000004",
                    "create_query": "CREATE ..."
                }]
            }
        )
    )
    clickhouse_manifest = await step.run_step(Cluster(nodes=[]), context)
    assert clickhouse_manifest == ClickHouseManifest(
        access_entities=[AccessEntity(type="U", uuid=uuid.UUID(int=2), name="default", attach_query="ATTACH USER ...")],
        replicated_databases=[ReplicatedDatabase(name="db-one")],
        tables=[Table(database="db-one", name="t1", engine="MergeTree", uuid=uuid.UUID(int=4), create_query="CREATE ...")],
    )


@pytest.mark.asyncio
async def test_creates_all_replicated_databases_and_tables_in_manifest() -> None:
    clients = [mock_clickhouse_client(), mock_clickhouse_client()]
    step = RestoreReplicatedDatabasesStep(
        clients=clients,
        replicated_databases_zookeeper_path="/clickhouse/databases",
        replicated_database_settings=ReplicatedDatabaseSettings(),
    )

    cluster = Cluster(nodes=[CoordinatorNode(url="node1"), CoordinatorNode(url="node2")])
    context = StepsContext()
    context.set_result(ClickHouseManifestStep, SAMPLE_MANIFEST)
    await step.run_step(cluster, context)
    first_client_queries = [
        "DROP DATABASE IF EXISTS `db-one` SYNC",
        "CREATE DATABASE `db-one` ENGINE = Replicated('/clickhouse/databases/db%2Done', '{shard}', '{replica}')",
        "DROP DATABASE IF EXISTS `db-two` SYNC",
        "CREATE DATABASE `db-two` ENGINE = Replicated('/clickhouse/databases/db%2Dtwo', '{shard}', '{replica}')",
        "CREATE TABLE db-one.table-uno ...", "CREATE TABLE db-one.table-dos ...", "CREATE TABLE db-two.table-eins ..."
    ]
    # CREATE TABLE is replicated, that why we only create the table on the first client
    second_client_queries = [
        "DROP DATABASE IF EXISTS `db-one` SYNC",
        "CREATE DATABASE `db-one` ENGINE = Replicated('/clickhouse/databases/db%2Done', '{shard}', '{replica}')",
        "DROP DATABASE IF EXISTS `db-two` SYNC",
        "CREATE DATABASE `db-two` ENGINE = Replicated('/clickhouse/databases/db%2Dtwo', '{shard}', '{replica}')",
    ]
    assert clients[0].mock_calls == list(map(mock.call.execute, first_client_queries))
    assert clients[1].mock_calls == list(map(mock.call.execute, second_client_queries))


@pytest.mark.asyncio
async def test_creates_all_replicated_databases_and_tables_in_manifest_with_custom_settings() -> None:
    client = mock_clickhouse_client()
    step = RestoreReplicatedDatabasesStep(
        clients=[client],
        replicated_databases_zookeeper_path="/clickhouse/databases",
        replicated_database_settings=ReplicatedDatabaseSettings(
            cluster_username="alice",
            cluster_password="alice_secret",
        ),
    )
    cluster = Cluster(nodes=[CoordinatorNode(url="node1")])
    context = StepsContext()
    context.set_result(ClickHouseManifestStep, SAMPLE_MANIFEST)
    await step.run_step(cluster, context)
    first_client_queries = [
        "DROP DATABASE IF EXISTS `db-one` SYNC",
        "CREATE DATABASE `db-one` ENGINE = Replicated('/clickhouse/databases/db%2Done', '{shard}', '{replica}') "
        "SETTINGS cluster_username='alice', cluster_password='alice_secret'",
        "DROP DATABASE IF EXISTS `db-two` SYNC",
        "CREATE DATABASE `db-two` ENGINE = Replicated('/clickhouse/databases/db%2Dtwo', '{shard}', '{replica}') "
        "SETTINGS cluster_username='alice', cluster_password='alice_secret'",
        "CREATE TABLE db-one.table-uno ...",
        "CREATE TABLE db-one.table-dos ...",
        "CREATE TABLE db-two.table-eins ...",
    ]
    assert client.mock_calls == list(map(mock.call.execute, first_client_queries))


@pytest.mark.asyncio
async def test_drops_each_database_on_all_servers_before_recreating_it() -> None:
    # We use the same client twice to record the global sequence of queries across all servers
    client = mock_clickhouse_client()
    step = RestoreReplicatedDatabasesStep(
        clients=[client, client],
        replicated_databases_zookeeper_path="/clickhouse/databases",
        replicated_database_settings=ReplicatedDatabaseSettings(),
    )
    cluster = Cluster(nodes=[CoordinatorNode(url="node1"), CoordinatorNode(url="node2")])
    context = StepsContext()
    context.set_result(ClickHouseManifestStep, SAMPLE_MANIFEST)
    await step.run_step(cluster, context)
    first_client_queries = [
        "DROP DATABASE IF EXISTS `db-one` SYNC", "DROP DATABASE IF EXISTS `db-one` SYNC",
        "CREATE DATABASE `db-one` ENGINE = Replicated('/clickhouse/databases/db%2Done', '{shard}', '{replica}')",
        "CREATE DATABASE `db-one` ENGINE = Replicated('/clickhouse/databases/db%2Done', '{shard}', '{replica}')",
        "DROP DATABASE IF EXISTS `db-two` SYNC", "DROP DATABASE IF EXISTS `db-two` SYNC",
        "CREATE DATABASE `db-two` ENGINE = Replicated('/clickhouse/databases/db%2Dtwo', '{shard}', '{replica}')",
        "CREATE DATABASE `db-two` ENGINE = Replicated('/clickhouse/databases/db%2Dtwo', '{shard}', '{replica}')",
        "CREATE TABLE db-one.table-uno ...", "CREATE TABLE db-one.table-dos ...", "CREATE TABLE db-two.table-eins ..."
    ]
    assert client.mock_calls == list(map(mock.call.execute, first_client_queries))


@pytest.mark.asyncio
async def test_creates_all_access_entities_in_manifest() -> None:
    client = FakeZooKeeperClient()
    step = RestoreAccessEntitiesStep(zookeeper_client=client, access_entities_path="/clickhouse/access")
    context = StepsContext()
    context.set_result(ClickHouseManifestStep, SAMPLE_MANIFEST)
    await step.run_step(Cluster(nodes=[]), context)
    await check_restored_entities(client)


@pytest.mark.asyncio
async def test_creating_all_access_entities_can_be_retried() -> None:
    client = FakeZooKeeperClient()
    step = RestoreAccessEntitiesStep(zookeeper_client=client, access_entities_path="/clickhouse/access")
    context = StepsContext()
    context.set_result(ClickHouseManifestStep, SAMPLE_MANIFEST)
    async with client.connect() as connection:
        # Simulate a first partial restoration
        await connection.create("/clickhouse/access/P/a_policy", str(uuid.UUID(int=1)).encode())
        await connection.create(f"/clickhouse/access/uuid/{str(uuid.UUID(int=1))}", b"ATTACH ROW POLICY ...")
        await connection.create("/clickhouse/access/Q/a_quota", str(uuid.UUID(int=2)).encode())
        await connection.create(f"/clickhouse/access/uuid/{str(uuid.UUID(int=3))}", b"ATTACH ROLE ...")
    await step.run_step(Cluster(nodes=[]), context)
    await check_restored_entities(client)


async def check_restored_entities(client: ZooKeeperClient) -> None:
    async with client.connect() as connection:
        assert await connection.get_children("/clickhouse/access") == ["P", "Q", "R", "S", "U", "uuid"]
        assert await connection.get_children("/clickhouse/access/uuid") == [str(uuid.UUID(int=i)) for i in range(1, 6)]
        assert await connection.get_children("/clickhouse/access/P") == ["a_policy"]
        assert await connection.get_children("/clickhouse/access/Q") == ["a_quota"]
        assert await connection.get_children("/clickhouse/access/R") == ["a_role"]
        assert await connection.get_children("/clickhouse/access/S") == ["a_settings_profile"]
        assert await connection.get_children("/clickhouse/access/U") == ["jos%C3%A9"]
        assert await connection.get("/clickhouse/access/P/a_policy") == str(uuid.UUID(int=1)).encode()
        assert await connection.get(f"/clickhouse/access/uuid/{str(uuid.UUID(int=1))}") == b"ATTACH ROW POLICY ..."
        assert await connection.get("/clickhouse/access/Q/a_quota") == str(uuid.UUID(int=2)).encode()
        assert await connection.get(f"/clickhouse/access/uuid/{str(uuid.UUID(int=2))}") == b"ATTACH QUOTA ..."
        assert await connection.get("/clickhouse/access/R/a_role") == str(uuid.UUID(int=3)).encode()
        assert await connection.get(f"/clickhouse/access/uuid/{str(uuid.UUID(int=3))}") == b"ATTACH ROLE ..."
        assert await connection.get("/clickhouse/access/S/a_settings_profile") == str(uuid.UUID(int=4)).encode()
        assert await connection.get(f"/clickhouse/access/uuid/{str(uuid.UUID(int=4))}") == b"ATTACH SETTINGS PROFILE ..."
        assert await connection.get("/clickhouse/access/U/jos%C3%A9") == str(uuid.UUID(int=5)).encode()
        assert await connection.get(f"/clickhouse/access/uuid/{str(uuid.UUID(int=5))}") == b"ATTACH USER ..."


@pytest.mark.asyncio
async def test_attaches_all_mergetree_parts_in_manifest() -> None:
    client_1 = mock_clickhouse_client()
    client_2 = mock_clickhouse_client()
    clients = [client_1, client_2]
    step = AttachMergeTreePartsStep(clients, attach_timeout=60, max_concurrent_attach=10)

    cluster = Cluster(nodes=[CoordinatorNode(url="node1"), CoordinatorNode(url="node2")])
    context = StepsContext()
    first_table_uuid = SAMPLE_TABLES[0].uuid
    second_table_uuid = SAMPLE_TABLES[1].uuid
    context.set_result(
        BackupManifestStep,
        BackupManifest(
            start=datetime.datetime(2020, 1, 2, 3, tzinfo=datetime.timezone.utc),
            attempt=1,
            snapshot_results=[
                SnapshotResult(
                    state=SnapshotState(
                        root_globs=["dont", "care"],
                        files=[
                            SnapshotFile(
                                relative_path=Path(f"store/000/{first_table_uuid}/detached/all_0_0_0/data.bin"),
                                file_size=0,
                                mtime_ns=0,
                            ),
                            SnapshotFile(
                                relative_path=Path(f"store/000/{second_table_uuid}/detached/all_1_1_0/data.bin"),
                                file_size=0,
                                mtime_ns=0,
                            ),
                        ]
                    )
                ),
                SnapshotResult(
                    state=SnapshotState(
                        root_globs=["dont", "care"],
                        files=[
                            SnapshotFile(
                                relative_path=Path(f"store/000/{first_table_uuid}/detached/all_0_0_0/data.bin"),
                                file_size=0,
                                mtime_ns=0,
                            ),
                            SnapshotFile(
                                relative_path=Path(f"store/000/{second_table_uuid}/detached/all_1_1_1/data.bin"),
                                file_size=0,
                                mtime_ns=0,
                            ),
                        ]
                    )
                )
            ],
            upload_results=[],
            plugin=Plugin.clickhouse,
        )
    )
    context.set_result(ClickHouseManifestStep, SAMPLE_MANIFEST)
    await step.run_step(cluster, context)
    # Note: parts list is different for each client
    # however, we can have identically named parts which are not the same file
    assert client_1.mock_calls == [
        mock.call.execute("SET receive_timeout=60", session_id=mock.ANY),
        mock.call.execute("ALTER TABLE `db-one`.`table-dos` ATTACH PART 'all_1_1_0'", session_id=mock.ANY, timeout=60),
        mock.call.execute("SET receive_timeout=60", session_id=mock.ANY),
        mock.call.execute("ALTER TABLE `db-one`.`table-uno` ATTACH PART 'all_0_0_0'", session_id=mock.ANY, timeout=60),
    ]
    check_each_pair_of_calls_has_the_same_session_id(client_1.mock_calls)
    assert client_2.mock_calls == [
        mock.call.execute("SET receive_timeout=60", session_id=mock.ANY),
        mock.call.execute("ALTER TABLE `db-one`.`table-dos` ATTACH PART 'all_1_1_1'", session_id=mock.ANY, timeout=60),
        mock.call.execute("SET receive_timeout=60", session_id=mock.ANY),
        mock.call.execute("ALTER TABLE `db-one`.`table-uno` ATTACH PART 'all_0_0_0'", session_id=mock.ANY, timeout=60),
    ]
    check_each_pair_of_calls_has_the_same_session_id(client_2.mock_calls)


@pytest.mark.asyncio
async def test_sync_replicas_for_replicated_mergetree_tables() -> None:
    clients = [mock_clickhouse_client(), mock_clickhouse_client()]
    step = SyncReplicasStep(clients, sync_timeout=180, max_concurrent_sync=10)
    cluster = Cluster(nodes=[CoordinatorNode(url="node1"), CoordinatorNode(url="node2")])
    context = StepsContext()
    context.set_result(ClickHouseManifestStep, SAMPLE_MANIFEST)
    await step.run_step(cluster, context)
    for client_index, client in enumerate(clients):
        assert client.mock_calls == [
            mock.call.execute("SET receive_timeout=180", session_id=mock.ANY),
            mock.call.execute("SYSTEM SYNC REPLICA `db-one`.`table-uno`", session_id=mock.ANY, timeout=180),
            mock.call.execute("SET receive_timeout=180", session_id=mock.ANY),
            mock.call.execute("SYSTEM SYNC REPLICA `db-two`.`table-eins`", session_id=mock.ANY, timeout=180)
        ], f"Wrong list of queries for client {client_index} of {len(clients)}"
        check_each_pair_of_calls_has_the_same_session_id(client.mock_calls)


def check_each_pair_of_calls_has_the_same_session_id(mock_calls: Sequence[MockCall]) -> None:
    session_ids = [mock_call[2]["session_id"] for mock_call in mock_calls]
    assert len(session_ids) % 2 == 0
    assert None not in session_ids
    for start_index in range(0, len(session_ids), 2):
        assert session_ids[start_index] == session_ids[start_index + 1]
