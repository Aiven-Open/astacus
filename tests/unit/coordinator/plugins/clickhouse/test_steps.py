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
from astacus.coordinator.plugins.clickhouse.macros import Macros, MACROS_LIST_QUERY
from astacus.coordinator.plugins.clickhouse.manifest import AccessEntity, ClickHouseManifest, ReplicatedDatabase, Table
from astacus.coordinator.plugins.clickhouse.replication import DatabaseReplica
from astacus.coordinator.plugins.clickhouse.steps import (
    AttachMergeTreePartsStep,
    ClickHouseManifestStep,
    DistributeReplicatedPartsStep,
    FreezeTablesStep,
    ListDatabaseReplicasStep,
    PrepareClickHouseManifestStep,
    RemoveFrozenTablesStep,
    RestoreAccessEntitiesStep,
    RestoreReplicatedDatabasesStep,
    RetrieveAccessEntitiesStep,
    RetrieveDatabasesAndTablesStep,
    RetrieveMacrosStep,
    SyncDatabaseReplicasStep,
    SyncTableReplicasStep,
    TABLES_LIST_QUERY,
    UnfreezeTablesStep,
    ValidateConfigStep,
)
from astacus.coordinator.plugins.zookeeper import FakeZooKeeperClient, ZooKeeperClient
from base64 import b64encode
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
    AccessEntity(type="P", uuid=uuid.UUID(int=1), name=b"a_policy", attach_query=b"ATTACH ROW POLICY ..."),
    AccessEntity(type="Q", uuid=uuid.UUID(int=2), name=b"a_quota", attach_query=b"ATTACH QUOTA ..."),
    AccessEntity(type="R", uuid=uuid.UUID(int=3), name=b"a_role", attach_query=b"ATTACH ROLE ..."),
    AccessEntity(type="S", uuid=uuid.UUID(int=4), name=b"a_settings_profile", attach_query=b"ATTACH SETTINGS PROFILE ..."),
    AccessEntity(type="U", uuid=uuid.UUID(int=5), name="josé".encode(), attach_query=b"ATTACH USER ..."),
    AccessEntity(type="U", uuid=uuid.UUID(int=6), name=b"z\x80enjoyer", attach_query=b"ATTACH USER \x80 ..."),
]
SAMPLE_DATABASES = [
    ReplicatedDatabase(name=b"db-one", uuid=uuid.UUID(int=17), shard=b"{my_shard}", replica=b"{my_replica}"),
    ReplicatedDatabase(name=b"db-two", uuid=uuid.UUID(int=18), shard=b"{my_shard}", replica=b"{my_replica}"),
]
SAMPLE_TABLES = [
    Table(
        database=b"db-one",
        name=b"table-uno",
        uuid=uuid.UUID("00000000-0000-0000-0000-100000000001"),
        engine="ReplicatedMergeTree",
        create_query=b"CREATE TABLE db-one.table-uno ...",
        dependencies=[(b"db-one", b"table-dos"), (b"db-two", b"table-eins")],
    ),
    Table(
        database=b"db-one",
        name=b"table-dos",
        uuid=uuid.UUID("00000000-0000-0000-0000-100000000002"),
        engine="MergeTree",
        create_query=b"CREATE TABLE db-one.table-dos ...",
    ),
    Table(
        database=b"db-two",
        name=b"table-eins",
        uuid=uuid.UUID("00000000-0000-0000-0000-200000000001"),
        engine="ReplicatedMergeTree",
        create_query=b"CREATE TABLE db-two.table-eins ...",
    ),
]

SAMPLE_MANIFEST = ClickHouseManifest(
    access_entities=SAMPLE_ENTITIES,
    replicated_databases=SAMPLE_DATABASES,
    tables=SAMPLE_TABLES,
)

SAMPLE_MANIFEST_ENCODED = SAMPLE_MANIFEST.to_plugin_data()


def mock_clickhouse_client() -> mock.Mock:
    mock_client = mock.Mock(spec_set=ClickHouseClient)
    if sys.version_info < (3, 8):
        awaitable = asyncio.Future()
        awaitable.set_result(mock.Mock(spec_set=list))
        mock_client.execute.return_value = awaitable
    return mock_client


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "clickhouse_count,coordinator_count,success",
    [
        (3, 3, True),
        (0, 0, True),
        (1, 2, False),
        (0, 1, False),
        (2, 1, False),
        (1, 0, False),
    ],
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
            connection.create("/clickhouse/access/P/a_policy", str(uuid.UUID(int=1)).encode()),
            connection.create(f"/clickhouse/access/uuid/{str(uuid.UUID(int=1))}", b"ATTACH ROW POLICY ..."),
            connection.create("/clickhouse/access/Q/a_quota", str(uuid.UUID(int=2)).encode()),
            connection.create(f"/clickhouse/access/uuid/{str(uuid.UUID(int=2))}", b"ATTACH QUOTA ..."),
            connection.create("/clickhouse/access/R/a_role", str(uuid.UUID(int=3)).encode()),
            connection.create(f"/clickhouse/access/uuid/{str(uuid.UUID(int=3))}", b"ATTACH ROLE ..."),
            connection.create("/clickhouse/access/S/a_settings_profile", str(uuid.UUID(int=4)).encode()),
            connection.create(f"/clickhouse/access/uuid/{str(uuid.UUID(int=4))}", b"ATTACH SETTINGS PROFILE ..."),
            connection.create("/clickhouse/access/U/jos%C3%A9", str(uuid.UUID(int=5)).encode()),
            connection.create(f"/clickhouse/access/uuid/{str(uuid.UUID(int=5))}", b"ATTACH USER ..."),
            connection.create("/clickhouse/access/U/z%80enjoyer", str(uuid.UUID(int=6)).encode()),
            connection.create(f"/clickhouse/access/uuid/{str(uuid.UUID(int=6))}", b"ATTACH USER \x80 ..."),
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
            [
                b64_str(b"db-empty"),
                "00000000-0000-0000-0000-000000000010",
                b64_str(b""),
                "",
                "00000000-0000-0000-0000-000000000000",
                b64_str(b""),
                [],
            ],
            [
                b64_str(b"db-one"),
                "00000000-0000-0000-0000-000000000011",
                b64_str(b"table-uno"),
                "ReplicatedMergeTree",
                "00000000-0000-0000-0000-100000000001",
                b64_str(b"CREATE TABLE db-one.table-uno ..."),
                [
                    (b64_str(b"db-one"), b64_str(b"table-dos")),
                    (b64_str(b"db-two"), b64_str(b"table-eins")),
                ],
            ],
            [
                b64_str(b"db-one"),
                "00000000-0000-0000-0000-000000000011",
                b64_str(b"table-dos"),
                "MergeTree",
                "00000000-0000-0000-0000-100000000002",
                b64_str(b"CREATE TABLE db-one.table-dos ..."),
                [],
            ],
            [
                b64_str(b"db-two"),
                "00000000-0000-0000-0000-000000000012",
                b64_str(b"table-eins"),
                "ReplicatedMergeTree",
                "00000000-0000-0000-0000-200000000001",
                b64_str(b"CREATE TABLE db-two.table-eins ..."),
                [],
            ],
        ],
    )
    clients[0].set_response(
        b"SHOW CREATE DATABASE `db-empty`",
        [
            [
                "CREATE DATABASE `db-empty` ENGINE = "
                "Replicated('/clickhouse/databases/db%2Dempty', '{my_other_shard}', '{my_other_replica}')"
            ]
        ],
    )
    clients[0].set_response(
        b"SHOW CREATE DATABASE `db-one`",
        [
            [
                "CREATE DATABASE `db-one` ENGINE = "
                "Replicated('/clickhouse/databases/db%2Done', '{my_shard}', '{my_replica}')"
            ]
        ],
    )
    clients[0].set_response(
        b"SHOW CREATE DATABASE `db-two`",
        [
            [
                "CREATE DATABASE `db-two` ENGINE = "
                "Replicated('/clickhouse/databases/db%2Dtwo', '{my_shard}', '{my_replica}')"
            ]
        ],
    )
    step = RetrieveDatabasesAndTablesStep(clients=clients)
    context = StepsContext()
    databases, tables = await step.run_step(Cluster(nodes=[]), context)
    assert (
        databases
        == [
            ReplicatedDatabase(
                name=b"db-empty",
                uuid=uuid.UUID(int=16),
                shard=b"{my_other_shard}",
                replica=b"{my_other_replica}",
            )
        ]
        + SAMPLE_DATABASES
    )
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
    clients[0].set_response(
        TABLES_LIST_QUERY,
        [
            [
                b64_str(b"db-empty"),
                "00000000-0000-0000-0000-000000000010",
                b64_str(b""),
                "",
                "00000000-0000-0000-0000-000000000000",
                b64_str(b""),
                [],
            ],
        ],
    )
    clients[0].set_response(
        b"SHOW CREATE DATABASE `db-empty`",
        [
            [
                "CREATE DATABASE `db-empty` ENGINE = "
                "Replicated('/clickhouse/databases/db%2Dempty', '{my_other_shard}', '{my_other_replica}')"
            ]
        ],
    )
    step = RetrieveDatabasesAndTablesStep(clients=clients)
    context = StepsContext()
    databases, tables = await step.run_step(Cluster(nodes=[]), context)
    assert databases == [
        ReplicatedDatabase(
            name=b"db-empty",
            uuid=uuid.UUID(int=16),
            shard=b"{my_other_shard}",
            replica=b"{my_other_replica}",
        )
    ]
    assert tables == []


@pytest.mark.asyncio
async def test_retrieve_macros() -> None:
    clients = [StubClickHouseClient(), StubClickHouseClient()]
    for (
        client,
        replica_name,
    ) in zip(clients, [b"node_1", b"node_2"]):
        client.set_response(
            MACROS_LIST_QUERY,
            [
                [b64_str(b"shard"), b64_str(b"a_shard")],
                [b64_str(b"replica"), b64_str(replica_name)],
            ],
        )
    step = RetrieveMacrosStep(clients=clients)
    cluster = Cluster(nodes=[CoordinatorNode(url="node1"), CoordinatorNode(url="node2")])
    context = StepsContext()
    servers_macros = await step.run_step(cluster, context)
    assert servers_macros == [
        Macros.from_mapping({b"shard": b"a_shard", b"replica": b"node_1"}),
        Macros.from_mapping({b"shard": b"a_shard", b"replica": b"node_2"}),
    ]


@pytest.mark.asyncio
async def test_create_clickhouse_manifest() -> None:
    step = PrepareClickHouseManifestStep()
    context = StepsContext()
    context.set_result(RetrieveAccessEntitiesStep, SAMPLE_ENTITIES)
    context.set_result(RetrieveDatabasesAndTablesStep, (SAMPLE_DATABASES, SAMPLE_TABLES))
    assert await step.run_step(Cluster(nodes=[]), context) == SAMPLE_MANIFEST_ENCODED


@pytest.mark.asyncio
async def test_remove_frozen_tables_step() -> None:
    step = RemoveFrozenTablesStep(freeze_name="some-thing+special")
    cluster = Cluster(nodes=[CoordinatorNode(url="http://node1/node"), CoordinatorNode(url="http://node2/node")])
    with respx.mock:
        respx.post("http://node1/node/clear").respond(
            json=Op.StartResult(op_id=123, status_url="http://node1/clear/123").jsondict()
        )
        respx.post("http://node2/node/clear").respond(
            json=Op.StartResult(op_id=456, status_url="http://node2/clear/456").jsondict()
        )
        respx.get("http://node1/clear/123").respond(json=NodeResult(progress=Progress(final=True)).jsondict())
        respx.get("http://node2/clear/456").respond(json=NodeResult(progress=Progress(final=True)).jsondict())
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
    if operation == "FREEZE":
        assert first_client.mock_calls == [
            mock.call.execute(b"ALTER TABLE `db-one`.`table-uno` FREEZE WITH NAME '\\xc3\\x84s`t:/.././@c\\'_\\'s'"),
            mock.call.execute(b"ALTER TABLE `db-one`.`table-dos` FREEZE WITH NAME '\\xc3\\x84s`t:/.././@c\\'_\\'s'"),
            mock.call.execute(b"ALTER TABLE `db-two`.`table-eins` FREEZE WITH NAME '\\xc3\\x84s`t:/.././@c\\'_\\'s'"),
        ]
    else:
        assert first_client.mock_calls == [
            mock.call.execute(b"ALTER TABLE `db-one`.`table-uno` UNFREEZE WITH NAME '\\xc3\\x84s`t:/.././@c\\'_\\'s'"),
            mock.call.execute(b"ALTER TABLE `db-one`.`table-dos` UNFREEZE WITH NAME '\\xc3\\x84s`t:/.././@c\\'_\\'s'"),
            mock.call.execute(b"ALTER TABLE `db-two`.`table-eins` UNFREEZE WITH NAME '\\xc3\\x84s`t:/.././@c\\'_\\'s'"),
        ]
    # The operation is replicated, so we'll only do it on the first client
    assert second_client.mock_calls == []


@pytest.mark.asyncio
async def test_distribute_parts_of_replicated_tables() -> None:
    step = DistributeReplicatedPartsStep()
    context = StepsContext()
    context.set_result(
        SnapshotStep,
        [
            SnapshotResult(
                state=SnapshotState(
                    root_globs=[],
                    files=[
                        SnapshotFile(
                            relative_path=Path("store/000/00000000-0000-0000-0000-100000000001/detached/all_0_0_0/data.bin"),
                            file_size=1000,
                            mtime_ns=0,
                            hexdigest="0001",
                        ),
                        SnapshotFile(
                            relative_path=Path("store/000/00000000-0000-0000-0000-100000000001/detached/all_1_1_0/data.bin"),
                            file_size=1000,
                            mtime_ns=0,
                            hexdigest="0002",
                        ),
                        SnapshotFile(
                            relative_path=Path("store/000/00000000-0000-0000-0000-100000000002/detached/all_0_0_0/data.bin"),
                            file_size=1000,
                            mtime_ns=0,
                            hexdigest="0003",
                        ),
                    ],
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
                            hexdigest="0001",
                        ),
                        SnapshotFile(
                            relative_path=Path("store/000/00000000-0000-0000-0000-100000000001/detached/all_1_1_0/data.bin"),
                            file_size=1000,
                            mtime_ns=0,
                            hexdigest="0002",
                        ),
                        SnapshotFile(
                            relative_path=Path("store/000/00000000-0000-0000-0000-100000000002/detached/all_0_0_0/data.bin"),
                            file_size=1000,
                            mtime_ns=0,
                            hexdigest="0004",
                        ),
                    ],
                ),
            ),
        ],
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
            hexdigest="0001",
        ),
        SnapshotFile(
            relative_path=Path("store/000/00000000-0000-0000-0000-100000000002/detached/all_0_0_0/data.bin"),
            file_size=1000,
            mtime_ns=0,
            hexdigest="0003",
        ),
    ]
    assert sorted(snapshot_results[1].state.files) == [
        SnapshotFile(
            relative_path=Path("store/000/00000000-0000-0000-0000-100000000001/detached/all_1_1_0/data.bin"),
            file_size=1000,
            mtime_ns=0,
            hexdigest="0002",
        ),
        SnapshotFile(
            relative_path=Path("store/000/00000000-0000-0000-0000-100000000002/detached/all_0_0_0/data.bin"),
            file_size=1000,
            mtime_ns=0,
            hexdigest="0004",
        ),
    ]


def b64_str(b: bytes) -> str:
    return b64encode(b).decode()


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
                "access_entities": [
                    {
                        "name": b64_str(b"default_\x80"),
                        "uuid": "00000000-0000-0000-0000-000000000002",
                        "type": "U",
                        "attach_query": b64_str(b"ATTACH USER \x80 ..."),
                    }
                ],
                "replicated_databases": [
                    {
                        "name": b64_str(b"db-one"),
                        "uuid": "00000000-0000-0000-0000-000000000010",
                        "shard": b64_str(b"{my_shard}"),
                        "replica": b64_str(b"{my_replica}"),
                    }
                ],
                "tables": [
                    {
                        "database": b64_str(b"db-one"),
                        "name": b64_str(b"t1"),
                        "engine": "MergeTree",
                        "uuid": "00000000-0000-0000-0000-000000000004",
                        "create_query": b64_str(b"CREATE ..."),
                        "dependencies": [],
                    }
                ],
            },
        ),
    )
    clickhouse_manifest = await step.run_step(Cluster(nodes=[]), context)
    assert clickhouse_manifest == ClickHouseManifest(
        access_entities=[
            AccessEntity(type="U", uuid=uuid.UUID(int=2), name=b"default_\x80", attach_query=b"ATTACH USER \x80 ...")
        ],
        replicated_databases=[
            ReplicatedDatabase(name=b"db-one", uuid=uuid.UUID(int=16), shard=b"{my_shard}", replica=b"{my_replica}")
        ],
        tables=[
            Table(database=b"db-one", name=b"t1", engine="MergeTree", uuid=uuid.UUID(int=4), create_query=b"CREATE ...")
        ],
    )


@pytest.mark.asyncio
async def test_list_database_replicas_step() -> None:
    step = ListDatabaseReplicasStep()
    cluster = Cluster(nodes=[CoordinatorNode(url="node1"), CoordinatorNode(url="node2")])
    context = StepsContext()
    context.set_result(
        ClickHouseManifestStep,
        ClickHouseManifest(
            replicated_databases=[
                ReplicatedDatabase(name=b"db-one", shard=b"pre_{shard_group_a}", replica=b"{replica}"),
                ReplicatedDatabase(name=b"db-two", shard=b"{shard_group_b}", replica=b"{replica}_suf"),
            ]
        ),
    )
    context.set_result(
        RetrieveMacrosStep,
        [
            Macros.from_mapping({b"shard_group_a": b"a1", b"shard_group_b": b"b1", b"replica": b"node1"}),
            Macros.from_mapping({b"shard_group_a": b"a2", b"shard_group_b": b"b2", b"replica": b"node2"}),
        ],
    )
    database_replicas = await step.run_step(cluster, context)
    assert database_replicas == {
        b"db-one": [
            DatabaseReplica(shard_name="pre_a1", replica_name="node1"),
            DatabaseReplica(shard_name="pre_a2", replica_name="node2"),
        ],
        b"db-two": [
            DatabaseReplica(shard_name="b1", replica_name="node1_suf"),
            DatabaseReplica(shard_name="b2", replica_name="node2_suf"),
        ],
    }


@pytest.mark.parametrize("missing_macro", [b"shard", b"replica"])
@pytest.mark.asyncio
async def test_list_database_replicas_step_fails_on_missing_macro(missing_macro: bytes) -> None:
    server_1_macros = Macros()
    server_2_macros = Macros()
    if missing_macro != b"shard":
        server_1_macros.add(b"shard", b"s1")
        server_2_macros.add(b"shard", b"s1")
    elif missing_macro != b"replica":
        server_1_macros.add(b"replica", b"r1")
        server_2_macros.add(b"replica", b"r2")
    step = ListDatabaseReplicasStep()
    cluster = Cluster(nodes=[CoordinatorNode(url="node1"), CoordinatorNode(url="node2")])
    context = StepsContext()
    context.set_result(
        ClickHouseManifestStep,
        ClickHouseManifest(
            replicated_databases=[
                ReplicatedDatabase(name=b"db-one", shard=b"{shard}", replica=b"{replica}"),
            ]
        ),
    )
    context.set_result(RetrieveMacrosStep, [server_1_macros, server_2_macros])
    with pytest.raises(StepFailedError, match=f"Error in macro of server 1: No macro named {missing_macro!r}"):
        await step.run_step(cluster, context)


@pytest.mark.asyncio
async def test_list_sync_database_replicas_step() -> None:
    zookeeper_client = FakeZooKeeperClient()
    async with zookeeper_client.connect() as connection:
        await connection.create("/clickhouse/databases", b"")
        # In the first database, node1 is done but node2 is late
        await connection.create("/clickhouse/databases/db%2Done", b"")
        await connection.create("/clickhouse/databases/db%2Done/max_log_ptr", b"100")
        await connection.create("/clickhouse/databases/db%2Done/replicas", b"")
        await connection.create("/clickhouse/databases/db%2Done/replicas/all|node1", b"")
        await connection.create("/clickhouse/databases/db%2Done/replicas/all|node1/log_ptr", b"95")
        await connection.create("/clickhouse/databases/db%2Done/replicas/all|node2", b"")
        await connection.create("/clickhouse/databases/db%2Done/replicas/all|node2/log_ptr", b"90")
    step = SyncDatabaseReplicasStep(
        zookeeper_client=zookeeper_client, replicated_databases_zookeeper_path="/clickhouse/databases", sync_timeout=1.0
    )
    cluster = Cluster(nodes=[CoordinatorNode(url="node1"), CoordinatorNode(url="node2")])
    context = StepsContext()
    context.set_result(
        ClickHouseManifestStep,
        ClickHouseManifest(
            replicated_databases=[
                ReplicatedDatabase(name=b"db-one", uuid=uuid.UUID(int=16), shard=b"{my_shard}", replica=b"{my_replica}")
            ]
        ),
    )
    context.set_result(
        ListDatabaseReplicasStep,
        {
            b"db-one": [
                DatabaseReplica(shard_name="all", replica_name="node1"),
                DatabaseReplica(shard_name="all", replica_name="node2"),
            ]
        },
    )

    async def advance_node1():
        async with zookeeper_client.connect() as connection:
            for ptr in range(96, 101):
                await connection.set("/clickhouse/databases/db%2Done/replicas/all|node1/log_ptr", str(ptr).encode())

    async def advance_node2():
        async with zookeeper_client.connect() as connection:
            for ptr in range(91, 101):
                await connection.set("/clickhouse/databases/db%2Done/replicas/all|node2/log_ptr", str(ptr).encode())

    await asyncio.gather(step.run_step(cluster, context), advance_node1(), advance_node2())
    async with zookeeper_client.connect() as connection:
        assert await connection.get("/clickhouse/databases/db%2Done/replicas/all|node1/log_ptr") == b"100"
        assert await connection.get("/clickhouse/databases/db%2Done/replicas/all|node2/log_ptr") == b"100"


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
        b"DROP DATABASE IF EXISTS `db-one` SYNC",
        b"CREATE DATABASE `db-one` UUID '00000000-0000-0000-0000-000000000011'"
        b" ENGINE = Replicated('/clickhouse/databases/db%2Done', '{my_shard}', '{my_replica}')",
        b"DROP DATABASE IF EXISTS `db-two` SYNC",
        b"CREATE DATABASE `db-two` UUID '00000000-0000-0000-0000-000000000012'"
        b" ENGINE = Replicated('/clickhouse/databases/db%2Dtwo', '{my_shard}', '{my_replica}')",
        b"CREATE TABLE db-one.table-uno ...",
        b"CREATE TABLE db-one.table-dos ...",
        b"CREATE TABLE db-two.table-eins ...",
    ]
    # CREATE TABLE is replicated, that why we only create the table on the first client
    second_client_queries = [
        b"DROP DATABASE IF EXISTS `db-one` SYNC",
        b"CREATE DATABASE `db-one` UUID '00000000-0000-0000-0000-000000000011'"
        b" ENGINE = Replicated('/clickhouse/databases/db%2Done', '{my_shard}', '{my_replica}')",
        b"DROP DATABASE IF EXISTS `db-two` SYNC",
        b"CREATE DATABASE `db-two` UUID '00000000-0000-0000-0000-000000000012'"
        b" ENGINE = Replicated('/clickhouse/databases/db%2Dtwo', '{my_shard}', '{my_replica}')",
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
        b"DROP DATABASE IF EXISTS `db-one` SYNC",
        b"CREATE DATABASE `db-one` UUID '00000000-0000-0000-0000-000000000011'"
        b" ENGINE = Replicated('/clickhouse/databases/db%2Done', '{my_shard}', '{my_replica}') "
        b"SETTINGS cluster_username='alice', cluster_password='alice_secret'",
        b"DROP DATABASE IF EXISTS `db-two` SYNC",
        b"CREATE DATABASE `db-two` UUID '00000000-0000-0000-0000-000000000012'"
        b" ENGINE = Replicated('/clickhouse/databases/db%2Dtwo', '{my_shard}', '{my_replica}') "
        b"SETTINGS cluster_username='alice', cluster_password='alice_secret'",
        b"CREATE TABLE db-one.table-uno ...",
        b"CREATE TABLE db-one.table-dos ...",
        b"CREATE TABLE db-two.table-eins ...",
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
    all_client_queries = [
        b"DROP DATABASE IF EXISTS `db-one` SYNC",
        b"DROP DATABASE IF EXISTS `db-one` SYNC",
        b"CREATE DATABASE `db-one` UUID '00000000-0000-0000-0000-000000000011'"
        b" ENGINE = Replicated('/clickhouse/databases/db%2Done', '{my_shard}', '{my_replica}')",
        b"CREATE DATABASE `db-one` UUID '00000000-0000-0000-0000-000000000011'"
        b" ENGINE = Replicated('/clickhouse/databases/db%2Done', '{my_shard}', '{my_replica}')",
        b"DROP DATABASE IF EXISTS `db-two` SYNC",
        b"DROP DATABASE IF EXISTS `db-two` SYNC",
        b"CREATE DATABASE `db-two` UUID '00000000-0000-0000-0000-000000000012'"
        b" ENGINE = Replicated('/clickhouse/databases/db%2Dtwo', '{my_shard}', '{my_replica}')",
        b"CREATE DATABASE `db-two` UUID '00000000-0000-0000-0000-000000000012'"
        b" ENGINE = Replicated('/clickhouse/databases/db%2Dtwo', '{my_shard}', '{my_replica}')",
        b"CREATE TABLE db-one.table-uno ...",
        b"CREATE TABLE db-one.table-dos ...",
        b"CREATE TABLE db-two.table-eins ...",
    ]
    assert client.mock_calls == list(map(mock.call.execute, all_client_queries))


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
        await connection.create(f"/clickhouse/access/uuid/{str(uuid.UUID(int=2))}", b"ATTACH QUOTA ...")
    await step.run_step(Cluster(nodes=[]), context)
    await check_restored_entities(client)


async def check_restored_entities(client: ZooKeeperClient) -> None:
    async with client.connect() as connection:
        assert await connection.get_children("/clickhouse/access") == ["P", "Q", "R", "S", "U", "uuid"]
        assert await connection.get_children("/clickhouse/access/uuid") == [str(uuid.UUID(int=i)) for i in range(1, 7)]
        assert await connection.get_children("/clickhouse/access/P") == ["a_policy"]
        assert await connection.get_children("/clickhouse/access/Q") == ["a_quota"]
        assert await connection.get_children("/clickhouse/access/R") == ["a_role"]
        assert await connection.get_children("/clickhouse/access/S") == ["a_settings_profile"]
        assert await connection.get_children("/clickhouse/access/U") == ["jos%C3%A9", "z%80enjoyer"]
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
        assert await connection.get("/clickhouse/access/U/z%80enjoyer") == str(uuid.UUID(int=6)).encode()
        assert await connection.get(f"/clickhouse/access/uuid/{str(uuid.UUID(int=6))}") == b"ATTACH USER \x80 ..."


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
                        ],
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
                        ],
                    )
                ),
            ],
            upload_results=[],
            plugin=Plugin.clickhouse,
        ),
    )
    context.set_result(ClickHouseManifestStep, SAMPLE_MANIFEST)
    await step.run_step(cluster, context)
    # Note: parts list is different for each client
    # however, we can have identically named parts which are not the same file
    assert client_1.mock_calls == [
        mock.call.execute(b"SET receive_timeout=60", session_id=mock.ANY),
        mock.call.execute(b"ALTER TABLE `db-one`.`table-dos` ATTACH PART 'all_1_1_0'", session_id=mock.ANY, timeout=60),
        mock.call.execute(b"SET receive_timeout=60", session_id=mock.ANY),
        mock.call.execute(b"ALTER TABLE `db-one`.`table-uno` ATTACH PART 'all_0_0_0'", session_id=mock.ANY, timeout=60),
    ]
    check_each_pair_of_calls_has_the_same_session_id(client_1.mock_calls)
    assert client_2.mock_calls == [
        mock.call.execute(b"SET receive_timeout=60", session_id=mock.ANY),
        mock.call.execute(b"ALTER TABLE `db-one`.`table-dos` ATTACH PART 'all_1_1_1'", session_id=mock.ANY, timeout=60),
        mock.call.execute(b"SET receive_timeout=60", session_id=mock.ANY),
        mock.call.execute(b"ALTER TABLE `db-one`.`table-uno` ATTACH PART 'all_0_0_0'", session_id=mock.ANY, timeout=60),
    ]
    check_each_pair_of_calls_has_the_same_session_id(client_2.mock_calls)


@pytest.mark.asyncio
async def test_sync_replicas_for_replicated_mergetree_tables() -> None:
    clients = [mock_clickhouse_client(), mock_clickhouse_client()]
    step = SyncTableReplicasStep(clients, sync_timeout=180, max_concurrent_sync=10)
    cluster = Cluster(nodes=[CoordinatorNode(url="node1"), CoordinatorNode(url="node2")])
    context = StepsContext()
    context.set_result(ClickHouseManifestStep, SAMPLE_MANIFEST)
    await step.run_step(cluster, context)
    for client_index, client in enumerate(clients):
        assert client.mock_calls == [
            mock.call.execute(b"SET receive_timeout=180", session_id=mock.ANY),
            mock.call.execute(b"SYSTEM SYNC REPLICA `db-one`.`table-uno`", session_id=mock.ANY, timeout=180),
            mock.call.execute(b"SET receive_timeout=180", session_id=mock.ANY),
            mock.call.execute(b"SYSTEM SYNC REPLICA `db-two`.`table-eins`", session_id=mock.ANY, timeout=180),
        ], f"Wrong list of queries for client {client_index} of {len(clients)}"
        check_each_pair_of_calls_has_the_same_session_id(client.mock_calls)


def check_each_pair_of_calls_has_the_same_session_id(mock_calls: Sequence[MockCall]) -> None:
    session_ids = [mock_call[2]["session_id"] for mock_call in mock_calls]
    assert len(session_ids) % 2 == 0
    assert None not in session_ids
    for start_index in range(0, len(session_ids), 2):
        assert session_ids[start_index] == session_ids[start_index + 1]
