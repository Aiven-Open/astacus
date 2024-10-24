"""Copyright (c) 2021 Aiven Ltd
See LICENSE for details.
"""

from astacus.common.asyncstorage import AsyncJsonStorage
from astacus.common.exceptions import TransientException
from astacus.common.ipc import BackupManifest, ManifestMin, Plugin, SnapshotFile, SnapshotResult, SnapshotState
from astacus.common.statsd import StatsClient
from astacus.coordinator.cluster import Cluster
from astacus.coordinator.config import CoordinatorNode
from astacus.coordinator.plugins.base import (
    BackupManifestStep,
    ComputeKeptBackupsStep,
    SnapshotStep,
    StepFailedError,
    StepsContext,
)
from astacus.coordinator.plugins.clickhouse.client import ClickHouseClient, StubClickHouseClient
from astacus.coordinator.plugins.clickhouse.config import (
    ClickHouseConfiguration,
    ClickHouseNode,
    DirectCopyConfig,
    DiskConfiguration,
    DiskType,
    LocalCopyConfig,
    ReplicatedDatabaseSettings,
)
from astacus.coordinator.plugins.clickhouse.disks import Disk, Disks
from astacus.coordinator.plugins.clickhouse.macros import Macros, MACROS_LIST_QUERY
from astacus.coordinator.plugins.clickhouse.manifest import (
    AccessEntity,
    ClickHouseBackupVersion,
    ClickHouseManifest,
    ClickHouseObjectStorageFile,
    ClickHouseObjectStorageFiles,
    KeeperMapRow,
    KeeperMapTable,
    ReplicatedDatabase,
    Table,
    UserDefinedFunction,
)
from astacus.coordinator.plugins.clickhouse.object_storage import copy_items_between, ObjectStorage, ObjectStorageItem
from astacus.coordinator.plugins.clickhouse.replication import DatabaseReplica
from astacus.coordinator.plugins.clickhouse.steps import (
    AttachMergeTreePartsStep,
    ClickHouseManifestStep,
    ClickHouseVersion,
    CollectObjectStorageFilesStep,
    DATABASES_LIST_QUERY,
    DeleteDanglingObjectStorageFilesStep,
    FreezeTablesStep,
    FreezeUnfreezeTablesStepBase,
    get_restore_table_query,
    GetVersionsStep,
    KeeperMapTablesReadabilityStepBase,
    KeeperMapTablesReadOnlyStep,
    KeeperMapTablesReadWriteStep,
    ListDatabaseReplicasStep,
    MoveFrozenPartsStep,
    PrepareClickHouseManifestStep,
    RemoveFrozenTablesStep,
    RestoreAccessEntitiesStep,
    RestoreKeeperMapTableDataStep,
    RestoreObjectStorageFilesStep,
    RestoreReplicaStep,
    RestoreReplicatedDatabasesStep,
    RestoreUserDefinedFunctionsStep,
    RetrieveAccessEntitiesStep,
    RetrieveDatabasesAndTablesStep,
    RetrieveKeeperMapTableDataStep,
    RetrieveMacrosStep,
    RetrieveUserDefinedFunctionsStep,
    run_partition_cmd_on_every_node,
    SyncDatabaseReplicasStep,
    SyncTableReplicasStep,
    TABLES_LIST_QUERY,
    UnfreezeTablesStep,
    ValidateConfigStep,
    wait_for_condition_on_every_node,
)
from astacus.coordinator.plugins.zookeeper import FakeZooKeeperClient, ZooKeeperClient
from base64 import b64encode
from collections.abc import Awaitable, Collection, Iterable, Sequence
from pathlib import Path
from rohmu import BaseTransfer
from rohmu.object_storage.base import ObjectTransferProgressCallback
from tests.unit.coordinator.plugins.clickhouse.object_storage import MemoryObjectStorage
from tests.unit.storage import MemoryJsonStorage
from typing import Any
from unittest import mock
from unittest.mock import _Call as MockCall, patch

import asyncio
import base64
import datetime
import msgspec
import pytest
import tempfile
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

SAMPLE_OBJECT_STORAGE_FILES = [
    ClickHouseObjectStorageFiles(
        disk_name="remote",
        files=[
            ClickHouseObjectStorageFile(path="abc/defghi"),
            ClickHouseObjectStorageFile(path="jkl/mnopqr"),
            ClickHouseObjectStorageFile(path="stu/vwxyza"),
        ],
        total_size_bytes=400,
    )
]

SAMPLE_USER_DEFINED_FUNCTIONS = [
    UserDefinedFunction(
        path="user_defined_function_1.sql", create_query=b"CREATE FUNCTION user_defined_function_1 AS (x) -> x + 1;\n"
    ),
    UserDefinedFunction(
        path="user_defined_function_2.sql", create_query=b"CREATE FUNCTION user_defined_function_2 AS (x) -> x + 2;\n"
    ),
]
SAMPLE_KEEPER_MAP_TABLES = [
    KeeperMapTable(
        name="name1",
        data=[
            KeeperMapRow(key="key1", value=b"value1"),
            KeeperMapRow(key="key2", value=b"value2"),
        ],
    ),
    KeeperMapTable(
        name="name2",
        data=[
            KeeperMapRow(key="key3", value=b"value3"),
        ],
    ),
]

SAMPLE_MANIFEST_V1 = ClickHouseManifest(
    version=ClickHouseBackupVersion.V1,
    access_entities=SAMPLE_ENTITIES,
    replicated_databases=SAMPLE_DATABASES,
    tables=SAMPLE_TABLES,
)

SAMPLE_MANIFEST = ClickHouseManifest(
    version=ClickHouseBackupVersion.V2,
    access_entities=SAMPLE_ENTITIES,
    replicated_databases=SAMPLE_DATABASES,
    tables=SAMPLE_TABLES,
    object_storage_files=SAMPLE_OBJECT_STORAGE_FILES,
    user_defined_functions=SAMPLE_USER_DEFINED_FUNCTIONS,
    keeper_map_tables=SAMPLE_KEEPER_MAP_TABLES,
)

SAMPLE_MANIFEST_ENCODED = SAMPLE_MANIFEST.to_plugin_data()


def mock_clickhouse_client() -> mock.Mock:
    mock_client = mock.Mock(spec_set=ClickHouseClient)
    return mock_client


@pytest.mark.parametrize(
    ("clickhouse_count", "coordinator_count", "success"),
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


async def create_zookeeper_access_entities(zookeeper_client: ZooKeeperClient) -> None:
    async with zookeeper_client.connect() as connection:
        await asyncio.gather(
            connection.create("/clickhouse/access/P/a_policy", str(uuid.UUID(int=1)).encode()),
            connection.create(f"/clickhouse/access/uuid/{uuid.UUID(int=1)!s}", b"ATTACH ROW POLICY ..."),
            connection.create("/clickhouse/access/Q/a_quota", str(uuid.UUID(int=2)).encode()),
            connection.create(f"/clickhouse/access/uuid/{uuid.UUID(int=2)!s}", b"ATTACH QUOTA ..."),
            connection.create("/clickhouse/access/R/a_role", str(uuid.UUID(int=3)).encode()),
            connection.create(f"/clickhouse/access/uuid/{uuid.UUID(int=3)!s}", b"ATTACH ROLE ..."),
            connection.create("/clickhouse/access/S/a_settings_profile", str(uuid.UUID(int=4)).encode()),
            connection.create(f"/clickhouse/access/uuid/{uuid.UUID(int=4)!s}", b"ATTACH SETTINGS PROFILE ..."),
            connection.create("/clickhouse/access/U/jos%C3%A9", str(uuid.UUID(int=5)).encode()),
            connection.create(f"/clickhouse/access/uuid/{uuid.UUID(int=5)!s}", b"ATTACH USER ..."),
            connection.create("/clickhouse/access/U/z%80enjoyer", str(uuid.UUID(int=6)).encode()),
            connection.create(f"/clickhouse/access/uuid/{uuid.UUID(int=6)!s}", b"ATTACH USER \x80 ..."),
        )


async def test_retrieve_access_entities() -> None:
    zookeeper_client = FakeZooKeeperClient()
    await create_zookeeper_access_entities(zookeeper_client)
    step = RetrieveAccessEntitiesStep(zookeeper_client=zookeeper_client, access_entities_path="/clickhouse/access")
    access_entities = await step.run_step(Cluster(nodes=[]), StepsContext())
    assert access_entities == SAMPLE_ENTITIES


async def create_zookeeper_user_defined_functions(zookeeper_client: ZooKeeperClient) -> None:
    async with zookeeper_client.connect() as connection:
        await asyncio.gather(
            connection.create(
                "/clickhouse/user_defined_functions/user_defined_function_1.sql",
                b"CREATE FUNCTION user_defined_function_1 AS (x) -> x + 1;\n",
            ),
            connection.create(
                "/clickhouse/user_defined_functions/user_defined_function_2.sql",
                b"CREATE FUNCTION user_defined_function_2 AS (x) -> x + 2;\n",
            ),
        )


async def test_retrieve_user_defined_functions() -> None:
    zookeeper_client = FakeZooKeeperClient()
    await create_zookeeper_user_defined_functions(zookeeper_client)
    step = RetrieveUserDefinedFunctionsStep(
        zookeeper_client=zookeeper_client, replicated_user_defined_zookeeper_path="/clickhouse/user_defined_functions/"
    )
    user_defined_functions = await step.run_step(Cluster(nodes=[]), StepsContext())
    assert user_defined_functions == SAMPLE_USER_DEFINED_FUNCTIONS


async def create_zookeeper_keeper_map_table_data(zookeeper_client: ZooKeeperClient) -> None:
    async with zookeeper_client.connect() as connection:
        await asyncio.gather(
            connection.create("/clickhouse/keeper_map/name1/data/key1", b"value1"),
            connection.create("/clickhouse/keeper_map/name1/data/key2", b"value2"),
            connection.create("/clickhouse/keeper_map/name2/data/key3", b"value3"),
        )


async def test_retrieve_keeper_map_table_data() -> None:
    zookeeper_client = FakeZooKeeperClient()
    clickhouse_client = mock_clickhouse_client()
    await create_zookeeper_keeper_map_table_data(zookeeper_client)
    step = RetrieveKeeperMapTableDataStep(
        zookeeper_client=zookeeper_client,
        keeper_map_path_prefix="/clickhouse/keeper_map/",
        clients=[clickhouse_client],
    )
    keeper_map_data = await step.run_step(Cluster(nodes=[]), StepsContext())
    assert keeper_map_data == SAMPLE_KEEPER_MAP_TABLES


async def test_retrieve_keeper_map_table_data_raises_step_error_on_zookeeper_error() -> None:
    zookeeper_client = FakeZooKeeperClient()
    clickhouse_client = mock_clickhouse_client()
    await create_zookeeper_keeper_map_table_data(zookeeper_client)
    step = RetrieveKeeperMapTableDataStep(
        zookeeper_client=zookeeper_client,
        keeper_map_path_prefix="/clickhouse/keeper_map/",
        clients=[clickhouse_client],
    )
    keeper_map_data = await step.run_step(Cluster(nodes=[]), StepsContext())
    assert keeper_map_data == SAMPLE_KEEPER_MAP_TABLES


class TrappedZooKeeperClient(FakeZooKeeperClient):
    """A fake ZooKeeper client with a trap: it will inject a concurrent write after a few reads."""

    def __init__(self) -> None:
        super().__init__()
        self.calls_until_failure: int | None = None

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


async def test_retrieve_access_entities_fails_from_concurrent_updates() -> None:
    zookeeper_client = TrappedZooKeeperClient()
    await create_zookeeper_access_entities(zookeeper_client)
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


async def test_retrieve_tables() -> None:
    clients: Sequence[StubClickHouseClient] = [StubClickHouseClient(), StubClickHouseClient()]
    clients[0].set_response(
        DATABASES_LIST_QUERY,
        [
            [b64_str(b"db-empty"), "00000000-0000-0000-0000-000000000010"],
            [b64_str(b"db-one"), "00000000-0000-0000-0000-000000000011"],
            [b64_str(b"db-two"), "00000000-0000-0000-0000-000000000012"],
        ],
    )
    clients[0].set_response(
        TABLES_LIST_QUERY,
        [
            [
                b64_str(b"db-one"),
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
                b64_str(b"table-dos"),
                "MergeTree",
                "00000000-0000-0000-0000-100000000002",
                b64_str(b"CREATE TABLE db-one.table-dos ..."),
                [],
            ],
            [
                b64_str(b"db-two"),
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


async def test_retrieve_tables_without_any_database_or_table() -> None:
    clients = [StubClickHouseClient(), StubClickHouseClient()]
    clients[0].set_response(DATABASES_LIST_QUERY, [])
    clients[0].set_response(TABLES_LIST_QUERY, [])
    step = RetrieveDatabasesAndTablesStep(clients=clients)
    context = StepsContext()
    assert await step.run_step(Cluster(nodes=[]), context) == ([], [])


async def test_retrieve_tables_without_any_table() -> None:
    clients = [StubClickHouseClient(), StubClickHouseClient()]
    clients[0].set_response(
        DATABASES_LIST_QUERY,
        [
            [b64_str(b"db-empty"), "00000000-0000-0000-0000-000000000010"],
        ],
    )
    clients[0].set_response(TABLES_LIST_QUERY, [])
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


def create_remote_file(path: str, remote_path: str, size: int) -> SnapshotFile:
    metadata = f"""3\n1\t{size}\n{size}\t{remote_path}\n1\n0\n""".encode()
    return SnapshotFile(
        relative_path=f"disks/remote/{path}",
        file_size=len(metadata),
        mtime_ns=1,
        content_b64=base64.b64encode(metadata).decode(),
    )


async def test_collect_object_storage_file_steps() -> None:
    disks = Disks.from_disk_configs(
        [
            DiskConfiguration(type=DiskType.local, path=Path(), name="default"),
            DiskConfiguration(type=DiskType.object_storage, path=Path("disks/remote"), name="remote"),
        ]
    )
    step = CollectObjectStorageFilesStep(disks=disks)
    context = StepsContext()
    table_uuid_parts = "000/00000000-0000-0000-0000-100000000001"
    snapshot_results = [
        SnapshotResult(
            state=SnapshotState(
                root_globs=["dont", "care"],
                files=[
                    create_remote_file(
                        f"store/{table_uuid_parts}/all_0_0_0/columns.txt",
                        "abc/defghi",
                        size=100,
                    ),
                    create_remote_file(
                        f"store/{table_uuid_parts}/all_0_0_0/data.bin",
                        "jkl/mnopqr",
                        size=100,
                    ),
                ],
            )
        ),
        SnapshotResult(
            state=SnapshotState(
                root_globs=["dont", "care"],
                files=[
                    create_remote_file(
                        f"store/{table_uuid_parts}/all_0_0_0/columns.txt",
                        "abc/defghi",
                        size=0,
                    ),
                    create_remote_file(
                        f"store/{table_uuid_parts}/all_0_0_0/data.bin",
                        "stu/vwxyza",
                        size=200,
                    ),
                ],
            )
        ),
    ]
    context.set_result(SnapshotStep, snapshot_results)
    assert await step.run_step(Cluster(nodes=[]), context) == SAMPLE_OBJECT_STORAGE_FILES


async def test_move_frozen_parts_steps() -> None:
    disks = Disks.from_disk_configs(
        [
            DiskConfiguration(type=DiskType.local, path=Path(), name="default"),
            DiskConfiguration(type=DiskType.object_storage, path=Path("disks/remote"), name="remote"),
        ]
    )
    step = MoveFrozenPartsStep(disks=disks)
    context = StepsContext()
    table_uuid_parts = "000/00000000-0000-0000-0000-100000000001"
    snapshot_results = [
        SnapshotResult(
            state=SnapshotState(
                root_globs=["dont", "care"],
                files=[
                    SnapshotFile(
                        relative_path=f"shadow/astacus/store/{table_uuid_parts}/detached/all_0_0_0/columns.txt",
                        file_size=100,
                        mtime_ns=1,
                    ),
                    SnapshotFile(
                        relative_path=f"disks/remote/shadow/astacus/store/{table_uuid_parts}/detached/all_0_0_0/data.bin",
                        file_size=100,
                        mtime_ns=1,
                    ),
                ],
            )
        ),
    ]
    context.set_result(SnapshotStep, snapshot_results)
    await step.run_step(Cluster(nodes=[]), context)
    # This step is mutating the snapshot
    assert snapshot_results[0].state == SnapshotState(
        root_globs=["dont", "care"],
        files=[
            SnapshotFile(
                relative_path=f"store/{table_uuid_parts}/all_0_0_0/columns.txt",
                file_size=100,
                mtime_ns=1,
            ),
            SnapshotFile(
                relative_path=f"disks/remote/store/{table_uuid_parts}/all_0_0_0/data.bin",
                file_size=100,
                mtime_ns=1,
            ),
        ],
    )


async def test_create_clickhouse_manifest() -> None:
    step = PrepareClickHouseManifestStep()
    context = StepsContext()
    context.set_result(RetrieveAccessEntitiesStep, SAMPLE_ENTITIES)
    context.set_result(RetrieveDatabasesAndTablesStep, (SAMPLE_DATABASES, SAMPLE_TABLES))
    context.set_result(CollectObjectStorageFilesStep, SAMPLE_OBJECT_STORAGE_FILES)
    context.set_result(RetrieveUserDefinedFunctionsStep, SAMPLE_USER_DEFINED_FUNCTIONS)
    context.set_result(RetrieveKeeperMapTableDataStep, SAMPLE_KEEPER_MAP_TABLES)
    assert await step.run_step(Cluster(nodes=[]), context) == SAMPLE_MANIFEST_ENCODED


async def test_remove_frozen_tables_step_using_system_unfreeze() -> None:
    first_client, second_client = mock_clickhouse_client(), mock_clickhouse_client()
    step = RemoveFrozenTablesStep(
        clients=[first_client, second_client],
        freeze_name="some-thing+special",
        unfreeze_timeout=3600.0,
    )
    cluster = Cluster(nodes=[CoordinatorNode(url="http://node1/node"), CoordinatorNode(url="http://node2/node")])
    await step.run_step(cluster, StepsContext())
    client_queries = [b"SET receive_timeout=3600.0", b"SYSTEM UNFREEZE WITH NAME 'some-thing+special'"]
    assert [call.args[0] for call in first_client.execute.mock_calls] == client_queries
    assert [call.args[0] for call in second_client.execute.mock_calls] == client_queries


@pytest.mark.parametrize("all_clients", [True, False])
@pytest.mark.parametrize("operation", ["FREEZE", "UNFREEZE"])
async def test_freezes_all_mergetree_tables_listed_in_manifest(all_clients: bool, operation: str) -> None:
    if operation == "FREEZE":
        step_class: type[FreezeUnfreezeTablesStepBase] = FreezeTablesStep
    else:
        step_class = UnfreezeTablesStep

    if all_clients:
        versions = [(23, 8), (23, 8)]
    else:
        versions = [(23, 3), (23, 8)]

    context = StepsContext()
    context.set_result(GetVersionsStep, versions)
    first_client, second_client = mock_clickhouse_client(), mock_clickhouse_client()
    step = step_class(clients=[first_client, second_client], freeze_name="Äs`t:/.././@c'_'s", freeze_unfreeze_timeout=3600.0)
    cluster = Cluster(nodes=[CoordinatorNode(url="node1"), CoordinatorNode(url="node2")])
    context.set_result(RetrieveDatabasesAndTablesStep, (SAMPLE_DATABASES, SAMPLE_TABLES))
    await step.run_step(cluster, context)
    queries = [
        b"SET receive_timeout=3600.0",
        b"ALTER TABLE `db-one`.`table-uno` " + operation.encode() + b" WITH NAME '\\xc3\\x84s`t:/.././@c\\'_\\'s'"
        b" SETTINGS distributed_ddl_task_timeout=3600.0",
        b"SET receive_timeout=3600.0",
        b"ALTER TABLE `db-one`.`table-dos` " + operation.encode() + b" WITH NAME '\\xc3\\x84s`t:/.././@c\\'_\\'s'"
        b" SETTINGS distributed_ddl_task_timeout=3600.0",
        b"SET receive_timeout=3600.0",
        b"ALTER TABLE `db-two`.`table-eins` " + operation.encode() + b" WITH NAME '\\xc3\\x84s`t:/.././@c\\'_\\'s'"
        b" SETTINGS distributed_ddl_task_timeout=3600.0",
    ]
    assert [call.args[0] for call in first_client.execute.mock_calls] == queries
    if all_clients:
        assert [call.args[0] for call in second_client.execute.mock_calls] == queries
    else:
        assert second_client.mock_calls == []


def b64_str(b: bytes) -> str:
    return b64encode(b).decode()


async def test_parse_clickhouse_manifest() -> None:
    step = ClickHouseManifestStep()
    context = StepsContext()
    context.set_result(
        BackupManifestStep,
        BackupManifest(
            start=datetime.datetime(2020, 1, 2, 3, 4, 5, 678, tzinfo=datetime.UTC),
            end=datetime.datetime(2020, 1, 2, 5, 6, 7, 891, tzinfo=datetime.UTC),
            attempt=1,
            snapshot_results=[],
            upload_results=[],
            plugin=Plugin.clickhouse,
            plugin_data={
                "version": "v2",
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
        version=ClickHouseBackupVersion.V2,
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


async def test_list_database_replicas_step() -> None:
    step = ListDatabaseReplicasStep()
    cluster = Cluster(nodes=[CoordinatorNode(url="node1"), CoordinatorNode(url="node2")])
    context = StepsContext()
    context.set_result(
        ClickHouseManifestStep,
        ClickHouseManifest(
            version=ClickHouseBackupVersion.V2,
            replicated_databases=[
                ReplicatedDatabase(name=b"db-one", shard=b"pre_{shard_group_a}", replica=b"{replica}"),
                ReplicatedDatabase(name=b"db-two", shard=b"{shard_group_b}", replica=b"{replica}_suf"),
            ],
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
            version=ClickHouseBackupVersion.V2,
            replicated_databases=[
                ReplicatedDatabase(name=b"db-one", shard=b"{shard}", replica=b"{replica}"),
            ],
        ),
    )
    context.set_result(RetrieveMacrosStep, [server_1_macros, server_2_macros])
    with pytest.raises(StepFailedError, match=f"Error in macro of server 1: No macro named {missing_macro!r}"):
        await step.run_step(cluster, context)


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
            version=ClickHouseBackupVersion.V2,
            replicated_databases=[
                ReplicatedDatabase(name=b"db-one", uuid=uuid.UUID(int=16), shard=b"{my_shard}", replica=b"{my_replica}")
            ],
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


async def test_creates_all_replicated_databases_and_tables_in_manifest() -> None:
    clients = [mock_clickhouse_client(), mock_clickhouse_client()]
    step = RestoreReplicatedDatabasesStep(
        clients=clients,
        replicated_databases_zookeeper_path="/clickhouse/databases",
        replicated_database_settings=ReplicatedDatabaseSettings(),
        drop_databases_timeout=20.0,
        max_concurrent_drop_databases_per_node=10,
        create_databases_timeout=10.0,
        max_concurrent_create_database_per_node=10,
    )

    cluster = Cluster(nodes=[CoordinatorNode(url="node1"), CoordinatorNode(url="node2")])
    context = StepsContext()
    context.set_result(ClickHouseManifestStep, SAMPLE_MANIFEST)
    await step.run_step(cluster, context)
    first_client_queries = [
        b"SET receive_timeout=20.0",
        b"DROP DATABASE IF EXISTS `db-one` SYNC",
        b"SET receive_timeout=20.0",
        b"DROP DATABASE IF EXISTS `db-two` SYNC",
        b"SET receive_timeout=10.0",
        b"CREATE DATABASE `db-one` UUID '00000000-0000-0000-0000-000000000011'"
        b" ENGINE = Replicated('/clickhouse/databases/db%2Done', '{my_shard}', '{my_replica}')",
        b"SET receive_timeout=10.0",
        b"CREATE DATABASE `db-two` UUID '00000000-0000-0000-0000-000000000012'"
        b" ENGINE = Replicated('/clickhouse/databases/db%2Dtwo', '{my_shard}', '{my_replica}')",
        b"SET allow_experimental_inverted_index=true",
        b"SET allow_experimental_codecs=true",
        b"SET allow_experimental_live_view=true",
        b"SET allow_experimental_window_view=true",
        b"SET allow_experimental_funnel_functions=true",
        b"SET allow_experimental_nlp_functions=true",
        b"SET allow_experimental_hash_functions=true",
        b"SET allow_experimental_object_type=true",
        b"SET allow_experimental_annoy_index=true",
        b"SET allow_experimental_usearch_index=true",
        b"SET allow_experimental_bigint_types=true",
        b"SET allow_experimental_window_functions=true",
        b"SET allow_experimental_geo_types=true",
        b"SET allow_experimental_map_type=true",
        b"SET allow_suspicious_low_cardinality_types=true",
        b"SET allow_suspicious_fixed_string_types=true",
        b"SET allow_suspicious_indices=true",
        b"SET allow_suspicious_codecs=true",
        b"SET allow_hyperscan=true",
        b"SET allow_simdjson=true",
        b"SET allow_deprecated_syntax_for_merge_tree=true",
        b"SET flatten_nested=0",
        b"CREATE TABLE db-one.table-uno ...",
        b"CREATE TABLE db-one.table-dos ...",
        b"CREATE TABLE db-two.table-eins ...",
    ]
    # CREATE TABLE is replicated, that why we only create the table on the first client
    second_client_queries = [
        b"SET receive_timeout=20.0",
        b"DROP DATABASE IF EXISTS `db-one` SYNC",
        b"SET receive_timeout=20.0",
        b"DROP DATABASE IF EXISTS `db-two` SYNC",
        b"SET receive_timeout=10.0",
        b"CREATE DATABASE `db-one` UUID '00000000-0000-0000-0000-000000000011'"
        b" ENGINE = Replicated('/clickhouse/databases/db%2Done', '{my_shard}', '{my_replica}')",
        b"SET receive_timeout=10.0",
        b"CREATE DATABASE `db-two` UUID '00000000-0000-0000-0000-000000000012'"
        b" ENGINE = Replicated('/clickhouse/databases/db%2Dtwo', '{my_shard}', '{my_replica}')",
    ]
    assert [call.args[0] for call in clients[0].execute.mock_calls] == first_client_queries
    assert [call.args[0] for call in clients[1].execute.mock_calls] == second_client_queries


async def test_creates_all_replicated_databases_and_tables_in_manifest_with_custom_settings() -> None:
    client = mock_clickhouse_client()
    step = RestoreReplicatedDatabasesStep(
        clients=[client],
        replicated_databases_zookeeper_path="/clickhouse/databases",
        replicated_database_settings=ReplicatedDatabaseSettings(
            cluster_username="alice",
            cluster_password="alice_secret",
        ),
        drop_databases_timeout=20.0,
        max_concurrent_drop_databases_per_node=10,
        create_databases_timeout=10.0,
        max_concurrent_create_database_per_node=10,
    )
    cluster = Cluster(nodes=[CoordinatorNode(url="node1")])
    context = StepsContext()
    context.set_result(ClickHouseManifestStep, SAMPLE_MANIFEST)
    await step.run_step(cluster, context)
    first_client_queries = [
        b"SET receive_timeout=20.0",
        b"DROP DATABASE IF EXISTS `db-one` SYNC",
        b"SET receive_timeout=20.0",
        b"DROP DATABASE IF EXISTS `db-two` SYNC",
        b"SET receive_timeout=10.0",
        b"CREATE DATABASE `db-one` UUID '00000000-0000-0000-0000-000000000011'"
        b" ENGINE = Replicated('/clickhouse/databases/db%2Done', '{my_shard}', '{my_replica}') "
        b"SETTINGS cluster_username='alice', cluster_password='alice_secret'",
        b"SET receive_timeout=10.0",
        b"CREATE DATABASE `db-two` UUID '00000000-0000-0000-0000-000000000012'"
        b" ENGINE = Replicated('/clickhouse/databases/db%2Dtwo', '{my_shard}', '{my_replica}') "
        b"SETTINGS cluster_username='alice', cluster_password='alice_secret'",
        b"SET allow_experimental_inverted_index=true",
        b"SET allow_experimental_codecs=true",
        b"SET allow_experimental_live_view=true",
        b"SET allow_experimental_window_view=true",
        b"SET allow_experimental_funnel_functions=true",
        b"SET allow_experimental_nlp_functions=true",
        b"SET allow_experimental_hash_functions=true",
        b"SET allow_experimental_object_type=true",
        b"SET allow_experimental_annoy_index=true",
        b"SET allow_experimental_usearch_index=true",
        b"SET allow_experimental_bigint_types=true",
        b"SET allow_experimental_window_functions=true",
        b"SET allow_experimental_geo_types=true",
        b"SET allow_experimental_map_type=true",
        b"SET allow_suspicious_low_cardinality_types=true",
        b"SET allow_suspicious_fixed_string_types=true",
        b"SET allow_suspicious_indices=true",
        b"SET allow_suspicious_codecs=true",
        b"SET allow_hyperscan=true",
        b"SET allow_simdjson=true",
        b"SET allow_deprecated_syntax_for_merge_tree=true",
        b"SET flatten_nested=0",
        b"CREATE TABLE db-one.table-uno ...",
        b"CREATE TABLE db-one.table-dos ...",
        b"CREATE TABLE db-two.table-eins ...",
    ]
    assert [call.args[0] for call in client.execute.mock_calls] == first_client_queries


async def test_drops_each_database_on_all_servers_before_recreating_it() -> None:
    # We use the same client twice to record the global sequence of queries across all servers
    client1 = mock_clickhouse_client()
    client2 = mock_clickhouse_client()
    step = RestoreReplicatedDatabasesStep(
        clients=[client1, client2],
        replicated_databases_zookeeper_path="/clickhouse/databases",
        replicated_database_settings=ReplicatedDatabaseSettings(),
        drop_databases_timeout=20.0,
        max_concurrent_drop_databases_per_node=10,
        create_databases_timeout=10.0,
        max_concurrent_create_database_per_node=10,
    )
    cluster = Cluster(nodes=[CoordinatorNode(url="node1"), CoordinatorNode(url="node2")])
    context = StepsContext()
    context.set_result(ClickHouseManifestStep, SAMPLE_MANIFEST)
    await step.run_step(cluster, context)
    queries_expected_on_every_node = [
        b"SET receive_timeout=20.0",
        b"DROP DATABASE IF EXISTS `db-one` SYNC",
        b"SET receive_timeout=20.0",
        b"DROP DATABASE IF EXISTS `db-two` SYNC",
        b"SET receive_timeout=10.0",
        b"CREATE DATABASE `db-one` UUID '00000000-0000-0000-0000-000000000011'"
        b" ENGINE = Replicated('/clickhouse/databases/db%2Done', '{my_shard}', '{my_replica}')",
        b"SET receive_timeout=10.0",
        b"CREATE DATABASE `db-two` UUID '00000000-0000-0000-0000-000000000012'"
        b" ENGINE = Replicated('/clickhouse/databases/db%2Dtwo', '{my_shard}', '{my_replica}')",
    ]
    queries_expected_on_a_single_node = [
        b"SET allow_experimental_inverted_index=true",
        b"SET allow_experimental_codecs=true",
        b"SET allow_experimental_live_view=true",
        b"SET allow_experimental_window_view=true",
        b"SET allow_experimental_funnel_functions=true",
        b"SET allow_experimental_nlp_functions=true",
        b"SET allow_experimental_hash_functions=true",
        b"SET allow_experimental_object_type=true",
        b"SET allow_experimental_annoy_index=true",
        b"SET allow_experimental_usearch_index=true",
        b"SET allow_experimental_bigint_types=true",
        b"SET allow_experimental_window_functions=true",
        b"SET allow_experimental_geo_types=true",
        b"SET allow_experimental_map_type=true",
        b"SET allow_suspicious_low_cardinality_types=true",
        b"SET allow_suspicious_fixed_string_types=true",
        b"SET allow_suspicious_indices=true",
        b"SET allow_suspicious_codecs=true",
        b"SET allow_hyperscan=true",
        b"SET allow_simdjson=true",
        b"SET allow_deprecated_syntax_for_merge_tree=true",
        b"SET flatten_nested=0",
        b"CREATE TABLE db-one.table-uno ...",
        b"CREATE TABLE db-one.table-dos ...",
        b"CREATE TABLE db-two.table-eins ...",
    ]
    client1_queries = [call.args[0] for call in client1.execute.mock_calls]
    client2_queries = [call.args[0] for call in client2.execute.mock_calls]
    assert all(query in client1_queries for query in queries_expected_on_every_node)
    assert all(query in client2_queries for query in queries_expected_on_every_node)
    assert all(query in client1_queries or client2_queries for query in queries_expected_on_a_single_node)


async def test_creates_all_access_entities_in_manifest() -> None:
    client = FakeZooKeeperClient()
    step = RestoreAccessEntitiesStep(zookeeper_client=client, access_entities_path="/clickhouse/access")
    context = StepsContext()
    context.set_result(ClickHouseManifestStep, SAMPLE_MANIFEST)
    await step.run_step(Cluster(nodes=[]), context)
    await check_restored_entities(client)


async def test_creating_all_access_entities_can_be_retried() -> None:
    client = FakeZooKeeperClient()
    step = RestoreAccessEntitiesStep(zookeeper_client=client, access_entities_path="/clickhouse/access")
    context = StepsContext()
    context.set_result(ClickHouseManifestStep, SAMPLE_MANIFEST)
    async with client.connect() as connection:
        # Simulate a first partial restoration
        await connection.create("/clickhouse/access/P/a_policy", str(uuid.UUID(int=1)).encode())
        await connection.create(f"/clickhouse/access/uuid/{uuid.UUID(int=1)!s}", b"ATTACH ROW POLICY ...")
        await connection.create("/clickhouse/access/Q/a_quota", str(uuid.UUID(int=2)).encode())
        await connection.create(f"/clickhouse/access/uuid/{uuid.UUID(int=2)!s}", b"ATTACH QUOTA ...")
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
        assert await connection.get(f"/clickhouse/access/uuid/{uuid.UUID(int=1)!s}") == b"ATTACH ROW POLICY ..."
        assert await connection.get("/clickhouse/access/Q/a_quota") == str(uuid.UUID(int=2)).encode()
        assert await connection.get(f"/clickhouse/access/uuid/{uuid.UUID(int=2)!s}") == b"ATTACH QUOTA ..."
        assert await connection.get("/clickhouse/access/R/a_role") == str(uuid.UUID(int=3)).encode()
        assert await connection.get(f"/clickhouse/access/uuid/{uuid.UUID(int=3)!s}") == b"ATTACH ROLE ..."
        assert await connection.get("/clickhouse/access/S/a_settings_profile") == str(uuid.UUID(int=4)).encode()
        assert await connection.get(f"/clickhouse/access/uuid/{uuid.UUID(int=4)!s}") == b"ATTACH SETTINGS PROFILE ..."
        assert await connection.get("/clickhouse/access/U/jos%C3%A9") == str(uuid.UUID(int=5)).encode()
        assert await connection.get(f"/clickhouse/access/uuid/{uuid.UUID(int=5)!s}") == b"ATTACH USER ..."
        assert await connection.get("/clickhouse/access/U/z%80enjoyer") == str(uuid.UUID(int=6)).encode()
        assert await connection.get(f"/clickhouse/access/uuid/{uuid.UUID(int=6)!s}") == b"ATTACH USER \x80 ..."


async def test_restore_replica() -> None:
    zookeeper_client = FakeZooKeeperClient()
    client_1 = mock_clickhouse_client()
    client_2 = mock_clickhouse_client()
    clients = [client_1, client_2]
    step = RestoreReplicaStep(
        zookeeper_client=zookeeper_client,
        clients=clients,
        disks=Disks(),
        restart_timeout=30,
        max_concurrent_restart_per_node=20,
        restore_timeout=60,
        max_concurrent_restore_per_node=10,
    )
    context = StepsContext()
    context.set_result(ClickHouseManifestStep, SAMPLE_MANIFEST)
    cluster = Cluster(nodes=[CoordinatorNode(url="node1"), CoordinatorNode(url="node2")])
    async with zookeeper_client.connect() as connection:
        await connection.create("/clickhouse/tables/00000000-0000-0000-0000-100000000001", b"")
        await connection.create("/clickhouse/tables/00000000-0000-0000-0000-100000000001/thing", b"")
        await connection.create("/clickhouse/tables/00000000-0000-0000-0000-200000000001", b"")
        await connection.create("/clickhouse/tables/00000000-0000-0000-0000-200000000001/thing", b"")
    await step.run_step(cluster, context)
    async with zookeeper_client.connect() as connection:
        assert not await connection.exists("/clickhouse/tables/00000000-0000-0000-0000-100000000001")
        assert not await connection.exists("/clickhouse/tables/00000000-0000-0000-0000-200000000001")
    for client in clients:
        assert client.mock_calls == [
            mock.call.execute(b"SET receive_timeout=30", session_id=mock.ANY),
            mock.call.execute(b"SYSTEM RESTART REPLICA `db-one`.`table-uno`", session_id=mock.ANY, timeout=30),
            mock.call.execute(b"SET receive_timeout=30", session_id=mock.ANY),
            mock.call.execute(b"SYSTEM RESTART REPLICA `db-two`.`table-eins`", session_id=mock.ANY, timeout=30),
            mock.call.execute(b"SET receive_timeout=60", session_id=mock.ANY),
            mock.call.execute(b"SYSTEM RESTORE REPLICA `db-one`.`table-uno`", session_id=mock.ANY, timeout=60),
            mock.call.execute(b"SET receive_timeout=60", session_id=mock.ANY),
            mock.call.execute(b"SYSTEM RESTORE REPLICA `db-two`.`table-eins`", session_id=mock.ANY, timeout=60),
        ]
        check_each_pair_of_calls_has_the_same_session_id(client.mock_calls)


@pytest.mark.parametrize("with_stats", [False, True])
async def test_restore_object_storage_files(with_stats: bool) -> None:
    clickhouse_source_object_storage_files = SAMPLE_MANIFEST.object_storage_files[0].files
    object_storage_items = [
        ObjectStorageItem(key=file.path, last_modified=datetime.datetime(2020, 1, 2, tzinfo=datetime.UTC))
        for file in clickhouse_source_object_storage_files
    ]
    source_object_storage = MemoryObjectStorage.from_items(object_storage_items)
    target_object_storage = MemoryObjectStorage()
    source_disks = Disks(disks=[create_object_storage_disk("remote", source_object_storage)])
    target_disks = Disks(disks=[create_object_storage_disk("remote", target_object_storage)])
    step = RestoreObjectStorageFilesStep(source_disks=source_disks, target_disks=target_disks)
    stats = mock.Mock(spec_set=StatsClient)
    cluster = Cluster(
        nodes=[CoordinatorNode(url="node1"), CoordinatorNode(url="node2")], stats=stats if with_stats else None
    )
    context = StepsContext()
    context.set_result(ClickHouseManifestStep, SAMPLE_MANIFEST)

    await step.run_step(cluster, context)

    assert target_object_storage.list_items() == object_storage_items
    if with_stats:
        assert len(clickhouse_source_object_storage_files) == stats.gauge.call_count
        assert stats.gauge.call_args_list == [
            mock.call("astacus_copy_tiered_object_storage_files_remaining", 2, tags={"copy_method": "memory"}),
            mock.call("astacus_copy_tiered_object_storage_files_remaining", 1, tags={"copy_method": "memory"}),
            mock.call("astacus_copy_tiered_object_storage_files_remaining", 0, tags={"copy_method": "memory"}),
        ]


def _mock_copy_files_from(
    *,
    source: BaseTransfer[Any],
    keys: Collection[str],
    progress_fn: ObjectTransferProgressCallback | None = None,
) -> None:
    total_files = len(keys)
    for index, _ in enumerate(keys, start=1):
        if progress_fn is not None:
            progress_fn(index, total_files)


async def test_copy_items_between_with_direct_copy_method() -> None:
    stats = mock.Mock(spec_set=StatsClient)
    source_object_storage = mock.Mock(spec_set=BaseTransfer[Any])
    target_object_storage = mock.Mock(spec_set=BaseTransfer[Any])
    target_object_storage.copy_files_from = _mock_copy_files_from
    copy_items_between(
        keys=["key1", "key2"],
        copy_config=DirectCopyConfig(),
        source_storage=source_object_storage,
        target_storage=target_object_storage,
        stats=stats,
    )
    assert stats.gauge.call_args_list == [
        mock.call("astacus_copy_tiered_object_storage_files_remaining", 1, tags={"copy_method": "direct"}),
        mock.call("astacus_copy_tiered_object_storage_files_remaining", 0, tags={"copy_method": "direct"}),
    ]


async def test_copy_items_between_with_local_copy_method() -> None:
    stats = mock.Mock(spec_set=StatsClient)
    source_object_storage = mock.Mock(spec_set=BaseTransfer[Any])
    target_object_storage = mock.Mock(spec_set=BaseTransfer[Any])
    with patch.object(tempfile, "TemporaryFile") as mock_tempfile:
        copy_items_between(
            keys=["key1", "key2"],
            copy_config=LocalCopyConfig(temporary_directory="tmp"),
            source_storage=source_object_storage,
            target_storage=target_object_storage,
            stats=stats,
        )
    assert stats.gauge.call_args_list == [
        mock.call("astacus_copy_tiered_object_storage_files_remaining", 1, tags={"copy_method": "local"}),
        mock.call("astacus_copy_tiered_object_storage_files_remaining", 0, tags={"copy_method": "local"}),
    ]
    assert mock_tempfile.call_args_list == [mock.call(dir="tmp"), mock.call(dir="tmp")]


async def test_restore_object_storage_files_does_nothing_if_storages_have_same_config() -> None:
    same_object_storage = mock.Mock(spec_set=ObjectStorage)
    source_disks = Disks(disks=[create_object_storage_disk("remote", same_object_storage)])
    target_disks = Disks(disks=[create_object_storage_disk("remote", same_object_storage)])
    step = RestoreObjectStorageFilesStep(source_disks=source_disks, target_disks=target_disks)
    cluster = Cluster(nodes=[CoordinatorNode(url="node1"), CoordinatorNode(url="node2")])
    context = StepsContext()
    context.set_result(ClickHouseManifestStep, SAMPLE_MANIFEST)
    await step.run_step(cluster, context)
    same_object_storage.copy_items_from.assert_not_called()


async def test_restore_object_storage_files_fails_if_source_disk_has_no_object_storage_config() -> None:
    source_disks = Disks(disks=[create_object_storage_disk("remote", None)])
    target_disks = Disks(disks=[create_object_storage_disk("remote", MemoryObjectStorage())])
    step = RestoreObjectStorageFilesStep(source_disks=source_disks, target_disks=target_disks)
    cluster = Cluster(nodes=[CoordinatorNode(url="node1"), CoordinatorNode(url="node2")])
    context = StepsContext()
    context.set_result(ClickHouseManifestStep, SAMPLE_MANIFEST)
    with pytest.raises(StepFailedError, match="Source disk named 'remote' isn't configured as object storage"):
        await step.run_step(cluster, context)


async def test_restore_object_storage_files_fails_if_target_disk_has_no_object_storage_config() -> None:
    source_disks = Disks(disks=[create_object_storage_disk("remote", MemoryObjectStorage())])
    target_disks = Disks(disks=[create_object_storage_disk("remote", None)])
    step = RestoreObjectStorageFilesStep(source_disks=source_disks, target_disks=target_disks)
    cluster = Cluster(nodes=[CoordinatorNode(url="node1"), CoordinatorNode(url="node2")])
    context = StepsContext()
    context.set_result(ClickHouseManifestStep, SAMPLE_MANIFEST)
    with pytest.raises(StepFailedError, match="Target disk named 'remote' isn't configured as object storage"):
        await step.run_step(cluster, context)


async def test_restore_user_defined_functions_step() -> None:
    clickhouse_client = mock_clickhouse_client()
    clickhouse_client.execute.return_value = [["2"]]
    zk_client = FakeZooKeeperClient()
    context = StepsContext()
    context.set_result(ClickHouseManifestStep, SAMPLE_MANIFEST)
    step = RestoreUserDefinedFunctionsStep(
        zookeeper_client=zk_client,
        replicated_user_defined_zookeeper_path="/clickhouse/user_defined_functions/",
        clients=[clickhouse_client],
        sync_user_defined_functions_timeout=10.0,
    )
    await step.run_step(Cluster(nodes=[]), context)
    async with zk_client.connect() as connection:
        for user_defined_function in SAMPLE_USER_DEFINED_FUNCTIONS:
            assert (
                await connection.get(f"/clickhouse/user_defined_functions/{user_defined_function.path}")
                == user_defined_function.create_query
            )
    assert clickhouse_client.mock_calls == [
        mock.call.execute(b"SELECT count(*) FROM system.functions WHERE origin = 'SQLUserDefined'")
    ]


async def test_restore_keeper_map_table_data_step() -> None:
    clickhouse_client = mock_clickhouse_client()
    clickhouse_client.execute.return_value = [["1"]]
    zk_client = FakeZooKeeperClient()
    context = StepsContext()
    context.set_result(ClickHouseManifestStep, SAMPLE_MANIFEST)
    step = RestoreKeeperMapTableDataStep(
        zookeeper_client=zk_client,
        keeper_map_path_prefix="/clickhouse/keeper_map_table/",
        clients=[clickhouse_client],
        sync_keeper_map_data_timeout=10.0,
    )
    async with zk_client.connect() as connection:
        await connection.create("/clickhouse/keeper_map_table/name1", b"")
        await connection.create("/clickhouse/keeper_map_table/name1/data", b"")
        await connection.create("/clickhouse/keeper_map_table/name2", b"")
        await connection.create("/clickhouse/keeper_map_table/name2/data", b"")
    await step.run_step(Cluster(nodes=[]), context)
    async with zk_client.connect() as connection:
        assert (await connection.get_children_with_data("/clickhouse/keeper_map_table/name1/data/")) == {
            "key1": b"value1",
            "key2": b"value2",
        }
        assert (await connection.get_children_with_data("/clickhouse/keeper_map_table/name2/data/")) == {
            "key3": b"value3",
        }
    assert clickhouse_client.mock_calls == [
        mock.call.execute(
            b"SELECT count(*) FROM system.zookeeper WHERE path = '/clickhouse/keeper_map_table/name2/data' AND name = 'key3'"
        ),
    ]


@pytest.mark.parametrize(
    ("step_class", "expected_statements"),
    [
        (
            KeeperMapTablesReadOnlyStep,
            [b"ALTER TABLE `db-two`.`table-keeper` MODIFY SETTING read_only=true"],
        ),
        (
            KeeperMapTablesReadWriteStep,
            [b"ALTER TABLE `db-two`.`table-keeper` MODIFY SETTING read_only=false"],
        ),
    ],
    ids=["read-only", "read-write"],
)
async def test_keeper_map_table_read_only_setting(
    step_class: type[KeeperMapTablesReadabilityStepBase], expected_statements: list[bytes]
) -> None:
    clickhouse_client = mock_clickhouse_client()
    sample_tables = SAMPLE_TABLES + [
        Table(
            database=b"db-two",
            name=b"table-keeper",
            uuid=uuid.UUID("00000000-0000-0000-0000-200000000008"),
            engine="KeeperMap",
            create_query=b"CREATE TABLE db-two.table-keeper ...",
        ),
    ]
    context = StepsContext()
    context.set_result(RetrieveDatabasesAndTablesStep, (SAMPLE_DATABASES, sample_tables))
    step = step_class(clients=[clickhouse_client])
    await step.run_step(Cluster(nodes=[]), context)
    mock_calls = clickhouse_client.mock_calls
    assert mock_calls == [mock.call.execute(statement) for statement in expected_statements]


async def test_attaches_all_mergetree_parts_in_manifest() -> None:
    client_1 = mock_clickhouse_client()
    client_2 = mock_clickhouse_client()
    clients = [client_1, client_2]
    step = AttachMergeTreePartsStep(clients, disks=Disks(), attach_timeout=60, max_concurrent_attach_per_node=10)

    cluster = Cluster(nodes=[CoordinatorNode(url="node1"), CoordinatorNode(url="node2")])
    context = StepsContext()
    first_table_uuid = SAMPLE_TABLES[0].uuid
    second_table_uuid = SAMPLE_TABLES[1].uuid
    context.set_result(
        BackupManifestStep,
        BackupManifest(
            start=datetime.datetime(2020, 1, 2, 3, tzinfo=datetime.UTC),
            attempt=1,
            snapshot_results=[
                SnapshotResult(
                    state=SnapshotState(
                        root_globs=["dont", "care"],
                        files=[
                            SnapshotFile(
                                relative_path=f"store/000/{first_table_uuid}/detached/all_0_0_0/data.bin",
                                file_size=0,
                                mtime_ns=0,
                            ),
                            SnapshotFile(
                                relative_path=f"store/000/{second_table_uuid}/detached/all_1_1_0/data.bin",
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
                                relative_path=f"store/000/{first_table_uuid}/detached/all_0_0_0/data.bin",
                                file_size=0,
                                mtime_ns=0,
                            ),
                            SnapshotFile(
                                relative_path=f"store/000/{second_table_uuid}/detached/all_1_1_1/data.bin",
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
    context.set_result(ClickHouseManifestStep, SAMPLE_MANIFEST_V1)
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


async def test_sync_replicas_for_replicated_mergetree_tables() -> None:
    clients = [mock_clickhouse_client(), mock_clickhouse_client()]
    step = SyncTableReplicasStep(clients, sync_timeout=180, max_concurrent_sync_per_node=10)
    cluster = Cluster(nodes=[CoordinatorNode(url="node1"), CoordinatorNode(url="node2")])
    context = StepsContext()
    context.set_result(ClickHouseManifestStep, SAMPLE_MANIFEST_V1)
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


async def test_delete_object_storage_files_step(tmp_path: Path) -> None:
    object_storage = MemoryObjectStorage.from_items(
        [
            ObjectStorageItem(key="not_used/and_old", last_modified=datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC)),
            ObjectStorageItem(key="abc/defghi", last_modified=datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC)),
            ObjectStorageItem(key="jkl/mnopqr", last_modified=datetime.datetime(2020, 1, 2, tzinfo=datetime.UTC)),
            ObjectStorageItem(key="stu/vwxyza", last_modified=datetime.datetime(2020, 1, 3, tzinfo=datetime.UTC)),
            ObjectStorageItem(key="not_used/and_new", last_modified=datetime.datetime(2020, 1, 4, tzinfo=datetime.UTC)),
        ]
    )
    manifests = [
        BackupManifest(
            start=datetime.datetime(2020, 1, 2, 10, tzinfo=datetime.UTC),
            end=datetime.datetime(2020, 1, 2, 11, tzinfo=datetime.UTC),
            attempt=1,
            snapshot_results=[],
            upload_results=[],
            plugin=Plugin.clickhouse,
            plugin_data=ClickHouseManifest(
                version=ClickHouseBackupVersion.V2,
                object_storage_files=[
                    ClickHouseObjectStorageFiles(
                        disk_name="remote",
                        files=[
                            ClickHouseObjectStorageFile(path="abc/defghi"),
                            ClickHouseObjectStorageFile(path="jkl/mnopqr"),
                        ],
                    )
                ],
            ).to_plugin_data(),
            filename="backup-2",
        ),
        BackupManifest(
            start=datetime.datetime(2020, 1, 3, 10, tzinfo=datetime.UTC),
            end=datetime.datetime(2020, 1, 3, 11, tzinfo=datetime.UTC),
            attempt=1,
            snapshot_results=[],
            upload_results=[],
            plugin=Plugin.clickhouse,
            plugin_data=ClickHouseManifest(
                version=ClickHouseBackupVersion.V2,
                object_storage_files=[
                    ClickHouseObjectStorageFiles(
                        disk_name="remote",
                        files=[
                            ClickHouseObjectStorageFile(path="jkl/mnopqr"),
                            ClickHouseObjectStorageFile(path="stu/vwxyza"),
                        ],
                    )
                ],
            ).to_plugin_data(),
            filename="backup-3",
        ),
    ]
    async_json_storage = AsyncJsonStorage(
        storage=MemoryJsonStorage(items={b.filename: msgspec.json.encode(b) for b in manifests})
    )
    disks = Disks(disks=[create_object_storage_disk("remote", object_storage)])
    step = DeleteDanglingObjectStorageFilesStep(disks=disks, json_storage=async_json_storage.storage)
    cluster = Cluster(nodes=[CoordinatorNode(url="node1"), CoordinatorNode(url="node2")])
    context = StepsContext()
    context.set_result(ComputeKeptBackupsStep, [ManifestMin.from_manifest(b) for b in manifests])
    await step.run_step(cluster, context)
    assert object_storage.list_items() == [
        # Only not_used/and_old was deleted
        ObjectStorageItem(key="abc/defghi", last_modified=datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC)),
        ObjectStorageItem(key="jkl/mnopqr", last_modified=datetime.datetime(2020, 1, 2, tzinfo=datetime.UTC)),
        ObjectStorageItem(key="stu/vwxyza", last_modified=datetime.datetime(2020, 1, 3, tzinfo=datetime.UTC)),
        ObjectStorageItem(key="not_used/and_new", last_modified=datetime.datetime(2020, 1, 4, tzinfo=datetime.UTC)),
    ]


async def test_get_versions_step() -> None:
    client_1 = mock_clickhouse_client()
    client_1.execute.return_value = [["23.8.6.1"]]
    client_2 = mock_clickhouse_client()
    client_2.execute.return_value = [["23.3.15.1"]]
    clients = [client_1, client_2]
    step = GetVersionsStep(clients)
    result = await step.run_step(Cluster(nodes=[]), StepsContext())
    assert result == [(23, 8), (23, 3)]


@pytest.mark.parametrize(
    ("versions", "called_node_indicies"),
    [
        ([(23, 8), (23, 12)], [0, 1]),
        ([(23, 3), (23, 8)], [0]),
        ([(23, 3), (22, 8)], [0]),
        ([(23, 8), (23, 3)], [1]),
    ],
)
async def test_run_partition_cmd_on_every_node(
    versions: Sequence[ClickHouseVersion], called_node_indicies: Sequence[int]
) -> None:
    def call_execute(client: ClickHouseClient) -> Iterable[Awaitable[None]]:
        async def call() -> None:
            await client.execute(b"blah")
            return None

        yield from (call(), call())

    clients = [mock_clickhouse_client() for _ in versions]
    await run_partition_cmd_on_every_node(versions, clients, call_execute, 1)
    for idx, client in enumerate(clients):
        if idx in called_node_indicies:
            client.execute.assert_called()
        else:
            client.execute.assert_not_called()


def create_object_storage_disk(name: str, object_storage: ObjectStorage | None) -> Disk:
    return Disk(
        type=DiskType.object_storage,
        name=name,
        path_parts=("disks", name),
        object_storage_factory=lambda: object_storage,
    )


@pytest.mark.parametrize(
    ("original_query", "rewritten_query"),
    [
        [
            b"CREATE VIEW FOO AS SELECT 1",
            b"CREATE VIEW FOO AS SELECT 1",
        ],
        [
            b"CREATE VIEW `CREATE MATERIALIZED VIEW` AS SELECT 1",
            b"CREATE VIEW `CREATE MATERIALIZED VIEW` AS SELECT 1",
        ],
        [
            b"CREATE MATERIALIZED VIEW FOO AS SELECT 1",
            b"ATTACH MATERIALIZED VIEW FOO AS SELECT 1",
        ],
        [
            b"CREATE MATERIALIZED VIEW `CREATE MATERIALIZED VIEW` AS SELECT 1",
            b"ATTACH MATERIALIZED VIEW `CREATE MATERIALIZED VIEW` AS SELECT 1",
        ],
    ],
)
def test_get_restore_table_query(original_query: bytes, rewritten_query: bytes):
    table = Table(
        database=b"db",
        name=b"table",
        uuid=uuid.UUID("00000000-0000-0000-0000-100000000001"),
        engine="Engine",
        create_query=original_query,
        dependencies=[],
    )
    assert get_restore_table_query(table) == rewritten_query


class TestWaitForConditionOnEveryNode:
    async def test_succeeds(self) -> None:
        client_1 = mock_clickhouse_client()
        client_2 = mock_clickhouse_client()
        clients = [client_1, client_2]
        for client in clients:
            client.execute.return_value = [["1"]]

        async def cond(client: ClickHouseClient) -> bool:
            return await client.execute(b"SELECT 1") == [["1"]]

        await wait_for_condition_on_every_node(clients, cond, "for select 1", 1, 0.5)
        for client in clients:
            assert client.mock_calls == [mock.call.execute(b"SELECT 1")]

    async def test_timeout(self) -> None:
        client = mock_clickhouse_client()
        client.execute.return_value = [["0"]]

        async def cond(client: ClickHouseClient) -> bool:
            return False

        with pytest.raises(StepFailedError, match="Timeout while waiting for for select 1"):
            await wait_for_condition_on_every_node([client], cond, "for select 1", 0.1, 0.05)


def test_collect_tiered_storage_results_step() -> None:
    context = StepsContext()
    context.set_result(
        CollectObjectStorageFilesStep,
        [
            ClickHouseObjectStorageFiles(
                disk_name="remote1",
                files=[ClickHouseObjectStorageFile(path=path) for path in ("abc", "def")],
                total_size_bytes=1000,
            ),
            ClickHouseObjectStorageFiles(
                disk_name="remote2",
                files=[ClickHouseObjectStorageFile(path=path) for path in ("ghi",)],
                total_size_bytes=2000,
            ),
        ],
    )
