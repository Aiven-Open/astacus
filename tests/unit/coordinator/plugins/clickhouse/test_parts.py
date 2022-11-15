"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from astacus.common.ipc import SnapshotFile, SnapshotResult, SnapshotState
from astacus.coordinator.plugins.clickhouse.manifest import Table
from astacus.coordinator.plugins.clickhouse.parts import (
    add_file_to_parts,
    distribute_parts_to_servers,
    get_frozen_parts_pattern,
    get_part_servers,
    group_files_into_parts,
    list_parts_to_attach,
    Part,
    PartFile,
    PartKey,
)
from astacus.coordinator.plugins.clickhouse.replication import DatabaseReplica
from pathlib import Path
from typing import List
from uuid import UUID

import copy
import hashlib
import pytest

pytestmark = [pytest.mark.clickhouse]


def create_part_files(*, table_uuid: UUID, part_name: str, digest_seed: str) -> List[SnapshotFile]:
    uuid_head = str(table_uuid)[:3]
    return [
        SnapshotFile(
            relative_path=Path(f"store/{uuid_head}/{str(table_uuid)}/detached/{part_name}/{filename}"),
            file_size=file_size,
            mtime_ns=0,
            hexdigest=hashlib.sha256(f"{digest_seed}-{part_name}-{filename}".encode()).hexdigest(),
        )
        for filename, file_size in [("data.bin", 1000), ("checksums.txt", 100), ("count.txt", 20)]
    ]


def create_part_from_part_files(snapshot_files: List[SnapshotFile]) -> Part:
    return Part(
        table_uuid=T1_UUID,
        part_name="all_0_0_0",
        servers={0, 1},
        snapshot_files=snapshot_files,
        total_size=sum(snapshot_file.file_size for snapshot_file in snapshot_files),
    )


T1_UUID, T2_UUID, T3_UUID = [UUID(int=i) for i in range(1, 4)]
TABLE_1_PART_1 = create_part_files(table_uuid=T1_UUID, part_name="all_0_0_0", digest_seed="same")
TABLE_1_PART_2 = create_part_files(table_uuid=T1_UUID, part_name="all_1_1_0", digest_seed="same")
TABLE_1_PART_3 = create_part_files(table_uuid=T1_UUID, part_name="all_2_2_0", digest_seed="same")
TABLE_3_PART_1A = create_part_files(table_uuid=T3_UUID, part_name="all_0_0_0", digest_seed="one")
TABLE_3_PART_1B = create_part_files(table_uuid=T3_UUID, part_name="all_0_0_0", digest_seed="other")
DATABASES_REPLICAS = [
    DatabaseReplica(shard_name="shard_1", replica_name="node_1"),
    DatabaseReplica(shard_name="shard_1", replica_name="node_2"),
]
SHARDED_DATABASE_REPLICAS = [
    DatabaseReplica(shard_name="shard_1", replica_name="node_1"),
    DatabaseReplica(shard_name="shard_2", replica_name="node_2"),
]


def test_group_files_into_parts_collects_parts_from_selected_tables() -> None:
    first_server_files = copy.deepcopy([*TABLE_1_PART_1, *TABLE_1_PART_2, *TABLE_1_PART_3])
    second_server_files = copy.deepcopy([*TABLE_1_PART_1, *TABLE_1_PART_2, *TABLE_1_PART_3])
    tables_replicas = {T1_UUID: DATABASES_REPLICAS, T2_UUID: DATABASES_REPLICAS}
    parts, other_files = group_files_into_parts([first_server_files, second_server_files], tables_replicas)
    assert len(parts) == 3
    for part, original_files in zip(parts, [TABLE_1_PART_1, TABLE_1_PART_2, TABLE_1_PART_3]):
        assert part.snapshot_files == original_files
        assert part.total_size == 1120
        assert part.servers == {0, 1}
    assert other_files == [[], []]


def test_group_files_into_parts_does_not_merge_unreplicated_parts_within_shard() -> None:
    first_server_files = copy.deepcopy(TABLE_1_PART_1)
    second_server_files = copy.deepcopy(TABLE_1_PART_2)
    tables_replicas = {T1_UUID: DATABASES_REPLICAS, T2_UUID: DATABASES_REPLICAS}
    parts, other_files = group_files_into_parts([first_server_files, second_server_files], tables_replicas)
    server_1_parts = [part for part in parts if part.servers == {0}]
    server_2_parts = [part for part in parts if part.servers == {1}]
    assert len(server_1_parts) == 1
    assert server_1_parts[0].snapshot_files == TABLE_1_PART_1
    assert len(server_2_parts) == 1
    assert server_2_parts[0].snapshot_files == TABLE_1_PART_2
    assert other_files == [[], []]


def test_group_files_into_parts_does_not_merge_parts_across_shards() -> None:
    first_server_files = copy.deepcopy([*TABLE_1_PART_1, *TABLE_1_PART_2, *TABLE_1_PART_3])
    second_server_files = copy.deepcopy([*TABLE_1_PART_1, *TABLE_1_PART_2, *TABLE_1_PART_3])
    tables_replicas = {T1_UUID: SHARDED_DATABASE_REPLICAS, T2_UUID: SHARDED_DATABASE_REPLICAS}
    parts, other_files = group_files_into_parts([first_server_files, second_server_files], tables_replicas)
    # Because each server is in a different shard, despite having the same data
    # we have twice as many part as the test where all servers were in the
    # same shard.
    server_1_parts = [part for part in parts if part.servers == {0}]
    server_2_parts = [part for part in parts if part.servers == {1}]
    assert len(server_1_parts) == 3
    assert len(server_2_parts) == 3
    for part, original_files in zip(server_1_parts, [TABLE_1_PART_1, TABLE_1_PART_2, TABLE_1_PART_3]):
        assert part.snapshot_files == original_files
    for part, original_files in zip(server_2_parts, [TABLE_1_PART_1, TABLE_1_PART_2, TABLE_1_PART_3]):
        assert part.snapshot_files == original_files
    assert other_files[0] == []
    assert other_files[1] == []


def test_group_files_into_parts_ignores_parts_from_unselected_tables() -> None:
    first_server_files = copy.deepcopy([*TABLE_3_PART_1A])
    second_server_files = copy.deepcopy([*TABLE_3_PART_1B])
    tables_replicas = {T1_UUID: DATABASES_REPLICAS, T2_UUID: DATABASES_REPLICAS}
    parts, other_files = group_files_into_parts([first_server_files, second_server_files], tables_replicas)
    assert parts == []
    assert other_files[0] == TABLE_3_PART_1A
    assert other_files[1] == TABLE_3_PART_1B


def test_group_files_into_parts_ignores_unknown_files() -> None:
    first_server_files = [SnapshotFile(relative_path=Path("something/random_a"), file_size=100, mtime_ns=0)]
    second_server_files = [SnapshotFile(relative_path=Path("something/random_B"), file_size=100, mtime_ns=0)]
    tables_replicas = {T1_UUID: DATABASES_REPLICAS, T2_UUID: DATABASES_REPLICAS}
    parts, other_files = group_files_into_parts([first_server_files, second_server_files], tables_replicas)
    assert parts == []
    assert other_files[0] == first_server_files
    assert other_files[1] == second_server_files


@pytest.mark.parametrize(
    "file_path",
    [
        Path("something/random_a"),
        Path("notstore/000/00000000-0000-0000-0000-000000000001/detached/all_0_0_0/data.bin"),
        Path("store/123/00000000-0000-0000-0000-000000000001/detached/all_0_0_0/data.bin"),
        Path("store/000/00000000-0000-0bad0-0000-000000000002/detached/all_0_0_0/data.bin"),
        Path("store/000/00000000-0000-0000-0000-000000000002/detached/all_0_0_0/data.bin"),
        Path("store/000/00000000-0000-0000-0000-000000000001/notdetached/all_0_0_0/data.bin"),
        Path("store/000/00000000-0000-0000-0000-000000000001/detached/notapart.txt"),
    ],
    ids=["random", "not-store", "wrong-uuid-prefix", "invalid-uuid", "wrong-uuid", "not-detached", "not-folder"],
)
def test_add_file_to_parts_ignores_unknown_files(file_path: Path) -> None:
    snapshot_file = SnapshotFile(relative_path=file_path, file_size=100, mtime_ns=0)
    parts_files = {}
    tables_replicas = {T1_UUID: [DatabaseReplica(shard_name="shard_1", replica_name="node_1")]}
    added = add_file_to_parts(
        snapshot_file=snapshot_file, server_index=0, tables_replicas=tables_replicas, parts_files=parts_files
    )
    assert added is False
    assert len(parts_files) == 0


def test_add_file_to_parts_adds_file() -> None:
    file_a = SnapshotFile(
        relative_path=Path("store/000/00000000-0000-0000-0000-000000000001/detached/all_0_0_0/data.bin"),
        file_size=100,
        mtime_ns=0,
        hexdigest="0001",
    )
    parts_files = {}
    tables_replicas = {T1_UUID: [DatabaseReplica(shard_name="shard_1", replica_name="node_1")]}
    added = add_file_to_parts(snapshot_file=file_a, server_index=0, tables_replicas=tables_replicas, parts_files=parts_files)
    assert added is True
    part_files = parts_files[PartKey(table_uuid=T1_UUID, shard_name="shard_1", part_name="all_0_0_0")]
    assert part_files == {file_a.relative_path: PartFile(snapshot_file=file_a, servers={0})}


def test_add_file_to_parts_collects_all_servers() -> None:
    file_a = SnapshotFile(
        relative_path=Path("store/000/00000000-0000-0000-0000-000000000001/detached/all_0_0_0/data.bin"),
        file_size=100,
        mtime_ns=0,
        hexdigest="0001",
    )
    parts = {}
    tables_replicas = {
        T1_UUID: [
            DatabaseReplica(shard_name="shard_1", replica_name="node_1"),
            DatabaseReplica(shard_name="shard_1", replica_name="node_2"),
            DatabaseReplica(shard_name="shard_1", replica_name="node_3"),
        ]
    }
    for server_index in range(3):
        add_file_to_parts(
            snapshot_file=file_a, server_index=server_index, tables_replicas=tables_replicas, parts_files=parts
        )
    part_files = parts[PartKey(table_uuid=T1_UUID, shard_name="shard_1", part_name="all_0_0_0")]
    part_file = part_files[file_a.relative_path]
    assert part_file.servers == {0, 1, 2}


def test_add_file_to_parts_fails_on_inconsistent_file() -> None:
    # Same filename from two different servers but different hash
    file_a = SnapshotFile(
        relative_path=Path("store/000/00000000-0000-0000-0000-000000000001/detached/all_0_0_0/data.bin"),
        file_size=100,
        mtime_ns=0,
        hexdigest="0001",
    )
    file_b = file_a.copy(update={"hexdigest": "0002"})
    parts = {}
    tables_replicas = {T1_UUID: DATABASES_REPLICAS}
    add_file_to_parts(snapshot_file=file_a, server_index=0, tables_replicas=tables_replicas, parts_files=parts)
    with pytest.raises(ValueError):
        add_file_to_parts(snapshot_file=file_b, server_index=1, tables_replicas=tables_replicas, parts_files=parts)


def test_add_file_to_parts_ignores_inconsistent_modification_time() -> None:
    file_a = SnapshotFile(
        relative_path=Path("store/000/00000000-0000-0000-0000-000000000001/detached/all_0_0_0/data.bin"),
        file_size=100,
        mtime_ns=123456,
        hexdigest="0001",
    )
    file_b = file_a.copy(update={"mtime_ns": 789789})
    parts = {}
    tables_replicas = {T1_UUID: DATABASES_REPLICAS}
    add_file_to_parts(snapshot_file=file_a, server_index=0, tables_replicas=tables_replicas, parts_files=parts)
    added = add_file_to_parts(snapshot_file=file_b, server_index=1, tables_replicas=tables_replicas, parts_files=parts)
    assert added is True


def test_get_part_servers_succeeds_on_valid_parts() -> None:
    get_part_servers([PartFile(snapshot_file=file, servers={0, 1}) for file in TABLE_1_PART_1])
    get_part_servers([PartFile(snapshot_file=file, servers={0}) for file in TABLE_3_PART_1B])


def test_get_part_servers_fails_on_inconsistent_servers_set() -> None:
    part_files = [PartFile(snapshot_file=file, servers={0, 1}) for file in TABLE_1_PART_1]
    part_files[1].servers = {0, 1, 2}
    with pytest.raises(ValueError):
        get_part_servers(part_files)


def test_distribute_parts_to_servers_balances_parts() -> None:
    table_parts = [TABLE_1_PART_1, TABLE_1_PART_2, TABLE_1_PART_3]
    parts = [create_part_from_part_files(part_files) for part_files in table_parts]
    server_files: List[List[SnapshotFile]] = [[], []]
    distribute_parts_to_servers(parts, server_files)
    for part, server_index in zip(table_parts, (0, 1, 0)):
        for part_file in part:
            assert part_file in server_files[server_index]


def test_distribute_parts_to_servers_balances_part_sizes() -> None:
    large_part_3 = copy.deepcopy(TABLE_1_PART_3)
    large_part_3[0].file_size *= 2
    table_parts = [TABLE_1_PART_1, TABLE_1_PART_2, large_part_3]
    parts = [create_part_from_part_files(part_files) for part_files in table_parts]
    server_files: List[List[SnapshotFile]] = [[], []]
    distribute_parts_to_servers(parts, server_files)
    for part, server_index in zip(table_parts, (1, 1, 0)):
        for part_file in part:
            assert part_file in server_files[server_index]


def test_get_frozen_parts_pattern_escapes_backup_name() -> None:
    assert get_frozen_parts_pattern("something+stra/../nge") == "shadow/something%2Bstra%2F%2E%2E%2Fnge/store/**/*"


def test_list_parts_to_attach() -> None:
    parts_to_attach = list_parts_to_attach(
        SnapshotResult(state=SnapshotState(files=TABLE_1_PART_1 + TABLE_1_PART_2)),
        {
            T1_UUID: Table(
                database=b"default",
                name=b"table_1",
                engine="ReplicatedMergeTree",
                uuid=T1_UUID,
                create_query=b"CREATE TABLE ...",
            ),
            T2_UUID: Table(
                database=b"default",
                name=b"table_2",
                engine="ReplicatedMergeTree",
                uuid=T2_UUID,
                create_query=b"CREATE TABLE ...",
            ),
        },
    )
    assert parts_to_attach == [
        ("`default`.`table_1`", b"all_0_0_0"),
        ("`default`.`table_1`", b"all_1_1_0"),
    ]
