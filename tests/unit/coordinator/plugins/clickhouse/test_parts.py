"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from astacus.common.ipc import SnapshotFile, SnapshotResult, SnapshotState
from astacus.coordinator.plugins.clickhouse.manifest import Table
from astacus.coordinator.plugins.clickhouse.parts import (
    add_file_to_parts,
    check_parts_replication,
    distribute_parts_to_servers,
    get_frozen_parts_pattern,
    group_files_into_parts,
    list_parts_to_attach,
    Part,
    PartFile,
    PartKey,
)
from pathlib import Path
from typing import Dict, List
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


def create_part_from_part_files(part_files: List[SnapshotFile]) -> Part:
    return Part(
        files={file.relative_path: PartFile(snapshot_file=file, servers={0, 1}) for file in part_files},
        total_size=sum(file.file_size for file in part_files),
    )


T1_UUID, T2_UUID, T3_UUID = [UUID(int=i) for i in range(1, 4)]
TABLE_1_PART_1 = create_part_files(table_uuid=T1_UUID, part_name="all_0_0_0", digest_seed="same")
TABLE_1_PART_2 = create_part_files(table_uuid=T1_UUID, part_name="all_1_1_0", digest_seed="same")
TABLE_1_PART_3 = create_part_files(table_uuid=T1_UUID, part_name="all_2_2_0", digest_seed="same")
TABLE_3_PART_1A = create_part_files(table_uuid=T3_UUID, part_name="all_0_0_0", digest_seed="one")
TABLE_3_PART_1B = create_part_files(table_uuid=T3_UUID, part_name="all_0_0_0", digest_seed="other")


def test_group_files_into_parts_collects_parts_from_selected_tables() -> None:
    first_server_files = copy.deepcopy([*TABLE_1_PART_1, *TABLE_1_PART_2, *TABLE_1_PART_3])
    second_server_files = copy.deepcopy([*TABLE_1_PART_1, *TABLE_1_PART_2, *TABLE_1_PART_3])
    parts, other_files = group_files_into_parts([first_server_files, second_server_files], {T1_UUID, T2_UUID})
    assert len(parts) == 3
    for part, original_files in zip(parts, [TABLE_1_PART_1, TABLE_1_PART_2, TABLE_1_PART_3]):
        assert sorted(part.files.keys()) == sorted([original_file.relative_path for original_file in original_files])
        for file_path, file in part.files.items():
            assert file_path == file.snapshot_file.relative_path
            assert file.servers == {0, 1}
    assert other_files[0] == []
    assert other_files[1] == []


def test_group_files_into_parts_ignores_parts_from_unselected_tables() -> None:
    first_server_files = copy.deepcopy([*TABLE_3_PART_1A])
    second_server_files = copy.deepcopy([*TABLE_3_PART_1B])
    parts, other_files = group_files_into_parts([first_server_files, second_server_files], {T1_UUID, T2_UUID})
    assert parts == []
    assert other_files[0] == TABLE_3_PART_1A
    assert other_files[1] == TABLE_3_PART_1B


def test_group_files_into_parts_ignores_unknown_files() -> None:
    first_server_files = [SnapshotFile(relative_path=Path("something/random_a"), file_size=100, mtime_ns=0)]
    second_server_files = [SnapshotFile(relative_path=Path("something/random_B"), file_size=100, mtime_ns=0)]
    parts, other_files = group_files_into_parts([first_server_files, second_server_files], {T1_UUID, T2_UUID})
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
    parts: Dict[PartKey, Part] = {}
    added = add_file_to_parts(snapshot_file=snapshot_file, server_index=0, table_uuids={T1_UUID}, parts=parts)
    assert added is False
    assert len(parts) == 0


def test_add_file_to_parts_adds_file() -> None:
    file_a = SnapshotFile(
        relative_path=Path("store/000/00000000-0000-0000-0000-000000000001/detached/all_0_0_0/data.bin"),
        file_size=100,
        mtime_ns=0,
        hexdigest="0001",
    )
    parts = {}
    added = add_file_to_parts(snapshot_file=file_a, server_index=0, table_uuids={T1_UUID}, parts=parts)
    assert added is True
    part = parts[PartKey(table_uuid=T1_UUID, part_name="all_0_0_0")]
    assert part == Part(files={file_a.relative_path: PartFile(snapshot_file=file_a, servers={0})}, total_size=100)


def test_add_file_to_parts_collects_all_servers() -> None:
    file_a = SnapshotFile(
        relative_path=Path("store/000/00000000-0000-0000-0000-000000000001/detached/all_0_0_0/data.bin"),
        file_size=100,
        mtime_ns=0,
        hexdigest="0001",
    )
    parts = {}
    for server_index in range(3):
        add_file_to_parts(snapshot_file=file_a, server_index=server_index, table_uuids={T1_UUID}, parts=parts)
    part = parts[PartKey(table_uuid=T1_UUID, part_name="all_0_0_0")]
    part_file = part.files[file_a.relative_path]
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
    add_file_to_parts(snapshot_file=file_a, server_index=0, table_uuids={T1_UUID}, parts=parts)
    with pytest.raises(ValueError):
        add_file_to_parts(snapshot_file=file_b, server_index=1, table_uuids={T1_UUID}, parts=parts)


def test_add_file_to_parts_ignores_inconsistent_modification_time() -> None:
    file_a = SnapshotFile(
        relative_path=Path("store/000/00000000-0000-0000-0000-000000000001/detached/all_0_0_0/data.bin"),
        file_size=100,
        mtime_ns=123456,
        hexdigest="0001",
    )
    file_b = file_a.copy(update={"mtime_ns": 789789})
    parts = {}
    add_file_to_parts(snapshot_file=file_a, server_index=0, table_uuids={T1_UUID}, parts=parts)
    added = add_file_to_parts(snapshot_file=file_b, server_index=1, table_uuids={T1_UUID}, parts=parts)
    assert added is True


def test_add_file_to_parts_computes_total_part_size() -> None:
    file_a = SnapshotFile(
        relative_path=Path("store/000/00000000-0000-0000-0000-000000000001/detached/all_0_0_0/data.bin"),
        file_size=1000,
        mtime_ns=0,
        hexdigest="0001",
    )
    file_b = SnapshotFile(
        relative_path=Path("store/000/00000000-0000-0000-0000-000000000001/detached/all_0_0_0/checksums.txt"),
        file_size=138,
        mtime_ns=0,
        hexdigest="0002",
    )
    parts = {}
    for server_index in range(3):
        add_file_to_parts(snapshot_file=file_a, server_index=server_index, table_uuids={T1_UUID}, parts=parts)
        add_file_to_parts(snapshot_file=file_b, server_index=server_index, table_uuids={T1_UUID}, parts=parts)
    # The total size was not tripled because of the multiple server
    assert parts[PartKey(table_uuid=T1_UUID, part_name="all_0_0_0")].total_size == 1138


def test_check_parts_replication_succeeds_on_valid_parts() -> None:
    parts = [
        Part(
            files={file.relative_path: PartFile(snapshot_file=file, servers={0, 1}) for file in TABLE_1_PART_1},
            total_size=1120,
        ),
        Part(
            files={file.relative_path: PartFile(snapshot_file=file, servers={0}) for file in TABLE_3_PART_1B},
            total_size=1120,
        ),
    ]
    check_parts_replication(parts)


def test_check_parts_replication_fails_on_inconsistent_servers_set() -> None:
    parts = [
        Part(
            files={file.relative_path: PartFile(snapshot_file=file, servers={0, 1}) for file in TABLE_1_PART_1},
            total_size=1120,
        )
    ]
    list(parts[0].files.values())[1].servers = {0, 1, 2}
    with pytest.raises(ValueError):
        check_parts_replication(parts)


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
