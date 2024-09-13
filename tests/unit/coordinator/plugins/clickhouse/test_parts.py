"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""

from astacus.common.ipc import SnapshotFile, SnapshotResult, SnapshotState
from astacus.coordinator.plugins.clickhouse.disks import Disks
from astacus.coordinator.plugins.clickhouse.manifest import Table
from astacus.coordinator.plugins.clickhouse.parts import get_part_servers, list_parts_to_attach, Part, PartFile
from astacus.coordinator.plugins.clickhouse.replication import DatabaseReplica
from collections.abc import Sequence
from uuid import UUID

import dataclasses
import hashlib
import pytest

pytestmark = [pytest.mark.clickhouse]


def create_part_files(*, table_uuid: UUID, part_name: str, digest_seed: str) -> Sequence[SnapshotFile]:
    uuid_head = str(table_uuid)[:3]
    return [
        SnapshotFile(
            relative_path=f"store/{uuid_head}/{str(table_uuid)}/detached/{part_name}/{filename}",
            file_size=file_size,
            mtime_ns=0,
            hexdigest=hashlib.sha256(f"{digest_seed}-{part_name}-{filename}".encode()).hexdigest(),
        )
        for filename, file_size in [("data.bin", 1000), ("checksums.txt", 100), ("count.txt", 20)]
    ]


def create_part_from_part_files(snapshot_files: Sequence[SnapshotFile]) -> Part:
    return Part(
        table_uuid=T1_UUID,
        part_name=b"all_0_0_0",
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


def test_get_part_servers_succeeds_on_valid_parts() -> None:
    get_part_servers([PartFile(snapshot_file=file, servers={0, 1}) for file in TABLE_1_PART_1])
    get_part_servers([PartFile(snapshot_file=file, servers={0}) for file in TABLE_3_PART_1B])


def test_get_part_servers_fails_on_inconsistent_servers_set() -> None:
    part_files = [PartFile(snapshot_file=file, servers={0, 1}) for file in TABLE_1_PART_1]
    part_files[1] = dataclasses.replace(part_files[1], servers={0, 1, 2})
    with pytest.raises(ValueError):
        get_part_servers(part_files)


def test_list_parts_to_attach() -> None:
    parts_to_attach = list_parts_to_attach(
        SnapshotResult(state=SnapshotState(files=[*TABLE_1_PART_1, *TABLE_1_PART_2])),
        Disks(),
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
