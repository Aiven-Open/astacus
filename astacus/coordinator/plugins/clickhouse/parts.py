"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details

Algorithms to help with redistributing parts across servers for tables using the
Replicated family of table engines.

This does not support shards, but this is the right place to add support for them.
"""
from .disks import Disks
from .manifest import Table
from astacus.common.ipc import SnapshotFile, SnapshotResult
from typing import AbstractSet, Iterable, Mapping, Optional, Sequence, Set, Tuple

import dataclasses
import uuid


@dataclasses.dataclass(frozen=True, slots=True)
class PartFile:
    snapshot_file: SnapshotFile
    servers: AbstractSet[int]


@dataclasses.dataclass(frozen=True, slots=True)
class PartKey:
    table_uuid: uuid.UUID
    shard_name: str
    part_name: bytes


@dataclasses.dataclass(frozen=True, slots=True)
class Part:
    table_uuid: uuid.UUID
    part_name: bytes
    servers: AbstractSet[int]
    snapshot_files: Sequence[SnapshotFile]
    total_size: int


def get_part_servers(part_files: Iterable[PartFile]) -> AbstractSet[int]:
    """
    Return the list of server indices where the part made of all these files is present.

    Raises a ValueError if not all servers contain all files.
    """
    part_servers: Optional[AbstractSet[int]] = None
    for part_file in part_files:
        if part_servers is None:
            part_servers = part_file.servers
        elif part_servers != part_file.servers:
            raise ValueError(
                f"Inconsistent part, not all files are identically replicated: "
                f"some files are on servers {part_servers} "
                f"while {part_file.snapshot_file.relative_path} is on servers {part_file.servers}"
            )
    assert part_servers is not None
    return part_servers


def list_parts_to_attach(
    snapshot_result: SnapshotResult,
    disks: Disks,
    tables_by_uuid: Mapping[uuid.UUID, Table],
) -> Sequence[Tuple[str, bytes]]:
    """
    Returns a list of table identifiers and part names to attach from the snapshot.
    """
    parts_to_attach: Set[Tuple[str, bytes]] = set()
    assert snapshot_result.state is not None
    for snapshot_file in snapshot_result.state.files:
        parsed_path = disks.parse_part_file_path(snapshot_file.relative_path)
        table = tables_by_uuid.get(parsed_path.table_uuid)
        if table is not None:
            parts_to_attach.add((table.escaped_sql_identifier, parsed_path.part_name))
    return sorted(parts_to_attach)
