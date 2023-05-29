"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details

Algorithms to help with redistributing parts across servers for tables using the
Replicated family of table engines.

This does not support shards, but this is the right place to add support for them.
"""
from .disks import DiskPaths
from .escaping import escape_for_file_name, unescape_from_file_name
from .manifest import Table
from .replication import DatabaseReplica
from astacus.common.ipc import SnapshotFile, SnapshotResult
from pathlib import Path
from typing import AbstractSet, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

import dataclasses
import re
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


def group_files_into_parts(
    snapshot_files: List[List[SnapshotFile]],
    tables_replicas: Mapping[uuid.UUID, Sequence[DatabaseReplica]],
) -> Tuple[List[Part], List[List[SnapshotFile]]]:
    """
    Regroup all files that form a MergeTree table part together in a `Part`.

    Only parts from the provided map of `tables_replicas` are regrouped.

    Returns the list of `Part` and a separate list of list of `SnapshotFile` that
    were not selected to make a `Part`.

    The input and output list of lists will have the same length: the number
    of server in the cluster (the first list is for the first server, etc.)
    """
    other_files: List[List[SnapshotFile]] = [[] for _ in snapshot_files]
    keyed_parts: Dict[PartKey, Dict[Path, PartFile]] = {}
    for server_index, server_files in enumerate(snapshot_files):
        for snapshot_file in server_files:
            if not add_file_to_parts(snapshot_file, server_index, tables_replicas, keyed_parts):
                other_files[server_index].append(snapshot_file)
    parts: list[Part] = []
    for part_key, part_files in keyed_parts.items():
        part_servers = get_part_servers(part_files.values())
        part_snapshot_files = [part_file.snapshot_file for part_file in part_files.values()]
        parts.append(
            Part(
                table_uuid=part_key.table_uuid,
                part_name=part_key.part_name,
                servers=part_servers,
                snapshot_files=part_snapshot_files,
                total_size=sum(snapshot_file.file_size for snapshot_file in part_snapshot_files),
            )
        )
    return parts, other_files


def add_file_to_parts(
    snapshot_file: SnapshotFile,
    server_index: int,
    tables_replicas: Mapping[uuid.UUID, Sequence[DatabaseReplica]],
    parts_files: Dict[PartKey, Dict[Path, PartFile]],
) -> bool:
    """
    If the `snapshot_file` is a file from a part of one of the tables listed in
    `replicated_tables`, add it to the corresponding dictionary in `parts_files`.

    A file is from a part if its path starts with
    "store/3_first_char_of_table_uuid/table_uuid/detached/part_name".

    If a file already exists in a part, the `server_index` is added to the `server` set
    of the `PartFile` for that file.

    Raises a `ValueError` if a different file with the same name already exists in a
    part: a `PartFile` must be the identical on all servers where it is present.

    Returns `True` if and only if the file was added to a `Part`.
    """
    path_parts = snapshot_file.relative_path.parts
    has_enough_depth = len(path_parts) >= 6
    if not has_enough_depth:
        return False
    has_store_and_detached = path_parts[0] == "store" and path_parts[3] == "detached"
    has_uuid_prefix = path_parts[1] == path_parts[2][:3]
    has_valid_uuid = re.match(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", path_parts[2])
    if not (has_store_and_detached and has_uuid_prefix and has_valid_uuid):
        return False
    table_uuid = uuid.UUID(path_parts[2])
    if table_uuid not in tables_replicas:
        return False
    shard_name = tables_replicas[table_uuid][server_index].shard_name
    part_key = PartKey(table_uuid=table_uuid, shard_name=shard_name, part_name=unescape_from_file_name(path_parts[4]))
    part_files = parts_files.setdefault(part_key, {})
    part_file = part_files.get(snapshot_file.relative_path)
    if part_file is None:
        part_files[snapshot_file.relative_path] = PartFile(snapshot_file=snapshot_file, servers={server_index})
    elif part_file.snapshot_file.equals_excluding_mtime(snapshot_file):
        part_files[snapshot_file.relative_path] = dataclasses.replace(part_file, servers=part_file.servers | {server_index})
    else:
        raise ValueError(
            f"Inconsistent part file {snapshot_file.relative_path} of part {part_key} "
            f"between servers {part_file.servers} and server {server_index}:\n"
            f"    {part_file.snapshot_file}\n"
            f"    {snapshot_file}"
        )
    return True


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


def distribute_parts_to_servers(parts: Sequence[Part], server_files: List[List[SnapshotFile]]):
    """
    Distributes each part to only one of the multiple servers where the part was
    during the backup.

    Parts are distributed to each server such as the total download size for each
    server is roughly equal (using a greedy algorithm).
    """
    total_file_sizes = [0 for _ in server_files]
    for part in sorted(parts, key=lambda p: p.total_size, reverse=True):
        server_index = min(part.servers, key=total_file_sizes.__getitem__)
        server_files[server_index].extend(part.snapshot_files)
        total_file_sizes[server_index] += part.total_size


def get_frozen_parts_pattern(freeze_name: str) -> str:
    """
    Returns the glob pattern inside ClickHouse data dir where frozen table parts are stored.
    """
    escaped_freeze_name = escape_for_file_name(freeze_name.encode())
    return f"shadow/{escaped_freeze_name}/store/**/*"


def list_parts_to_attach(
    snapshot_result: SnapshotResult,
    disk_paths: DiskPaths,
    tables_by_uuid: Mapping[uuid.UUID, Table],
) -> Sequence[Tuple[str, bytes]]:
    """
    Returns a list of table identifiers and part names to attach from the snapshot.
    """
    parts_to_attach: Set[Tuple[str, bytes]] = set()
    assert snapshot_result.state is not None
    for snapshot_file in snapshot_result.state.files:
        parsed_path = disk_paths.parse_part_file_path(snapshot_file.relative_path)
        table = tables_by_uuid.get(parsed_path.table_uuid)
        if table is not None:
            parts_to_attach.add((table.escaped_sql_identifier, parsed_path.part_name))
    return sorted(parts_to_attach)
