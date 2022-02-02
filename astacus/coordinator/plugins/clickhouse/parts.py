"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details

Algorithms to help with redistributing parts across servers for tables using the
Replicated family of table engines.

This does not support shards, but this is the right place to add support for them.
"""
from astacus.common.ipc import SnapshotFile, SnapshotResult
from astacus.coordinator.plugins.clickhouse.escaping import escape_for_file_name, unescape_from_file_name
from astacus.coordinator.plugins.clickhouse.manifest import Table
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

import dataclasses
import re
import uuid


@dataclasses.dataclass
class PartFile:
    snapshot_file: SnapshotFile
    servers: Set[int]


@dataclasses.dataclass
class Part:
    files: Dict[Path, PartFile]
    total_size: int


@dataclasses.dataclass(frozen=True)
class PartKey:
    table_uuid: uuid.UUID
    part_name: str


def group_files_into_parts(snapshot_files: List[List[SnapshotFile]],
                           table_uuids: Set[uuid.UUID]) -> Tuple[List[Part], List[List[SnapshotFile]]]:
    """
    Regroup all files that form a MergeTree table parts together in a `Part`.

    Only parts from the provided list of `table_uuids` are regrouped.

    Returns the list of `Part` and a separate list of list of `SnapshotFile` that
    were not selected to make a `Part`.

    The input and output list of lists will have the same length: the number
    of server in the cluster (the first list is for the first server, etc.)
    """
    other_files: List[List[SnapshotFile]] = [[] for _ in snapshot_files]
    keyed_parts: Dict[PartKey, Part] = {}
    for server_index, server_files in enumerate(snapshot_files):
        for snapshot_file in server_files:
            if not add_file_to_parts(snapshot_file, server_index, table_uuids, keyed_parts):
                other_files[server_index].append(snapshot_file)
    return list(keyed_parts.values()), other_files


def add_file_to_parts(
    snapshot_file: SnapshotFile, server_index: int, table_uuids: Set[uuid.UUID], parts: Dict[PartKey, Part]
) -> bool:
    """
    If the `snapshot_file` is a file from a part of one of the tables listed in
    `table_uuids`, add it to the corresponding Part in `parts`.

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
    if table_uuid not in table_uuids:
        return False
    part_key = PartKey(table_uuid=table_uuid, part_name=path_parts[4])
    part = parts.setdefault(part_key, Part(files={}, total_size=0))
    part_file = part.files.get(snapshot_file.relative_path)
    if part_file is None:
        part.files[snapshot_file.relative_path] = PartFile(snapshot_file=snapshot_file, servers={server_index})
        part.total_size += snapshot_file.file_size
    elif part_file.snapshot_file.equals_excluding_mtime(snapshot_file):
        part_file.servers.add(server_index)
    else:
        raise ValueError(
            f"Inconsistent part file {snapshot_file.relative_path} of part {part_key} "
            f"between servers {part_file.servers} and server {server_index}:\n"
            f"    {part_file.snapshot_file}\n"
            f"    {snapshot_file}"
        )
    return True


def check_parts_replication(parts: Iterable[Part]):
    """
    Checks that within a single part, all files are present on the same set of servers.
    """
    for part in parts:
        part_servers: Optional[Set[int]] = None
        for file_path, file in part.files.items():
            if part_servers is None:
                part_servers = file.servers
            elif part_servers != file.servers:
                raise ValueError(
                    f"Inconsistent part, not all files are identically replicated: "
                    f"some files are on servers {part_servers} while {file_path} is on servers {file.servers}"
                )


def distribute_parts_to_servers(parts: List[Part], server_files: List[List[SnapshotFile]]):
    """
    Distributes each part to only one of the multiple servers where the part was
    during the backup.

    Parts are distributed to each server such as the total download size for each
    server is roughly equal (using a greedy algorithm).
    """
    total_file_sizes = [0 for _ in server_files]
    for part in sorted(parts, key=lambda p: p.total_size, reverse=True):
        server_index = None
        for file in part.files.values():
            if server_index is None:
                server_index = min(file.servers, key=total_file_sizes.__getitem__)
            total_file_sizes[server_index] += file.snapshot_file.file_size
            server_files[server_index].append(file.snapshot_file)


def get_frozen_parts_pattern(freeze_name: str) -> str:
    """
    Returns the glob pattern inside ClickHouse data dir where frozen table parts are stored.
    """
    escaped_freeze_name = escape_for_file_name(freeze_name.encode())
    return f"shadow/{escaped_freeze_name}/store/**/*"


def list_parts_to_attach(
    snapshot_result: SnapshotResult,
    tables_by_uuid: Mapping[uuid.UUID, Table],
) -> Sequence[Tuple[str, bytes]]:
    """
    Returns a list of table identifiers and part names to attach from the snapshot.
    """
    parts_to_attach: Set[Tuple[str, bytes]] = set()
    for snapshot_file in snapshot_result.state.files:
        table_uuid = uuid.UUID(snapshot_file.relative_path.parts[2])
        table = tables_by_uuid.get(table_uuid)
        if table is not None:
            part_name = unescape_from_file_name(snapshot_file.relative_path.parts[4])
            parts_to_attach.add((table.escaped_sql_identifier, part_name))
    return sorted(parts_to_attach)
