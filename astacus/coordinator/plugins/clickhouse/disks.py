"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from .escaping import escape_for_file_name, unescape_from_file_name
from pathlib import Path
from typing import Sequence
from uuid import UUID

import dataclasses
import re


class PartFilePathError(ValueError):
    def __init__(self, file_path: Path, error: str):
        super().__init__(f"Unexpected part file path {file_path}: {error}")


@dataclasses.dataclass(frozen=True)
class ParsedPath:
    disk_parts: tuple[str, ...]
    freeze_name: bytes | None
    table_uuid: UUID
    detached: bool
    part_name: bytes
    file_parts: tuple[str, ...]

    def to_path(self) -> Path:
        parts = []
        if self.freeze_name is not None:
            parts.append("shadow")
            parts.append(escape_for_file_name(self.freeze_name))
        parts.append("store")
        table_uuid_str = str(self.table_uuid)
        parts.append(table_uuid_str[:3])
        parts.append(table_uuid_str)
        if self.detached:
            parts.append("detached")
        parts.append(escape_for_file_name(self.part_name))
        return Path(*self.disk_parts, *parts, *self.file_parts)


@dataclasses.dataclass(frozen=True)
class DiskPaths:
    _disk_paths_parts: Sequence[tuple[str, ...]] = dataclasses.field(default_factory=lambda: [()])

    def get_frozen_parts_patterns(self, freeze_name: str) -> Sequence[str]:
        """
        Returns the glob patterns inside ClickHouse data dirs where frozen table parts are stored.
        """
        escaped_freeze_name = escape_for_file_name(freeze_name.encode())
        frozen_parts_pattern = f"shadow/{escaped_freeze_name}/store/**/*"
        return ["/".join((*disk_path_parts, frozen_parts_pattern)) for disk_path_parts in self._disk_paths_parts]

    def parse_part_file_path(self, file_path: Path) -> ParsedPath:
        """
        Parse component of a file path relative to one of the ClickHouse disks.

        The path can be in the normal store or in the frozen shadow store:
            - [disk_path]/store/123/12345678-1234-1234-1234-12345678abcd/all_1_1_0/[file.ext]
            - [disk_path]/shadow/[freeze_name]/store/123/12345678-1234-1234-1234-12345678abcd/all_1_1_0/[file.ext]
        The part can be attached or detached:
            - [disk_path]/store/123/12345678-1234-1234-1234-12345678abcd/all_1_1_0/[file.ext]
            - [disk_path]/store/123/12345678-1234-1234-1234-12345678abcd/detached/all_1_1_0/[file.ext]
        """
        parts = file_path.parts
        store_or_shadow_index = None
        for disk_path_parts in self._disk_paths_parts:
            if parts[: len(disk_path_parts)] == disk_path_parts:
                store_or_shadow_index = len(disk_path_parts)
                break
        if store_or_shadow_index is None:
            raise PartFilePathError(file_path, "should start with a disk path")
        if parts[store_or_shadow_index] == "store":
            freeze_name = None
            uuid_index = store_or_shadow_index + 2
        elif parts[store_or_shadow_index] == "shadow":
            freeze_name = unescape_from_file_name(parts[store_or_shadow_index + 1])
            uuid_index = store_or_shadow_index + 4
        else:
            raise PartFilePathError(file_path, "should start with 'store' or 'shadow' after the disk path")
        if not re.match(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", parts[uuid_index]):
            raise PartFilePathError(file_path, "invalid table UUID")
        if parts[uuid_index - 1] != parts[uuid_index][:3]:
            raise PartFilePathError(
                file_path,
                " the parent folder to the UUID folder should have the 3 first characters of the UUID",
            )
        detached = parts[uuid_index + 1] == "detached"
        part_name_index = uuid_index + (2 if detached else 1)
        return ParsedPath(
            disk_parts=parts[:store_or_shadow_index],
            freeze_name=freeze_name,
            table_uuid=UUID(parts[uuid_index]),
            detached=detached,
            part_name=unescape_from_file_name(parts[part_name_index]),
            file_parts=parts[part_name_index + 1 :],
        )

    @classmethod
    def from_disk_paths(cls, disk_paths: Sequence[Path]) -> "DiskPaths":
        return DiskPaths(_disk_paths_parts=sorted([disk_path.parts for disk_path in disk_paths], key=len, reverse=True))
