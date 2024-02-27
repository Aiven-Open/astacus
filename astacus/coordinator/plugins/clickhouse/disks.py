"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from .async_object_storage import AsyncObjectStorage, RohmuAsyncObjectStorage, ThreadSafeRohmuStorage
from .config import DiskConfiguration, DiskType
from .escaping import escape_for_file_name, unescape_from_file_name
from astacus.common.magic import DEFAULT_EMBEDDED_FILE_SIZE
from astacus.common.snapshot import SnapshotGroup
from collections.abc import Sequence
from typing import Final
from uuid import UUID

import dataclasses
import msgspec
import re

UUID_RE: Final = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


class PartFilePathError(ValueError):
    def __init__(self, file_path: str, error: str):
        super().__init__(f"Unexpected part file path {file_path}: {error}")


@dataclasses.dataclass(frozen=True, slots=True)
class Disk:
    type: DiskType
    name: str
    path_parts: tuple[str, ...]
    object_storage: AsyncObjectStorage | None = None

    @classmethod
    def from_disk_config(cls, config: DiskConfiguration, storage_name: str | None = None) -> "Disk":
        if config.object_storage is None:
            object_storage: RohmuAsyncObjectStorage | None = None
        else:
            config_name = storage_name if storage_name is not None else config.object_storage.default_storage
            storage_config = config.object_storage.storages[config_name]
            object_storage = RohmuAsyncObjectStorage(storage=ThreadSafeRohmuStorage(config=storage_config))
        return Disk(
            type=config.type,
            name=config.name,
            path_parts=config.path.parts,
            object_storage=object_storage,
        )


class ParsedPath(msgspec.Struct, kw_only=True, frozen=True):
    disk: Disk
    freeze_name: bytes | None
    table_uuid: UUID
    detached: bool
    part_name: bytes
    file_parts: tuple[str, ...]

    def to_path(self) -> str:
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
        return "/".join([*self.disk.path_parts, *parts, *self.file_parts])


@dataclasses.dataclass(frozen=True)
class Disks:
    disks: Sequence[Disk] = dataclasses.field(
        default_factory=lambda: [Disk(type=DiskType.local, name="default", path_parts=())]
    )

    def get_snapshot_groups(self, freeze_name: str) -> Sequence[SnapshotGroup]:
        """
        Returns the glob groups inside ClickHouse data dirs where frozen table parts are stored.

        For local disk, the maximum embedded file size is the default one,
        For remote disks the embedded file size is unlimited: we want to embed all metadata files.
        """
        escaped_freeze_name = escape_for_file_name(freeze_name.encode())
        frozen_parts_pattern = f"shadow/{escaped_freeze_name}/store/**/*"
        return [
            SnapshotGroup(
                root_glob="/".join((*disk.path_parts, frozen_parts_pattern)),
                excluded_names=("frozen_metadata.txt",) if disk.type == DiskType.object_storage else (),
                embedded_file_size_max=None if disk.type == DiskType.object_storage else DEFAULT_EMBEDDED_FILE_SIZE,
            )
            for disk in self.disks
        ]

    def get_object_storage(self, *, disk_name: str) -> AsyncObjectStorage | None:
        for disk in self.disks:
            if disk.name == disk_name:
                return disk.object_storage
        return None

    def _get_disk(self, path_parts: Sequence[str]) -> Disk | None:
        for disk in self.disks:
            if path_parts[: len(disk.path_parts)] == disk.path_parts:
                return disk
        return None

    def parse_part_file_path(self, file_path: str) -> ParsedPath:
        """
        Parse component of a file path relative to one of the ClickHouse disks.

        The path can be in the normal store or in the frozen shadow store:
            - [disk_path]/store/123/12345678-1234-1234-1234-12345678abcd/all_1_1_0/[file.ext]
            - [disk_path]/shadow/[freeze_name]/store/123/12345678-1234-1234-1234-12345678abcd/all_1_1_0/[file.ext]
        The part can be attached or detached:
            - [disk_path]/store/123/12345678-1234-1234-1234-12345678abcd/all_1_1_0/[file.ext]
            - [disk_path]/store/123/12345678-1234-1234-1234-12345678abcd/detached/all_1_1_0/[file.ext]
        """
        parts = tuple(file_path.split("/"))
        disk = self._get_disk(parts)
        if disk is None:
            raise PartFilePathError(file_path, "should start with a disk path")
        store_or_shadow_index = len(disk.path_parts)
        if parts[store_or_shadow_index] == "store":
            freeze_name = None
            uuid_index = store_or_shadow_index + 2
        elif parts[store_or_shadow_index] == "shadow":
            freeze_name = unescape_from_file_name(parts[store_or_shadow_index + 1])
            uuid_index = store_or_shadow_index + 4
        else:
            raise PartFilePathError(file_path, "should start with 'store' or 'shadow' after the disk path")
        if not UUID_RE.fullmatch(parts[uuid_index]):
            raise PartFilePathError(file_path, "invalid table UUID")
        if parts[uuid_index - 1] != parts[uuid_index][:3]:
            raise PartFilePathError(
                file_path,
                " the parent folder to the UUID folder should have the 3 first characters of the UUID",
            )
        detached = parts[uuid_index + 1] == "detached"
        part_name_index = uuid_index + (2 if detached else 1)
        return ParsedPath(
            disk=disk,
            freeze_name=freeze_name,
            table_uuid=UUID(parts[uuid_index]),
            detached=detached,
            part_name=unescape_from_file_name(parts[part_name_index]),
            file_parts=tuple(parts[part_name_index + 1 :]),
        )

    @classmethod
    def from_disk_configs(cls, disk_configs: Sequence[DiskConfiguration], storage_name: str | None = None) -> "Disks":
        return Disks(
            disks=sorted(
                [Disk.from_disk_config(disk_config, storage_name) for disk_config in disk_configs],
                key=lambda disk: len(disk.path_parts),
                reverse=True,
            )
        )
