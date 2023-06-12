"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from astacus.common.snapshot import SnapshotGroup
from astacus.coordinator.plugins.clickhouse.config import DiskConfiguration, DiskType
from astacus.coordinator.plugins.clickhouse.disks import Disk, DiskPaths, ParsedPath
from pathlib import Path
from uuid import UUID


def test_get_snapshot_groups_pattern_escapes_backup_name() -> None:
    disk_paths = DiskPaths()
    assert disk_paths.get_snapshot_groups("something+stra/../nge") == [
        SnapshotGroup(root_glob="shadow/something%2Bstra%2F%2E%2E%2Fnge/store/**/*")
    ]


def test_parse_part_file_path() -> None:
    disk_paths = DiskPaths()
    parsed_path = disk_paths.parse_part_file_path(
        Path("store/123/12345678-1234-1234-1234-12345678abcd/all_1_1_0/checksum.txt")
    )
    assert parsed_path == ParsedPath(
        disk=Disk(type=DiskType.local, path_parts=()),
        freeze_name=None,
        table_uuid=UUID("12345678-1234-1234-1234-12345678abcd"),
        detached=False,
        part_name=b"all_1_1_0",
        file_parts=("checksum.txt",),
    )


def test_parse_detached_part_file_path() -> None:
    disk_paths = DiskPaths()
    parsed_path = disk_paths.parse_part_file_path(
        Path("store/123/12345678-1234-1234-1234-12345678abcd/detached/all_1_1_0/checksum.txt")
    )
    assert parsed_path == ParsedPath(
        disk=Disk(type=DiskType.local, path_parts=()),
        freeze_name=None,
        table_uuid=UUID("12345678-1234-1234-1234-12345678abcd"),
        detached=True,
        part_name=b"all_1_1_0",
        file_parts=("checksum.txt",),
    )


def test_parse_shadow_part_file_path() -> None:
    disk_paths = DiskPaths()
    parsed_path = disk_paths.parse_part_file_path(
        Path("shadow/astacus/store/123/12345678-1234-1234-1234-12345678abcd/detached/all_1_1_0/checksum.txt")
    )
    assert parsed_path == ParsedPath(
        disk=Disk(type=DiskType.local, path_parts=()),
        freeze_name=b"astacus",
        table_uuid=UUID("12345678-1234-1234-1234-12345678abcd"),
        detached=True,
        part_name=b"all_1_1_0",
        file_parts=("checksum.txt",),
    )


def test_parse_part_file_path_on_other_disk() -> None:
    disk_paths = DiskPaths.from_disk_configs(
        [
            DiskConfiguration(type=DiskType.local, path=Path(), name="default"),
            DiskConfiguration(type=DiskType.object_storage, path=Path("disks/secondary"), name="secondary"),
        ]
    )
    parsed_path = disk_paths.parse_part_file_path(
        Path("disks/secondary/store/123/12345678-1234-1234-1234-12345678abcd/all_1_1_0/checksum.txt")
    )
    assert parsed_path == ParsedPath(
        disk=Disk(type=DiskType.object_storage, path_parts=("disks", "secondary")),
        freeze_name=None,
        table_uuid=UUID("12345678-1234-1234-1234-12345678abcd"),
        detached=False,
        part_name=b"all_1_1_0",
        file_parts=("checksum.txt",),
    )


def test_parsed_path_to_path() -> None:
    assert ParsedPath(
        disk=Disk(type=DiskType.local, path_parts=()),
        freeze_name=None,
        table_uuid=UUID("12345678-1234-1234-1234-12345678abcd"),
        detached=False,
        part_name=b"all_1_1_0",
        file_parts=("checksum.txt",),
    ).to_path() == Path("store/123/12345678-1234-1234-1234-12345678abcd/all_1_1_0/checksum.txt")


def test_detached_parsed_path_to_path() -> None:
    assert ParsedPath(
        disk=Disk(type=DiskType.local, path_parts=()),
        freeze_name=None,
        table_uuid=UUID("12345678-1234-1234-1234-12345678abcd"),
        detached=True,
        part_name=b"all_1_1_0",
        file_parts=("checksum.txt",),
    ).to_path() == Path("store/123/12345678-1234-1234-1234-12345678abcd/detached/all_1_1_0/checksum.txt")


def test_shadow_parsed_path_to_path() -> None:
    assert ParsedPath(
        disk=Disk(type=DiskType.local, path_parts=()),
        freeze_name=b"this/is\x80weird",
        table_uuid=UUID("12345678-1234-1234-1234-12345678abcd"),
        detached=False,
        part_name=b"all_1_1_0",
        file_parts=("checksum.txt",),
    ).to_path() == Path("shadow/this%2Fis%80weird/store/123/12345678-1234-1234-1234-12345678abcd/all_1_1_0/checksum.txt")


def test_other_disk_parsed_path_to_path() -> None:
    assert ParsedPath(
        disk=Disk(type=DiskType.object_storage, path_parts=("disks", "secondary")),
        freeze_name=None,
        table_uuid=UUID("12345678-1234-1234-1234-12345678abcd"),
        detached=False,
        part_name=b"all_1_1_0",
        file_parts=("checksum.txt",),
    ).to_path() == Path("disks/secondary/store/123/12345678-1234-1234-1234-12345678abcd/all_1_1_0/checksum.txt")
