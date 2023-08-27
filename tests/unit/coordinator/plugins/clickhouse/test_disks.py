"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from astacus.common.snapshot import SnapshotGroup
from astacus.coordinator.plugins.clickhouse.config import DiskConfiguration, DiskType
from astacus.coordinator.plugins.clickhouse.disks import Disk, Disks, ParsedPath, PartFilePathError
from pathlib import Path
from uuid import UUID


def test_get_snapshot_groups_pattern_escapes_backup_name() -> None:
    disks = Disks()
    assert disks.get_snapshot_groups("something+stra/../nge") == [
        SnapshotGroup(root_glob="shadow/something%2Bstra%2F%2E%2E%2Fnge/store/**/*")
    ]


def test_parse_part_file_path() -> None:
    disks = Disks()
    parsed_path = disks.parse_part_file_path(Path("store/123/12345678-1234-1234-1234-12345678abcd/all_1_1_0/checksum.txt"))
    assert parsed_path == ParsedPath(
        disk=Disk(type=DiskType.local, name="default", path_parts=()),
        freeze_name=None,
        table_uuid=UUID("12345678-1234-1234-1234-12345678abcd"),
        detached=False,
        part_name=b"all_1_1_0",
        file_parts=("checksum.txt",),
    )


def test_parse_detached_part_file_path() -> None:
    disks = Disks()
    parsed_path = disks.parse_part_file_path(
        Path("store/123/12345678-1234-1234-1234-12345678abcd/detached/all_1_1_0/checksum.txt")
    )
    assert parsed_path == ParsedPath(
        disk=Disk(type=DiskType.local, name="default", path_parts=()),
        freeze_name=None,
        table_uuid=UUID("12345678-1234-1234-1234-12345678abcd"),
        detached=True,
        part_name=b"all_1_1_0",
        file_parts=("checksum.txt",),
    )


def test_parse_shadow_part_file_path() -> None:
    disks = Disks()
    parsed_path = disks.parse_part_file_path(
        Path("shadow/astacus/store/123/12345678-1234-1234-1234-12345678abcd/detached/all_1_1_0/checksum.txt")
    )
    assert parsed_path == ParsedPath(
        disk=Disk(type=DiskType.local, name="default", path_parts=()),
        freeze_name=b"astacus",
        table_uuid=UUID("12345678-1234-1234-1234-12345678abcd"),
        detached=True,
        part_name=b"all_1_1_0",
        file_parts=("checksum.txt",),
    )


def test_parse_part_file_path_on_other_disk() -> None:
    disks = Disks.from_disk_configs(
        [
            DiskConfiguration(type=DiskType.local, path=Path(), name="default"),
            DiskConfiguration(type=DiskType.object_storage, path=Path("disks/secondary"), name="secondary"),
        ]
    )
    parsed_path = disks.parse_part_file_path(
        Path("disks/secondary/store/123/12345678-1234-1234-1234-12345678abcd/all_1_1_0/checksum.txt")
    )
    assert parsed_path == ParsedPath(
        disk=Disk(type=DiskType.object_storage, name="secondary", path_parts=("disks", "secondary")),
        freeze_name=None,
        table_uuid=UUID("12345678-1234-1234-1234-12345678abcd"),
        detached=False,
        part_name=b"all_1_1_0",
        file_parts=("checksum.txt",),
    )


def test_parse_part_file_path_matching_no_disk() -> None:
    disks = Disks(disks=[Disk(type=DiskType.local, name="non_default", path_parts=("disks", "non_default"))])
    with pytest.raises(PartFilePathError, match="should start with a disk path"):
        disks.parse_part_file_path(Path("store/123/12345678-1234-1234-1234-12345678abcd/detached/all_1_1_0/checksum.txt"))


def test_parse_part_file_path_not_in_store_or_shadow() -> None:
    disks = Disks()
    with pytest.raises(PartFilePathError, match="should start with 'store' or 'shadow' after the disk path"):
        disks.parse_part_file_path(Path("config/config.xml"))


def test_parse_part_file_path_invalid_uuid() -> None:
    disks = Disks()
    with pytest.raises(PartFilePathError, match="invalid table UUID"):
        disks.parse_part_file_path(Path("store/123/12345678-XXXX-1234-1234-12345678abcd/detached/all_1_1_0/checksum.txt"))


def test_parse_part_file_path_consistent_uuid() -> None:
    disks = Disks()
    with pytest.raises(PartFilePathError, match="the UUID folder should have the 3 first characters of the UUID"):
        disks.parse_part_file_path(Path("store/999/12345678-1234-1234-1234-12345678abcd/detached/all_1_1_0/checksum.txt"))


def test_parsed_path_to_path() -> None:
    assert ParsedPath(
        disk=Disk(type=DiskType.local, name="default", path_parts=()),
        freeze_name=None,
        table_uuid=UUID("12345678-1234-1234-1234-12345678abcd"),
        detached=False,
        part_name=b"all_1_1_0",
        file_parts=("checksum.txt",),
    ).to_path() == Path("store/123/12345678-1234-1234-1234-12345678abcd/all_1_1_0/checksum.txt")


def test_detached_parsed_path_to_path() -> None:
    assert ParsedPath(
        disk=Disk(type=DiskType.local, name="default", path_parts=()),
        freeze_name=None,
        table_uuid=UUID("12345678-1234-1234-1234-12345678abcd"),
        detached=True,
        part_name=b"all_1_1_0",
        file_parts=("checksum.txt",),
    ).to_path() == Path("store/123/12345678-1234-1234-1234-12345678abcd/detached/all_1_1_0/checksum.txt")


def test_shadow_parsed_path_to_path() -> None:
    assert ParsedPath(
        disk=Disk(type=DiskType.local, name="default", path_parts=()),
        freeze_name=b"this/is\x80weird",
        table_uuid=UUID("12345678-1234-1234-1234-12345678abcd"),
        detached=False,
        part_name=b"all_1_1_0",
        file_parts=("checksum.txt",),
    ).to_path() == Path("shadow/this%2Fis%80weird/store/123/12345678-1234-1234-1234-12345678abcd/all_1_1_0/checksum.txt")


def test_other_disk_parsed_path_to_path() -> None:
    assert ParsedPath(
        disk=Disk(type=DiskType.object_storage, name="secondary", path_parts=("disks", "secondary")),
        freeze_name=None,
        table_uuid=UUID("12345678-1234-1234-1234-12345678abcd"),
        detached=False,
        part_name=b"all_1_1_0",
        file_parts=("checksum.txt",),
    ).to_path() == Path("disks/secondary/store/123/12345678-1234-1234-1234-12345678abcd/all_1_1_0/checksum.txt")
