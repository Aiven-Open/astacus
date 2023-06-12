"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details

When using remote storage disk in ClickHouse, the actual data is in object storage
but there are still metadata files in local storage. This module contains functions
required to parse these metadata files.
"""
from pathlib import Path
from typing import Sequence

import dataclasses
import re

FILE_METADATA_RE = re.compile(
    rb"""
    (?P<metadata_version>3)\n
    (?P<objects_count>\d+)\t(?P<objects_size_bytes>\d+)\n
    (?P<objects>
        (?:
            \d+\t[^\n]+\n
        )*
    )
    (?P<ref_count>\d+)\n
    (?P<read_only>[01])\n$
    """,
    flags=re.VERBOSE,
)

OBJECT_METADATA_RE = re.compile(rb"(?P<size_bytes>\d+)\t(?P<relative_path>[^\n]+)$", flags=re.MULTILINE)


class InvalidFileMetadata(ValueError):
    pass


@dataclasses.dataclass(frozen=True)
class ObjectMetadata:
    size_bytes: int
    relative_path: Path


@dataclasses.dataclass(frozen=True)
class FileMetadata:
    metadata_version: int
    objects: Sequence[ObjectMetadata]
    ref_count: int
    read_only: bool

    @classmethod
    def from_bytes(cls, content: bytes) -> "FileMetadata":
        file_match = FILE_METADATA_RE.match(content)
        if file_match is None:
            raise InvalidFileMetadata(f"Invalid file metadata content: {content!r}")
        objects_metadata = [
            ObjectMetadata(
                size_bytes=int(object_match.group("size_bytes")),
                relative_path=Path(object_match.group("relative_path").decode()),
            )
            for object_match in OBJECT_METADATA_RE.finditer(file_match.group("objects"))
        ]
        expected_objects_count = int(file_match.group("objects_count"))
        expected_objects_size = int(file_match.group("objects_size_bytes"))
        total_objects_size = sum((object_metadata.size_bytes for object_metadata in objects_metadata))
        if len(objects_metadata) != expected_objects_count:
            raise InvalidFileMetadata(
                f"Invalid file metadata objects count: expected {expected_objects_size}, got {len(objects_metadata)}"
            )
        if total_objects_size != expected_objects_size:
            raise InvalidFileMetadata(
                f"Invalid file metadata objects size: expected {expected_objects_size}, got {total_objects_size}"
            )
        return FileMetadata(
            metadata_version=int(file_match.group("metadata_version")),
            objects=objects_metadata,
            ref_count=int(file_match.group("ref_count")),
            read_only=file_match.group("read_only") == b"1",
        )
