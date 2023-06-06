"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from astacus.coordinator.plugins.clickhouse.file_metadata import FileMetadata, InvalidFileMetadata, ObjectMetadata
from pathlib import Path

import pytest


def test_file_metadata_from_bytes() -> None:
    expected = FileMetadata(
        metadata_version=3,
        objects=[
            ObjectMetadata(size_bytes=100, relative_path=Path("abc/0123456789abcdef")),
            ObjectMetadata(size_bytes=200, relative_path=Path("def/aaaaaaaaaaaaaaaa")),
        ],
        ref_count=1,
        read_only=False,
    )
    assert (
        FileMetadata.from_bytes(
            b"""3
2\t300
100\tabc/0123456789abcdef
200\tdef/aaaaaaaaaaaaaaaa
1
0
"""
        )
        == expected
    )


@pytest.mark.parametrize(
    "bad_payload",
    [
        b"""4
2\t300
100\tabc/0123456789abcdef
200\tdef/aaaaaaaaaaaaaaaa
1
0
""",
        b"""3
3\t300
100\tabc/0123456789abcdef
200\tdef/aaaaaaaaaaaaaaaa
1
0
""",
        b"""3
2\t600
100\tabc/0123456789abcdef
200\tdef/aaaaaaaaaaaaaaaa
1
0
""",
    ],
    ids=["new_version", "bad_count", "bad_size"],
)
def test_file_metadata_validation(bad_payload: bytes) -> None:
    with pytest.raises(InvalidFileMetadata):
        FileMetadata.from_bytes(bad_payload)
