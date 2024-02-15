"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from astacus.common.exceptions import NotFoundException
from astacus.common.storage.hexidigest import HexDigestStore
from astacus.common.storage.memory import MemoryStorage
from pathlib import Path

import io
import pytest


@pytest.fixture(name="hex_storage")
def fixture_json_storage() -> HexDigestStore:
    return HexDigestStore(storage=MemoryStorage())


def test_upload_download_hexdigest(hex_storage: HexDigestStore) -> None:
    hex_storage.upload_hexdigest_from_file("test", io.BytesIO(b"test"), 4)
    assert hex_storage.download_hexdigest_bytes("test") == b"test"


def test_delete_hexdigest(hex_storage: HexDigestStore) -> None:
    hex_storage.upload_hexdigest_from_file("test", io.BytesIO(b"test"), 4)
    hex_storage.delete_hexdigest("test")
    assert hex_storage.list_hexdigests() == []
    with pytest.raises(NotFoundException):
        hex_storage.download_hexdigest_bytes("test")


def test_download_to_path(hex_storage: HexDigestStore, tmp_path: Path):
    hex_storage.upload_hexdigest_from_file("test", io.BytesIO(b"test"), 4)
    hex_storage.download_hexdigest_to_path("test", tmp_path / "test")
    assert (tmp_path / "test").read_bytes() == b"test"


def test_download_to_file(hex_storage: HexDigestStore, tmp_path: Path):
    hex_storage.upload_hexdigest_from_file("test", io.BytesIO(b"test"), 4)
    with open(tmp_path / "test", "wb") as f:
        hex_storage.download_hexdigest_to_file("test", f)
    assert (tmp_path / "test").read_bytes() == b"test"


def test_upload_bytes(hex_storage: HexDigestStore) -> None:
    hex_storage.upload_hexdigest_bytes("test", b"test")
    assert hex_storage.download_hexdigest_bytes("test") == b"test"


def test_list_hexdigests(hex_storage: HexDigestStore) -> None:
    hex_storage.upload_hexdigest_from_file("test", io.BytesIO(b"test"), 4)
    hex_storage.upload_hexdigest_from_file("test2", io.BytesIO(b"test2"), 5)
    assert hex_storage.list_hexdigests() == ["test", "test2"]
