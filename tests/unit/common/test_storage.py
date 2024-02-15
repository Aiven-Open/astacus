"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

"""

from astacus.common import exceptions
from astacus.common.storage.base import Storage
from astacus.common.storage.cache import CachedStorage
from astacus.common.storage.file import FileStorage
from astacus.common.storage.memory import MemoryStorage
from astacus.common.storage.rohmu import RohmuConfig, RohmuStorage
from io import BytesIO
from pathlib import Path
from pytest_mock import MockerFixture
from rohmu.object_storage import google
from tests.utils import create_rohmu_config
from unittest.mock import Mock, patch

import pytest


def create_storage(*, tmp_path: Path, engine: type[Storage], **kw) -> Storage:
    if engine == RohmuStorage:
        config = create_rohmu_config(tmp_path, **kw)
        return RohmuStorage(config=config, prefix="json")
    if engine == FileStorage:
        return FileStorage(tmp_path)
    if engine == CachedStorage:
        # FileStorage cache, and then rohmu filestorage underneath
        cache_path = tmp_path / "cache"
        storage_path = tmp_path / "storage"
        cache_path.mkdir()
        storage_path.mkdir()
        cache = FileStorage(cache_path)
        config = create_rohmu_config(storage_path, **kw)
        storage = RohmuStorage(config=config)
        return CachedStorage(storage=storage, cache=cache)
    if engine == MemoryStorage:
        return MemoryStorage()
    raise NotImplementedError(f"unknown storage {engine}")


TEST_CASES = [
    (FileStorage, {}),
    (RohmuStorage, {}),
    (CachedStorage, {}),  # simple path - see test_caching_storage for rest
    (RohmuStorage, {"compression": False}),
    (RohmuStorage, {"encryption": False}),
]


@pytest.mark.parametrize("engine,kw", TEST_CASES)
def test_storage_write_read(tmp_path: Path, engine: type[Storage], kw: dict[str, bool]) -> None:
    storage = create_storage(tmp_path=tmp_path, engine=engine, **kw)
    data = BytesIO(b"123")
    data.seek(0)
    storage.upload_key_from_file("abc", data, 3)
    result = BytesIO()
    storage.download_key_to_file("abc", result)
    assert result.getvalue() == b"123"


@pytest.mark.parametrize("engine,kw", TEST_CASES)
def test_storage_read_raises_not_found(tmp_path: Path, engine: type[Storage], kw: dict[str, bool]) -> None:
    storage = create_storage(tmp_path=tmp_path, engine=engine, **kw)
    with pytest.raises(exceptions.NotFoundException):
        storage.download_key_to_file("abc", BytesIO())


@pytest.mark.parametrize("engine,kw", TEST_CASES)
def test_storage_list(tmp_path: Path, engine: type[Storage], kw: dict[str, bool]) -> None:
    storage = create_storage(tmp_path=tmp_path, engine=engine, **kw)
    data = BytesIO(b"123")
    data.seek(0)
    storage.upload_key_from_file("abc", data, 3)
    data = BytesIO(b"world")
    data.seek(0)
    storage.upload_key_from_file("hello", data, 3)
    assert storage.list_key() == ["abc", "hello"]


@pytest.mark.parametrize("engine,kw", TEST_CASES)
def test_storage_delete(tmp_path: Path, engine: type[Storage], kw: dict[str, bool]) -> None:
    storage = create_storage(tmp_path=tmp_path, engine=engine, **kw)
    data = BytesIO(b"123")
    data.seek(0)
    storage.upload_key_from_file("abc", data, 3)
    storage.delete_key("abc")
    assert storage.list_key() == []
    with pytest.raises(exceptions.NotFoundException):
        storage.download_key_to_file("abc", BytesIO())


@pytest.mark.parametrize("engine,kw", TEST_CASES)
def test_storage_delete_raises_not_found(tmp_path: Path, engine: type[Storage], kw: dict[str, bool]) -> None:
    storage = create_storage(tmp_path=tmp_path, engine=engine, **kw)
    with pytest.raises(exceptions.NotFoundException):
        storage.delete_key("abc")


@pytest.mark.parametrize("engine,kw", TEST_CASES)
def test_storage_upload_overwrites(tmp_path: Path, engine: type[Storage], kw: dict[str, bool]) -> None:
    storage = create_storage(tmp_path=tmp_path, engine=engine, **kw)
    data = BytesIO(b"123")
    data.seek(0)
    storage.upload_key_from_file("abc", data, 3)
    data = BytesIO(b"world")
    data.seek(0)
    storage.upload_key_from_file("abc", data, 3)
    result = BytesIO()
    storage.download_key_to_file("abc", result)
    assert result.getvalue() == b"world"


def test_storage_must_have_compression_or_encryption(tmp_path: Path) -> None:
    config = create_rohmu_config(tmp_path, compression=False, encryption=False)
    with pytest.raises(exceptions.CompressionOrEncryptionRequired):
        RohmuStorage(config)


def test_caching_storage(tmp_path: Path, mocker: MockerFixture) -> None:
    storage = create_storage(tmp_path=tmp_path, engine=CachedStorage)
    assert isinstance(storage, CachedStorage)
    data = BytesIO(b"123")
    data.seek(0)
    storage.upload_key_from_file("abc", data, 3)

    # # We shouldn't wind up in download method of rohmu at all.
    mockdown = mocker.patch.object(storage.storage, "download_key_to_file")

    result = BytesIO()
    storage.download_key_to_file("abc", result)
    assert result.getvalue() == b"123"
    assert not mockdown.called

    # Nor list hexdigests
    mocklist = mocker.patch.object(storage.storage, "list_key")
    assert storage.list_key() == ["abc"]
    assert not mocklist.called


@patch("rohmu.object_storage.google.get_credentials")
@patch.object(google.GoogleTransfer, "_init_google_client")
def test_proxy_storage(mock_google_client: Mock, mock_get_credentials: Mock) -> None:
    rs = RohmuStorage(
        config=RohmuConfig.parse_obj(
            {
                "temporary_directory": "/tmp/astacus/backup-tmp",
                "default_storage": "x",
                "compression": {"algorithm": "zstd"},
                "storages": {
                    "x-proxy": {
                        "bucket_name": "REDACTED",
                        "credentials": {
                            "token_uri": "https://accounts.google.com/o/oauth2/token",
                            "type": "service_account",
                        },
                        "prefix": "REDACTED",
                        "project_id": "REDACTED",
                        "storage_type": "google",
                        "proxy_info": {
                            "type": "socks5",
                            "host": "localhost",
                            "port": 1080,
                            "user": "REDACTED",
                            "pass": "REDACTED",
                        },
                    }
                },
            }
        ),
        storage="x-proxy",
    )
    assert rs.storage.proxy_info["user"] == "REDACTED"
    assert rs.storage.proxy_info["pass"] == "REDACTED"
    assert rs.storage.proxy_info["type"] == "socks5"
    assert rs.storage.proxy_info["host"] == "localhost"
    assert rs.storage.proxy_info["port"] == 1080
