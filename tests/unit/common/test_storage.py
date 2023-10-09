"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Test that RohmuStorage works as advertised.

TBD: Test with something else than local files?

"""

from astacus.common import exceptions
from astacus.common.cachingjsonstorage import CachingJsonStorage
from astacus.common.rohmustorage import RohmuConfig, RohmuStorage
from astacus.common.storage import FileStorage, JsonStorage
from contextlib import nullcontext as does_not_raise
from pathlib import Path
from rohmu.object_storage import google
from tests.utils import create_rohmu_config
from unittest.mock import patch

import pytest

TEST_HEXDIGEST = "deadbeef"
TEXT_HEXDIGEST_DATA = b"data" * 15

TEST_JSON = "jsonblob"
TEST_JSON_DATA = {"foo": 7, "array": [1, 2, 3], "true": True}


def create_storage(*, tmpdir, engine, **kw):
    if engine == "rohmu":
        config = create_rohmu_config(tmpdir, **kw)
        return RohmuStorage(config=config)
    if engine == "file":
        path = Path(tmpdir / "test-storage-file")
        return FileStorage(path)
    if engine == "cache":
        # FileStorage cache, and then rohmu filestorage underneath
        cache_storage = FileStorage(Path(tmpdir / "test-storage-file"))
        config = create_rohmu_config(tmpdir, **kw)
        backend_storage = RohmuStorage(config=config)
        return CachingJsonStorage(backend_storage=backend_storage, cache_storage=cache_storage)
    raise NotImplementedError(f"unknown storage {engine}")


def _test_hexdigeststorage(storage: FileStorage):
    storage.upload_hexdigest_bytes(TEST_HEXDIGEST, TEXT_HEXDIGEST_DATA)
    assert storage.download_hexdigest_bytes(TEST_HEXDIGEST) == TEXT_HEXDIGEST_DATA
    # Ensure that download attempts of nonexistent keys give NotFoundException
    with pytest.raises(exceptions.NotFoundException):
        storage.download_hexdigest_bytes(TEST_HEXDIGEST + "x")
    assert storage.list_hexdigests() == [TEST_HEXDIGEST]
    storage.delete_hexdigest(TEST_HEXDIGEST)
    with pytest.raises(exceptions.NotFoundException):
        storage.delete_hexdigest(TEST_HEXDIGEST + "x")
    assert storage.list_hexdigests() == []


def _test_jsonstorage(storage: JsonStorage):
    assert storage.list_jsons() == []
    storage.upload_json(TEST_JSON, TEST_JSON_DATA)
    assert storage.download_json(TEST_JSON) == TEST_JSON_DATA
    with pytest.raises(exceptions.NotFoundException):
        storage.download_json(TEST_JSON + "x")
    assert storage.list_jsons() == [TEST_JSON]
    storage.delete_json(TEST_JSON)
    with pytest.raises(exceptions.NotFoundException):
        storage.delete_json(TEST_JSON + "x")
    assert storage.list_jsons() == []


@pytest.mark.parametrize(
    "engine,kw,ex",
    [
        ("file", {}, None),
        ("rohmu", {}, None),
        ("cache", {}, None),  # simple path - see test_caching_storage for rest
        ("rohmu", {"compression": False}, None),
        ("rohmu", {"encryption": False}, None),
        ("rohmu", {"compression": False, "encryption": False}, pytest.raises(exceptions.CompressionOrEncryptionRequired)),
    ],
)
def test_storage(tmpdir, engine, kw, ex):
    if ex is None:
        ex = does_not_raise()
    with ex:
        storage = create_storage(tmpdir=tmpdir, engine=engine, **kw)
        if isinstance(storage, FileStorage):
            _test_hexdigeststorage(storage)
        if isinstance(storage, JsonStorage):
            _test_jsonstorage(storage)


def test_caching_storage(tmpdir, mocker):
    storage = create_storage(tmpdir=tmpdir, engine="cache")
    storage.upload_json(TEST_JSON, TEST_JSON_DATA)

    storage = create_storage(tmpdir=tmpdir, engine="cache")
    assert storage.list_jsons() == [TEST_JSON]

    # We shouldn't wind up in download method of rohmu at all.
    mockdown = mocker.patch.object(storage.backend_storage, "download_json")
    # Nor list hexdigests
    mocklist = mocker.patch.object(storage.backend_storage, "list_jsons")

    assert storage.download_json(TEST_JSON) == TEST_JSON_DATA
    assert storage.list_jsons() == [TEST_JSON]
    assert not mockdown.called
    assert not mocklist.called


@patch("rohmu.object_storage.google.get_credentials")
@patch.object(google.GoogleTransfer, "_init_google_client")
def test_proxy_storage(mock_google_client, mock_get_credentials):
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
