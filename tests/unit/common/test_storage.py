"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Test that RohmuStorage works as advertised.

TBD: Test with something else than local files?

"""

from astacus.common import exceptions
from astacus.common.rohmustorage import RohmuStorage
from astacus.common.storage import FileStorage
from contextlib import nullcontext as does_not_raise
from pathlib import Path
from tests.utils import create_rohmu_config

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
    raise NotImplementedError(f"unknown storage {engine}")


def _test_hexdigeststorage(storage):
    storage.upload_hexdigest_bytes(TEST_HEXDIGEST, TEXT_HEXDIGEST_DATA)
    assert storage.download_hexdigest_bytes(TEST_HEXDIGEST) == TEXT_HEXDIGEST_DATA
    assert storage.list_hexdigests() == [TEST_HEXDIGEST]
    storage.delete_hexdigest(TEST_HEXDIGEST)
    assert storage.list_hexdigests() == []


def _test_jsonstorage(storage):
    assert storage.list_jsons() == []
    storage.upload_json(TEST_JSON, TEST_JSON_DATA)
    assert storage.download_json(TEST_JSON) == TEST_JSON_DATA
    assert storage.list_jsons() == [TEST_JSON]
    storage.delete_json(TEST_JSON)
    assert storage.list_jsons() == []


@pytest.mark.parametrize(
    "engine,kw,ex", [
        ("file", {}, None),
        ("rohmu", {}, None),
        ("rohmu", {
            "compression": False
        }, None),
        ("rohmu", {
            "encryption": False
        }, None),
        ("rohmu", {
            "compression": False,
            "encryption": False
        }, pytest.raises(exceptions.CompressionOrEncryptionRequired)),
    ]
)
def test_storage(tmpdir, engine, kw, ex):
    if ex is None:
        ex = does_not_raise()
    with ex:
        storage = create_storage(tmpdir=tmpdir, engine=engine, **kw)
        _test_hexdigeststorage(storage)
        _test_jsonstorage(storage)
