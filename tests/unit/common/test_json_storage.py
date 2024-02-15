"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from astacus.common.exceptions import NotFoundException
from astacus.common.storage.json import JsonStore
from astacus.common.storage.memory import MemoryStorage
from astacus.common.utils import AstacusModel

import pytest


class TestModel(AstacusModel):
    test: str


@pytest.fixture(name="json_storage")
def fixture_json_storage() -> JsonStore:
    return JsonStore(storage=MemoryStorage())


def test_upload_download_json(json_storage: JsonStore) -> None:
    json_storage.upload_json("test", {"test": "test"})
    json_storage.upload_json("test2", TestModel(test="test"))
    json_storage.upload_json_str("test3", '{"test": "test"}')
    assert json_storage.download_json("test") == {"test": "test"}
    assert json_storage.download_json("test2") == {"test": "test"}
    assert json_storage.download_json("test3") == {"test": "test"}


def test_list_jsons(json_storage: JsonStore) -> None:
    json_storage.upload_json("test", {"test": "test"})
    json_storage.upload_json("test2", TestModel(test="test"))
    json_storage.upload_json_str("test3", '{"test": "test"}')
    assert json_storage.list_jsons() == ["test", "test2", "test3"]


def test_delete_json(json_storage: JsonStore) -> None:
    json_storage.upload_json("test", {"test": "test"})
    json_storage.delete_json("test")
    assert json_storage.list_jsons() == []
    with pytest.raises(NotFoundException):
        json_storage.download_json("test")
