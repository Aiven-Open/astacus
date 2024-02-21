"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from .test_restore import BACKUP_MANIFEST
from astacus.common.ipc import Plugin
from astacus.common.storage.cache import CachedMultiStorage
from astacus.common.storage.file import FileMultiStorage
from astacus.common.storage.manager import StorageManager
from astacus.common.storage.rohmu import RohmuMultiStorage
from astacus.coordinator.api import router
from astacus.coordinator.config import CoordinatorConfig, CoordinatorNode
from astacus.coordinator.coordinator import LockedCoordinatorOp
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pathlib import Path
from pytest_mock import MockerFixture
from tests.utils import create_rohmu_config

import asyncio
import pytest

_original_asyncio_sleep = asyncio.sleep


@pytest.fixture(name="storage")
def fixture_storage(tmp_path: Path) -> StorageManager:
    config = create_rohmu_config(tmp_path)
    return StorageManager(
        n_nodes=2,
        default_storage_name=config.default_storage,
        json_storage=CachedMultiStorage(
            storage=RohmuMultiStorage(config=config, prefix="json"),
            cache=FileMultiStorage(tmp_path / "cache"),
        ),
        hexdigest_storage=RohmuMultiStorage(config=config, prefix="hexdigest"),
        node_storage=RohmuMultiStorage(config=config, prefix="node"),
        tmp_path=tmp_path / "node_tmp_storage",
    )


@pytest.fixture(name="populated_storage")
def fixture_populated_storage(storage: StorageManager) -> StorageManager:
    x = storage.get_json_store("x")
    x.upload_json("backup-1", BACKUP_MANIFEST)
    x.upload_json("backup-2", BACKUP_MANIFEST)
    storage.get_hexdigest_store("x").upload_hexdigest_bytes("DEADBEEF", b"foobar")
    storage.get_json_store("y").upload_json("backup-3", BACKUP_MANIFEST)
    storage.get_hexdigest_store("y").upload_hexdigest_bytes("DEADBEEF", b"foobar")
    return storage


@pytest.fixture(name="client")
def fixture_client(app: FastAPI) -> TestClient:
    client = TestClient(app)

    # One ping at API to populate the fixtures (ugh)
    response = client.post("/lock")
    assert response.status_code == 422, response.json()

    return client


async def _sleep_little(value: float) -> None:
    await _original_asyncio_sleep(min(value, 0.01))


@pytest.fixture(name="sleepless")
def fixture_sleepless(mocker: MockerFixture) -> None:
    mocker.patch.object(asyncio, "sleep", new=_sleep_little)


COORDINATOR_NODES = [
    CoordinatorNode(url="http://localhost:12345/asdf"),
    CoordinatorNode(url="http://localhost:12346/asdf"),
]


@pytest.fixture(name="app")
def fixture_app(mocker: MockerFixture, sleepless: None, tmp_path: Path) -> FastAPI:
    app = FastAPI()
    app.include_router(router, tags=["coordinator"])
    app.state.coordinator_config = CoordinatorConfig(
        object_storage=create_rohmu_config(tmp_path),
        plugin=Plugin.files,
        plugin_config={"root_globs": ["*"]},
        object_storage_cache=tmp_path / "cache",
    )
    app.state.coordinator_config.nodes = COORDINATOR_NODES[:]
    mocker.patch.object(LockedCoordinatorOp, "get_locker", return_value="x")
    return app
