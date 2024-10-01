"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from .test_restore import BACKUP_MANIFEST
from astacus.common.ipc import Plugin
from astacus.common.rohmustorage import RohmuStorage
from astacus.coordinator.api import router
from astacus.coordinator.config import CoordinatorConfig, CoordinatorNode
from astacus.coordinator.coordinator import LockedCoordinatorOp
from astacus.coordinator.storage_factory import StorageFactory
from pathlib import Path
from pytest_mock import MockerFixture
from starlette.applications import Starlette
from starlette.testclient import TestClient
from tests.utils import create_rohmu_config

import asyncio
import pytest

_original_asyncio_sleep = asyncio.sleep


@pytest.fixture(name="storage")
def fixture_storage(tmp_path: Path) -> RohmuStorage:
    return RohmuStorage(config=create_rohmu_config(tmp_path))


@pytest.fixture(name="populated_storage_factory")
def fixture_populated_storage_factory(tmp_path: Path) -> StorageFactory:
    storage_factory = StorageFactory(storage_config=create_rohmu_config(tmp_path))
    x_json = storage_factory.create_json_storage("x")
    x_json.upload_json("backup-1", BACKUP_MANIFEST)
    x_json.upload_json("backup-2", BACKUP_MANIFEST)
    x_hexdigest = storage_factory.create_hexdigest_storage("x")
    x_hexdigest.upload_hexdigest_bytes("DEADBEEF", b"foobar")
    y_json = storage_factory.create_json_storage("y")
    y_json.upload_json("backup-3", BACKUP_MANIFEST)
    y_hexdigest = storage_factory.create_hexdigest_storage("y")
    y_hexdigest.upload_hexdigest_bytes("DEADBEEF", b"foobar")
    return storage_factory


@pytest.fixture(name="client")
def fixture_client(app: Starlette) -> TestClient:
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
def fixture_app(mocker: MockerFixture, sleepless: None, storage: RohmuStorage, tmp_path: Path) -> Starlette:
    app = Starlette(routes=router.get_routes())
    app.state.coordinator_config = CoordinatorConfig(
        object_storage=create_rohmu_config(tmp_path),
        plugin=Plugin.files,
        plugin_config={"root_globs": ["*"]},
        object_storage_cache=tmp_path / "cache/is/somewhere",
    )
    app.state.coordinator_config.nodes = COORDINATOR_NODES[:]
    mocker.patch.object(LockedCoordinatorOp, "get_locker", return_value="x")
    return app
