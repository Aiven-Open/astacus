"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""
from .test_restore import BACKUP_MANIFEST
from astacus.common.rohmustorage import MultiRohmuStorage, RohmuStorage
from astacus.coordinator.api import router
from astacus.coordinator.config import CoordinatorConfig, CoordinatorNode
from astacus.coordinator.coordinator import CoordinatorOpWithClusterLock
from fastapi import FastAPI
from fastapi.testclient import TestClient
from tests.utils import create_rohmu_config

import asyncio
import pytest

_original_asyncio_sleep = asyncio.sleep


@pytest.fixture(name="storage")
def fixture_storage(tmpdir):
    return RohmuStorage(config=create_rohmu_config(tmpdir))


@pytest.fixture(name="mstorage")
def fixture_mstorage(tmpdir):
    return MultiRohmuStorage(config=create_rohmu_config(tmpdir))


@pytest.fixture(name="populated_mstorage")
def fixture_populated_mstorage(mstorage):
    x = mstorage.get_storage("x")
    x.upload_json("backup-1", BACKUP_MANIFEST)
    x.upload_json("backup-2", BACKUP_MANIFEST)
    x.upload_hexdigest_bytes("DEADBEEF", b"foobar")
    y = mstorage.get_storage("y")
    y.upload_json("backup-3", BACKUP_MANIFEST)
    y.upload_hexdigest_bytes("DEADBEEF", b"foobar")
    return mstorage


@pytest.fixture(name="client")
def fixture_client(app):
    client = TestClient(app)

    # One ping at API to populate the fixtures (ugh)
    response = client.post("/lock")
    assert response.status_code == 422, response.json()

    yield client


async def _sleep_little(value):
    await _original_asyncio_sleep(min(value, 0.01))


@pytest.fixture(name="sleepless")
def fixture_sleepless(mocker):
    mocker.patch.object(asyncio, "sleep", new=_sleep_little)


COORDINATOR_NODES = [
    CoordinatorNode(url="http://localhost:12345/asdf"),
    CoordinatorNode(url="http://localhost:12346/asdf"),
]


@pytest.fixture(name="app")
def fixture_app(mocker, sleepless, storage, tmpdir):  # pylint: disable=unused-argument
    app = FastAPI()
    app.include_router(router, tags=["coordinator"])
    app.state.coordinator_config = CoordinatorConfig(
        object_storage=create_rohmu_config(tmpdir),
        plugin="files",
        plugin_config={"root_globs": ["*"]},
        object_storage_cache=f"{tmpdir}/cache/is/somewhere",
    )
    app.state.coordinator_config.nodes = COORDINATOR_NODES[:]
    mocker.patch.object(CoordinatorOpWithClusterLock, "get_locker", return_value="x")
    yield app
