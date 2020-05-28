"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""
from astacus.common.rohmustorage import RohmuStorage
from astacus.coordinator.api import router
from astacus.coordinator.config import CoordinatorConfig
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


@pytest.fixture(name="app")
def fixture_app(mocker, sleepless, storage, tmpdir):  # pylint: disable=unused-argument
    app = FastAPI()
    app.include_router(router, tags=["coordinator"])
    app.state.coordinator_config = CoordinatorConfig(object_storage=create_rohmu_config(tmpdir))
    mocker.patch.object(CoordinatorOpWithClusterLock, "get_locker", return_value="x")
    yield app
