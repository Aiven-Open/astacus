"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""
from astacus.coordinator.api import router
from astacus.coordinator.config import CoordinatorConfig
from fastapi import FastAPI
from fastapi.testclient import TestClient

import pytest


@pytest.fixture(name="app")
def fixture_app():
    app = FastAPI()
    app.include_router(router, tags=["coordinator"])
    app.state.coordinator_config = CoordinatorConfig()
    yield app


@pytest.fixture(name="client")
def fixture_client(app):
    client = TestClient(app)

    # One ping at API to populate the fixtures (ugh)
    response = client.post("/lock")
    assert response.status_code == 422, response.json()

    yield client
