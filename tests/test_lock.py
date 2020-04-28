"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from astacus.node.api import router as node_router
from fastapi import FastAPI
from fastapi.testclient import TestClient

import pytest


@pytest.fixture(name="app")
def fixture_app():
    app = FastAPI()
    app.include_router(node_router, prefix="/node", tags=["node"])
    yield app


@pytest.fixture(name="client")
def fixture_client(app):
    yield TestClient(app)


def test_lock_unlock(client):
    # Play with lock
    response = client.get("/node/lock")
    assert response.status_code == 422, response.json()
    response = client.get("/node/lock?locker=x&ttl=10")
    assert response.status_code == 200, response.json()
    response = client.get("/node/lock?locker=x&ttl=10")
    assert response.status_code == 409, response.json()
    assert response.json()["detail"] == "Already locked"

    # Play with unlock
    response = client.get("/node/unlock?locker=y")
    assert response.status_code == 403, response.json()
    assert response.json()["detail"] == "Locked by someone else"

    response = client.get("/node/unlock?locker=x")
    assert response.status_code == 200, response.json()

    response = client.get("/node/unlock?locker=x")
    assert response.status_code == 409, response.json()
    assert response.json()["detail"] == "Already unlocked"
