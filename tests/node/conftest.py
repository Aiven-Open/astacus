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
