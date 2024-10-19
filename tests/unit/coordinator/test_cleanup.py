"""Copyright (c) 2020 Aiven Ltd
See LICENSE for details.

Test that the cleanup endpoint behaves as advertised
"""

from astacus.common import ipc
from astacus.coordinator.storage_factory import StorageFactory
from fastapi import FastAPI
from fastapi.testclient import TestClient

import pytest
import respx

FAILS = [1, None]


def _run(
    *,
    client: TestClient,
    storage_factory: StorageFactory,
    app: FastAPI,
    fail_at: int | None = None,
    retention: ipc.Retention,
    exp_jsons: int,
    exp_digests: int,
) -> None:
    app.state.coordinator_config.retention = retention
    assert len(storage_factory.create_json_storage("x").list_jsons()) == 2
    storage_factory.create_hexdigest_storage("x").upload_hexdigest_bytes("TOBEDELETED", b"x")
    assert len(storage_factory.create_hexdigest_storage("x").list_hexdigests()) == 2
    nodes = app.state.coordinator_config.nodes
    with respx.mock:
        for node in nodes:
            respx.post(f"{node.url}/unlock?locker=x&ttl=0").respond(json={"locked": False})
            # Failure point 1: Lock fails
            respx.post(f"{node.url}/lock?locker=x&ttl=600").respond(json={"locked": fail_at != 1})
        response = client.post("/cleanup")
        if fail_at == 1:
            # Cluster lock failure is immediate
            assert response.status_code == 409, response.json()
            assert app.state.coordinator_state.op_info.op_id == 0
            return
        assert response.status_code == 200, response.json()

        response = client.get(response.json()["status_url"])

    assert response.status_code == 200, response.json()
    if fail_at:
        assert response.json() == {"state": "fail"}
        return
    assert response.json() == {"state": "done"}
    assert len(storage_factory.create_json_storage("x").list_jsons()) == exp_jsons
    assert len(storage_factory.create_hexdigest_storage("x").list_hexdigests()) == exp_digests


@pytest.mark.parametrize("fail_at", FAILS)
def test_api_cleanup_flow(
    fail_at: int | None, client: TestClient, populated_storage_factory: StorageFactory, app: FastAPI
) -> None:
    _run(
        fail_at=fail_at,
        client=client,
        storage_factory=populated_storage_factory,
        app=app,
        retention=ipc.Retention(maximum_backups=1),
        exp_jsons=1,
        exp_digests=1,
    )


@pytest.mark.parametrize(
    "data",
    [
        (ipc.Retention(maximum_backups=1), 1, 1),
        (ipc.Retention(keep_days=-1), 0, 0),
        (ipc.Retention(minimum_backups=1, keep_days=-1), 1, 1),
        (ipc.Retention(minimum_backups=3, maximum_backups=1, keep_days=-1), 2, 1),
        (ipc.Retention(keep_days=100), 0, 0),
        (ipc.Retention(keep_days=10000), 2, 1),
        (ipc.Retention(minimum_backups=1, keep_days=10000), 2, 1),
        (ipc.Retention(maximum_backups=1, keep_days=10000), 1, 1),
    ],
)
def test_api_cleanup_retention(
    data: tuple[ipc.Retention, int, int], client: TestClient, populated_storage_factory: StorageFactory, app: FastAPI
) -> None:
    retention, exp_jsons, exp_digests = data
    _run(
        client=client,
        storage_factory=populated_storage_factory,
        app=app,
        retention=retention,
        exp_jsons=exp_jsons,
        exp_digests=exp_digests,
    )
