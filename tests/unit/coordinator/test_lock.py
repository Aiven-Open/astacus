"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Test that the coordinator lock endpoint works.

"""

from astacus.common.magic import LockCall
from astacus.common.statsd import StatsClient
from starlette.applications import Starlette
from starlette.testclient import TestClient
from unittest.mock import patch

import respx


def test_status_nonexistent(client: TestClient) -> None:
    response = client.get("/lock/123")
    assert response.status_code == 404
    assert response.json() == {"detail": {"code": "operation_id_mismatch", "message": "Unknown operation id", "op": 123}}


def test_lock_no_nodes(app: Starlette, client: TestClient) -> None:
    nodes = app.state.coordinator_config.nodes
    nodes.clear()

    # Without nodes, normal lock calls should be about instant
    response = client.post("/lock?locker=x")
    assert response.status_code == 200, response.json()

    response = client.post("/lock?locker=y")
    assert response.status_code == 200, response.json()
    status_url = response.json()["status_url"]
    response = client.get(status_url)
    assert response.status_code == 200, response.json()
    assert response.json() == {"state": "done"}


def test_lock_ok(app: Starlette, client: TestClient) -> None:
    nodes = app.state.coordinator_config.nodes
    with respx.mock:
        for node in nodes:
            respx.post(f"{node.url}/lock?locker=z&ttl=60").respond(json={"locked": True})
        response = client.post("/lock?locker=z")
        assert response.status_code == 200, response.json()

        response = client.get(response.json()["status_url"])
        assert response.status_code == 200, response.json()
        assert response.json() == {"state": "done"}

        assert app.state.coordinator_state.op_info.op_id == 1


def test_lock_onefail(app: Starlette, client: TestClient) -> None:
    nodes = app.state.coordinator_config.nodes
    with respx.mock:
        for i, node in enumerate(nodes):
            respx.post(f"{node.url}/lock?locker=z&ttl=60").respond(json={"locked": i == 0})
            respx.post(f"{node.url}/unlock?locker=z").respond(content=None)
        with patch.object(StatsClient, "increase", return_value=None) as mock_stats_increase:
            response = client.post("/lock?locker=z")
            assert response.status_code == 200, response.json()
        mock_stats_increase.assert_any_call("astacus_lock_call_failure", tags={"call": LockCall.lock, "locker": "z"})

    response = client.get(response.json()["status_url"])
    assert response.status_code == 200, response.json()
    assert response.json() == {"state": "fail"}
