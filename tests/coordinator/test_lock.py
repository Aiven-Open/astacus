"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Test that the coordinator lock endpoint works.

"""

from astacus.coordinator.config import CoordinatorConfig

import respx

_CCNode = CoordinatorConfig.Node


def test_status_nonexistent(client):
    response = client.get("/lock/123")
    assert response.status_code == 404
    assert response.json() == {
        "detail": {
            "code": "operation_id_mismatch",
            "message": "Unknown operation id",
            "op": 123
        }
    }


def test_lock_no_nodes(client):
    # Without nodes, normal lock calls should be about instant
    response = client.post("/lock?locker=x")
    assert response.status_code == 200, response.json()

    response = client.post("/lock?locker=y")
    assert response.status_code == 200, response.json()
    status_url = response.json()["status-url"]
    response = client.get(status_url)
    assert response.status_code == 200, response.json()
    assert response.json() == {"state": "done"}


def test_lock_fail(app, client):
    # Add fake node that won't respond to API -> state won't be done
    app.state.coordinator_config.nodes = [
        _CCNode(url="http://localhost:12345/asdf")
    ]
    with respx.mock:
        respx.post("http://localhost:12345/asdf/lock?locker=z&ttl=60",
                   content=None)
        response = client.post("/lock?locker=z")
        assert response.status_code == 200, response.json()

    response = client.get(response.json()["status-url"])
    assert response.status_code == 200, response.json()
    assert response.json() == {"state": "fail"}


def test_lock_ok(app, client):
    nodes = [
        _CCNode(url="http://localhost:12346/asdf"),
        _CCNode(url="http://localhost:12345/asdf"),
    ]
    app.state.coordinator_config.nodes = nodes
    with respx.mock:
        for node in nodes:
            respx.post(f"{node.url}/lock?locker=z&ttl=60",
                       content={"locked": True})
        response = client.post("/lock?locker=z")
        assert response.status_code == 200, response.json()

        response = client.get(response.json()["status-url"])
        assert response.status_code == 200, response.json()
        assert response.json() == {"state": "done"}

        assert app.state.coordinator_state.op_info.op_id == 1


def test_lock_onefail(app, client):
    nodes = [
        _CCNode(url="http://localhost:12346/asdf"),
        _CCNode(url="http://localhost:12345/asdf"),
    ]
    app.state.coordinator_config.nodes = nodes
    with respx.mock:
        for i, node in enumerate(nodes):
            respx.post(f"{node.url}/lock?locker=z&ttl=60",
                       content={"locked": i == 0})
            respx.post(f"{node.url}/unlock?locker=z", content=None)
        response = client.post("/lock?locker=z")
        assert response.status_code == 200, response.json()

    response = client.get(response.json()["status-url"])
    assert response.status_code == 200, response.json()
    assert response.json() == {"state": "fail"}
