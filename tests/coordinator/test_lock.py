"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Test that the coordinator lock endpoint works.

"""

from astacus.coordinator.config import Node


def test_lock_no_nodes(client):
    # Without nodes, normal lock calls should be about instant
    response = client.post("/lock?locker=x")
    assert response.status_code == 200, response.json()

    response = client.post("/lock?locker=y")
    assert response.status_code == 200, response.json()
    response = client.get(response.json()["status-url"])
    assert response.status_code == 200, response.json()
    assert response.json() == {"state": "done"}


def test_lock_fail(app, client, utils_http_request_list):
    # Add fake node that won't respond to API -> state won't be done
    app.state.coordinator_config.nodes = [
        Node(url="http://localhost:12345/asdf")
    ]
    utils_http_request_list.append({
        "args": ["http://localhost:12345/asdf/lock?locker=z&ttl=60"],
        "kwargs": {
            "caller": "LockerFSM.lock"
        },
        "result":
        None
    })
    response = client.post("/lock?locker=z")
    assert response.status_code == 200, response.json()
    assert not utils_http_request_list

    response = client.get(response.json()["status-url"])
    assert response.status_code == 200, response.json()
    assert response.json() == {"state": "fail"}


def test_lock_ok(app, client, utils_http_request_list):
    app.state.coordinator_config.nodes = [
        Node(url="http://localhost:12346/asdf"),
        Node(url="http://localhost:12345/asdf"),
    ]
    # locker handles locking from last to first
    utils_http_request_list.extend([{
        "args": ["http://localhost:12345/asdf/lock?locker=z&ttl=60"],
        "result": {
            "locked": True
        }
    }, {
        "args": ["http://localhost:12346/asdf/lock?locker=z&ttl=60"],
        "result": {
            "locked": True
        }
    }])
    response = client.post("/lock?locker=z")
    assert response.status_code == 200, response.json()

    response = client.get(response.json()["status-url"])
    assert response.status_code == 200, response.json()
    assert response.json() == {"state": "done"}

    assert app.state.coordinator_state.op == 1
    assert app.state.coordinator_state.fsm.fsm_class == "astacus.coordinator.locker.LockerFSM"


def test_lock_onefail(app, client, utils_http_request_list):
    app.state.coordinator_config.nodes = [
        Node(url="http://localhost:12346/asdf"),
        Node(url="http://localhost:12345/asdf"),
    ]
    # locker handles locking from last to first
    utils_http_request_list.extend([{
        "args": ["http://localhost:12345/asdf/lock?locker=z&ttl=60"],
        "result": {
            "locked": True
        }
    }, {
        "args": ["http://localhost:12346/asdf/lock?locker=z&ttl=60"],
        "result":
        None
    }, {
        "args": ["http://localhost:12345/asdf/unlock?locker=z"],
        "result": {
            "locked": False
        },
    }])
    response = client.post("/lock?locker=z")
    assert response.status_code == 200, response.json()
    assert not utils_http_request_list

    response = client.get(response.json()["status-url"])
    assert response.status_code == 200, response.json()
    assert response.json() == {"state": "fail"}
