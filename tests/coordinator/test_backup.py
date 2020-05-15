"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Test that the coordinator backup endpoint works.

"""

from astacus.coordinator.config import CoordinatorConfig
from astacus.coordinator.coordinator import CoordinatorOpWithClusterLock

import pytest
import respx

_CCNode = CoordinatorConfig.Node

FAILS = [1, 2, 3, 4, 5, None]


@pytest.mark.parametrize("fail_at", FAILS)
def test_backup(fail_at, app, client, mocker, sleepless):  # pylint: disable=unused-argument
    mocker.patch.object(CoordinatorOpWithClusterLock, 'get_locker', return_value='x')
    nodes = [
        _CCNode(url="http://localhost:12345/asdf"),
        _CCNode(url="http://localhost:12346/asdf"),
    ]
    app.state.coordinator_config.nodes = nodes
    with respx.mock:
        for node in nodes:
            respx.post(f"{node.url}/unlock?locker=x&ttl=0", content={"locked": False})
            # Failure point 1: Lock fails
            respx.post(f"{node.url}/lock?locker=x&ttl=60", content={"locked": fail_at != 1})
            # Failure point 2: snapshot call fails
            if fail_at != 2:
                respx.post(f"{node.url}/snapshot", content={"op_id": 42, "status_url": f"{node.url}/snapshot/result"})
            # Failure point 3: snapshot result call fails
            if fail_at != 3:
                respx.get(f"{node.url}/snapshot/result", content={"progress": {"final": True}, "hashes": ["HASH"]})
            # Failure point 4: upload call fails
            if fail_at != 4:
                respx.post(f"{node.url}/upload", content={"op_id": 43, "status_url": f"{node.url}/upload/result"})

            # Failure point 5: upload result call fails
            if fail_at != 5:
                respx.get(f"{node.url}/upload/result", content={"progress": {"final": True}})

        response = client.post("/backup")
        assert response.status_code == 200, response.json()

        response = client.get(response.json()["status_url"])
        assert response.status_code == 200, response.json()
        if fail_at:
            assert response.json() == {"state": "fail"}
        else:
            assert response.json() == {"state": "done"}

        assert app.state.coordinator_state.op_info.op_id == 1
