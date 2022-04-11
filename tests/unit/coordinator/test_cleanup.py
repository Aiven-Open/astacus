"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Test that the cleanup endpoint behaves as advertised
"""

from astacus.common import ipc

import pytest
import respx

FAILS = [1, None]


def _run(*, client, populated_mstorage, app, fail_at=None, retention, exp_jsons, exp_digests):
    app.state.coordinator_config.retention = retention
    assert len(populated_mstorage.get_storage("x").list_jsons()) == 2
    populated_mstorage.get_storage("x").upload_hexdigest_bytes("TOBEDELETED", b"x")
    assert len(populated_mstorage.get_storage("x").list_hexdigests()) == 2
    nodes = app.state.coordinator_config.nodes
    with respx.mock:
        for node in nodes:
            respx.post(f"{node.url}/unlock?locker=x&ttl=0").respond(json={"locked": False})
            # Failure point 1: Lock fails
            respx.post(f"{node.url}/lock?locker=x&ttl=60").respond(json={"locked": fail_at != 1})
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
    assert len(populated_mstorage.get_storage("x").list_jsons()) == exp_jsons
    assert len(populated_mstorage.get_storage("x").list_hexdigests()) == exp_digests


@pytest.mark.parametrize("fail_at", FAILS)
def test_api_cleanup_flow(fail_at, client, populated_mstorage, app):
    _run(
        fail_at=fail_at,
        client=client,
        populated_mstorage=populated_mstorage,
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
        (ipc.Retention(minimum_backups=3, maximum_backups=1, keep_days=-1), 2, 2),
        (ipc.Retention(keep_days=100), 0, 0),
        (ipc.Retention(keep_days=10000), 2, 2),
        (ipc.Retention(minimum_backups=1, keep_days=10000), 2, 2),
        (ipc.Retention(maximum_backups=1, keep_days=10000), 1, 1),
    ],
)
def test_api_cleanup_retention(data, client, populated_mstorage, app):
    retention, exp_jsons, exp_digests = data
    _run(
        client=client,
        populated_mstorage=populated_mstorage,
        app=app,
        retention=retention,
        exp_jsons=exp_jsons,
        exp_digests=exp_digests,
    )
