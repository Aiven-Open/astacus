"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Test that the coordinator restore endpoint works.

"""
from astacus.common import exceptions, ipc
from astacus.common.ipc import Plugin
from astacus.coordinator.config import CoordinatorNode
from astacus.coordinator.plugins.base import get_node_to_backup_index
from contextlib import nullcontext as does_not_raise
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

import httpx
import json
import pydantic
import pytest
import respx

BACKUP_NAME = "dummybackup"

BACKUP_MANIFEST = ipc.BackupManifest(
    start="2020-01-01 21:43:00Z",
    end="2020-02-02 12:34:56Z",
    attempt=1,
    snapshot_results=[
        ipc.SnapshotResult(
            state=ipc.SnapshotState(
                root_globs=["*"],
                files=[ipc.SnapshotFile(relative_path=Path("foo"), file_size=6, mtime_ns=0, hexdigest="DEADBEEF")],
            ),
            hashes=[ipc.SnapshotHash(hexdigest="DEADBEEF", size=6)],
            files=1,
            total_size=6,
        )
    ],
    upload_results=[
        ipc.SnapshotUploadResult(total_size=6, total_stored_size=10),
    ],
    plugin=Plugin.files,
)


@dataclass
class RestoreTest:
    fail_at: Optional[int] = None
    partial: bool = False
    storage_name: Optional[str] = None


@pytest.mark.parametrize(
    "rt",
    # failures: [0] - [4]
    [RestoreTest(fail_at=i) for i in range(1, 6)]
    + [
        # success cases
        RestoreTest(),  # default
        # named storage
        RestoreTest(storage_name="x"),
        RestoreTest(storage_name="y"),
        # partial
        RestoreTest(partial=True),
    ],
)
def test_restore(rt, app, client, mstorage):
    # pylint: disable=too-many-statements
    # Create fake backup (not pretty but sufficient?)
    storage = mstorage.get_storage(rt.storage_name)
    storage.upload_json(BACKUP_NAME, BACKUP_MANIFEST)
    nodes = app.state.coordinator_config.nodes
    with respx.mock:
        for i, node in enumerate(nodes):
            respx.post(f"{node.url}/unlock?locker=x&ttl=0").respond(json={"locked": False})
            # Failure point 1: Lock fails
            respx.post(f"{node.url}/lock?locker=x&ttl=60").respond(json={"locked": rt.fail_at != 1})
            if i == 0:
                # Failure point 2: download call fails
                def get_match_download(node_url: str) -> Callable[[httpx.Request], httpx.Response]:
                    def match_download(request: httpx.Request) -> Optional[httpx.Response]:
                        if rt.fail_at == 2:
                            return None
                        if json.loads(request.read())["storage"] != storage.storage_name:
                            return None
                        if json.loads(request.read())["root_globs"] != ["*"]:
                            return None
                        return httpx.Response(
                            status_code=200, json={"op_id": 42, "status_url": f"{node_url}/download/result"}
                        )

                    return match_download

                respx.post(url=f"{node.url}/download").mock(side_effect=get_match_download(node.url))

                # Failure point 3: download result call fails
                respx.get(f"{node.url}/download/result").respond(
                    json={
                        "progress": {"handled": 10, "failed": 0, "total": 10, "final": True},
                    },
                    status_code=200 if rt.fail_at != 3 else 500,
                )
            else:

                def get_match_clear(node_url: str) -> Callable[[httpx.Request], httpx.Response]:
                    def match_clear(request: httpx.Request) -> Optional[httpx.Response]:
                        if rt.fail_at == 4:
                            return None
                        if json.loads(request.read())["root_globs"] != ["*"]:
                            return None
                        return httpx.Response(status_code=200, json={"op_id": 42, "status_url": f"{node_url}/clear/result"})

                    return match_clear

                respx.post(url=f"{node.url}/clear").mock(side_effect=get_match_clear(node.url))

                # Failure point 5: clear result call fails
                respx.get(f"{node.url}/clear/result").respond(
                    json={
                        "progress": {"final": True},
                    },
                    status_code=200 if rt.fail_at != 5 else 500,
                )

        req = {}
        if rt.storage_name:
            req["storage"] = rt.storage_name
        if rt.partial:
            req["partial_restore_nodes"] = [{"node_index": 0, "backup_index": 0}]
        response = client.post("/restore", json=req)
        if rt.fail_at == 1:
            # Cluster lock failure is immediate
            assert response.status_code == 409, response.json()
            assert app.state.coordinator_state.op_info.op_id == 0
            return
        assert response.status_code == 200, response.json()

        response = client.get(response.json()["status_url"])
        assert response.status_code == 200, response.json()
        if rt.fail_at:
            assert response.json().get("state") == "fail"
            assert response.json().get("progress") is not None
            assert response.json().get("progress")["final"]
        else:
            assert response.json().get("state") == "done"
            assert response.json().get("progress") is not None
            assert response.json().get("progress")["final"]
        if rt.fail_at == 5 or rt.fail_at is None:
            assert response.json().get("progress")["handled"] == 10
            assert response.json().get("progress")["failed"] == 0
            assert response.json().get("progress")["total"] == 10
        else:
            assert response.json().get("progress")["handled"] == 0
            assert response.json().get("progress")["failed"] == 0
            assert response.json().get("progress")["total"] == 0

        assert app.state.coordinator_state.op_info.op_id == 1


@pytest.mark.parametrize(
    "node_azlist,backup_azlist,expected_index,exception",
    [
        # successful cases
        ([], [], [], does_not_raise()),
        (["foo", "foo", "bar"], ["1", "2", "2"], [1, 2, 0], does_not_raise()),
        (["a", "bb", "bb", "ccc", "ccc", "ccc"], ["1", "2", "2"], [None, 0, None, 1, 2, None], does_not_raise()),
        (["a", "bb", "bb", "ccc", "ccc", "ccc"], ["3", "3", "3", "1", "2", "2"], [3, 4, 5, 0, 1, 2], does_not_raise()),
        # errors
        (["foo", "foo"], ["1", "2", "2"], None, pytest.raises(exceptions.InsufficientNodesException)),
        (["foo", "foo", "foo"], ["1", "2", "2"], None, pytest.raises(exceptions.InsufficientAZsException)),
        (["foo", "foo", "bar", "bar"], ["1", "3", "3", "3"], None, pytest.raises(exceptions.InsufficientNodesException)),
    ],
)
def test_node_to_backup_index(node_azlist, backup_azlist, expected_index, exception):
    snapshot_results = [ipc.SnapshotResult(az=az) for az in backup_azlist]
    nodes = [CoordinatorNode(url="unused", az=az) for az in node_azlist]
    with exception:
        assert expected_index == get_node_to_backup_index(
            partial_restore_nodes=None, snapshot_results=snapshot_results, nodes=nodes
        )


@pytest.mark.parametrize(
    "partial_node_spec,expected_index,exception",
    [
        # 4 (supported) ways of expressing same thing
        ({"backup_index": 1, "node_index": 2}, [None, None, 1], does_not_raise()),
        ({"backup_hostname": "host1", "node_index": 2}, [None, None, 1], does_not_raise()),
        ({"backup_index": 1, "node_url": "url2"}, [None, None, 1], does_not_raise()),
        ({"backup_hostname": "host1", "node_url": "url2"}, [None, None, 1], does_not_raise()),
        # errors - invalid node spec
        ({}, None, pytest.raises(pydantic.ValidationError)),
        ({"backup_index": 42, "backup_hostname": "foo", "node_index": 42}, None, pytest.raises(pydantic.ValidationError)),
        ({"backup_index": 42, "node_index": 42, "node_url": "foo"}, None, pytest.raises(pydantic.ValidationError)),
        # out of range
        ({"backup_index": -1, "node_index": 2}, [None, None, 1], pytest.raises(exceptions.NotFoundException)),
        ({"backup_index": 1, "node_index": -2}, [None, None, 1], pytest.raises(exceptions.NotFoundException)),
        ({"backup_index": 123, "node_index": 2}, [None, None, 1], pytest.raises(exceptions.NotFoundException)),
        ({"backup_index": 1, "node_index": 123}, [None, None, 1], pytest.raises(exceptions.NotFoundException)),
        # invalid url / hostname
        ({"backup_hostname": "host123", "node_index": 2}, [None, None, 1], pytest.raises(exceptions.NotFoundException)),
        ({"backup_index": 1, "node_url": "url123"}, [None, None, 1], pytest.raises(exceptions.NotFoundException)),
    ],
)
def test_partial_node_to_backup_index(partial_node_spec, expected_index, exception):
    num_nodes = 3
    snapshot_results = [ipc.SnapshotResult(hostname=f"host{i}") for i in range(num_nodes)]
    nodes = [CoordinatorNode(url=f"url{i}") for i in range(num_nodes)]
    with exception:
        partial_restore_nodes = [ipc.PartialRestoreRequestNode.parse_obj(partial_node_spec)]
        assert expected_index == get_node_to_backup_index(
            partial_restore_nodes=partial_restore_nodes, snapshot_results=snapshot_results, nodes=nodes
        )
