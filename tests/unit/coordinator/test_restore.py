"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Test that the coordinator restore endpoint works.

"""

from astacus.common import exceptions, ipc
from astacus.coordinator.config import CoordinatorNode
from astacus.coordinator.plugins import get_plugin_restore_class
from contextlib import nullcontext as does_not_raise
from datetime import datetime
from pathlib import Path

import pytest
import respx

FAILS = [1, 2, None]

BACKUP_NAME = "dummybackup"

BACKUP_MANIFEST = ipc.BackupManifest(
    start="2020-01-01 21:43:00",
    end="2020-02-02 12:34:56",
    attempt=1,
    snapshot_results=[
        ipc.SnapshotResult(
            state=ipc.SnapshotState(
                root_globs=["*"],
                files=[ipc.SnapshotFile(relative_path=Path("foo"), file_size=6, mtime_ns=0, hexdigest="DEADBEEF")]
            ),
            hashes=[ipc.SnapshotHash(hexdigest="DEADBEEF", size=6)],
            files=1,
            total_size=6,
        )
    ],
    upload_results=[
        ipc.SnapshotUploadResult(total_size=6, total_stored_size=10),
    ],
    plugin="files",
)


@pytest.mark.parametrize("fail_at", FAILS)
def test_restore(fail_at, app, client, storage):
    # Create fake backup (not pretty but sufficient?)
    storage.upload_json(BACKUP_NAME, BACKUP_MANIFEST)
    nodes = app.state.coordinator_config.nodes
    with respx.mock:
        for node in nodes:
            respx.post(f"{node.url}/unlock?locker=x&ttl=0", content={"locked": False})
            # Failure point 1: Lock fails
            respx.post(f"{node.url}/lock?locker=x&ttl=60", content={"locked": fail_at != 1})

            # Failure point 2: download call fails
            if fail_at != 2:
                respx.post(f"{node.url}/download", content={"op_id": 42, "status_url": f"{node.url}/download/result"})

            # Failure point 3: download result call fails
            if fail_at != 3:
                respx.get(f"{node.url}/download/result", content={
                    "progress": {
                        "final": True
                    },
                })

        response = client.post("/restore")
        assert response.status_code == 200, response.json()

        response = client.get(response.json()["status_url"])
        assert response.status_code == 200, response.json()
        if fail_at:
            assert response.json() == {"state": "fail"}
        else:
            assert response.json() == {"state": "done"}

        assert app.state.coordinator_state.op_info.op_id == 1


_RestoreOp = get_plugin_restore_class("files")


class DummyRestoreOp(_RestoreOp):
    def __init__(self, nodes, manifest):
        # pylint: disable=super-init-not-called
        # NOP __init__, we mock whatever we care about
        self.nodes = nodes
        self.result_backup_manifest = manifest

    def assert_node_to_backup_index_is(self, expected):
        assert self._get_node_to_backup_index() == expected


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
    ]
)
def test_node_to_backup_index(node_azlist, backup_azlist, expected_index, exception):
    nodes = [CoordinatorNode(url="unused", az=az) for az in node_azlist]
    manifest = ipc.BackupManifest(
        start=datetime.utcnow(),
        attempt=1,
        snapshot_results=[ipc.SnapshotResult(az=az) for az in backup_azlist],
        upload_results=[],
        plugin="files"
    )

    op = DummyRestoreOp(nodes, manifest)
    with exception:
        op.assert_node_to_backup_index_is(expected_index)
