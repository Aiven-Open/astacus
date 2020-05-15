"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from astacus.common import ipc, utils
from astacus.common.progress import Progress
from pathlib import Path

import pytest


@pytest.mark.timeout(2)
def test_snapshot(snapshotter, storage):
    # Start with empty
    assert snapshotter.snapshot(progress=Progress()) == 0
    src = snapshotter.src
    dst = snapshotter.dst
    assert not (dst / "foo").is_file()

    # Create files in src, run snapshot
    snapshotter.create_2foobar()
    ss2 = snapshotter.get_snapshot_state()

    assert (dst / "foo").is_file()
    assert (dst / "foo").read_text() == "foobar"
    assert (dst / "foo2").read_text() == "foobar"

    blocks = snapshotter.get_snapshot_hashes()
    assert len(blocks) == 1
    assert len(blocks[0]) == 64  # in hex, so 32 bytes of hash = 256 bit hash

    while True:
        (src / "foo").write_text("barfoo")  # same length
        if snapshotter.snapshot(progress=Progress()) > 0:
            # Sometimes fails on first iteration(s) due to same mtime
            # (inaccurate timestamps)
            break
    ss3 = snapshotter.get_snapshot_state()
    assert ss2 != ss3
    assert snapshotter.snapshot(progress=Progress()) == 0
    assert (dst / "foo").is_file()
    assert (dst / "foo").read_text() == "barfoo"

    snapshotter.write_hashes_to_storage(hashes=blocks, storage=storage, progress=Progress())

    # Remove file from src, run snapshot
    (src / "foo").unlink()
    assert snapshotter.snapshot(progress=Progress()) > 0
    assert snapshotter.snapshot(progress=Progress()) == 0
    assert not (dst / "foo").is_file()

    # Remove last file from src, run snapshot
    (src / "foo2").unlink()
    assert snapshotter.snapshot(progress=Progress()) > 0
    assert snapshotter.snapshot(progress=Progress()) == 0
    assert not (dst / "foo2").is_file()

    # Now shouldn't have any data blocks
    blocks_empty = snapshotter.get_snapshot_hashes()
    assert not blocks_empty


def test_api_snapshot_and_upload(client, mocker, tmpdir):
    url = 'http://addr/result'
    m = mocker.patch.object(utils, "http_request")
    response = client.post("/node/snapshot")
    assert response.status_code == 409, response.json()

    response = client.post("/node/lock?locker=x&ttl=10")
    assert response.status_code == 200, response.json()
    response = client.post("/node/snapshot")
    assert response.status_code == 200, response.json()

    response = client.post("/node/snapshot", json={"result_url": url})
    assert response.status_code == 200, response.json()

    # Decode the (result endpoint) response using the model
    response = m.call_args[1]["data"]
    result = ipc.SnapshotResult.parse_raw(response)
    assert result.progress.finished_successfully
    assert result.hashes
    hexdigest = result.hashes[0]

    # Ask it to be uploaded
    response = client.post("/node/upload", json={"hashes": result.hashes, "result_url": url})
    assert response.status_code == 200, response.json()
    response = m.call_args[1]["data"]
    result = ipc.SnapshotResult.parse_raw(response)
    assert result.progress.finished_successfully

    assert (Path(tmpdir) / "backup-root" / f"{hexdigest}.dat").is_file()
