"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from astacus.common import ipc, utils
from astacus.common.progress import Progress
from astacus.node.download import Downloader
from astacus.node.snapshotter import Snapshotter
from pathlib import Path


def test_download(snapshotter, uploader, storage, tmpdir):
    snapshotter.create_4foobar()
    ss1 = snapshotter.get_snapshot_state()
    hashes = snapshotter.get_snapshot_hashes()

    uploader.write_hashes_to_storage(snapshotter=snapshotter, hashes=hashes, progress=Progress(), parallel=1)

    # Download the old backup from storage
    dst2 = Path(tmpdir / "dst2")
    dst2.mkdir()

    dst3 = Path(tmpdir / "dst3")
    dst3.mkdir()
    snapshotter = Snapshotter(src=dst2, dst=dst3, globs=["*"])
    downloader = Downloader(storage=storage, snapshotter=snapshotter, dst=dst2, parallel=1)

    downloader.download_from_storage(progress=Progress(), snapshotstate=ss1)

    # And ensure we get same snapshot state by snapshotting it
    assert snapshotter.snapshot(progress=Progress()) > 0
    ss2 = snapshotter.get_snapshot_state()

    # Ensure the files are same (modulo mtime_ns, which doesn't
    # guaranteedly hit quite same numbers)
    for ssfile1, ssfile2 in zip(ss1.files, ss2.files):
        assert ssfile1.equals_excluding_mtime(ssfile2)


def test_api_download(client, mocker):
    url = "http://addr/result"
    m = mocker.patch.object(utils, "http_request")
    response = client.post("/node/download")
    assert response.status_code == 422, response.json()

    # Actual restoration is painful. So we trust above test_download to work,
    # and pass empty list of files to be downloaded
    req_json = {"result_url": url, "storage": "x", "state": {"root_globs": ["*"], "files": []}}

    response = client.post("/node/download", json=req_json)
    assert response.status_code == 409, response.json()

    response = client.post("/node/lock?locker=x&ttl=10")
    assert response.status_code == 200, response.json()
    response = client.post("/node/download")
    assert response.status_code == 422, response.json()

    response = client.post("/node/download", json=req_json)
    assert response.status_code == 200, response.json()

    # Decode the (result endpoint) response using the model
    response = m.call_args[1]["data"]
    result = ipc.NodeResult.parse_raw(response)
    assert result.progress.finished_successfully
