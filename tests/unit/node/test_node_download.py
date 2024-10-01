"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from astacus.common import ipc, magic, utils
from astacus.common.progress import Progress
from astacus.common.snapshot import SnapshotGroup
from astacus.common.storage import FileStorage, ThreadLocalStorage
from astacus.node.download import Downloader
from astacus.node.sqlite_snapshot import SQLiteSnapshot
from astacus.node.uploader import Uploader
from pathlib import Path
from pytest_mock import MockerFixture
from starlette.testclient import TestClient
from tests.unit.node.conftest import build_snapshot_and_snapshotter, create_files_at_path

import msgspec
import pytest


@pytest.mark.parametrize("src_is_dst", [True, False])
def test_download(
    storage: FileStorage,
    uploader: Uploader,
    root: Path,
    src: Path,
    dst: Path,
    db: Path,
    src_is_dst: bool,
) -> None:
    if src_is_dst:
        dst = src
    create_files_at_path(
        src,
        [
            ("foo", b"foo"),
            ("foo2", b"foo2"),
            ("foobig", b"foobig" * magic.DEFAULT_EMBEDDED_FILE_SIZE),
            ("foobig2", b"foobig2" * magic.DEFAULT_EMBEDDED_FILE_SIZE),
        ],
    )
    snapshot, snapshotter = build_snapshot_and_snapshotter(src, dst, db, SQLiteSnapshot, [SnapshotGroup("**")])
    with snapshotter.lock:
        snapshotter.perform_snapshot(progress=Progress())
        ss1 = snapshotter.get_snapshot_state()
        hashes = list(snapshot.get_all_digests())

    uploader.write_hashes_to_storage(snapshot=snapshot, hashes=hashes, progress=Progress(), parallel=1)

    # Download the old backup from storage
    dst2 = Path(root / "dst2")
    dst2.mkdir()

    dst3 = Path(root / "dst3")
    dst3.mkdir()

    db2 = Path(root / "db2")

    snapshot, snapshotter = build_snapshot_and_snapshotter(dst2, dst3, db2, SQLiteSnapshot, [SnapshotGroup("**")])
    thread_local_storage = ThreadLocalStorage(storage=storage)
    downloader = Downloader(thread_local_storage=thread_local_storage, snapshotter=snapshotter, dst=dst2, parallel=1)
    with snapshotter.lock:
        downloader.download_from_storage(progress=Progress(), snapshotstate=ss1)

        # And ensure we get same snapshot state by snapshotting it
        snapshotter.perform_snapshot(progress=Progress())
        ss2 = snapshotter.get_snapshot_state()

    # Ensure the files are same (modulo mtime_ns, which doesn't
    # guaranteedly hit quite same numbers)
    for ssfile1, ssfile2 in zip(ss1.files, ss2.files):
        assert ssfile1.equals_excluding_mtime(ssfile2)


def test_api_download(client: TestClient, mocker: MockerFixture) -> None:
    mocker.patch.object(utils, "http_request")
    response = client.post("/node/download")
    assert response.status_code == 422, response.json()


def test_api_clear(client: TestClient, mocker: MockerFixture) -> None:
    url = "http://addr/result"
    m = mocker.patch.object(utils, "http_request")
    # Actual restoration is painful. So we trust above test_download
    # (and systest) to work, and pass empty list of files to be
    # downloaded
    req_json = {"result_url": url, "root_globs": ["*"]}

    response = client.post("/node/clear", json=req_json)
    assert response.status_code == 409, response.json()

    response = client.post("/node/lock?locker=x&ttl=10")
    assert response.status_code == 200, response.json()
    response = client.post("/node/clear")
    assert response.status_code == 422, response.json()

    response = client.post("/node/clear", json=req_json)
    assert response.status_code == 200, response.json()

    # Decode the (result endpoint) response using the model
    response = m.call_args[1]["data"]
    assert isinstance(response, bytes)
    result = msgspec.json.decode(response, type=ipc.NodeResult)
    assert result.progress.finished_successfully
