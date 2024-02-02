"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from astacus.common.progress import Progress
from astacus.common.snapshot import SnapshotGroup
from astacus.node.snapshotter import hash_hexdigest_readable
from astacus.node.sqlite_snapshot import SQLiteSnapshot
from io import BytesIO
from pathlib import Path
from tests.unit.node.conftest import build_snapshot_and_snapshotter, create_files_at_path
from time import sleep

import base64
import pytest


@pytest.mark.parametrize("src_is_dst", [True, False])
def test_snapshotter(src: Path, dst: Path, db: Path, src_is_dst: bool) -> None:
    if src_is_dst:
        dst = src
    groups = [
        SnapshotGroup(root_glob="**/aa*", embedded_file_size_max=100),
        SnapshotGroup(root_glob="bb*", embedded_file_size_max=1),
        SnapshotGroup(root_glob="folder2/*c", embedded_file_size_max=None),
    ]
    snapshot, snapshotter = build_snapshot_and_snapshotter(src, dst, db, SQLiteSnapshot, groups)
    matched_under_embedded_file_size_max = [("folder1/aaa", b"aaaaa"), ("folder2/ccc", b"ccccc")]
    matched_over_embedded_file_size_max = [("bbb", b"bbbbb")]
    not_matched = [("ddd", b"ddddd"), ("eee", b"eeeee")]
    create_files_at_path(src, matched_under_embedded_file_size_max + matched_over_embedded_file_size_max + not_matched)
    snapshotter.perform_snapshot(progress=Progress())
    assert len(snapshot) == 3
    for path, content in matched_under_embedded_file_size_max + matched_over_embedded_file_size_max:
        snapshotfile = snapshot.get_file(path)
        assert snapshotfile is not None
        assert snapshotfile.relative_path == path
        assert snapshotfile.file_size == len(content)
        file = dst / path
        assert file.read_bytes() == content
        st = file.stat()
        assert snapshotfile.mtime_ns == st.st_mtime_ns
        assert snapshotfile.file_size == st.st_size
    for path, content in not_matched:
        snapshotfile = snapshot.get_file(path)
        assert snapshotfile is None
    for path, content in matched_over_embedded_file_size_max:
        snapshotfile = snapshot.get_file(path)
        assert snapshotfile is not None
        assert snapshotfile.hexdigest == hash_hexdigest_readable(BytesIO(content))
        assert snapshotfile.content_b64 is None
    for path, content in matched_under_embedded_file_size_max:
        snapshotfile = snapshot.get_file(path)
        assert snapshotfile is not None
        assert snapshotfile.hexdigest == ""
        assert snapshotfile.content_b64 == base64.b64encode(content).decode()


@pytest.mark.parametrize("src_is_dst", [True, False])
@pytest.mark.parametrize("new_file_size", [1024, 2048])
@pytest.mark.parametrize("embedded_file_size_max", [None, 1, 1500, 3000])
@pytest.mark.parametrize("path", ["abc123", "folder/abc123"])
def test_snapshotter_updates_changed_file(
    src: Path,
    dst: Path,
    db: Path,
    src_is_dst: bool,
    new_file_size: int,
    embedded_file_size_max: int,
    path: str,
) -> None:
    if src_is_dst:
        dst = src
    contents = b"x" * 1024
    create_files_at_path(src, [(path, contents)])
    groups = [SnapshotGroup(root_glob="**", embedded_file_size_max=embedded_file_size_max)]
    snapshot, snapshotter = build_snapshot_and_snapshotter(src, dst, db, SQLiteSnapshot, groups)
    snapshotter.perform_snapshot(progress=Progress())
    assert len(snapshot) == 1
    assert snapshot.get_file(path) is not None
    new_contents = b"y" * new_file_size
    if new_file_size == 1024:
        # Make sure mtimes are different
        sleep(0.01)
    create_files_at_path(src, [(path, new_contents)])
    snapshotter.perform_snapshot(progress=Progress())
    assert len(snapshot) == 1
    snapshotfile = snapshot.get_file(path)
    assert snapshotfile is not None
    assert snapshotfile.file_size == new_file_size
    if embedded_file_size_max is None or new_file_size <= embedded_file_size_max:
        assert snapshotfile.hexdigest == ""
        assert snapshotfile.content_b64 == base64.b64encode(new_contents).decode()
    elif new_file_size > embedded_file_size_max:
        assert snapshotfile.hexdigest == hash_hexdigest_readable(BytesIO(new_contents))
        assert snapshotfile.content_b64 is None
    assert (dst / path).read_bytes() == new_contents
    st = (dst / path).stat()
    assert snapshotfile.mtime_ns == st.st_mtime_ns
    assert snapshotfile.file_size == st.st_size


@pytest.mark.parametrize("src_is_dst", [True, False])
@pytest.mark.parametrize("path", ["abc123", "folder/abc123"])
def test_snapshotter_removes_removed_file(src: Path, dst: Path, db: Path, src_is_dst: bool, path: str) -> None:
    if src_is_dst:
        dst = src
    create_files_at_path(src, [(path, b"abc123")])
    snapshot, snapshotter = build_snapshot_and_snapshotter(src, dst, db, SQLiteSnapshot, [SnapshotGroup("**")])
    snapshotter.perform_snapshot(progress=Progress())
    assert len(snapshot) == 1
    assert snapshot.get_file(path) is not None
    (src / path).unlink()
    snapshotter.perform_snapshot(progress=Progress())
    assert len(snapshot) == 0
    assert snapshot.get_file(path) is None
    assert not (dst / path).exists()


def test_snapshotter_release_hash_unlinks_files_but_keeps_metadata(src: Path, dst: Path, db: Path) -> None:
    keep = "keep_this"
    release = "release_this"
    create_files_at_path(src, [(keep, b"this will be kept")])
    create_files_at_path(dst, [(keep, b"this will be kept")])
    create_files_at_path(src, [(release, b"this will be released")])
    create_files_at_path(dst, [(release, b"this will be released")])
    groups = [SnapshotGroup(root_glob="**", embedded_file_size_max=0)]
    snapshot, snapshotter = build_snapshot_and_snapshotter(src, dst, db, SQLiteSnapshot, groups)
    snapshotter.perform_snapshot(progress=Progress())
    to_release = snapshot.get_file(release)
    assert (dst / release).exists()
    assert to_release is not None
    snapshotter.release([to_release.hexdigest], progress=Progress())
    assert not (dst / release).exists()
    assert (dst / keep).exists()
    assert snapshot.get_file(release) is not None
