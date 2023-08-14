"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from astacus.common.progress import Progress
from astacus.common.snapshot import SnapshotGroup
from astacus.node.snapshotter import Snapshotter
from pathlib import Path


def test_snapshotter_with_src_equal_dst_forgets_file_from_previous_snapshot(tmp_path: Path) -> None:
    src_and_dst = tmp_path
    file_before = src_and_dst / "file_before"
    file_before.write_bytes(b"x" * 1024)
    snapshotter = Snapshotter(src=src_and_dst, dst=src_and_dst, groups=[SnapshotGroup(root_glob="*")], parallel=1)
    with snapshotter.lock:
        snapshotter.snapshot(progress=Progress())
        assert snapshotter.relative_path_to_snapshotfile.keys() == {Path("file_before")}
        file_before.unlink()
        file_after = src_and_dst / "file_after"
        file_after.write_bytes(b"y" * 1024)
        snapshotter.snapshot(progress=Progress())
        assert snapshotter.relative_path_to_snapshotfile.keys() == {Path("file_after")}
