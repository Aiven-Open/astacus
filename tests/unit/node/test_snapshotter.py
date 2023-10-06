"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from astacus.common.progress import Progress
from astacus.common.snapshot import SnapshotGroup
from astacus.node.snapshotter import hash_hexdigest_readable, Snapshotter
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


def assert_kept(hexdigest: str, dst_path: Path, snapshotter: Snapshotter):
    assert dst_path.exists()
    assert hexdigest in snapshotter.hexdigest_to_snapshotfiles
    assert Path(dst_path.name) in snapshotter.relative_path_to_snapshotfile


def assert_released(hexdigest: str, dst_path: Path, snapshotter: Snapshotter):
    assert not dst_path.exists()
    assert hexdigest in snapshotter.hexdigest_to_snapshotfiles
    assert Path(dst_path.name) in snapshotter.relative_path_to_snapshotfile


def test_snapshotter_release_hash_unlinks_files_but_keeps_metadata(tmp_path: Path) -> None:
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    src.mkdir()
    dst.mkdir()
    (src / "keep_this").write_text("this will be kept")
    kept_digest = hash_hexdigest_readable((src / "keep_this").open(mode="rb"))
    (src / "release_this").write_text("this will be released")
    released_digest = hash_hexdigest_readable((src / "release_this").open(mode="rb"))
    snapshotter = Snapshotter(src=src, dst=dst, groups=[SnapshotGroup(root_glob="*", embedded_file_size_max=0)], parallel=1)

    with snapshotter.lock:
        snapshotter.snapshot(progress=Progress())
        assert_kept(kept_digest, dst / "keep_this", snapshotter)
        assert_kept(released_digest, dst / "release_this", snapshotter)

        snapshotter.release(released_digest)
        assert_kept(kept_digest, dst / "keep_this", snapshotter)
        assert_released(released_digest, dst / "release_this", snapshotter)

        # re-snapshotting should restore the link
        (src / "add_this").write_text("this is added for the next snapshot")
        added_digest = hash_hexdigest_readable((src / "add_this").open(mode="rb"))
        snapshotter.snapshot(progress=Progress())
        assert_kept(kept_digest, dst / "keep_this", snapshotter)
        assert_kept(released_digest, dst / "release_this", snapshotter)
        assert_kept(added_digest, dst / "add_this", snapshotter)
