"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from astacus.node.progress import Progress
from astacus.node.restore import Restorer
from astacus.node.snapshot import Snapshotter
from pathlib import Path


def test_restore(snapshotter, storage, tmpdir):
    snapshotter.create_2foobar()
    ss1 = snapshotter.get_snapshot_state()
    blocks = snapshotter.get_snapshot_hashes()
    snapshotter.write_hashes_to_storage(hashes=blocks,
                                        storage=storage,
                                        progress=Progress())

    # Restore the old backup from storage
    dst2 = Path(tmpdir / "dst2")
    dst2.mkdir()

    restorer = Restorer(dst=dst2)

    restorer.restore_from_storage(storage=storage,
                                  progress=Progress(),
                                  snapshotstate=ss1)

    # And ensure we get same snapshot state by snapshotting it
    dst3 = Path(tmpdir / "dst3")
    dst3.mkdir()
    snapshotter = Snapshotter(src=dst2, dst=dst3, globs=["*"])
    assert snapshotter.snapshot(progress=Progress()) > 0
    ss2 = snapshotter.get_snapshot_state()
    # Unfortunately, mtimes aren't quite properly updated 1-1 (or
    # there is no guarantee of it). So fix the files' mtimes by hand.
    relpath2snapshotfile = {
        snapshotfile.relative_path: snapshotfile
        for snapshotfile in ss1.files
    }
    for snapshotfile in ss2.files:
        snapshotfile.mtime_ns = relpath2snapshotfile[
            snapshotfile.relative_path].mtime_ns
    assert ss1 == ss2
