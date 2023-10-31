from astacus.common.progress import Progress
from astacus.common.snapshot import SnapshotGroup
from astacus.node.memory_snapshot import MemorySnapshot, MemorySnapshotter
from astacus.node.sqlite_snapshot import SQLiteSnapshot, SQLiteSnapshotter
from pathlib import Path
from tempfile import TemporaryDirectory

import sys


def main() -> None:
    snapshot_type, src_str, dst_str, parallel_str = sys.argv[1:]
    src = Path(src_str)
    dst = Path(dst_str)
    parallel = int(parallel_str)
    groups = [SnapshotGroup(root_glob="**")]
    with TemporaryDirectory() as dir:
        if snapshot_type == "memory":
            snapshot = MemorySnapshot(dst)
            snapshotter = MemorySnapshotter(groups, src, dst, snapshot, parallel)
        elif snapshot_type == "sqlite":
            db = Path("/tmp") / "db.sqlite"
            db.unlink()
            snapshot = SQLiteSnapshot(dst, db)
            snapshotter = SQLiteSnapshotter(groups, src, dst, snapshot, parallel)
        else:
            raise ValueError(f"Unknown snapshot type {snapshot_type}")
        snapshotter.perform_snapshot(progress=Progress())


if __name__ == "__main__":
    main()
