"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Clearing of snapshot storage (separate from empty 'download' step for clarity)

"""

from .node import NodeOp
from .snapshotter import Snapshotter
from astacus.common import ipc
from astacus.common.progress import Progress
from astacus.common.snapshot import SnapshotGroup
from astacus.node.snapshot import Snapshot
from typing import Optional

import contextlib
import logging

logger = logging.getLogger(__name__)


class ClearOp(NodeOp[ipc.SnapshotClearRequest, ipc.NodeResult]):
    snapshotter: Optional[Snapshotter] = None
    snapshotter: Snapshotter | None = None
    snapshot: Snapshot | None = None
    is_snaphot_outdated: bool = True

    def create_result(self) -> ipc.NodeResult:
        return ipc.NodeResult()

    def start(self, snapshotter: Snapshotter, *, is_snapshot_outdated: bool) -> NodeOp.StartResult:
        groups = [SnapshotGroup(root_glob=root_glob) for root_glob in self.req.root_globs]
        self.snapshot, self.snapshotter = self.get_snapshot_and_snapshotter(groups)
        logger.info("start_clear %r", self.req)
        self.snapshotter = snapshotter
        return self.start_op(op_name="clear", op=self, fun=self.clear)

    def clear(self) -> None:
        assert self.snapshotter and self.snapshot
        # 'snapshotter' is global; ensure we have sole access to it
        with self.snapshot.lock:
            self.check_op_id()
            if self.is_snaphot_outdated:
                self.snapshotter.snapshot(progress=Progress())
            self.snapshotter.perform_snapshot(progress=Progress())
            progress = self.result.progress
            progress.start(len(self.snapshot))
            for relative_path in progress.wrap(self.snapshot.get_all_paths()):
                absolute_path = self.config.root / relative_path
                with contextlib.suppress(FileNotFoundError):
                    absolute_path.unlink()
