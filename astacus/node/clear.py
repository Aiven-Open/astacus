"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Clearing of snapshot storage (separate from empty 'download' step for clarity)

"""

from .node import NodeOp
from .snapshotter import Snapshotter
from astacus.common import ipc
from astacus.common.progress import Progress

import logging

logger = logging.getLogger(__name__)


class ClearOp(NodeOp[ipc.SnapshotClearRequest, ipc.NodeResult]):
    snapshotter: Snapshotter | None = None
    is_snaphot_outdated: bool = True

    def create_result(self) -> ipc.NodeResult:
        return ipc.NodeResult()

    def start(self, snapshotter: Snapshotter, *, is_snapshot_outdated: bool) -> NodeOp.StartResult:
        logger.info("start_clear %r", self.req)
        self.is_snaphot_outdated = is_snapshot_outdated
        self.snapshotter = snapshotter
        return self.start_op(op_name="clear", op=self, fun=self.clear)

    def clear(self) -> None:
        assert self.snapshotter is not None
        with self.snapshotter.lock:
            self.check_op_id()
            if self.is_snaphot_outdated:
                self.snapshotter.perform_snapshot(progress=Progress())
            progress = self.result.progress
            progress.start(len(self.snapshotter.snapshot))
            for relative_path in self.snapshotter.snapshot.get_all_paths():
                absolute_path = self.config.root / relative_path
                try:
                    absolute_path.unlink(missing_ok=True)
                except PermissionError as e:
                    logger.error("Failed to clear: %s", absolute_path)
                progress.add_success()
            progress.done()
