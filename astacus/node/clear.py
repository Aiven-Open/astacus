"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Clearing of snapshot storage (separate from empty 'download' step for clarity)

"""

from .node import NodeOp
from .snapshotter import Snapshotter
from astacus.common import ipc
from typing import Optional

import contextlib
import logging

logger = logging.getLogger(__name__)


class ClearOp(NodeOp):
    snapshotter: Optional[Snapshotter] = None

    def start(self, *, req: ipc.SnapshotClearRequest):
        self.req = req
        self.snapshotter = self.get_or_create_snapshotter(req.root_globs)
        logger.debug("start_clear %r", req)
        return self.start_op(op_name="clear", op=self, fun=self.clear)

    def clear(self):
        assert self.snapshotter
        # 'snapshotter' is global; ensure we have sole access to it
        with self.snapshotter.lock:
            self.check_op_id()
            self.snapshotter.snapshot()
            files = set(self.snapshotter.relative_path_to_snapshotfile.keys())
            progress = self.result.progress
            progress.start(len(files))
            for relative_path in files:
                absolute_path = self.config.root / relative_path
                with contextlib.suppress(FileNotFoundError):
                    absolute_path.unlink()
                progress.add_success()
            progress.done()
