"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

General snapshot utilities that are product independent.

Most of the snapshot steps should be implementable by using the API of
this module with proper parameters.

"""

from .node import NodeOp
from .snapshotter import Snapshotter
from .uploader import Uploader
from astacus.common import ipc, utils
from typing import Optional

import hashlib
import logging

_hash = hashlib.blake2s

logger = logging.getLogger(__name__)


class SnapshotOp(NodeOp):
    snapshotter: Optional[Snapshotter] = None

    def create_result(self):
        return ipc.SnapshotResult()

    def start(self, *, req: ipc.SnapshotRequest):
        self.req = req
        logger.debug("start_snapshot %r", req)
        self.snapshotter = self.get_or_create_snapshotter(req.root_globs)
        return self.start_op(op_name="snapshot", op=self, fun=self.snapshot)

    def snapshot(self):
        self.snapshotter.snapshot(progress=self.result.progress)
        self.result.state = self.snapshotter.get_snapshot_state()
        self.result.hashes = [
            ipc.SnapshotHash(hexdigest=ssfile.hexdigest, size=ssfile.file_size)
            for ssfile in self.result.state.files
            if ssfile.hexdigest
        ]
        self.result.files = len(self.result.state.files)
        self.result.total_size = sum(ssfile.file_size for ssfile in self.result.state.files)
        self.result.end = utils.now()
        self.result.progress.done()


class UploadOp(NodeOp):
    def create_result(self):
        return ipc.SnapshotUploadResult()

    def start(self, *, req: ipc.SnapshotUploadRequest):
        self.req = req
        logger.debug("start_upload %r", req)
        return self.start_op(op_name="upload", op=self, fun=self.upload)

    def upload(self):
        uploader = Uploader(storage=self.storage)
        self.result.total_size, self.result.total_stored_size = uploader.write_hashes_to_storage(
            snapshotter=self.get_snapshotter(),
            hashes=self.req.hashes,
            parallel=self.config.parallel.uploads,
            progress=self.result.progress,
            still_running_callback=self.still_running_callback
        )
        self.result.progress.done()
