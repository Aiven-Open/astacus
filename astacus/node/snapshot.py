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
from astacus.common.rohmustorage import RohmuStorage
from astacus.common.snapshot import SnapshotGroup
from typing import Callable, Optional, Sequence

import hashlib
import logging

_hash = hashlib.blake2s

logger = logging.getLogger(__name__)


class SnapshotOp(NodeOp[ipc.SnapshotRequestV2, ipc.SnapshotResult]):
    snapshotter: Optional[Snapshotter] = None

    def create_result(self) -> ipc.SnapshotResult:
        return ipc.SnapshotResult()

    def start(self, get_or_create_snapshotter: Callable[[Sequence[SnapshotGroup]], Snapshotter]) -> NodeOp.StartResult:
        logger.info("start_snapshot %r", self.req)
        # We merge the list of groups and simple root_globs
        # to handle backward compatibility if the controller is older than the nodes.
        groups = [
            SnapshotGroup(
                root_glob=group.root_glob,
                excluded_names=group.excluded_names,
                embedded_file_size_max=group.embedded_file_size_max,
            )
            for group in self.req.groups
        ]
        groups += [
            SnapshotGroup(root_glob=root_glob)
            for root_glob in self.req.root_globs
            if not any(group.root_glob == root_glob for group in groups)
        ]
        self.snapshotter = get_or_create_snapshotter(groups)
        return self.start_op(op_name="snapshot", op=self, fun=self.snapshot)

    def snapshot(self) -> None:
        assert self.snapshotter is not None
        # 'snapshotter' is global; ensure we have sole access to it
        with self.snapshotter.lock:
            self.check_op_id()
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


class UploadOp(NodeOp[ipc.SnapshotUploadRequestV20221129, ipc.SnapshotUploadResult]):
    snapshotter: Optional[Snapshotter] = None

    @property
    def storage(self) -> RohmuStorage:
        assert self.config.object_storage is not None
        return RohmuStorage(self.config.object_storage, storage=self.req.storage)

    def create_result(self) -> ipc.SnapshotUploadResult:
        return ipc.SnapshotUploadResult()

    def start(self, snapshotter: Snapshotter) -> NodeOp.StartResult:
        logger.info("start_upload %r", self.req)
        self.snapshotter = snapshotter
        return self.start_op(op_name="upload", op=self, fun=self.upload)

    def upload(self) -> None:
        uploader = Uploader(storage=self.storage)
        assert self.snapshotter
        # 'snapshotter' is global; ensure we have sole access to it
        with self.snapshotter.lock:
            self.check_op_id()
            self.result.total_size, self.result.total_stored_size = uploader.write_hashes_to_storage(
                snapshotter=self.snapshotter,
                hashes=self.req.hashes,
                parallel=self.config.parallel.uploads,
                progress=self.result.progress,
                still_running_callback=self.still_running_callback,
                validate_file_hashes=self.req.validate_file_hashes,
            )
            self.result.progress.done()
