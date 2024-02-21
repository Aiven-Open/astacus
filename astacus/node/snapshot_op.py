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
from astacus.common import ipc, node_manifest, utils
from astacus.common.pyarrow_utils import convert_rows_to_dataset
from astacus.node.snapshot import Snapshot

import logging

logger = logging.getLogger(__name__)


class SnapshotOp(NodeOp[ipc.SnapshotRequestV2, ipc.SnapshotResult]):
    snapshotter: Snapshotter | None = None

    def create_result(self) -> ipc.SnapshotResult:
        return ipc.SnapshotResult()

    def start(self, snapshotter: Snapshotter) -> NodeOp.StartResult:
        logger.info("start_snapshot %r", self.req)
        self.snapshotter = snapshotter
        return self.start_op(op_name="snapshot", op=self, fun=self.perform_snapshot)

    def perform_snapshot(self) -> None:
        assert self.snapshotter is not None
        # 'snapshotter' is global; ensure we have sole access to it
        with self.snapshotter.lock:
            self.check_op_id()
            self.snapshotter.perform_snapshot(progress=self.result.progress)
            if self.req.node_manifest_info is None:
                self.result.state = self.snapshotter.get_snapshot_state()
                self.result.hashes = [
                    ipc.SnapshotHash(hexdigest=ssfile.hexdigest, size=ssfile.file_size)
                    for ssfile in self.result.state.files
                    if ssfile.hexdigest
                ]
                self.result.files = len(self.result.state.files)
                self.result.total_size = sum(ssfile.file_size for ssfile in self.result.state.files)
            else:
                self.result.total_size = self.snapshotter.snapshot.get_total_size()
                self.result.files = len(self.snapshotter.snapshot)
                manifest_info = self.req.node_manifest_info
                manifest = convert_rows_to_dataset(
                    self.snapshotter.snapshot.get_all_files_tuples(),
                    schema=node_manifest.ARROW_SCHEMA,
                    batch_size=node_manifest.BATCH_SIZE,
                )
                self.get_node_store(manifest_info.snapshot_index, manifest_info.storage).upload_manifest(
                    manifest_info.backup_name, manifest
                )
        self.result.end = utils.now()
        self.result.progress.done()


class UploadOp(NodeOp[ipc.SnapshotUploadRequest, ipc.SnapshotUploadResult]):
    snapshot: Snapshot | None = None

    def create_result(self) -> ipc.SnapshotUploadResult:
        return ipc.SnapshotUploadResult()

    def start(self, snapshot: Snapshot) -> NodeOp.StartResult:
        logger.info("start_upload %r", self.req)
        self.snapshot = snapshot
        return self.start_op(op_name="upload", op=self, fun=self.upload)

    def upload(self) -> None:
        assert self.snapshot is not None
        uploader = Uploader(hexdigest_storage=self.get_hexdigest_store(self.req.storage))
        # 'snapshotter' is global; ensure we have sole access to it
        with self.snapshot.lock:
            self.check_op_id()
            self.result.total_size, self.result.total_stored_size = uploader.write_hashes_to_storage(
                snapshot=self.snapshot,
                hashes=self.req.hashes,
                parallel=self.config.parallel.uploads,
                progress=self.result.progress,
                still_running_callback=self.still_running_callback,
                validate_file_hashes=self.req.validate_file_hashes,
            )
            self.result.progress.done()


class ReleaseOp(NodeOp[ipc.SnapshotReleaseRequest, ipc.NodeResult]):
    snapshotter: Snapshotter | None = None

    def create_result(self) -> ipc.NodeResult:
        return ipc.NodeResult()

    def start(self, snapshotter: Snapshotter) -> NodeOp.StartResult:
        logger.info("start_release %r", self.req)
        self.snapshotter = snapshotter
        return self.start_op(op_name="release", op=self, fun=self.release)

    def release(self) -> None:
        assert self.snapshotter is not None
        with self.snapshotter.lock:
            self.check_op_id()
            self.snapshotter.release(self.req.hexdigests, progress=self.result.progress)
            self.result.progress.done()
