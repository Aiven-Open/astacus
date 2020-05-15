"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Generic backup mechanism, which

- repeatedly (up to N times)
  - takes snapshots on all nodes until it succeeds on all
  - uploads the snapshots to cloud
- declares result 'good enough'

Product-specific backup subclasses can inherit the class and/or some sort of
plugin mechanism can be added here.

"""

from .coordinator import CoordinatorOpWithClusterLock
from astacus.common.ipc import SnapshotResult, SnapshotUploadRequest, SnapshotUploadResult

import logging

logger = logging.getLogger(__name__)


class BackupOp(CoordinatorOpWithClusterLock):
    async def _snapshot(self):
        start_results = await self.request_from_nodes("snapshot", method="post", caller="BackupOp.snapshot")
        if not start_results:
            return []
        return await self.wait_successful_results(start_results, result_class=SnapshotResult)

    async def _upload(self, snapshot_results):
        start_results = []
        for node, snapshot_result in zip(self.nodes, snapshot_results):
            # TBD: filter hashes based on what is in the object storage
            req = SnapshotUploadRequest(hashes=snapshot_result.hashes)
            start_result = await self.request_from_nodes(
                "upload", caller="BackupOp.upload", method="post", data=req.json(), nodes=[node]
            )
            if len(start_result) != 1:
                return []
            start_results.extend(start_result)
        return await self.wait_successful_results(start_results, result_class=SnapshotUploadResult)

    async def run_with_lock(self):
        for _ in range(self.config.backup_attempts):
            snapshot_result = await self._snapshot()
            if not snapshot_result:
                logger.info("Unable to snapshot successfully")
                continue
            upload_result = await self._upload(snapshot_result)
            if not upload_result:
                logger.info("Unable to upload successfully")
                continue
            return
        self.set_status_fail()
