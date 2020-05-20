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
from astacus.common import ipc
from typing import Dict, List

import logging

logger = logging.getLogger(__name__)


class NodeIndexData(ipc.AstacusModel):
    node_index: int
    sshashes: List[ipc.SnapshotHash] = []
    total_size: int = 0

    def append_sshash(self, sshash):
        self.total_size += sshash.size
        self.sshashes.append(sshash)


class BackupOp(CoordinatorOpWithClusterLock):
    async def _snapshot(self):
        logger.debug("BackupOp._snapshot")
        start_results = await self.request_from_nodes("snapshot", method="post", caller="BackupOp.snapshot")
        if not start_results:
            return []
        return await self.wait_successful_results(start_results, result_class=ipc.SnapshotResult)

    def _snapshot_results_to_upload_node_index_datas(self, *, snapshot_results, hexdigests) -> List[NodeIndexData]:
        hexdigests = set(hexdigests)
        assert len(snapshot_results) == len(self.nodes)
        sshash_to_node_indexes: Dict[ipc.SnapshotHash, List[int]] = {}
        for i, snapshot_result in enumerate(snapshot_results):
            for sshash in snapshot_result.hashes or []:
                sshash_to_node_indexes.setdefault(sshash, []).append(i)

        node_index_datas = [NodeIndexData(node_index=i) for i in range(len(self.nodes))]

        # This is not really optimal algorithm, but probably good enough.

        # Allocate the things based on first off, how often they show
        # up (the least common first), and then reverse size order, to least loaded node.
        def _sshash_to_node_indexes_key(item):
            (sshash, indexes) = item
            return len(indexes), -sshash.size

        todo = sorted(sshash_to_node_indexes.items(), key=_sshash_to_node_indexes_key)
        for sshash, node_indexes in todo:
            if sshash.hexdigest in hexdigests:
                continue
            _, node_index = min((node_index_datas[node_index].total_size, node_index) for node_index in node_indexes)
            node_index_datas[node_index].append_sshash(sshash)
        return [data for data in node_index_datas if data.sshashes]

    async def _upload(self, node_index_datas: List[NodeIndexData]):
        logger.debug("BackupOp._upload")
        start_results = []
        for data in node_index_datas:
            node = self.nodes[data.node_index]
            req = ipc.SnapshotUploadRequest(hashes=data.sshashes)
            start_result = await self.request_from_nodes(
                "upload", caller="BackupOp.upload", method="post", data=req.json(), nodes=[node]
            )
            if len(start_result) != 1:
                return []
            start_results.extend(start_result)
        return await self.wait_successful_results(start_results, result_class=ipc.SnapshotUploadResult, all_nodes=False)

    async def run_with_lock(self):
        attempts = self.config.backup_attempts
        for attempt in range(1, attempts + 1):
            logger.debug("BackupOp - attempt #%d/%d", attempt, attempts)
            snapshot_results = await self._snapshot()
            if not snapshot_results:
                logger.info("Unable to snapshot successfully")
                continue
            hexdigests = await self.async_storage.list_hexdigests()
            node_index_datas = self._snapshot_results_to_upload_node_index_datas(
                snapshot_results=snapshot_results, hexdigests=hexdigests
            )
            if node_index_datas:
                upload_results = await self._upload(node_index_datas)
                if not upload_results:
                    logger.info("Unable to upload successfully")
                    continue
            return
        self.set_status_fail()
