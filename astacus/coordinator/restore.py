"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from .coordinator import Coordinator, CoordinatorOpWithClusterLock
from astacus.common import exceptions, ipc
from collections import Counter

import logging

logger = logging.getLogger(__name__)


class RestoreOp(CoordinatorOpWithClusterLock):
    restore_attempt = 0
    backup_manifest = None

    def __init__(self, *, c: Coordinator, name: str):
        super().__init__(c=c)
        self.name = name

    async def get_backup_name(self):
        if not self.name:
            self.name = sorted(await self.async_storage.list_jsons())[-1]
        return self.name

    async def _restore(self):
        # AZ distribution should in theory be forced to match, but in
        # practise it doesn't really matter. So we restore nodes 'as
        # well as we can' and hope that is well enough (or whoever
        # configures us may lie about the real availability zone of
        # the nodes anyway).

        node_to_backup_index = self._get_node_to_backup_index()
        start_results = []

        for idx, node in zip(node_to_backup_index, self.nodes):
            if idx is not None:
                # Restore whatever was backed up
                result = self.backup_manifest.snapshot_results[idx]
            else:
                # Restore fake, empty backup to ensure node is clean
                result = ipc.SnapshotResult(state=ipc.SnapshotState(files=[]))
            data = ipc.SnapshotDownloadRequest(state=result.state).json()
            start_result = await self.request_from_nodes(
                "download", caller="RestoreOp._restore", method="post", data=data, nodes=[node]
            )
            if len(start_result) != 1:
                return []
            start_results.extend(start_result)
        return await self.wait_successful_results(start_results, result_class=ipc.NodeResult)

    def _get_node_to_backup_index_from_azs(self, *, azs_in_backup, azs_in_nodes):
        node_to_backup_index = [None] * len(self.nodes)
        # This is strictly speaking just best-effort assignment
        for backup_t, node_t in zip(azs_in_backup.most_common(), azs_in_nodes.most_common()):
            (backup_az, backup_n) = backup_t
            (node_az, node_n) = node_t
            if backup_n > node_n:
                missing_n = backup_n - node_n
                raise exceptions.InsufficientNodesException(
                    f"AZ {node_az}, to be restored from {backup_az}, is missing {missing_n} nodes"
                )

            for j, result in enumerate(self.backup_manifest.snapshot_results):
                if result.az != backup_az:
                    continue
                for i, node in enumerate(self.nodes):
                    if node.az != node_az or node_to_backup_index[i] is not None:
                        continue
                    node_to_backup_index[i] = j
                    break
        return node_to_backup_index

    def _get_node_to_backup_index(self):
        covered_nodes = len(self.backup_manifest.snapshot_results)
        configured_nodes = len(self.nodes)
        if configured_nodes < covered_nodes:
            missing_nodes = covered_nodes - configured_nodes
            raise exceptions.InsufficientNodesException(f"{missing_nodes} node(s) missing - unable to restore backup")

        azs_in_backup = Counter(result.az for result in self.backup_manifest.snapshot_results)
        azs_in_nodes = Counter(node.az for node in self.nodes)
        if len(azs_in_backup) > len(azs_in_nodes):
            azs_missing = len(azs_in_backup) - len(azs_in_nodes)
            raise exceptions.InsufficientAZsException(f"{azs_missing} az(s) missing - unable to restore backup")

        return self._get_node_to_backup_index_from_azs(azs_in_backup=azs_in_backup, azs_in_nodes=azs_in_nodes)

    async def try_run(self) -> bool:
        return bool(await self._restore())

    async def run_with_lock(self):
        backup_name = await self.get_backup_name()
        backup_dict = await self.async_storage.download_json(backup_name)
        self.backup_manifest = ipc.BackupManifest.parse_obj(backup_dict)
        await self.run_attempts(self.config.restore_attempts)
