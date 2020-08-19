"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Common base classes for the plugins

"""

from astacus.common import exceptions, ipc, magic
from astacus.coordinator import plugins
from astacus.coordinator.coordinator import Coordinator, CoordinatorOpWithClusterLock
from collections import Counter
from typing import Dict, List, Optional, Set, Union

import logging

logger = logging.getLogger(__name__)


class NodeIndexData(ipc.AstacusModel):
    node_index: int
    sshashes: List[ipc.SnapshotHash] = []
    total_size: int = 0

    def append_sshash(self, sshash):
        self.total_size += sshash.size
        self.sshashes.append(sshash)


class OpBase(CoordinatorOpWithClusterLock):
    steps: List[str] = []

    plugin: Optional[ipc.Plugin] = None

    @property
    def plugin_config(self):
        assert self.plugin
        plugin_config_class = plugins.get_plugin_config_class(self.plugin)
        return plugin_config_class.parse_obj(self.config.plugin_config)

    async def try_run(self) -> bool:
        for i, step in enumerate(self.steps, 1):
            logger.debug("step %d/%d: %s", i, len(self.steps), step)
            step_name = f"step_{step}"
            step_callable = getattr(self, step_name)
            assert step_callable, f"Step method {step_name} not found in {self!r}"
            if self.stats is not None:
                name = self.__class__.__name__
                async with self.stats.async_timing_manager("astacus_step_duration", {"op": name, "step": step_name}):
                    r = await step_callable()
            else:
                r = await step_callable()
            if not r:
                logger.info("Step %s failed", step)
                return False
            setattr(self, f"result_{step}", r)
        return True

    config_attempts_var_name = "XXX"

    async def run_with_lock(self):
        await self.run_attempts(getattr(self.config, self.config_attempts_var_name))


class BackupOpBase(OpBase):
    config_attempts_var_name = "backup_attempts"
    steps = ["snapshot", "list_hexdigests", "upload_blocks", "upload_manifest"]

    snapshot_root_globs: List[str] = []

    async def step_snapshot(self) -> List[ipc.SnapshotResult]:
        """ Snapshot step. Has to be parametrized with the root_globs to use """
        logger.debug("BackupOp._snapshot")
        req = ipc.SnapshotRequest(root_globs=self.snapshot_root_globs)
        start_results = await self.request_from_nodes(
            "snapshot", method="post", caller="BackupOpBase.step_snapshot", req=req
        )
        if not start_results:
            return []
        return await self.wait_successful_results(start_results, result_class=ipc.SnapshotResult)

    result_snapshot: List[ipc.SnapshotResult] = []

    async def step_list_hexdigests(self) -> bool:
        assert self.hexdigest_storage
        self.hexdigests = set(await self.hexdigest_storage.list_hexdigests())
        return True

    hexdigests: Set[str] = set()

    def _snapshot_results_to_upload_node_index_datas(self) -> List[NodeIndexData]:
        assert len(self.result_snapshot) == len(self.nodes)
        sshash_to_node_indexes: Dict[ipc.SnapshotHash, List[int]] = {}
        for i, snapshot_result in enumerate(self.result_snapshot):
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
            if sshash.hexdigest in self.hexdigests:
                continue
            _, node_index = min((node_index_datas[node_index].total_size, node_index) for node_index in node_indexes)
            node_index_datas[node_index].append_sshash(sshash)
        return [data for data in node_index_datas if data.sshashes]

    async def _upload(self, node_index_datas: List[NodeIndexData]):
        logger.debug("BackupOp._upload")
        start_results = []
        for data in node_index_datas:
            node = self.nodes[data.node_index]
            req = ipc.SnapshotUploadRequest(hashes=data.sshashes, storage=self.default_storage_name)
            start_result = await self.request_from_nodes(
                "upload", caller="BackupOpBase._upload", method="post", req=req, nodes=[node]
            )
            if len(start_result) != 1:
                return []
            start_results.extend(start_result)
        return await self.wait_successful_results(start_results, result_class=ipc.SnapshotUploadResult, all_nodes=False)

    result_upload_blocks: Union[bool, List[ipc.SnapshotUploadResult]]

    async def step_upload_blocks(self):
        node_index_datas = self._snapshot_results_to_upload_node_index_datas()
        if node_index_datas:
            upload_results = await self._upload(node_index_datas)
            return upload_results
        return True

    plugin_data: dict = {}

    async def step_upload_manifest(self):
        """ Final backup manifest upload. It has to be parametrized with the plugin, and plugin_data """
        assert self.attempt_start
        iso = self.attempt_start.isoformat(timespec="seconds")
        filename = f"{magic.JSON_BACKUP_PREFIX}{iso}"
        manifest = ipc.BackupManifest(
            attempt=self.attempt,
            start=self.attempt_start,
            snapshot_results=self.result_snapshot,
            upload_results=[] if self.result_upload_blocks is True else self.result_upload_blocks,
            plugin=self.plugin,
            plugin_data=self.plugin_data
        )
        logger.debug("Storing backup manifest %s", filename)
        await self.json_storage.upload_json(filename, manifest)
        self.state.cached_list_response = None  # Invalidate cache
        return True


class RestoreOpBase(OpBase):
    config_attempts_var_name = "restore_attempts"
    steps = ["backup_name", "backup_manifest", "restore"]

    def __init__(self, *, c: Coordinator, req: ipc.RestoreRequest):
        super().__init__(c=c)
        self.req = req
        if req.storage:
            self.set_storage_name(req.storage)

    async def step_backup_name(self) -> str:
        assert self.json_storage
        name = self.req.name
        if not name:
            return sorted(await self.json_storage.list_jsons())[-1]
        if name.startswith(magic.JSON_BACKUP_PREFIX):
            return name
        return f"{magic.JSON_BACKUP_PREFIX}{name}"

    result_backup_name: str = ""

    async def step_backup_manifest(self):
        assert self.result_backup_name
        return await self.download_backup_manifest(self.result_backup_name)

    result_backup_manifest: Optional[ipc.BackupManifest] = None

    async def step_restore(self):
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
                result = self.result_backup_manifest.snapshot_results[idx]
            else:
                root_globs = self.result_backup_manifest.snapshot_results[0].state.root_globs
                # Restore fake, empty backup to ensure node is clean
                result = ipc.SnapshotResult(state=ipc.SnapshotState(root_globs=root_globs, files=[]))
            req = ipc.SnapshotDownloadRequest(state=result.state, storage=self.default_storage_name)
            start_result = await self.request_from_nodes(
                "download", caller="RestoreOpBase.step_restore", method="post", req=req, nodes=[node]
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

            for j, result in enumerate(self.result_backup_manifest.snapshot_results):
                if result.az != backup_az:
                    continue
                for i, node in enumerate(self.nodes):
                    if node.az != node_az or node_to_backup_index[i] is not None:
                        continue
                    node_to_backup_index[i] = j
                    break
        return node_to_backup_index

    def _get_node_to_backup_index(self):
        assert self.result_backup_manifest
        covered_nodes = len(self.result_backup_manifest.snapshot_results)
        configured_nodes = len(self.nodes)
        if configured_nodes < covered_nodes:
            missing_nodes = covered_nodes - configured_nodes
            raise exceptions.InsufficientNodesException(f"{missing_nodes} node(s) missing - unable to restore backup")

        azs_in_backup = Counter(result.az for result in self.result_backup_manifest.snapshot_results)
        azs_in_nodes = Counter(node.az for node in self.nodes)
        if len(azs_in_backup) > len(azs_in_nodes):
            azs_missing = len(azs_in_backup) - len(azs_in_nodes)
            raise exceptions.InsufficientAZsException(f"{azs_missing} az(s) missing - unable to restore backup")

        return self._get_node_to_backup_index_from_azs(azs_in_backup=azs_in_backup, azs_in_nodes=azs_in_nodes)

    @property
    def plugin_manifest(self):
        assert self.plugin
        assert self.result_backup_manifest
        plugin_manifest_class = plugins.get_plugin_manifest_class(self.plugin)
        return plugin_manifest_class.parse_obj(self.result_backup_manifest.plugin_data)
