"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Common base classes for the plugins

"""
from __future__ import annotations

from astacus.common import exceptions, ipc, magic, utils
from astacus.common.asyncstorage import AsyncHexDigestStorage, AsyncJsonStorage
from astacus.common.ipc import Retention
from astacus.common.snapshot import SnapshotGroup
from astacus.common.utils import AstacusModel
from astacus.coordinator.cluster import Cluster, Result
from astacus.coordinator.config import CoordinatorNode
from astacus.coordinator.manifest import download_backup_manifest, download_backup_min_manifest
from collections import Counter
from collections.abc import Sequence, Set
from typing import Any, Counter as TCounter, Generic, TypeVar

import dataclasses
import datetime
import httpx
import logging
import msgspec

logger = logging.getLogger(__name__)

T = TypeVar("T")
StepResult_co = TypeVar("StepResult_co", covariant=True)


class CoordinatorPlugin(AstacusModel):
    def get_backup_steps(self, *, context: OperationContext) -> Sequence[Step[Any]]:
        raise NotImplementedError

    def get_delta_backup_steps(self, *, context: OperationContext) -> Sequence[Step[Any]]:
        raise NotImplementedError

    def get_restore_steps(self, *, context: OperationContext, req: ipc.RestoreRequest) -> Sequence[Step[Any]]:
        raise NotImplementedError

    def get_cleanup_steps(
        self, *, context: OperationContext, retention: ipc.Retention, explicit_delete: Sequence[str]
    ) -> Sequence[Step[Any]]:
        return [
            ListBackupsStep(json_storage=context.json_storage),
            ComputeKeptBackupsStep(
                json_storage=context.json_storage,
                retention=retention,
                explicit_delete=explicit_delete,
            ),
            DeleteBackupManifestsStep(json_storage=context.json_storage),
            DeleteDanglingHexdigestsStep(
                json_storage=context.json_storage,
                hexdigest_storage=context.hexdigest_storage,
            ),
        ]


@dataclasses.dataclass
class OperationContext:
    storage_name: str
    json_storage: AsyncJsonStorage
    hexdigest_storage: AsyncHexDigestStorage


class Step(Generic[StepResult_co]):
    async def run_step(self, cluster: Cluster, context: StepsContext) -> StepResult_co:
        raise NotImplementedError


class StepFailedError(exceptions.PermanentException):
    pass


class StepsContext:
    def __init__(self, *, attempt: int = 1, attempt_start: datetime.datetime | None = None) -> None:
        self.attempt = attempt
        self.attempt_start = utils.now() if attempt_start is None else attempt_start
        self.step_results: dict[type[Step], Any] = {}

    def get_result(self, step_class: type[Step[T]]) -> T:
        return self.step_results[step_class]

    def set_result(self, step_class: type[Step[T]], result: T) -> None:
        if step_class in self.step_results:
            if self.step_results[step_class] is not None or result is not None:
                raise RuntimeError(f"result already set for step {step_class}")
        self.step_results[step_class] = result


@dataclasses.dataclass
class SnapshotStep(Step[Sequence[ipc.SnapshotResult]]):
    """
    Request a snapshot of all files matching the `snapshot_root_globs`, on each nodes.

    The snapshot for each file contains its path, size, modification time and hash,
    see `SnapshotFile` for details.
    """

    snapshot_groups: Sequence[SnapshotGroup]
    snapshot_request: str = "snapshot"
    nodes_to_snapshot: Sequence[CoordinatorNode] | None = None

    async def run_step(self, cluster: Cluster, context: StepsContext) -> Sequence[ipc.SnapshotResult]:
        nodes_metadata = await get_nodes_metadata(cluster)
        cluster_features = set.intersection(*(set(n.features) for n in nodes_metadata))
        req = ipc.create_snapshot_request(self.snapshot_groups, node_features=cluster_features)
        start_results = await cluster.request_from_nodes(
            self.snapshot_request, method="post", caller="SnapshotStep", req=req, nodes=self.nodes_to_snapshot
        )
        return await cluster.wait_successful_results(start_results=start_results, result_class=ipc.SnapshotResult)


@dataclasses.dataclass
class ListHexdigestsStep(Step[Set[str]]):
    """
    Fetch the list of all files already present in object storage, identified by their hexdigest.
    """

    hexdigest_storage: AsyncHexDigestStorage

    async def run_step(self, cluster: Cluster, context: StepsContext) -> Set[str]:
        return set(await self.hexdigest_storage.list_hexdigests())


@dataclasses.dataclass
class UploadBlocksStep(Step[Sequence[ipc.SnapshotUploadResult]]):
    """
    Upload to object storage all files that are not yet in that storage.

    The list of files to upload comes from the snapshot taken on each node during
    the `SnapshotStep`, the list of files already uploaded come from the `ListHexdigestsStep`.

    If multiple nodes have the same files (according to their hexdigest, the path is ignored),
    each file will be uploaded only once, with an effort to distribute the work fairly among
    all nodes.

    This returns a list of `SnapshotUploadResult`, one for each node, that collects statistics
    about the uploads.
    """

    storage_name: str
    validate_file_hashes: bool = True
    upload_request: str = "upload"
    list_hexdigests_step: type[Step[Set[str]]] = ListHexdigestsStep

    async def run_step(self, cluster: Cluster, context: StepsContext) -> Sequence[ipc.SnapshotUploadResult]:
        node_index_datas = build_node_index_datas(
            hexdigests=context.get_result(self.list_hexdigests_step),
            snapshots=context.get_result(SnapshotStep),
            node_indices=list(range(len(cluster.nodes))),
        )
        return await upload_node_index_datas(
            cluster,
            self.storage_name,
            node_index_datas,
            validate_file_hashes=self.validate_file_hashes,
            upload_request=self.upload_request,
        )


@dataclasses.dataclass
class SnapshotClearStep(Step[Sequence[ipc.NodeResult]]):
    """
    Request to clear the source hierarchy of the snapshotter on all nodes.

    Depending on the request, this can clear either the main snapshotter or the delta snapshotter.
    """

    clear_request: str = "clear"

    async def run_step(self, cluster: Cluster, context: StepsContext) -> Sequence[ipc.NodeResult]:
        snapshot_results = context.get_result(SnapshotStep)
        assert snapshot_results[0].state
        node_request = ipc.SnapshotClearRequest(root_globs=snapshot_results[0].state.root_globs)
        start_results = await cluster.request_from_nodes(
            self.clear_request, method="post", caller="SnapshotClearStep", req=node_request
        )
        return await cluster.wait_successful_results(start_results=start_results, result_class=ipc.NodeResult)


@dataclasses.dataclass
class SnapshotReleaseStep(Step[Sequence[ipc.NodeResult]]):
    """
    Request to release the files we don't need any more in the destination hierarchy.

    Allows to free some disk space before the next backup happens.
    """

    async def run_step(self, cluster: Cluster, context: StepsContext) -> Sequence[ipc.NodeResult]:
        snapshot_results = context.get_result(SnapshotStep)
        nodes_metadata = await get_nodes_metadata(cluster)
        all_nodes_have_release_feature = nodes_metadata and all(
            ipc.NodeFeatures.release_snapshot_files.value in n.features for n in nodes_metadata
        )
        if not all_nodes_have_release_feature:
            logger.info("Skipped SnapshotReleaseStep because some nodes don't support it, node features: %s", nodes_metadata)
            return []
        node_requests = [
            ipc.SnapshotReleaseRequest(hexdigests=self._hexdigests_from_hashes(s.hashes)) for s in snapshot_results
        ]
        start_results = await cluster.request_from_nodes(
            "release", method="post", caller="SnapshotReleaseStep", reqs=node_requests
        )
        return await cluster.wait_successful_results(start_results=start_results, result_class=ipc.NodeResult)

    def _hexdigests_from_hashes(self, hashes: Sequence[ipc.SnapshotHash] | None) -> Sequence[str]:
        assert hashes is not None
        return [h.hexdigest for h in hashes]


@dataclasses.dataclass
class UploadManifestStep(Step[None]):
    """
    Store the backup manifest in the object storage.

    The backup manifest contains the snapshot from the `SnapshotStep` as well as the
    statistics collected by the `UploadBlocksStep` and the plugin manifest.
    """

    json_storage: AsyncJsonStorage
    plugin: ipc.Plugin
    plugin_manifest_step: type[Step[dict[str, Any]]] | None = None
    snapshot_step: type[Step[Sequence[ipc.SnapshotResult]]] | None = SnapshotStep
    upload_step: type[Step[Sequence[ipc.SnapshotUploadResult]]] | None = UploadBlocksStep
    backup_prefix: str = magic.JSON_BACKUP_PREFIX

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        plugin_data = context.get_result(self.plugin_manifest_step) if self.plugin_manifest_step else {}
        manifest = msgspec.json.encode(
            ipc.BackupManifest(
                attempt=context.attempt,
                start=context.attempt_start,
                snapshot_results=context.get_result(self.snapshot_step) if self.snapshot_step else [],
                upload_results=context.get_result(self.upload_step) if self.upload_step else [],
                plugin=self.plugin,
                plugin_data=plugin_data,
            )
        )
        backup_name = self._make_backup_name(context)
        logger.info("Storing backup manifest %s", backup_name)
        await self.json_storage.upload_json_bytes(backup_name, manifest)

    def _make_backup_name(self, context: StepsContext) -> str:
        iso = context.attempt_start.isoformat(timespec="seconds")
        return f"{self.backup_prefix}{iso}"


@dataclasses.dataclass
class BackupNameStep(Step[str]):
    """
    Select the name of the backup to restore.

    If the backup name was not specified in the restore request, this will select the
    most recent backup available in object storage, and fail if there are no backup.
    """

    json_storage: AsyncJsonStorage
    requested_name: str

    async def run_step(self, cluster: Cluster, context: StepsContext) -> str:
        if not self.requested_name:
            return sorted(await self.json_storage.list_jsons())[-1]
        if self.requested_name.startswith(magic.JSON_BACKUP_PREFIX):
            return self.requested_name
        return f"{magic.JSON_BACKUP_PREFIX}{self.requested_name}"


@dataclasses.dataclass
class BackupManifestStep(Step[ipc.BackupManifest]):
    """
    Download the backup manifest from object storage.
    """

    json_storage: AsyncJsonStorage

    async def run_step(self, cluster: Cluster, context: StepsContext) -> ipc.BackupManifest:
        backup_name = context.get_result(BackupNameStep)
        assert backup_name
        return await download_backup_manifest(self.json_storage, backup_name)


@dataclasses.dataclass
class MapNodesStep(Step[Sequence[int | None]]):
    """
    Create an index mapping nodes from cluster configuration to nodes in the backup manifest.
    """

    partial_restore_nodes: Sequence[ipc.PartialRestoreRequestNode] | None = None

    async def run_step(self, cluster: Cluster, context: StepsContext) -> Sequence[int | None]:
        # AZ distribution should in theory be forced to match, but in
        # practise it doesn't really matter. So we restore nodes 'as
        # well as we can' and hope that is well enough (or whoever
        # configures us may lie about the real availability zone of
        # the nodes anyway).

        backup_manifest = context.get_result(BackupManifestStep)
        snapshot_results = backup_manifest.snapshot_results

        return get_node_to_backup_index(
            partial_restore_nodes=self.partial_restore_nodes,
            snapshot_results=snapshot_results,
            nodes=cluster.nodes,
        )


@dataclasses.dataclass
class RestoreStep(Step[Sequence[ipc.NodeResult]]):
    """
    Request each node to download and restore all files listed in the backup manifest.
    """

    storage_name: str
    partial_restore_nodes: Sequence[ipc.PartialRestoreRequestNode] | None = None

    async def run_step(self, cluster: Cluster, context: StepsContext) -> Sequence[ipc.NodeResult]:
        # AZ distribution should in theory be forced to match, but in
        # practise it doesn't really matter. So we restore nodes 'as
        # well as we can' and hope that is well enough (or whoever
        # configures us may lie about the real availability zone of
        # the nodes anyway).

        backup_name = context.get_result(BackupNameStep)
        backup_manifest = context.get_result(BackupManifestStep)
        snapshot_results = backup_manifest.snapshot_results
        node_to_backup_index = context.get_result(MapNodesStep)

        if not snapshot_results:
            raise exceptions.MissingSnapshotResultsException(
                f"No snapshot results, yet full restore desired; {node_to_backup_index!r} {cluster.nodes!r}"
            )

        start_results: list[Result | None] = []
        for node, backup_index in zip(cluster.nodes, node_to_backup_index):
            if backup_index is not None:
                # Restore whatever was backed up
                snapshot_result = snapshot_results[backup_index]
                assert snapshot_result.state is not None
                node_request: ipc.NodeRequest = ipc.SnapshotDownloadRequest(
                    storage=self.storage_name,
                    backup_name=backup_name,
                    snapshot_index=backup_index,
                    root_globs=snapshot_result.state.root_globs,
                )
                op = "download"
            elif self.partial_restore_nodes:
                # If partial restore, do not clear other nodes
                continue
            else:
                assert snapshot_results[0].state is not None
                node_request = ipc.SnapshotClearRequest(root_globs=snapshot_results[0].state.root_globs)
                op = "clear"
            start_result = await cluster.request_from_nodes(
                op, caller="RestoreSnapshotStep", method="post", req=node_request, nodes=[node]
            )
            if len(start_result) != 1:
                return []
            start_results.extend(start_result)
        return await cluster.wait_successful_results(start_results=start_results, result_class=ipc.NodeResult)


@dataclasses.dataclass
class ListBackupsStep(Step[set[str]]):
    """
    List all available backups and return their name.
    """

    json_storage: AsyncJsonStorage

    async def run_step(self, cluster: Cluster, context: StepsContext) -> set[str]:
        return set(b for b in await self.json_storage.list_jsons() if b.startswith(magic.JSON_BACKUP_PREFIX))


@dataclasses.dataclass
class ListDeltaBackupsStep(Step[set[str]]):
    """
    List all available delta backups and return their name.
    """

    json_storage: AsyncJsonStorage

    async def run_step(self, cluster: Cluster, context: StepsContext) -> set[str]:
        return set(b for b in await self.json_storage.list_jsons() if b.startswith(magic.JSON_DELTA_PREFIX))


@dataclasses.dataclass
class DeltaManifestsStep(Step[Sequence[ipc.BackupManifest]]):
    """
    Download and parse all delta manifests necessary for restore.

    Includes only the deltas created after the base backup selected for restore.
    Returns manifests sorted by start time.
    """

    json_storage: AsyncJsonStorage

    async def run_step(self, cluster: Cluster, context: StepsContext) -> Sequence[ipc.BackupManifest]:
        backup_manifest = context.get_result(BackupManifestStep)
        # Right now does not really matter whether it's end or start, since backup and
        # delta operations are mutually exclusive.
        # Theoretically we might allow uploading deltas in parallel with base backup,
        # in that scenario it makes sense to rely on backup start (because a delta might
        # finish uploading while the base is still being uploaded).
        delta_names = sorted(d for d in await self.json_storage.list_jsons() if d.startswith(magic.JSON_DELTA_PREFIX))
        matching_delta_manifests = []
        for delta_name in delta_names:
            delta_manifest = await download_backup_manifest(self.json_storage, delta_name)
            if delta_manifest.start >= backup_manifest.start:
                matching_delta_manifests.append(delta_manifest)
        return sorted(matching_delta_manifests, key=lambda m: m.start)


@dataclasses.dataclass
class RestoreDeltasStep(Step[None]):
    """
    Restore the delta backups: download and apply to the node.
    """

    json_storage: AsyncJsonStorage
    storage_name: str
    # Delta restore is plugin-dependent, allow to customize it.
    restore_delta_url: str
    restore_delta_request: ipc.NodeRequest
    partial_restore_nodes: Sequence[ipc.PartialRestoreRequestNode] | None = None
    delta_manifests_step: type[Step[Sequence[ipc.BackupManifest]]] = DeltaManifestsStep

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        deltas_to_restore = sorted(context.get_result(self.delta_manifests_step), key=lambda m: m.start)

        for delta_manifest in deltas_to_restore:
            delta_name = delta_manifest.filename
            # Since deltas can be uploaded from a different set of nodes than the base backup,
            # explicitly re-match current nodes to delta manifest nodes on each restore.
            if self.partial_restore_nodes and not self.contains_partial_restore_hostnames(delta_manifest):
                logger.info("Skipped %s, because no data from it maps to partial restore nodes")
                continue
            node_to_backup_index = get_node_to_backup_index(
                partial_restore_nodes=self.partial_restore_nodes,
                snapshot_results=delta_manifest.snapshot_results,
                nodes=cluster.nodes,
            )
            nodes = [
                cluster.nodes[node_index]
                for node_index, backup_index in enumerate(node_to_backup_index)
                if backup_index is not None
            ]
            await self.download_delta(
                delta_name,
                nodes=nodes,
                cluster=cluster,
                node_to_backup_index=node_to_backup_index,
                delta_manifest=delta_manifest,
            )
            await self.restore_delta(delta_name, nodes=nodes, cluster=cluster)
            await self.clear_delta(
                delta_name,
                nodes=nodes,
                cluster=cluster,
                node_to_backup_index=node_to_backup_index,
                delta_manifest=delta_manifest,
            )

    def contains_partial_restore_hostnames(self, manifest: ipc.BackupManifest) -> bool:
        assert self.partial_restore_nodes is not None
        partial_restore_hostnames = {pr.backup_hostname for pr in self.partial_restore_nodes}
        return any(sr.hostname in partial_restore_hostnames for sr in manifest.snapshot_results)

    async def download_delta(
        self,
        delta_name: str,
        *,
        nodes: Sequence[CoordinatorNode],
        cluster: Cluster,
        node_to_backup_index: Sequence[int | None],
        delta_manifest: ipc.BackupManifest,
    ) -> None:
        reqs: list[ipc.NodeRequest] = []
        for backup_index in node_to_backup_index:
            if backup_index is not None:
                snapshot_result = delta_manifest.snapshot_results[backup_index]
                assert snapshot_result.state is not None
                reqs.append(
                    ipc.SnapshotDownloadRequest(
                        storage=self.storage_name,
                        backup_name=delta_name,
                        snapshot_index=backup_index,
                        root_globs=snapshot_result.state.root_globs,
                    )
                )
        start_results = await cluster.request_from_nodes(
            "delta/download",
            method="post",
            caller="restore_deltas",
            reqs=reqs,
            nodes=nodes,
        )
        if not start_results:
            raise StepFailedError(f"Initiating delta {delta_name} download failed")
        await cluster.wait_successful_results(start_results=start_results, result_class=ipc.NodeResult)

    async def restore_delta(self, delta_name: str, *, nodes: Sequence[CoordinatorNode], cluster: Cluster) -> None:
        start_results = await cluster.request_from_nodes(
            self.restore_delta_url,
            method="post",
            caller="restore_deltas",
            req=self.restore_delta_request,
            nodes=nodes,
        )
        if not start_results:
            raise StepFailedError(f"Initiating delta {delta_name} restore failed")
        await cluster.wait_successful_results(start_results=start_results, result_class=ipc.NodeResult)

    async def clear_delta(
        self,
        delta_name: str,
        *,
        nodes: Sequence[CoordinatorNode],
        cluster: Cluster,
        node_to_backup_index: Sequence[int | None],
        delta_manifest: ipc.BackupManifest,
    ) -> None:
        reqs: list[ipc.NodeRequest] = []
        for backup_index in node_to_backup_index:
            if backup_index is not None:
                snapshot_result = delta_manifest.snapshot_results[backup_index]
                assert snapshot_result.state is not None
                reqs.append(ipc.SnapshotClearRequest(root_globs=snapshot_result.state.root_globs))
        start_results = await cluster.request_from_nodes(
            "delta/clear",
            method="post",
            caller="restore_deltas",
            reqs=reqs,
            nodes=nodes,
        )
        if not start_results:
            raise StepFailedError(f"Initiating delta {delta_name} clear failed")
        await cluster.wait_successful_results(start_results=start_results, result_class=ipc.NodeResult)


def _prune_manifests(manifests: Sequence[ipc.ManifestMin], retention: Retention) -> list[ipc.ManifestMin]:
    manifests = sorted(manifests, key=lambda m: (m.start, m.end, m.filename), reverse=True)
    if retention.minimum_backups is not None and retention.minimum_backups >= len(manifests):
        return manifests

    while manifests:
        if retention.maximum_backups is not None:
            if retention.maximum_backups < len(manifests):
                manifests.pop()
                continue

        # Ok, so now we have at most <maximum_backups> (if set) backups
        # Do we have too _few_ backups to delete any more?
        if retention.minimum_backups is not None:
            if retention.minimum_backups >= len(manifests):
                break

        if retention.keep_days is not None:
            now = utils.now()
            manifest = manifests[-1]
            if (now - manifest.end).days > retention.keep_days:
                manifests.pop()
                continue

        # We don't have any other criteria to filter the backup manifests with
        break

    return manifests


@dataclasses.dataclass
class ComputeKeptBackupsStep(Step[Sequence[ipc.ManifestMin]]):
    """
    Return a list of backup manifests we want to keep, after excluding the explicitly deleted
    backups and applying the retention rules.
    """

    json_storage: AsyncJsonStorage
    retention: Retention
    explicit_delete: Sequence[str]
    retain_deltas: bool = False

    async def run_step(self, cluster: Cluster, context: StepsContext) -> Sequence[ipc.ManifestMin]:
        kept_manifests = await self.compute_kept_basebackups(context)
        if self.retain_deltas:
            kept_manifests += await self.compute_kept_deltas(kept_manifests, context)
        return kept_manifests

    async def compute_kept_basebackups(self, context: StepsContext) -> list[ipc.ManifestMin]:
        all_backup_names = context.get_result(ListBackupsStep)
        kept_backup_names = all_backup_names.difference(set(self.explicit_delete))
        manifests = []
        for backup_name in kept_backup_names:
            manifests.append(await download_backup_min_manifest(self.json_storage, backup_name))

        return _prune_manifests(manifests, self.retention)

    async def compute_kept_deltas(
        self, kept_backups: Sequence[ipc.ManifestMin], context: StepsContext
    ) -> Sequence[ipc.ManifestMin]:
        if not kept_backups:
            return []
        all_delta_names = context.get_result(ListDeltaBackupsStep)
        oldest_kept_backup = min(kept_backups, key=lambda b: b.start)
        kept_deltas: list[ipc.ManifestMin] = []
        for delta_name in all_delta_names:
            delta_manifest = await download_backup_min_manifest(self.json_storage, delta_name)
            if delta_manifest.end < oldest_kept_backup.end:
                continue
            kept_deltas.append(delta_manifest)
        return kept_deltas


@dataclasses.dataclass
class DeleteBackupManifestsStep(Step[set[str]]):
    """
    Delete all backup manifests that are not kept.
    """

    json_storage: AsyncJsonStorage

    async def run_step(self, cluster: Cluster, context: StepsContext) -> set[str]:
        all_backups = self.get_all_backups(context)
        kept_backups = {b.filename for b in context.get_result(ComputeKeptBackupsStep)}
        deleted_backups = all_backups - kept_backups
        for backup in deleted_backups:
            logger.info("deleting backup manifest %r", backup)
            await self.json_storage.delete_json(backup)
        return deleted_backups

    def get_all_backups(self, context: StepsContext) -> set[str]:
        return context.get_result(ListBackupsStep)


@dataclasses.dataclass
class DeleteBackupAndDeltaManifestsStep(DeleteBackupManifestsStep):
    def get_all_backups(self, context: StepsContext) -> set[str]:
        return context.get_result(ListBackupsStep) | context.get_result(ListDeltaBackupsStep)


@dataclasses.dataclass
class DeleteDanglingHexdigestsStep(Step[None]):
    """
    Delete all hexdigests that are not referenced by backup manifests.
    """

    hexdigest_storage: AsyncHexDigestStorage
    json_storage: AsyncJsonStorage

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        kept_manifests = context.get_result(ComputeKeptBackupsStep)
        logger.info("listing extra hexdigests")
        extra_hexdigests = set(await self.hexdigest_storage.list_hexdigests())
        for manifest_min in kept_manifests:
            manifest = await download_backup_manifest(self.json_storage, manifest_min.filename)
            for result in manifest.snapshot_results:
                assert result.hashes is not None
                for hash_ in result.hashes:
                    extra_hexdigests.discard(hash_.hexdigest)
        logger.info("deleting %d hexdigests from object storage", len(extra_hexdigests))
        for i, hexdigest in enumerate(extra_hexdigests, 1):
            # Due to rate limiting, it might be better to not do this in parallel
            await self.hexdigest_storage.delete_hexdigest(hexdigest)
            if i % 100 == 0 and cluster.stats is not None:
                cluster.stats.gauge("astacus_cleanup_hexdigest_progress", i)
                cluster.stats.gauge("astacus_cleanup_hexdigest_progress_percent", 100.0 * i / len(extra_hexdigests))


def get_node_to_backup_index(
    *,
    partial_restore_nodes: Sequence[ipc.PartialRestoreRequestNode] | None,
    snapshot_results: Sequence[ipc.SnapshotResult],
    nodes: Sequence[CoordinatorNode],
) -> Sequence[int | None]:
    if partial_restore_nodes:
        return get_node_to_backup_index_from_partial_restore_nodes(
            partial_restore_nodes=partial_restore_nodes,
            snapshot_results=snapshot_results,
            nodes=nodes,
        )
    covered_nodes = len(snapshot_results)
    configured_nodes = len(nodes)
    if configured_nodes < covered_nodes:
        missing_nodes = covered_nodes - configured_nodes
        raise exceptions.InsufficientNodesException(f"{missing_nodes} node(s) missing - unable to restore backup")

    azs_in_backup = Counter(result.az for result in snapshot_results)
    azs_in_nodes = Counter(node.az for node in nodes)
    if len(azs_in_backup) > len(azs_in_nodes):
        azs_missing = len(azs_in_backup) - len(azs_in_nodes)
        raise exceptions.InsufficientAZsException(f"{azs_missing} az(s) missing - unable to restore backup")

    return get_node_to_backup_index_from_azs(
        snapshot_results=snapshot_results,
        nodes=nodes,
        azs_in_backup=azs_in_backup,
        azs_in_nodes=azs_in_nodes,
    )


def get_node_to_backup_index_from_partial_restore_nodes(
    *,
    partial_restore_nodes: Sequence[ipc.PartialRestoreRequestNode],
    snapshot_results: Sequence[ipc.SnapshotResult],
    nodes: Sequence[CoordinatorNode],
) -> Sequence[int | None]:
    node_to_backup_index: list[int | None] = [None] * len(nodes)
    hostname_to_backup_index: dict[str | None, int] = {}
    url_to_node_index: dict[str | None, int] = {}
    for i, node in enumerate(nodes):
        url_to_node_index[node.url] = i
    for i, res in enumerate(snapshot_results):
        hostname_to_backup_index[res.hostname] = i
    for req_node in partial_restore_nodes:
        node_index = req_node.node_index
        if node_index is not None:
            num_nodes = len(nodes)
            if node_index < 0 or node_index >= num_nodes:
                raise exceptions.NotFoundException(
                    f"Invalid node_index in partial restore: Must be 0 <= {node_index} < {num_nodes}"
                )
        else:
            node_index = url_to_node_index.get(req_node.node_url)
            if node_index is None:
                raise exceptions.NotFoundException(
                    f"Partial restore url {req_node.node_url} not found in active configuration"
                )
        backup_index = req_node.backup_index
        if backup_index is not None:
            num_backup_nodes = len(snapshot_results)
            if backup_index < 0 or backup_index >= num_backup_nodes:
                raise exceptions.NotFoundException(
                    f"Invalid backup_index in partial restore: Must be 0 <= {backup_index} < {num_backup_nodes}"
                )
        else:
            backup_index = hostname_to_backup_index.get(req_node.backup_hostname)
            if backup_index is None:
                raise exceptions.NotFoundException(
                    f"Partial restore hostname {req_node.backup_hostname} not found in backup manifest"
                )
        node_to_backup_index[node_index] = backup_index
    return node_to_backup_index


def get_node_to_backup_index_from_azs(
    *,
    snapshot_results: Sequence[ipc.SnapshotResult],
    nodes: Sequence[CoordinatorNode],
    azs_in_backup: TCounter[str],
    azs_in_nodes: TCounter[str],
) -> Sequence[int | None]:
    node_to_backup_index: list[int | None] = [None] * len(nodes)
    # This is strictly speaking just best-effort assignment
    for (backup_az, backup_n), (node_az, node_n) in zip(azs_in_backup.most_common(), azs_in_nodes.most_common()):
        if backup_n > node_n:
            missing_n = backup_n - node_n
            raise exceptions.InsufficientNodesException(
                f"AZ {node_az}, to be restored from {backup_az}, is missing {missing_n} nodes"
            )

        for backup_index, snapshot_result in enumerate(snapshot_results):
            if snapshot_result.az != backup_az:
                continue
            for node_index, node in enumerate(nodes):
                if node.az != node_az or node_to_backup_index[node_index] is not None:
                    continue
                node_to_backup_index[node_index] = backup_index
                break
    return node_to_backup_index


class NodeIndexData(msgspec.Struct, kw_only=True):
    node_index: int
    sshashes: list[ipc.SnapshotHash] = msgspec.field(default_factory=list)
    total_size: int = 0

    def append_sshash(self, sshash: ipc.SnapshotHash) -> None:
        self.total_size += sshash.size
        self.sshashes.append(sshash)


def build_node_index_datas(
    *, hexdigests: Set[str], snapshots: Sequence[ipc.SnapshotResult], node_indices: Sequence[int]
) -> Sequence[NodeIndexData]:
    assert len(snapshots) == len(node_indices)
    sshash_to_node_indexes: dict[ipc.SnapshotHash, list[int]] = {}
    for i, snapshot_result in enumerate(snapshots):
        for snapshot_hash in snapshot_result.hashes or []:
            sshash_to_node_indexes.setdefault(snapshot_hash, []).append(i)

    node_index_datas = [NodeIndexData(node_index=node_index) for node_index in node_indices]

    # This is not really optimal algorithm, but probably good enough.

    # Allocate the things based on first off, how often they show
    # up (the least common first), and then reverse size order, to least loaded node.
    def _sshash_to_node_indexes_key(item):
        (sshash, indexes) = item
        return len(indexes), -sshash.size

    todo = sorted(sshash_to_node_indexes.items(), key=_sshash_to_node_indexes_key)
    for snapshot_hash, node_indexes in todo:
        if snapshot_hash.hexdigest in hexdigests:
            continue
        _, node_index = min((node_index_datas[node_index].total_size, node_index) for node_index in node_indexes)
        node_index_datas[node_index].append_sshash(snapshot_hash)
    return [data for data in node_index_datas if data.sshashes]


async def upload_node_index_datas(
    cluster: Cluster,
    storage_name: str,
    node_index_datas: Sequence[NodeIndexData],
    validate_file_hashes: bool,
    upload_request: str,
):
    logger.info("upload_node_index_datas")
    start_results: list[Result | None] = []
    nodes_metadata = await get_nodes_metadata(cluster)
    for data in node_index_datas:
        if ipc.NodeFeatures.validate_file_hashes.value in nodes_metadata[data.node_index].features:
            req: ipc.NodeRequest = ipc.SnapshotUploadRequestV20221129(
                hashes=data.sshashes, storage=storage_name, validate_file_hashes=validate_file_hashes
            )
        else:
            req = ipc.SnapshotUploadRequest(hashes=data.sshashes, storage=storage_name)
        start_result = await cluster.request_from_nodes(
            upload_request, caller="upload_node_index_datas", method="post", req=req, nodes=[cluster.nodes[data.node_index]]
        )
        if len(start_result) != 1:
            raise StepFailedError("upload failed")
        start_results.extend(start_result)
    return await cluster.wait_successful_results(start_results=start_results, result_class=ipc.SnapshotUploadResult)


async def get_nodes_metadata(
    cluster: Cluster, *, nodes: Sequence[CoordinatorNode] | None = None
) -> list[ipc.MetadataResult]:
    metadata_responses: Sequence[httpx.Response | None] = await cluster.request_from_nodes(
        "metadata", caller="get_nodes_metadata", method="get", nodes=nodes, json=False
    )
    return [
        ipc.MetadataResult(version="", features=[])
        if response is None
        else msgspec.json.decode(response.content, type=ipc.MetadataResult)
        for response in metadata_responses
    ]
