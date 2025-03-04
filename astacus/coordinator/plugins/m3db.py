"""Copyright (c) 2020 Aiven Ltd
See LICENSE for details.

m3db backup/restore plugin

All of the actual heavy lifting is done using the base file
snapshot/restore functionality. m3db plugin will simply ensure etcd
state is consistent.

"""

from .base import (
    BackupManifestStep,
    BackupNameStep,
    CoordinatorPlugin,
    ListHexdigestsStep,
    MapNodesStep,
    OperationContext,
    RestoreStep,
    SnapshotStep,
    Step,
    StepFailedError,
    StepsContext,
    UploadBlocksStep,
    UploadManifestStep,
)
from .etcd import ETCDDump, ETCDKey, get_etcd_dump, restore_etcd_dump
from astacus.common import exceptions, ipc, m3placement
from astacus.common.etcd import ETCDClient
from astacus.common.ipc import Plugin
from astacus.common.snapshot import SnapshotGroup
from astacus.common.utils import AstacusModel
from astacus.coordinator.cluster import Cluster
from astacus.coordinator.config import CoordinatorNode
from collections.abc import Sequence
from typing import Any

import dataclasses
import logging

logger = logging.getLogger(__name__)

ETCD_PREFIX_FORMATS = ["_kv/{env}/m3db.node.namespaces", "_sd.placement/{env}/m3db"]


class M3DBManifest(AstacusModel):
    etcd: ETCDDump
    placement_nodes: Sequence[m3placement.M3PlacementNode]


class M3DBPlugin(CoordinatorPlugin):
    etcd_url: str
    environment: str
    placement_nodes: Sequence[m3placement.M3PlacementNode]

    def get_backup_steps(self, *, context: OperationContext) -> Sequence[Step[Any]]:
        etcd_client = ETCDClient(self.etcd_url)
        etcd_prefixes = get_etcd_prefixes(self.environment)
        return [
            InitStep(placement_nodes=self.placement_nodes),
            RetrieveEtcdStep(etcd_client=etcd_client, etcd_prefixes=etcd_prefixes),
            SnapshotStep(snapshot_groups=[SnapshotGroup(root_glob="**/*.db")]),
            ListHexdigestsStep(hexdigest_storage=context.hexdigest_storage),
            UploadBlocksStep(storage_name=context.storage_name),
            RetrieveEtcdAgainStep(etcd_client=etcd_client, etcd_prefixes=etcd_prefixes),
            PrepareM3ManifestStep(placement_nodes=self.placement_nodes),
            # upload backup manifest only after we've retrieved again etcd
            # contents and found it consistent
            UploadManifestStep(
                json_storage=context.json_storage, plugin=Plugin.m3db, plugin_manifest_step=PrepareM3ManifestStep
            ),
        ]

    def get_delta_backup_steps(self, *, context: OperationContext) -> Sequence[Step[Any]]:
        raise NotImplementedError

    def get_restore_steps(self, *, context: OperationContext, req: ipc.RestoreRequest) -> Sequence[Step[Any]]:
        etcd_client = ETCDClient(self.etcd_url)
        return [
            InitStep(placement_nodes=self.placement_nodes),
            BackupNameStep(json_storage=context.json_storage, requested_name=req.name),
            BackupManifestStep(json_storage=context.json_storage),
            MapNodesStep(partial_restore_nodes=req.partial_restore_nodes),
            RewriteEtcdStep(placement_nodes=self.placement_nodes, partial_restore_nodes=req.partial_restore_nodes),
            RestoreEtcdStep(etcd_client=etcd_client, partial_restore_nodes=req.partial_restore_nodes),
            RestoreStep(storage_name=context.storage_name, partial_restore_nodes=req.partial_restore_nodes),
        ]


def get_etcd_prefixes(environment: str) -> Sequence[bytes]:
    return [p.format(env=environment).encode() for p in ETCD_PREFIX_FORMATS]


@dataclasses.dataclass
class InitStep(Step[None]):
    placement_nodes: Sequence[m3placement.M3PlacementNode]

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        validate_m3_config(self.placement_nodes, cluster.nodes)


@dataclasses.dataclass
class RetrieveEtcdStep(Step[ETCDDump]):
    etcd_client: ETCDClient
    etcd_prefixes: Sequence[bytes]

    async def run_step(self, cluster: Cluster, context: StepsContext) -> ETCDDump:
        etcd_dump = await get_etcd_dump(self.etcd_client, self.etcd_prefixes)
        if etcd_dump is None:
            raise StepFailedError("etcd dump failed")
        return etcd_dump


@dataclasses.dataclass
class RetrieveEtcdAgainStep(Step[None]):
    etcd_client: ETCDClient
    etcd_prefixes: Sequence[bytes]

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        etcd_before = context.get_result(RetrieveEtcdStep)
        etcd_now = await get_etcd_dump(self.etcd_client, self.etcd_prefixes)
        if etcd_now is None:
            raise StepFailedError("second etcd dump failed")
        if etcd_before != etcd_now:
            raise StepFailedError("etcd state changed during backup")


@dataclasses.dataclass
class PrepareM3ManifestStep(Step[dict[str, Any]]):
    placement_nodes: Sequence[m3placement.M3PlacementNode]

    async def run_step(self, cluster: Cluster, context: StepsContext) -> dict[str, Any]:
        etcd = context.get_result(RetrieveEtcdStep)
        return M3DBManifest(etcd=etcd, placement_nodes=self.placement_nodes).dict()


@dataclasses.dataclass
class RewriteEtcdStep(Step[ETCDDump | None]):
    placement_nodes: Sequence[m3placement.M3PlacementNode]
    partial_restore_nodes: Sequence[ipc.PartialRestoreRequestNode] | None

    async def run_step(self, cluster: Cluster, context: StepsContext) -> ETCDDump | None:
        if self.partial_restore_nodes:
            logger.info("Skipping etcd rewrite due to partial backup restoration")
            return None
        backup_manifest = context.get_result(BackupManifestStep)
        node_to_backup_index = context.get_result(MapNodesStep)
        plugin_manifest = M3DBManifest.parse_obj(backup_manifest.plugin_data)
        etcd = plugin_manifest.etcd.copy(deep=True)
        for prefix in etcd.prefixes:
            for key in prefix.keys:
                key_bytes = key.key_bytes
                if key_bytes.startswith(b"_sd.placement/") and key_bytes.endswith(b"/m3db"):
                    rewrite_m3db_placement(
                        key=key,
                        node_to_backup_index=node_to_backup_index,
                        src_placement_nodes=plugin_manifest.placement_nodes,
                        dst_placement_nodes=self.placement_nodes,
                        nodes=cluster.nodes,
                    )
        return etcd


@dataclasses.dataclass
class RestoreEtcdStep(Step[None]):
    etcd_client: ETCDClient
    partial_restore_nodes: Sequence[ipc.PartialRestoreRequestNode] | None

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        if self.partial_restore_nodes:
            logger.info("Skipping etcd rewrite due to partial backup restoration")
        dump = context.get_result(RewriteEtcdStep)
        if dump is not None:
            if not await restore_etcd_dump(client=self.etcd_client, dump=dump):
                raise StepFailedError("etcd dump restoration failed")


class M3IncorrectPlacementNodesLengthException(exceptions.PermanentException):
    pass


def validate_m3_config(placement_nodes: Sequence[m3placement.M3PlacementNode], nodes: Sequence[CoordinatorNode]) -> None:
    pnode_count = len(placement_nodes)
    node_count = len(nodes)
    if pnode_count != node_count:
        diff = node_count - pnode_count
        raise M3IncorrectPlacementNodesLengthException(
            f"{node_count} nodes, yet {pnode_count} nodes in the m3 placement_nodes; difference of {diff}"
        )


def rewrite_m3db_placement(
    *,
    key: ETCDKey,
    node_to_backup_index: Sequence[int | None],
    src_placement_nodes: Sequence[m3placement.M3PlacementNode],
    dst_placement_nodes: Sequence[m3placement.M3PlacementNode],
    nodes: Sequence[CoordinatorNode],
) -> None:
    replacements = []
    for idx, node, pnode in zip(node_to_backup_index, nodes, dst_placement_nodes):
        if idx is None:
            continue
        src_pnode = src_placement_nodes[idx]
        replacements.append(
            m3placement.M3PlacementNodeReplacement(src_pnode=src_pnode, dst_isolation_group=node.az, dst_pnode=pnode)
        )
    value = key.value_bytes
    value = m3placement.rewrite_m3_placement_bytes(value, replacements)
    key.set_value_bytes(value)
