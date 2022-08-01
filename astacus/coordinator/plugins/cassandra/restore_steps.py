"""Copyright (c) 2022 Aiven Ltd
See LICENSE for details

cassandra backup/restore plugin steps

"""

from .model import CassandraManifest
from .utils import get_schema_hash, run_subop
from astacus.common import ipc, utils
from astacus.common.cassandra.client import CassandraClient
from astacus.coordinator.cluster import Cluster
from astacus.coordinator.config import CoordinatorNode
from astacus.coordinator.plugins.base import (
    BackupManifestStep,
    get_node_to_backup_index,
    Step,
    StepFailedError,
    StepsContext,
)
from dataclasses import dataclass
from typing import List, Optional

import logging

logger = logging.getLogger(__name__)


@dataclass
class ParsePluginManifestStep(Step[CassandraManifest]):
    async def run_step(self, cluster: Cluster, context: StepsContext) -> CassandraManifest:
        backup_manifest = context.get_result(BackupManifestStep)
        return CassandraManifest.parse_obj(backup_manifest.plugin_data)


@dataclass
class StartCassandraStep(Step[None]):
    partial_restore_nodes: Optional[List[ipc.PartialRestoreRequestNode]]

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        backup_manifest = context.get_result(BackupManifestStep)

        node_to_backup_index = get_node_to_backup_index(
            partial_restore_nodes=self.partial_restore_nodes,
            snapshot_results=backup_manifest.snapshot_results,
            nodes=cluster.nodes,
        )

        reqs: List[ipc.NodeRequest] = []
        nodes: List[CoordinatorNode] = []
        plugin_manifest = context.get_result(ParsePluginManifestStep)
        for node_index, backup_index in enumerate(node_to_backup_index):
            if backup_index is None:
                continue
            nodes.append(cluster.nodes[node_index])
            reqs.append(ipc.CassandraStartRequest(tokens=plugin_manifest.nodes[backup_index].tokens))

        await run_subop(cluster, ipc.CassandraSubOp.start_cassandra, nodes=nodes, reqs=reqs, result_class=ipc.NodeResult)


@dataclass
class WaitCassandraUpStep(Step[None]):
    duration: int

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        last_err = None
        async for _ in utils.exponential_backoff(initial=1, maximum=60, duration=self.duration):
            current_hash, err = await get_schema_hash(cluster=cluster)
            if current_hash:
                return
            if err:
                last_err = err
        raise StepFailedError(f"Cassandra did not successfully come up: {last_err}")


@dataclass
class RestorePreDataStep(Step[None]):
    client: CassandraClient

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        # TBD: How does partial restore work in this case?
        manifest = context.get_result(ParsePluginManifestStep)
        await self.client.run_sync(manifest.cassandra_schema.restore_pre_data)


@dataclass
class RestorePostDataStep(Step[None]):
    client: CassandraClient

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        # TBD: How does partial restore work in this case?
        manifest = context.get_result(ParsePluginManifestStep)
        await self.client.run_sync(manifest.cassandra_schema.restore_post_data)