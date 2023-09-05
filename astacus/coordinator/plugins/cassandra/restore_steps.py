"""Copyright (c) 2022 Aiven Ltd
See LICENSE for details

cassandra backup/restore plugin steps

"""

from .model import CassandraConfigurationNode, CassandraManifest, CassandraManifestNode
from .utils import delta_snapshot_groups, get_schema_hash, run_subop
from astacus.common import ipc, utils
from astacus.common.asyncstorage import AsyncJsonStorage
from astacus.common.cassandra.client import CassandraClient
from astacus.common.cassandra.schema import CassandraKeyspace
from astacus.common.magic import JSON_DELTA_PREFIX
from astacus.coordinator.cluster import Cluster
from astacus.coordinator.config import CoordinatorNode
from astacus.coordinator.plugins.base import (
    BackupManifestStep,
    MapNodesStep,
    SnapshotStep,
    Step,
    StepFailedError,
    StepsContext,
)
from cassandra import metadata as cm
from dataclasses import dataclass
from typing import Iterable, List, Optional

import logging

logger = logging.getLogger(__name__)


def _create_network_topology_strategy_cql(keyspace: CassandraKeyspace) -> str:
    keyspace_metadata = cm.KeyspaceMetadata(
        name=keyspace.name,
        durable_writes=keyspace.durable_writes,
        strategy_class="NetworkTopologyStrategy",
        strategy_options=keyspace.network_topology_strategy_dcs,
    )
    return keyspace_metadata.as_cql_query()


def _rewrite_datacenters(keyspaces: Iterable[CassandraKeyspace]) -> None:
    for keyspace in keyspaces:
        if keyspace.network_topology_strategy_dcs:
            keyspace.cql_create_self = _create_network_topology_strategy_cql(keyspace)


@dataclass
class ParsePluginManifestStep(Step[CassandraManifest]):
    async def run_step(self, cluster: Cluster, context: StepsContext) -> CassandraManifest:
        backup_manifest = context.get_result(BackupManifestStep)
        cassandra_manifest = CassandraManifest.parse_obj(backup_manifest.plugin_data)
        _rewrite_datacenters(cassandra_manifest.cassandra_schema.keyspaces)
        return cassandra_manifest


@dataclass
class StopReplacedNodesStep(Step[List[CoordinatorNode]]):
    partial_restore_nodes: Optional[List[ipc.PartialRestoreRequestNode]]
    cassandra_nodes: List[CassandraConfigurationNode]

    async def run_step(self, cluster: Cluster, context: StepsContext) -> List[CoordinatorNode]:
        node_to_backup_index = context.get_result(MapNodesStep)
        cassandra_manifest = context.get_result(ParsePluginManifestStep)

        nodes_to_stop = []
        for backup_index in node_to_backup_index:
            if backup_index is None:
                continue
            backup_node = cassandra_manifest.nodes[backup_index]
            node_index = self.find_matching_cassandra_index(backup_node)
            if node_index is None:
                logger.warning("Failed to match backup node %s, assuming no longer in cluster -> not stopping it")
                continue
            nodes_to_stop.append(cluster.nodes[node_index])

        if nodes_to_stop:
            await run_subop(cluster, ipc.CassandraSubOp.stop_cassandra, nodes=nodes_to_stop, result_class=ipc.NodeResult)
        return nodes_to_stop

    def find_matching_cassandra_index(self, backup_node: CassandraManifestNode) -> Optional[int]:
        for cassandra_index, cassandra_node in enumerate(self.cassandra_nodes):
            if backup_node.matches_configuration_node(cassandra_node):
                return cassandra_index
        return None


@dataclass
class UploadFinalDeltaStep(Step[List[ipc.BackupManifest]]):
    json_storage: AsyncJsonStorage
    storage_name: str

    async def run_step(self, cluster: Cluster, context: StepsContext) -> List[ipc.BackupManifest]:
        replaced_nodes = context.get_result(StopReplacedNodesStep)
        assert len(replaced_nodes) == 1, "Multiple node replacement not supported"
        replaced_node = replaced_nodes[0]

        snapshot_results = await self.snapshot_delta(replaced_node, cluster=cluster, context=context)
        assert len(snapshot_results) == 1
        assert snapshot_results[0].hashes is not None
        upload_results = await self.upload_delta(replaced_node, snapshot_results[0].hashes, cluster=cluster)
        return await self.upload_manifest(snapshot_results, upload_results, context=context)

    async def snapshot_delta(
        self, node: CoordinatorNode, *, cluster: Cluster, context: StepsContext
    ) -> List[ipc.SnapshotResult]:
        snapshot_results = await SnapshotStep(
            snapshot_groups=delta_snapshot_groups(), snapshot_request="delta/snapshot", nodes_to_snapshot=[node]
        ).run_step(cluster, context)
        return snapshot_results

    async def upload_delta(
        self, node: CoordinatorNode, hashes: List[ipc.SnapshotHash], *, cluster: Cluster
    ) -> List[ipc.SnapshotUploadResult]:
        upload_req: ipc.NodeRequest = ipc.SnapshotUploadRequestV20221129(
            hashes=hashes, storage=self.storage_name, validate_file_hashes=False
        )
        upload_start_results = await cluster.request_from_nodes(
            "delta/upload", caller="upload_final_delta", method="post", req=upload_req, nodes=[node]
        )
        return await cluster.wait_successful_results(
            start_results=upload_start_results, result_class=ipc.SnapshotUploadResult
        )

    async def upload_manifest(
        self,
        snapshot_results: List[ipc.SnapshotResult],
        upload_results: List[ipc.SnapshotUploadResult],
        *,
        context: StepsContext,
    ) -> List[ipc.BackupManifest]:
        iso = context.attempt_start.isoformat(timespec="seconds")
        backup_name = f"{JSON_DELTA_PREFIX}{iso}"
        manifest = ipc.BackupManifest(
            attempt=context.attempt,
            start=context.attempt_start,
            snapshot_results=snapshot_results,
            upload_results=upload_results,
            plugin=ipc.Plugin.cassandra,
            plugin_data={},
            filename=backup_name,
        )
        logger.info("Storing backup manifest %s", backup_name)
        await self.json_storage.upload_json(backup_name, manifest)
        return [manifest]


@dataclass
class StartCassandraStep(Step[None]):
    override_tokens: bool
    cassandra_nodes: List[CassandraConfigurationNode]
    replace_backup_nodes: bool = False

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        node_to_backup_index = context.get_result(MapNodesStep)

        reqs: List[ipc.NodeRequest] = []
        nodes: List[CoordinatorNode] = []
        plugin_manifest = context.get_result(ParsePluginManifestStep)
        for node_index, backup_index in enumerate(node_to_backup_index):
            if backup_index is None:
                continue
            coordinator = cluster.nodes[node_index]
            backup_node = plugin_manifest.nodes[backup_index]
            nodes.append(coordinator)
            tokens: Optional[List[str]] = None
            replace_address_first_boot: Optional[str] = None
            skip_bootstrap_streaming: Optional[bool] = None
            if self.override_tokens:
                tokens = backup_node.tokens
            backup_node_in_cluster = any(n for n in self.cassandra_nodes if backup_node.matches_configuration_node(n))
            if self.replace_backup_nodes and backup_node_in_cluster:
                replace_address_first_boot = backup_node.listen_address
                skip_bootstrap_streaming = True
            reqs.append(
                ipc.CassandraStartRequest(
                    tokens=tokens,
                    replace_address_first_boot=replace_address_first_boot,
                    skip_bootstrap_streaming=skip_bootstrap_streaming,
                )
            )

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
