"""Copyright (c) 2022 Aiven Ltd
See LICENSE for details

cassandra backup/restore plugin steps

"""

from .model import CassandraConfigurationNode, CassandraManifest, CassandraManifestNode
from .utils import delta_snapshot_groups, get_schema_hash, run_subop
from astacus.common import ipc, utils
from astacus.common.asyncstorage import AsyncJsonStorage
from astacus.common.cassandra.client import CassandraClient
from astacus.common.cassandra.config import BACKUP_GLOB
from astacus.common.cassandra.schema import CassandraKeyspace
from astacus.common.cassandra.utils import SYSTEM_KEYSPACES
from astacus.common.magic import JSON_DELTA_PREFIX
from astacus.coordinator.cluster import Cluster
from astacus.coordinator.config import CoordinatorNode
from astacus.coordinator.plugins.base import (
    BackupManifestStep,
    get_nodes_metadata,
    MapNodesStep,
    RestoreDeltasStep,
    Step,
    StepFailedError,
    StepsContext,
)
from cassandra import metadata as cm
from dataclasses import dataclass, field
from typing import Iterable, List, Optional, Type

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
        if len(replaced_nodes) != 1:
            raise StepFailedError(f"Multiple node replacement not supported: {replaced_nodes}")
        replaced_node = replaced_nodes[0]

        snapshot_results = await self.snapshot_delta(replaced_node, cluster=cluster, context=context)
        if len(snapshot_results) != 1:
            raise StepFailedError(f"Unexpected snapshot results (expected only 1): {snapshot_results}")
        final_delta_hashes = snapshot_results[0].hashes
        if final_delta_hashes is None:
            raise StepFailedError("Final delta hashes are missing (None)")
        upload_results = await self.upload_delta(replaced_node, final_delta_hashes, cluster=cluster)
        return await self.upload_manifest(snapshot_results, upload_results, context=context)

    async def snapshot_delta(
        self, node: CoordinatorNode, *, cluster: Cluster, context: StepsContext
    ) -> List[ipc.SnapshotResult]:
        nodes_metadata = await get_nodes_metadata(cluster, nodes=[node])
        if len(nodes_metadata) != 1:
            raise StepFailedError(f"Unexpected node metadata results (expected only 1): {nodes_metadata}")
        node_features = set(nodes_metadata[0].features)
        req = ipc.create_snapshot_request(delta_snapshot_groups(), node_features=node_features)
        start_results = await cluster.request_from_nodes(
            "delta/snapshot", method="post", caller="UploadFinalDeltaStep", req=req, nodes=[node]
        )
        return await cluster.wait_successful_results(start_results=start_results, result_class=ipc.SnapshotResult)

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
    # Only StopReplacedNodesStep's result can be directly substracted from cluster.nodes
    # thus retricting the type to Optional[StopReplacedNodesStep]
    replaced_node_step: Optional[Type[StopReplacedNodesStep]] = None

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        last_err = None

        nodes = None
        if self.replaced_node_step:
            replaced_nodes = context.get_result(self.replaced_node_step)
            if len(replaced_nodes) != 1:
                raise StepFailedError(f"Multiple node replacement not supported: {replaced_nodes}")
            replaced_node = replaced_nodes[0]
            if replaced_node not in cluster.nodes:
                logger.warning("Replaced %s is not part of the cluster.nodes: %s", replaced_node, cluster.nodes)
            else:
                nodes = [n for n in cluster.nodes if n != replaced_node]

        async for _ in utils.exponential_backoff(initial=1, maximum=60, duration=self.duration):
            current_hash, err = await get_schema_hash(cluster=cluster, nodes=nodes)
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


def restore_cassandra_deltas_req() -> ipc.NodeRequest:
    return ipc.CassandraRestoreSSTablesRequest(
        table_glob=BACKUP_GLOB,
        keyspaces_to_skip=[ks for ks in SYSTEM_KEYSPACES if ks != "system_schema"],
        match_tables_by=ipc.CassandraTableMatching.cfid,
        expect_empty_target=False,
    )


@dataclass
class RestoreCassandraDeltasStep(RestoreDeltasStep):
    restore_delta_url: str = f"cassandra/{ipc.CassandraSubOp.restore_sstables}"
    restore_delta_request: ipc.NodeRequest = field(default_factory=restore_cassandra_deltas_req)


@dataclass
class RestoreFinalDeltasStep(RestoreDeltasStep):
    restore_delta_url: str = f"cassandra/{ipc.CassandraSubOp.restore_sstables}"
    restore_delta_request: ipc.NodeRequest = field(default_factory=restore_cassandra_deltas_req)
    delta_manifests_step: Type[Step[List[ipc.BackupManifest]]] = UploadFinalDeltaStep
