"""Copyright (c) 2022 Aiven Ltd
See LICENSE for details

cassandra backup/restore plugin steps

"""

from .model import CassandraConfigurationNode, CassandraManifest, CassandraManifestNode
from .utils import get_schema_hash, run_subop
from astacus.common import ipc, utils
from astacus.common.cassandra.client import CassandraClient
from astacus.common.cassandra.schema import CassandraKeyspace
from astacus.coordinator.cluster import Cluster
from astacus.coordinator.config import CoordinatorNode
from astacus.coordinator.plugins.base import BackupManifestStep, MapNodesStep, Step, StepFailedError, StepsContext
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
class StopReplacedNodesStep(Step[None]):
    partial_restore_nodes: Optional[List[ipc.PartialRestoreRequestNode]]
    cassandra_nodes: List[CassandraConfigurationNode]

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
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

    def find_matching_cassandra_index(self, backup_node: CassandraManifestNode) -> Optional[int]:
        for cassandra_index, cassandra_node in enumerate(self.cassandra_nodes):
            if backup_node.matches_configuration_node(cassandra_node):
                return cassandra_index
        return None


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
