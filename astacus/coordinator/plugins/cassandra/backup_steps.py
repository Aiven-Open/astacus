"""Copyright (c) 2022 Aiven Ltd
See LICENSE for details

cassandra backup/restore plugin steps

"""
from .model import CassandraConfigurationNode, CassandraManifest, CassandraManifestNode
from .utils import get_schema_hash
from astacus.common import exceptions
from astacus.common.cassandra.client import CassandraClient, CassandraSession
from astacus.common.cassandra.schema import CassandraKeyspace, CassandraSchema
from astacus.coordinator.cluster import Cluster
from astacus.coordinator.plugins.base import Step, StepFailedError, StepsContext
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Optional

import logging

logger = logging.getLogger(__name__)


class CassandraSchemaRetrievalFailureException(exceptions.TransientException):
    pass


def _remove_other_datacenters(keyspace: CassandraKeyspace, datacenter: str) -> None:
    remaining_dc_rf = keyspace.network_topology_strategy_dcs.get(datacenter)
    if not remaining_dc_rf:
        # Could be intentional, but most likely is a typo in configuration => raise.
        raise ValueError(f"Keyspace {keyspace.name} not replicated in configured DC {datacenter}")
    keyspace.network_topology_strategy_dcs = {datacenter: remaining_dc_rf}


def _retrieve_manifest_from_cassandra(
    cas: CassandraSession, config_nodes: Sequence[CassandraConfigurationNode], *, datacenter: Optional[str]
) -> CassandraManifest:
    host_id_to_node: dict[str, CassandraManifestNode] = {}
    for token, host in cas.cluster_metadata.token_map.token_to_host_owner.items():
        if datacenter and host.datacenter != datacenter:
            logger.debug(
                "Skipping host %s, because its datacenter %s doesn't match configured %s",
                host.host_id,
                host.datacenter,
                datacenter,
            )
            continue
        node = host_id_to_node.get(host.host_id)
        if node is None:
            # host.listen_address is for the local host in system.local
            # host.broadcast_address is for the peers in system.peers ('peer' field)
            node = CassandraManifestNode(
                address=host.address,
                host_id=host.host_id,
                listen_address=host.listen_address or host.broadcast_address,
                rack=host.rack,
                tokens=[],
            )
            host_id_to_node[host.host_id] = node
            # Assume Murmur3Token
        node.tokens.append(str(token.value))

    logger.debug("Cassandra nodes: %r", list(host_id_to_node.values()))
    nodes = []
    for configured_node in config_nodes:
        matching_manifest_nodes = [
            manifest_node
            for manifest_node in host_id_to_node.values()
            if manifest_node.matches_configuration_node(configured_node)
        ]

        # Ensure it matches only 1
        if len(matching_manifest_nodes) == 0:
            raise ValueError(f"No matching cluster node for {configured_node}")
        if len(matching_manifest_nodes) > 1:
            raise ValueError(f"Too many matching cluster nodes for {configured_node}: {matching_manifest_nodes}")

        # And the matched one hasn't been matched before
        if matching_manifest_nodes[0] in nodes:
            raise ValueError(f"{matching_manifest_nodes[0]} matched more than once")

        nodes.append(matching_manifest_nodes[0])

    if len(nodes) != len(host_id_to_node):
        raise ValueError(f"Incorrect node count: {len(nodes)} <> {len(host_id_to_node)}")

    schema = CassandraSchema.from_cassandra_session(cas)
    if datacenter:
        for keyspace in schema.keyspaces:
            if keyspace.network_topology_strategy_dcs:
                _remove_other_datacenters(keyspace, datacenter)
    return CassandraManifest(cassandra_schema=schema, nodes=nodes)


@dataclass
class RetrieveSchemaHashStep(Step[str]):
    async def run_step(self, cluster: Cluster, context: StepsContext) -> str:
        schema_hash, err = await get_schema_hash(cluster=cluster)
        if not schema_hash:
            raise CassandraSchemaRetrievalFailureException(err)
        return schema_hash


@dataclass
class AssertSchemaUnchanged(Step[None]):
    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        schema_hash, err = await get_schema_hash(cluster=cluster)
        if not schema_hash:
            raise CassandraSchemaRetrievalFailureException(err)
        if schema_hash != context.get_result(RetrieveSchemaHashStep):
            raise StepFailedError("schema hash mismatch snapshot")


@dataclass
class PrepareCassandraManifestStep(Step[dict[str, Any]]):
    client: CassandraClient
    nodes: Sequence[CassandraConfigurationNode]
    datacenter: Optional[str]

    async def run_step(self, cluster: Cluster, context: StepsContext) -> dict[str, Any]:
        result = await self.client.run_sync(_retrieve_manifest_from_cassandra, self.nodes, datacenter=self.datacenter)
        assert result
        return result.dict()
