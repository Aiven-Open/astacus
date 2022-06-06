"""Copyright (c) 2022 Aiven Ltd
See LICENSE for details

cassandra backup/restore plugin steps

"""

from .model import CassandraConfigurationNode, CassandraManifest, CassandraManifestNode
from .utils import get_schema_hash
from astacus.common import exceptions
from astacus.common.cassandra.client import CassandraClient, CassandraSession
from astacus.common.cassandra.schema import CassandraSchema
from astacus.coordinator.cluster import Cluster
from astacus.coordinator.plugins.base import Step, StepFailedError, StepsContext
from dataclasses import dataclass
from typing import Dict, List

import logging

logger = logging.getLogger(__name__)


class CassandraSchemaRetrievalFailureException(exceptions.TransientException):
    pass


def _retrieve_manifest_from_cassandra(
    cas: CassandraSession, config_nodes: List[CassandraConfigurationNode]
) -> CassandraManifest:
    host_id_to_node: Dict[str, CassandraManifestNode] = {}
    for token, host in cas.cluster_metadata.token_map.token_to_host_owner.items():
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
class PrepareCassandraManifestStep(Step[Dict]):
    client: CassandraClient
    nodes: List[CassandraConfigurationNode]

    async def run_step(self, cluster: Cluster, context: StepsContext) -> Dict:
        result = await self.client.run_sync(_retrieve_manifest_from_cassandra, self.nodes)
        assert result
        return result.dict()
