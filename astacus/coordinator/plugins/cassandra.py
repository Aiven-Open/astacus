"""Copyright (c) 2020 Aiven Ltd
See LICENSE for details

cassandra backup/restore plugin

"""

from .base import BackupOpBase, RestoreOpBase
from astacus.common import exceptions, ipc, utils
from astacus.common.cassandra.client import client_context
from astacus.common.cassandra.config import CassandraClientConfig, SNAPSHOT_NAME
from astacus.common.cassandra.schema import CassandraSchema
from astacus.common.utils import AstacusModel
from starlette.concurrency import run_in_threadpool
from typing import List, Optional
from uuid import UUID

import logging

logger = logging.getLogger(__name__)


class CassandraInconsistentNodeLengthException(exceptions.PermanentException):
    pass


class CassandraConfigurationNode(AstacusModel):
    # The configured node order has to be identified _somehow_;
    # otherwise, we cannot map the same data to same set of
    # tokens. One of these is required.
    address: Optional[str]
    host_id: Optional[UUID]
    listen_address: Optional[str]
    tokens: Optional[List[str]]


class CassandraConfiguration(AstacusModel):
    client: CassandraClientConfig
    nodes: List[CassandraConfigurationNode]
    restore_start_timeout: int = 3600


class CassandraManifestNode(AstacusModel):
    address: str
    host_id: UUID
    listen_address: str
    rack: str
    tokens: List[str]

    def matches_configuration_node(self, node: CassandraConfigurationNode):
        for attribute in ["address", "host_id", "listen_address", "tokens"]:
            other_value = getattr(node, attribute)
            if other_value and getattr(self, attribute) != other_value:
                return False
        return True


class CassandraManifest(AstacusModel):
    cassandra_schema: CassandraSchema
    nodes: List[CassandraManifestNode]


class CassandraOpMixin:
    def get_indexed_cassandra_nodes(self):
        """Return the currently active Cassandra nodes, with index in backup (if any)."""
        return dict(enumerate(self.nodes))

    async def run_subop(self, *, subop, result_class, req=None, reqs=None):
        if not req and not reqs:
            req = ipc.NodeRequest()
        index_to_node = self.get_indexed_cassandra_nodes()
        assert index_to_node
        start_results = await self.request_from_nodes(
            f"cassandra/{subop.value}",
            method="post",
            caller="Cassandra.run_subop",
            req=req,
            reqs=reqs,
            nodes=index_to_node.values(),
        )
        if not start_results:
            return []
        return await self.wait_successful_results(start_results, result_class=result_class)

    async def run_sync_with_cassandra(self, fun):
        def run():
            with client_context(self.plugin_config.client) as cas:
                fun(cas)

        return await run_in_threadpool(run)

    async def get_schema_hash(self, *, log_errors: bool = True) -> str:
        hashes = [
            x.schema_hash for x in await
            self.run_subop(subop=ipc.CassandraSubOp.get_schema_hash, result_class=ipc.CassandraGetSchemaHashResult)
        ]
        if not hashes:
            if log_errors:
                logger.info("Unable to retrieve schema hash at all")
            return ""
        if len(set(hashes)) != 1:
            if log_errors:
                logger.info("Multiple schema hashes present: %r", hashes)
            return ""
        return hashes[0]


def _validate_cassandra_config(o):
    pnode_count = len(o.plugin_config.nodes)
    node_count = len(o.nodes)
    if pnode_count != node_count:
        diff = node_count - pnode_count
        raise CassandraInconsistentNodeLengthException(
            f"{node_count} nodes, yet {pnode_count} nodes in the cassandra nodes - diff:{diff}"
        )
    return True


class CassandraBackupOp(BackupOpBase, CassandraOpMixin):
    steps = [
        # local -->
        "init",
        "retrieve_schema_hash",
        "retrieve_manifest",
        "remove_cassandra_snapshot",
        "take_cassandra_snapshot",
        "retrieve_schema_hash_again",

        # base -->
        "snapshot",
        "list_hexdigests",
        "upload_blocks",
        "remove_cassandra_snapshot_again",  # local
        "upload_manifest",  # base
    ]
    plugin = ipc.Plugin.cassandra
    # first *: keyspace name; second *: table name
    snapshot_root_globs = [
        f"data/*/*/snapshots/{SNAPSHOT_NAME}/manifest.json",
        f"data/*/*/snapshots/{SNAPSHOT_NAME}/*.db",
        f"data/*/*/snapshots/{SNAPSHOT_NAME}/*.txt",
        f"data/*/*/snapshots/{SNAPSHOT_NAME}/*.cql",
    ]

    async def step_init(self):
        _validate_cassandra_config(self)
        return True

    async def step_retrieve_schema_hash(self):
        return await self.get_schema_hash()

    def retrieve_manifest_from_cassandra(self, cas):
        host_id_to_node = {}
        for token, host in cas.cluster.metadata.token_map.token_to_host_owner.items():
            node = host_id_to_node.get(host.host_id)
            if node is None:
                node = CassandraManifestNode(
                    address=host.address,
                    host_id=host.host_id,
                    listen_address=host.listen_address,
                    rack=host.rack,
                    tokens=[]
                )
                host_id_to_node[host.host_id] = node
            # Assume Murmur3Token
            node.tokens.append(str(token.value))
        logger.debug("Cassandra nodes: %r", list(host_id_to_node.values()))
        nodes = []
        for configured_node in self.plugin_config.nodes:
            matching_manifest_nodes = [
                manifest_node for manifest_node in host_id_to_node.values()
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

        schema = CassandraSchema.from_cassandra_client(cas)
        return CassandraManifest(cassandra_schema=schema, nodes=nodes)

    async def step_retrieve_manifest(self):
        manifest = await self.run_sync_with_cassandra(self.retrieve_manifest_from_cassandra)
        self.plugin_data = manifest.dict()
        return manifest

    async def step_remove_cassandra_snapshot(self):
        return await self.run_subop(subop=ipc.CassandraSubOp.remove_snapshot, result_class=ipc.NodeResult)

    async def step_remove_cassandra_snapshot_again(self):
        return await self.step_remove_cassandra_snapshot()

    async def step_take_cassandra_snapshot(self):
        return await self.run_subop(subop=ipc.CassandraSubOp.take_snapshot, result_class=ipc.NodeResult)

    async def step_retrieve_schema_hash_again(self):
        schema_hash = await self.get_schema_hash()
        if not schema_hash:
            # Error logged in get_schema_hash
            return False
        if schema_hash != self.result_retrieve_schema_hash:
            logger.info("Schema hash changed during operation: %r -> %r", self.result_retrieve_schema_hash, schema_hash)
            return False
        return True


class CassandraRestoreOp(RestoreOpBase, CassandraOpMixin):
    steps = [
        "init",  # local

        # base -->
        "backup_name",
        "backup_manifest",

        # local -->
        "start_cassandra",
        "wait_cassandra_up_again",
        "retrieve_schema_hash",
        "restore_schema_pre_data",
        "restore",  # base

        # local -->
        "restore_cassandra_snapshot",
        "wait_cassandra_up_again",
        "restore_schema_post_data",
    ]

    plugin = ipc.Plugin.cassandra

    def get_indexed_cassandra_nodes(self):
        # Restore operation is applied only to the nodes that should
        # be restored to, based on the backup index.  Other nodes are
        # silently ignored.

        # TBD: We should ensure that local node gets restored always;
        # cassandra restoration will not work without local node (of
        # the coordinator) being part of the cassandra cluster.

        return {idx: node for idx, node in zip(self._get_node_to_backup_index(), self.nodes) if idx is not None}

    async def step_init(self):
        _validate_cassandra_config(self)
        return True

    async def step_start_cassandra(self):
        reqs = [
            ipc.CassandraStartRequest(tokens=self.plugin_manifest.nodes[idx].tokens)
            for idx in self.get_indexed_cassandra_nodes()
        ]
        return await self.run_subop(subop=ipc.CassandraSubOp.start_cassandra, result_class=ipc.NodeResult, reqs=reqs)

    async def step_retrieve_schema_hash(self):
        return await self.get_schema_hash()

    async def step_restore_schema_pre_data(self):
        await self.run_sync_with_cassandra(self.plugin_manifest.cassandra_schema.restore_pre_data)
        return True

    async def step_restore_cassandra_snapshot(self):
        return await self.run_subop(subop=ipc.CassandraSubOp.restore_snapshot, result_class=ipc.NodeResult)

    async def step_wait_cassandra_up(self):
        async for _ in utils.exponential_backoff(initial=1, maximum=60, duration=self.plugin_config.restore_start_timeout):
            current_hash = await self.get_schema_hash(log_errors=False)
            if current_hash:
                return True
        return False

    async def step_wait_cassandra_up_again(self):
        return await self.step_wait_cassandra_up()

    async def step_restore_schema_post_data(self):
        await self.run_sync_with_cassandra(self.plugin_manifest.cassandra_schema.restore_post_data)
        return True


plugin_info = {
    "config": CassandraConfiguration,
    "backup": CassandraBackupOp,
    "manifest": CassandraManifest,
    "restore": CassandraRestoreOp,
}
