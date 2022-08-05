"""Copyright (c) 2020 Aiven Ltd
See LICENSE for details

cassandra backup/restore plugin

"""

from .model import CassandraConfigurationNode
from .utils import run_subop
from astacus.common import ipc
from astacus.common.cassandra.client import CassandraClient
from astacus.common.cassandra.config import CassandraClientConfiguration, SNAPSHOT_NAME
from astacus.coordinator.cluster import Cluster
from astacus.coordinator.plugins import base
from astacus.coordinator.plugins.base import CoordinatorPlugin, OperationContext, Step, StepFailedError, StepsContext
from astacus.coordinator.plugins.cassandra import backup_steps, restore_steps
from dataclasses import dataclass
from typing import List, Optional

import logging

logger = logging.getLogger(__name__)


@dataclass
class CassandraSubOpStep(Step[None]):
    op: ipc.CassandraSubOp

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        await run_subop(cluster, self.op, result_class=ipc.NodeResult)
        # We intentionally return always None so this same (class )
        # can be reused in set of plugin steps with different
        # parameters; if the operation fails, we throw exception
        # instead


@dataclass
class ValidateConfigurationStep(Step[None]):
    nodes: List[CassandraConfigurationNode]

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        cnode_count = len(self.nodes)
        node_count = len(cluster.nodes)
        if cnode_count != node_count:
            diff = node_count - cnode_count
            raise StepFailedError(f"{node_count} nodes, yet {cnode_count} nodes in the cassandra nodes - diff:{diff}")


class CassandraPlugin(CoordinatorPlugin):
    client: CassandraClientConfiguration
    nodes: Optional[List[CassandraConfigurationNode]]
    restore_start_timeout: int = 3600

    def get_backup_steps(self, *, context: OperationContext) -> List[Step]:
        nodes = self.nodes or [CassandraConfigurationNode(listen_address=self.client.get_listen_address())]
        client = CassandraClient(self.client)
        # first *: keyspace name; second *: table name
        snapshot_root_globs = [
            f"data/*/*/snapshots/{SNAPSHOT_NAME}/manifest.json",
            f"data/*/*/snapshots/{SNAPSHOT_NAME}/*.db",
            f"data/*/*/snapshots/{SNAPSHOT_NAME}/*.txt",
            f"data/*/*/snapshots/{SNAPSHOT_NAME}/*.cql",
        ]

        return [
            ValidateConfigurationStep(nodes=nodes),
            backup_steps.RetrieveSchemaHashStep(),
            backup_steps.PrepareCassandraManifestStep(client=client, nodes=nodes),
            CassandraSubOpStep(op=ipc.CassandraSubOp.remove_snapshot),
            CassandraSubOpStep(op=ipc.CassandraSubOp.take_snapshot),
            backup_steps.AssertSchemaUnchanged(),
            base.SnapshotStep(snapshot_root_globs=snapshot_root_globs),
            base.ListHexdigestsStep(hexdigest_storage=context.hexdigest_storage),
            base.UploadBlocksStep(storage_name=context.storage_name),
            CassandraSubOpStep(op=ipc.CassandraSubOp.remove_snapshot),
            base.UploadManifestStep(
                json_storage=context.json_storage,
                plugin=ipc.Plugin.cassandra,
                plugin_manifest_step=backup_steps.PrepareCassandraManifestStep,
            ),
        ]

    def get_restore_steps(self, *, context: OperationContext, req: ipc.RestoreRequest) -> List[Step]:
        # The nodes are not really used for now; perhaps they should be, at some point?
        # their validity is checked just for symmetry with backup, for now.
        nodes = self.nodes or [CassandraConfigurationNode(listen_address=self.client.get_listen_address())]
        client = CassandraClient(self.client)

        # TBD: partial_restore_nodes should be probably passed to about all steps?
        return [
            ValidateConfigurationStep(nodes=nodes),
            base.BackupNameStep(json_storage=context.json_storage, requested_name=req.name),
            base.BackupManifestStep(json_storage=context.json_storage),
            restore_steps.ParsePluginManifestStep(),
            # Start cassandra with backed up token distribution + set schema + stop it
            restore_steps.StartCassandraStep(partial_restore_nodes=req.partial_restore_nodes, override_tokens=True),
            restore_steps.WaitCassandraUpStep(duration=self.restore_start_timeout),
            restore_steps.RestorePreDataStep(client=client),
            CassandraSubOpStep(op=ipc.CassandraSubOp.stop_cassandra),
            # Restore snapshot
            base.RestoreStep(storage_name=context.storage_name, partial_restore_nodes=req.partial_restore_nodes),
            CassandraSubOpStep(op=ipc.CassandraSubOp.restore_snapshot),
            # restart cassandra and do the final actions with data available
            # not configuring tokens here, because we've already bootstrapped the ring when restoring schema
            restore_steps.StartCassandraStep(partial_restore_nodes=req.partial_restore_nodes, override_tokens=False),
            restore_steps.WaitCassandraUpStep(duration=self.restore_start_timeout),
            restore_steps.RestorePostDataStep(client=client),
        ]
