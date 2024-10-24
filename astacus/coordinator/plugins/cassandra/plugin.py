"""Copyright (c) 2020 Aiven Ltd
See LICENSE for details.

cassandra backup/restore plugin

"""

from .model import CassandraConfigurationNode
from .utils import delta_snapshot_groups, run_subop, snapshot_groups
from astacus.common import ipc
from astacus.common.cassandra.client import CassandraClient
from astacus.common.cassandra.config import CassandraClientConfiguration, SNAPSHOT_GLOB
from astacus.common.cassandra.utils import SYSTEM_KEYSPACES
from astacus.common.magic import JSON_DELTA_PREFIX
from astacus.coordinator.cluster import Cluster
from astacus.coordinator.plugins import base
from astacus.coordinator.plugins.base import (
    CoordinatorPlugin,
    MapNodesStep,
    OperationContext,
    Step,
    StepFailedError,
    StepsContext,
)
from astacus.coordinator.plugins.cassandra import backup_steps, restore_steps
from collections.abc import Sequence, Set
from dataclasses import dataclass
from typing import Any

import dataclasses
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
class CassandraRestoreSubOpStep(Step[None]):
    op: ipc.CassandraSubOp
    req: ipc.NodeRequest | None = None

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        node_to_backup_index = context.get_result(MapNodesStep)
        nodes = [
            cluster.nodes[node_index]
            for node_index, backup_index in enumerate(node_to_backup_index)
            if backup_index is not None
        ]

        await run_subop(cluster, self.op, nodes=nodes, req=self.req, result_class=ipc.NodeResult)


@dataclass
class ValidateConfigurationStep(Step[None]):
    nodes: Sequence[CassandraConfigurationNode]

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        cnode_count = len(self.nodes)
        node_count = len(cluster.nodes)
        if cnode_count != node_count:
            diff = node_count - cnode_count
            raise StepFailedError(f"{node_count} nodes, yet {cnode_count} nodes in the cassandra nodes - diff:{diff}")


class CassandraPlugin(CoordinatorPlugin):
    client: CassandraClientConfiguration
    nodes: Sequence[CassandraConfigurationNode] | None = None
    datacenter: str | None = None
    restore_start_timeout: int = 3600

    def get_backup_steps(self, *, context: OperationContext) -> Sequence[Step[Any]]:
        nodes = self.nodes or [CassandraConfigurationNode(listen_address=self.client.get_listen_address())]
        client = CassandraClient(self.client)

        return [
            ValidateConfigurationStep(nodes=nodes),
            backup_steps.RetrieveSchemaHashStep(),
            backup_steps.PrepareCassandraManifestStep(client=client, nodes=nodes, datacenter=self.datacenter),
            CassandraSubOpStep(op=ipc.CassandraSubOp.remove_snapshot),
            CassandraSubOpStep(op=ipc.CassandraSubOp.take_snapshot),
            backup_steps.AssertSchemaUnchanged(),
            base.SnapshotStep(snapshot_groups=snapshot_groups()),
            base.ListHexdigestsStep(hexdigest_storage=context.hexdigest_storage),
            base.UploadBlocksStep(storage_name=context.storage_name),
            CassandraSubOpStep(op=ipc.CassandraSubOp.remove_snapshot),
            base.SnapshotReleaseStep(),
            base.UploadManifestStep(
                json_storage=context.json_storage,
                plugin=ipc.Plugin.cassandra,
                plugin_manifest_step=backup_steps.PrepareCassandraManifestStep,
            ),
        ]

    def get_delta_backup_steps(self, *, context: OperationContext) -> Sequence[Step[Any]]:
        nodes = self.nodes or [CassandraConfigurationNode(listen_address=self.client.get_listen_address())]

        @dataclasses.dataclass
        class SkipHexdigestsListStep(Step[Set[str]]):
            async def run_step(self, cluster: Cluster, context: StepsContext) -> Set[str]:
                return set()

        return [
            ValidateConfigurationStep(nodes=nodes),
            base.SnapshotStep(snapshot_groups=delta_snapshot_groups(), snapshot_request="delta/snapshot"),
            SkipHexdigestsListStep(),
            base.UploadBlocksStep(
                storage_name=context.storage_name,
                upload_request="delta/upload",
                # Data in delta backups is assumed to be new, supply an empty list of uploaded hashes.
                list_hexdigests_step=SkipHexdigestsListStep,
            ),
            base.UploadManifestStep(
                json_storage=context.json_storage,
                plugin=ipc.Plugin.cassandra,
                plugin_manifest_step=None,
                backup_prefix=JSON_DELTA_PREFIX,
            ),
            base.SnapshotClearStep(clear_request="delta/clear"),
        ]

    def get_restore_steps(self, *, context: OperationContext, req: ipc.RestoreRequest) -> Sequence[Step[Any]]:
        nodes = self.nodes or [CassandraConfigurationNode(listen_address=self.client.get_listen_address())]
        cluster_restore_steps = (
            self.get_restore_schema_from_snapshot_steps(context=context, req=req)
            if req.partial_restore_nodes
            else self.get_restore_schema_from_manifest_steps(context=context, req=req)
        )

        return [
            ValidateConfigurationStep(nodes=nodes),
            base.BackupNameStep(json_storage=context.json_storage, requested_name=req.name),
            base.BackupManifestStep(json_storage=context.json_storage),
            restore_steps.ParsePluginManifestStep(),
            base.MapNodesStep(partial_restore_nodes=req.partial_restore_nodes),
            CassandraRestoreSubOpStep(op=ipc.CassandraSubOp.stop_cassandra),
            CassandraRestoreSubOpStep(op=ipc.CassandraSubOp.unrestore_sstables),
            *cluster_restore_steps,
        ]

    def get_restore_schema_from_snapshot_steps(
        self, *, context: OperationContext, req: ipc.RestoreRequest
    ) -> Sequence[Step[Any]]:
        assert self.nodes is not None
        restore_sstables_req = ipc.CassandraRestoreSSTablesRequest(
            table_glob=SNAPSHOT_GLOB,
            keyspaces_to_skip=[ks for ks in SYSTEM_KEYSPACES if ks != "system_schema"],
            match_tables_by=ipc.CassandraTableMatching.cfid,
            expect_empty_target=True,
        )

        return [
            base.RestoreStep(storage_name=context.storage_name, partial_restore_nodes=req.partial_restore_nodes),
            CassandraRestoreSubOpStep(op=ipc.CassandraSubOp.restore_sstables, req=restore_sstables_req),
            base.DeltaManifestsStep(json_storage=context.json_storage),
            restore_steps.RestoreCassandraDeltasStep(
                json_storage=context.json_storage,
                storage_name=context.storage_name,
                partial_restore_nodes=req.partial_restore_nodes,
            ),
            restore_steps.StopReplacedNodesStep(partial_restore_nodes=req.partial_restore_nodes, cassandra_nodes=self.nodes),
            restore_steps.UploadFinalDeltaStep(json_storage=context.json_storage, storage_name=context.storage_name),
            restore_steps.RestoreFinalDeltasStep(
                json_storage=context.json_storage,
                storage_name=context.storage_name,
                partial_restore_nodes=req.partial_restore_nodes,
            ),
            restore_steps.StartCassandraStep(replace_backup_nodes=True, override_tokens=True, cassandra_nodes=self.nodes),
            restore_steps.WaitCassandraUpStep(
                duration=self.restore_start_timeout, replaced_node_step=restore_steps.StopReplacedNodesStep
            ),
        ]

    def get_restore_schema_from_manifest_steps(
        self, *, context: OperationContext, req: ipc.RestoreRequest
    ) -> Sequence[Step[Any]]:
        assert self.nodes is not None
        client = CassandraClient(self.client)
        restore_sstables_req = ipc.CassandraRestoreSSTablesRequest(
            table_glob=SNAPSHOT_GLOB,
            keyspaces_to_skip=SYSTEM_KEYSPACES,
            match_tables_by=ipc.CassandraTableMatching.cfname,
            expect_empty_target=True,
        )
        return [
            # Start cassandra with backed up token distribution + set schema + stop it
            restore_steps.StartCassandraStep(override_tokens=True, cassandra_nodes=self.nodes),
            restore_steps.WaitCassandraUpStep(duration=self.restore_start_timeout),
            restore_steps.RestorePreDataStep(client=client),
            CassandraRestoreSubOpStep(op=ipc.CassandraSubOp.stop_cassandra),
            # Restore snapshot. Restoring deltas is not possible in this scenario,
            # because once we've created our own system_schema keyspace and written data to it,
            # we've started a new sequence of sstables that might clash with the sequence from the node
            # we took the backup from (e.g. the old node had nb-1, the new node has nb-1, unclear how to proceed).
            base.RestoreStep(storage_name=context.storage_name, partial_restore_nodes=req.partial_restore_nodes),
            CassandraRestoreSubOpStep(op=ipc.CassandraSubOp.restore_sstables, req=restore_sstables_req),
            # restart cassandra and do the final actions with data available
            # not configuring tokens here, because we've already bootstrapped the ring when restoring schema
            restore_steps.StartCassandraStep(override_tokens=False, cassandra_nodes=self.nodes),
            restore_steps.WaitCassandraUpStep(duration=self.restore_start_timeout),
            restore_steps.RestorePostDataStep(client=client),
        ]

    def get_cleanup_steps(
        self, *, context: OperationContext, retention: ipc.Retention, explicit_delete: Sequence[str]
    ) -> Sequence[Step[Any]]:
        return [
            base.ListBackupsStep(json_storage=context.json_storage),
            base.ListDeltaBackupsStep(json_storage=context.json_storage),
            base.ComputeKeptBackupsStep(
                json_storage=context.json_storage,
                retention=retention,
                explicit_delete=explicit_delete,
                retain_deltas=True,
            ),
            base.DeleteBackupAndDeltaManifestsStep(json_storage=context.json_storage),
            base.DeleteDanglingHexdigestsStep(
                json_storage=context.json_storage,
                hexdigest_storage=context.hexdigest_storage,
            ),
        ]
