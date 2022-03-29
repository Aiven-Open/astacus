"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from .config import (
    ClickHouseConfiguration,
    get_clickhouse_clients,
    get_zookeeper_client,
    ReplicatedDatabaseSettings,
    ZooKeeperConfiguration,
)
from .parts import get_frozen_parts_pattern
from .steps import (
    AttachMergeTreePartsStep,
    ClickHouseManifestStep,
    DistributeReplicatedPartsStep,
    FreezeTablesStep,
    MoveFrozenPartsStep,
    PrepareClickHouseManifestStep,
    RemoveFrozenTablesStep,
    RestoreAccessEntitiesStep,
    RestoreReplicatedDatabasesStep,
    RetrieveAccessEntitiesStep,
    RetrieveDatabasesAndTablesStep,
    SyncReplicasStep,
    UnfreezeTablesStep,
    ValidateConfigStep,
)
from astacus.common.ipc import Plugin, RestoreRequest
from astacus.coordinator.plugins.base import (
    BackupManifestStep,
    BackupNameStep,
    CoordinatorPlugin,
    ListHexdigestsStep,
    OperationContext,
    RestoreStep,
    SnapshotStep,
    Step,
    UploadBlocksStep,
    UploadManifestStep,
)
from typing import List


class ClickHousePlugin(CoordinatorPlugin):
    zookeeper: ZooKeeperConfiguration = ZooKeeperConfiguration()
    clickhouse: ClickHouseConfiguration = ClickHouseConfiguration()
    replicated_access_zookeeper_path: str = "/clickhouse/access"
    replicated_databases_zookeeper_path: str = "/clickhouse/databases"
    replicated_databases_settings: ReplicatedDatabaseSettings = ReplicatedDatabaseSettings()
    freeze_name: str = "astacus"
    attach_timeout: float = 300.0
    max_concurrent_attach: int = 100
    sync_timeout: float = 3600.0
    max_concurrent_sync: int = 100

    def get_backup_steps(self, *, context: OperationContext) -> List[Step]:
        zookeeper_client = get_zookeeper_client(self.zookeeper)
        clickhouse_clients = get_clickhouse_clients(self.clickhouse)
        return [
            ValidateConfigStep(clickhouse=self.clickhouse),
            # Cleanup old frozen parts from failed backup attempts
            RemoveFrozenTablesStep(freeze_name=self.freeze_name),
            # Collect the users, database and tables
            RetrieveAccessEntitiesStep(
                zookeeper_client=zookeeper_client,
                access_entities_path=self.replicated_access_zookeeper_path,
            ),
            RetrieveDatabasesAndTablesStep(clients=clickhouse_clients),
            # Then freeze all tables
            FreezeTablesStep(clients=clickhouse_clients, freeze_name=self.freeze_name),
            # Then snapshot and backup all frozen table parts
            SnapshotStep(snapshot_root_globs=[get_frozen_parts_pattern(self.freeze_name)]),
            ListHexdigestsStep(hexdigest_storage=context.hexdigest_storage),
            UploadBlocksStep(storage_name=context.storage_name),
            # Cleanup frozen parts
            UnfreezeTablesStep(clients=clickhouse_clients, freeze_name=self.freeze_name),
            # Prepare the manifest for restore
            MoveFrozenPartsStep(freeze_name=self.freeze_name),
            DistributeReplicatedPartsStep(),
            PrepareClickHouseManifestStep(),
            UploadManifestStep(
                json_storage=context.json_storage,
                plugin=Plugin.clickhouse,
                plugin_manifest_step=PrepareClickHouseManifestStep,
            ),
        ]

    def get_restore_steps(self, *, context: OperationContext, req: RestoreRequest) -> List[Step]:
        if req.partial_restore_nodes:
            # Required modifications to implement single-node restore:
            #  - don't restore tables inside Replicated databases (let ClickHouse do it)
            #  - don't restore data for ReplicatedMergeTree tables (let ClickHouse do it)
            #  - don't run AttachMergeTreePartsStep at all
            #  - identify all single-ClickHouse client operations and run them only on the restoring node
            #  - test it before enabling it
            raise NotImplementedError
        zookeeper_client = get_zookeeper_client(self.zookeeper)
        clients = get_clickhouse_clients(self.clickhouse)
        return [
            ValidateConfigStep(clickhouse=self.clickhouse),
            BackupNameStep(json_storage=context.json_storage, requested_name=req.name),
            BackupManifestStep(json_storage=context.json_storage),
            ClickHouseManifestStep(),
            RestoreReplicatedDatabasesStep(
                clients=clients,
                replicated_databases_zookeeper_path=self.replicated_databases_zookeeper_path,
                replicated_database_settings=self.replicated_databases_settings,
            ),
            # We should deduplicate parts of ReplicatedMergeTree tables to only download once from
            # backup storage and then let ClickHouse replicate between all servers.
            RestoreStep(storage_name=context.storage_name, partial_restore_nodes=req.partial_restore_nodes),
            AttachMergeTreePartsStep(
                clients=clients,
                attach_timeout=self.attach_timeout,
                max_concurrent_attach=self.max_concurrent_attach,
            ),
            SyncReplicasStep(
                clients=clients,
                sync_timeout=self.sync_timeout,
                max_concurrent_sync=self.max_concurrent_sync,
            ),
            # Keeping this step last avoids access from non-admin users while we are still restoring
            RestoreAccessEntitiesStep(
                zookeeper_client=zookeeper_client, access_entities_path=self.replicated_access_zookeeper_path
            ),
        ]
