"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from .config import (
    ClickHouseConfiguration,
    DiskConfiguration,
    DiskType,
    get_clickhouse_clients,
    get_zookeeper_client,
    ReplicatedDatabaseSettings,
)
from .disks import Disks
from .steps import (
    AttachMergeTreePartsStep,
    ClickHouseManifestStep,
    CollectObjectStorageFilesStep,
    DeleteDanglingObjectStorageFilesStep,
    FreezeTablesStep,
    GetVersionsStep,
    ListDatabaseReplicasStep,
    MoveFrozenPartsStep,
    PrepareClickHouseManifestStep,
    RemoveFrozenTablesStep,
    RestoreAccessEntitiesStep,
    RestoreObjectStorageFilesStep,
    RestoreReplicaStep,
    RestoreReplicatedDatabasesStep,
    RetrieveAccessEntitiesStep,
    RetrieveDatabasesAndTablesStep,
    RetrieveMacrosStep,
    SyncDatabaseReplicasStep,
    SyncTableReplicasStep,
    UnfreezeTablesStep,
    ValidateConfigStep,
)
from astacus.common.ipc import Plugin, RestoreRequest, Retention
from astacus.coordinator.plugins.base import (
    BackupManifestStep,
    BackupNameStep,
    ComputeKeptBackupsStep,
    CoordinatorPlugin,
    DeleteBackupManifestsStep,
    DeleteDanglingHexdigestsStep,
    ListBackupsStep,
    ListHexdigestsStep,
    MapNodesStep,
    OperationContext,
    RestoreStep,
    SnapshotStep,
    Step,
    UploadBlocksStep,
    UploadManifestStep,
)
from astacus.coordinator.plugins.zookeeper_config import ZooKeeperConfiguration
from pathlib import Path
from typing import Any, Sequence

import msgspec


class ClickHousePlugin(CoordinatorPlugin, frozen=True):
    zookeeper: ZooKeeperConfiguration = msgspec.field(default_factory=ZooKeeperConfiguration)
    clickhouse: ClickHouseConfiguration = msgspec.field(default_factory=ClickHouseConfiguration)
    replicated_access_zookeeper_path: str = "/clickhouse/access"
    replicated_databases_zookeeper_path: str = "/clickhouse/databases"
    replicated_databases_settings: ReplicatedDatabaseSettings = msgspec.field(default_factory=ReplicatedDatabaseSettings)
    freeze_name: str = "astacus"
    disks: Sequence[DiskConfiguration] = msgspec.field(
        default_factory=lambda: [DiskConfiguration(type=DiskType.local, path=Path(""), name="default")]
    )
    drop_databases_timeout: float = 300.0
    # Deprecated parameter, ignored
    max_concurrent_drop_databases: int = 10
    max_concurrent_drop_databases_per_node: int = 10
    create_databases_timeout: float = 60.0
    # Deprecated parameter, ignored
    max_concurrent_create_databases: int = 10
    max_concurrent_create_databases_per_node: int = 10
    sync_databases_timeout: float = 60.0
    restart_replica_timeout: float = 300.0
    # Deprecated parameter, ignored
    max_concurrent_restart_replica: int = 10
    max_concurrent_restart_replica_per_node: int = 10
    restore_replica_timeout: float = 300.0
    # Deprecated parameter, ignored
    max_concurrent_restore_replica: int = 10
    max_concurrent_restore_replica_per_node: int = 10
    freeze_timeout: float = 3600.0
    unfreeze_timeout: float = 3600.0
    # Deprecated parameter, ignored
    attach_timeout: float = 300.0
    # Deprecated parameter, ignored
    max_concurrent_attach: int = 10
    max_concurrent_attach_per_node: int = 10
    sync_tables_timeout: float = 3600.0
    # Deprecated parameter, ignored
    max_concurrent_sync: int = 100
    max_concurrent_sync_per_node: int = 10
    use_system_unfreeze: bool = True

    def get_backup_steps(self, *, context: OperationContext) -> Sequence[Step[Any]]:
        zookeeper_client = get_zookeeper_client(self.zookeeper)
        clickhouse_clients = get_clickhouse_clients(self.clickhouse)
        disks = Disks.from_disk_configs(self.disks)
        return [
            ValidateConfigStep(clickhouse=self.clickhouse),
            GetVersionsStep(clickhouse_clients),
            # Cleanup old frozen parts from failed backup attempts
            RemoveFrozenTablesStep(
                clients=clickhouse_clients,
                freeze_name=self.freeze_name,
                unfreeze_timeout=self.unfreeze_timeout,
            ),
            # Collect the users, database and tables
            RetrieveAccessEntitiesStep(
                zookeeper_client=zookeeper_client,
                access_entities_path=self.replicated_access_zookeeper_path,
            ),
            RetrieveDatabasesAndTablesStep(clients=clickhouse_clients),
            RetrieveMacrosStep(clients=clickhouse_clients),
            # Then freeze all tables
            FreezeTablesStep(
                clients=clickhouse_clients, freeze_name=self.freeze_name, freeze_unfreeze_timeout=self.freeze_timeout
            ),
            # Then snapshot and backup all frozen table parts
            SnapshotStep(
                snapshot_groups=disks.get_snapshot_groups(self.freeze_name),
            ),
            ListHexdigestsStep(hexdigest_storage=context.hexdigest_storage),
            UploadBlocksStep(storage_name=context.storage_name, validate_file_hashes=False),
            # Cleanup frozen parts
            UnfreezeTablesStep(
                clients=clickhouse_clients, freeze_name=self.freeze_name, freeze_unfreeze_timeout=self.unfreeze_timeout
            ),
            # Prepare the manifest for restore
            CollectObjectStorageFilesStep(disks=disks),
            MoveFrozenPartsStep(disks=disks),
            PrepareClickHouseManifestStep(),
            UploadManifestStep(
                json_storage=context.json_storage,
                plugin=Plugin.clickhouse,
                plugin_manifest_step=PrepareClickHouseManifestStep,
            ),
        ]

    def get_delta_backup_steps(self, *, context: OperationContext) -> Sequence[Step[Any]]:
        raise NotImplementedError

    def get_restore_steps(self, *, context: OperationContext, req: RestoreRequest) -> Sequence[Step[Any]]:
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
        disks = Disks.from_disk_configs(self.disks)
        source_disks = Disks.from_disk_configs(self.disks, storage_name=context.storage_name)
        return [
            ValidateConfigStep(clickhouse=self.clickhouse),
            BackupNameStep(json_storage=context.json_storage, requested_name=req.name),
            BackupManifestStep(json_storage=context.json_storage),
            ClickHouseManifestStep(),
            RetrieveMacrosStep(clients=clients),
            ListDatabaseReplicasStep(),
            RestoreReplicatedDatabasesStep(
                clients=clients,
                replicated_databases_zookeeper_path=self.replicated_databases_zookeeper_path,
                replicated_database_settings=self.replicated_databases_settings,
                drop_databases_timeout=self.drop_databases_timeout,
                max_concurrent_drop_databases_per_node=self.max_concurrent_drop_databases_per_node,
                create_databases_timeout=self.create_databases_timeout,
                max_concurrent_create_database_per_node=self.max_concurrent_create_databases_per_node,
            ),
            SyncDatabaseReplicasStep(
                zookeeper_client=zookeeper_client,
                replicated_databases_zookeeper_path=self.replicated_databases_zookeeper_path,
                sync_timeout=self.sync_databases_timeout,
            ),
            MapNodesStep(partial_restore_nodes=req.partial_restore_nodes),
            RestoreStep(storage_name=context.storage_name, partial_restore_nodes=req.partial_restore_nodes),
            RestoreObjectStorageFilesStep(source_disks=source_disks, target_disks=disks),
            AttachMergeTreePartsStep(
                clients=clients,
                disks=disks,
                attach_timeout=self.attach_timeout,
                max_concurrent_attach_per_node=self.max_concurrent_attach_per_node,
            ),
            SyncTableReplicasStep(
                clients=clients,
                sync_timeout=self.sync_tables_timeout,
                max_concurrent_sync_per_node=self.max_concurrent_sync_per_node,
            ),
            RestoreReplicaStep(
                zookeeper_client=zookeeper_client,
                clients=clients,
                disks=disks,
                restart_timeout=self.restart_replica_timeout,
                max_concurrent_restart_per_node=self.max_concurrent_restart_replica_per_node,
                restore_timeout=self.restore_replica_timeout,
                max_concurrent_restore_per_node=self.max_concurrent_restore_replica_per_node,
            ),
            # Keeping this step last avoids access from non-admin users while we are still restoring
            RestoreAccessEntitiesStep(
                zookeeper_client=zookeeper_client, access_entities_path=self.replicated_access_zookeeper_path
            ),
        ]

    def get_cleanup_steps(
        self, *, context: OperationContext, retention: Retention, explicit_delete: Sequence[str]
    ) -> Sequence[Step[Any]]:
        disks = Disks.from_disk_configs(self.disks)
        return [
            ListBackupsStep(json_storage=context.json_storage),
            ComputeKeptBackupsStep(
                json_storage=context.json_storage,
                retention=retention,
                explicit_delete=explicit_delete,
            ),
            DeleteBackupManifestsStep(json_storage=context.json_storage),
            DeleteDanglingHexdigestsStep(
                json_storage=context.json_storage,
                hexdigest_storage=context.hexdigest_storage,
            ),
            DeleteDanglingObjectStorageFilesStep(disks=disks, json_storage=context.json_storage),
        ]
