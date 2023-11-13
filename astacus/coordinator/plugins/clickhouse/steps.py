"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from .client import ClickHouseClient, ClickHouseClientQueryError, escape_sql_identifier, escape_sql_string
from .config import ClickHouseConfiguration, DiskType, ReplicatedDatabaseSettings
from .dependencies import access_entities_sorted_by_dependencies, tables_sorted_by_dependencies
from .disks import Disks
from .escaping import escape_for_file_name, unescape_from_file_name
from .file_metadata import FileMetadata, InvalidFileMetadata
from .macros import fetch_server_macros, Macros
from .manifest import (
    AccessEntity,
    ClickHouseBackupVersion,
    ClickHouseManifest,
    ClickHouseObjectStorageFile,
    ClickHouseObjectStorageFiles,
    ReplicatedDatabase,
    Table,
)
from .parts import list_parts_to_attach
from .replication import DatabaseReplica, get_databases_replicas, get_shard_and_replica, sync_replicated_database
from astacus.common import ipc
from astacus.common.exceptions import TransientException
from astacus.common.limiter import gather_limited
from astacus.coordinator.cluster import Cluster
from astacus.coordinator.plugins.base import (
    BackupManifestStep,
    ComputeKeptBackupsStep,
    SnapshotStep,
    Step,
    StepFailedError,
    StepsContext,
)
from astacus.coordinator.plugins.zookeeper import ChangeWatch, TransactionError, ZooKeeperClient
from base64 import b64decode
from pathlib import Path
from typing import Any, Awaitable, Callable, cast, Dict, Iterable, Iterator, List, Mapping, Sequence, Tuple, TypeVar

import asyncio
import base64
import dataclasses
import logging
import secrets
import uuid

logger = logging.getLogger(__name__)

DatabasesAndTables = Tuple[List[ReplicatedDatabase], List[Table]]

TABLES_LIST_QUERY = b"""SELECT
    base64Encode(system.databases.name),
    system.databases.uuid,
    base64Encode(system.tables.name),
    system.tables.engine,
    system.tables.uuid,
    base64Encode(system.tables.create_table_query),
    arrayZip(
        arrayMap(x -> base64Encode(x), system.tables.dependencies_database),
        arrayMap(x -> base64Encode(x), system.tables.dependencies_table))
FROM system.databases LEFT JOIN system.tables ON system.tables.database == system.databases.name
WHERE
    system.databases.engine == 'Replicated'
    AND NOT system.tables.is_temporary
ORDER BY (system.databases.name,system.tables.name)
SETTINGS show_table_uuid_in_table_create_query_if_not_nil=true
"""
_T = TypeVar("_T")


def get_setting_repr(setting_name: str, value: _T) -> str:
    escaped_value = escape_sql_string(value.encode()) if isinstance(value, str) else value
    return f"{setting_name}={escaped_value}"


@dataclasses.dataclass
class ValidateConfigStep(Step[None]):
    """
    Validates that we have the same number of astacus node and clickhouse nodes.
    """

    clickhouse: ClickHouseConfiguration

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        if len(self.clickhouse.nodes) != len(cluster.nodes):
            raise StepFailedError("Inconsistent number of nodes in the config")


@dataclasses.dataclass
class RetrieveAccessEntitiesStep(Step[List[AccessEntity]]):
    """
    Backups access entities (user, roles, quotas, row_policies, settings profiles) and their grants
    from ZooKeeper. This requires using the replicated storage engine for users.

    Inside the `access_entities_path` ZooKeeper node, there is one child znode for each type of
    access entity: each one with a single letter uppercase name (`P`, `Q`, `R`, `S`, `U`).

    Inside that same znode, there is also a child znode named `uuid`.

    Inside each single letter znode, there is one child znode for each entity of that type,
    the key is the entity name (escaped for zookeeper), the value is the entity uuid.

    Inside the `uuid` znode node, there is one child for each entity, the key is the entity uuid
    and the value is the SQL queries required to recreate that entity. Some entities have more
    than one query because they need separate queries to add grants related to the entity.
    """

    zookeeper_client: ZooKeeperClient
    access_entities_path: str

    async def run_step(self, cluster: Cluster, context: StepsContext) -> List[AccessEntity]:
        access_entities = []
        async with self.zookeeper_client.connect() as connection:
            change_watch = ChangeWatch()
            entity_types = await connection.get_children(self.access_entities_path, watch=change_watch)
            for entity_type in entity_types:
                if entity_type != "uuid":
                    entity_type_path = f"{self.access_entities_path}/{entity_type}"
                    node_names = await connection.get_children(entity_type_path, watch=change_watch)
                    for node_name in node_names:
                        uuid_bytes = await connection.get(f"{entity_type_path}/{node_name}", watch=change_watch)
                        entity_uuid = uuid.UUID(uuid_bytes.decode())
                        entity_path = f"{self.access_entities_path}/uuid/{entity_uuid}"
                        attach_query_bytes = await connection.get(entity_path, watch=change_watch)
                        access_entities.append(
                            AccessEntity(
                                type=entity_type,
                                uuid=entity_uuid,
                                name=unescape_from_file_name(node_name),
                                attach_query=attach_query_bytes,
                            )
                        )
            if change_watch.has_changed:
                # With care, we could instead look at what exactly changed and just update the minimum
                raise TransientException("Concurrent modification during access entities retrieval")
        return access_entities


@dataclasses.dataclass
class RetrieveDatabasesAndTablesStep(Step[DatabasesAndTables]):
    """
    Retrieves the list of all databases that use the replicated database engine and their tables.

    The table names, uuids and schemas of all tables are collected.
    The database names, uuids, shard and replica parameters are collected.

    This assumes that all servers of the cluster have created the same replicated
    databases (with the same database name pointing on the same ZooKeeper
    node), and relies on that to query only the first server of the cluster.
    """

    clients: Sequence[ClickHouseClient]

    async def run_step(self, cluster: Cluster, context: StepsContext) -> DatabasesAndTables:
        clickhouse_client = self.clients[0]
        # We fetch databases and tables in a single query, we don't have to care about consistency within that step.
        # However, the schema could be modified between now and the freeze step.
        databases: Dict[bytes, ReplicatedDatabase] = {}
        tables: List[Table] = []
        rows = await clickhouse_client.execute(TABLES_LIST_QUERY)
        for (
            base64_db_name,
            db_uuid_str,
            base64_table_name,
            table_engine,
            table_uuid,
            base64_table_query,
            base64_dependencies,
        ) in rows:
            assert isinstance(base64_db_name, str)
            assert isinstance(db_uuid_str, str)
            assert isinstance(base64_table_name, str)
            assert isinstance(table_engine, str)
            assert isinstance(base64_table_query, str)
            assert isinstance(base64_dependencies, list)
            db_uuid = uuid.UUID(db_uuid_str)
            db_name = b64decode(base64_db_name)
            if db_name not in databases:
                shard, replica = await get_shard_and_replica(clickhouse_client, db_name)
                databases[db_name] = ReplicatedDatabase(
                    name=db_name,
                    uuid=db_uuid,
                    shard=shard,
                    replica=replica,
                )
            # Thanks to the LEFT JOIN, an empty database without table will still return a row.
            # Unlike standard SQL, the table properties will have a default value instead of NULL,
            # that's why we skip tables with an empty name.
            # We need these rows and the LEFT JOIN that makes them: we want to list all
            # Replicated databases, including those without any table.
            if base64_table_name != "":
                tables.append(
                    Table(
                        database=db_name,
                        name=b64decode(base64_table_name),
                        engine=table_engine,
                        uuid=uuid.UUID(cast(str, table_uuid)),
                        create_query=b64decode(base64_table_query),
                        dependencies=[(b64decode(d), b64decode(t)) for d, t in base64_dependencies],
                    )
                )
        databases_list = sorted(databases.values(), key=lambda d: d.name)
        return databases_list, tables


@dataclasses.dataclass
class RetrieveMacrosStep(Step[Sequence[Macros]]):
    """
    Retrieves the value of all macros on each server.

    Returns a list of `Macros` objects, each item of the list matches one server.
    """

    clients: Sequence[ClickHouseClient]

    async def run_step(self, cluster: Cluster, context: StepsContext) -> Sequence[Macros]:
        return await asyncio.gather(*[fetch_server_macros(client) for client in self.clients])


@dataclasses.dataclass
class CollectObjectStorageFilesStep(Step[list[ClickHouseObjectStorageFiles]]):
    """
    Collects the list of files that are referenced by metadata files in the backup.
    """

    disks: Disks

    async def run_step(self, cluster: Cluster, context: StepsContext) -> list[ClickHouseObjectStorageFiles]:
        snapshot_results: Sequence[ipc.SnapshotResult] = context.get_result(SnapshotStep)
        object_storage_files: dict[str, set[Path]] = {}
        for snapshot_result in snapshot_results:
            assert snapshot_result.state is not None
            for snapshot_file in snapshot_result.state.files:
                parsed_path = self.disks.parse_part_file_path(snapshot_file.relative_path)
                if parsed_path.disk.type == DiskType.object_storage:
                    if snapshot_file.content_b64 is None:
                        # This shouldn't happen because the snapshot glob will be configured to embed all files
                        # when globbing on remote disks.
                        raise StepFailedError("Metadata files should be embedded in the snapshot")
                    content = base64.b64decode(snapshot_file.content_b64)
                    try:
                        file_metadata = FileMetadata.from_bytes(content)
                    except InvalidFileMetadata as e:
                        raise StepFailedError(f"Invalid file metadata in {snapshot_file.relative_path}: {e}") from e
                    for object_metadata in file_metadata.objects:
                        object_storage_files.setdefault(parsed_path.disk.name, set()).add(object_metadata.relative_path)
        return [
            ClickHouseObjectStorageFiles(
                disk_name=disk_name, files=[ClickHouseObjectStorageFile(path=path) for path in sorted(paths)]
            )
            for disk_name, paths in sorted(object_storage_files.items())
        ]


@dataclasses.dataclass
class PrepareClickHouseManifestStep(Step[Dict[str, Any]]):
    """
    Collects access entities, databases and tables from previous steps into an uploadable manifest.
    """

    async def run_step(self, cluster: Cluster, context: StepsContext) -> Dict[str, Any]:
        databases, tables = context.get_result(RetrieveDatabasesAndTablesStep)
        manifest = ClickHouseManifest(
            version=ClickHouseBackupVersion.V2,
            access_entities=context.get_result(RetrieveAccessEntitiesStep),
            replicated_databases=databases,
            tables=tables,
            object_storage_files=context.get_result(CollectObjectStorageFilesStep),
        )
        return manifest.to_plugin_data()


@dataclasses.dataclass
class RemoveFrozenTablesStep(Step[None]):
    """
    Removes traces of previous backups that might have failed.
    When the system unfreeze flag is enabled, clears frozen parts from all disks in a single go.
    """

    clients: Sequence[ClickHouseClient]
    freeze_name: str
    unfreeze_timeout: float

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        escaped_freeze_name = escape_sql_string(self.freeze_name.encode())
        unfreeze_statement = f"SYSTEM UNFREEZE WITH NAME {escaped_freeze_name}".encode()
        await asyncio.gather(
            *[execute_with_timeout(client, self.unfreeze_timeout, unfreeze_statement) for client in self.clients]
        )


@dataclasses.dataclass
class FreezeUnfreezeTablesStepBase(Step[None]):
    clients: Sequence[ClickHouseClient]
    freeze_name: str
    freeze_unfreeze_timeout: float

    @property
    def operation(self) -> str:
        # It's a bit silly to have this as a property but it let's us keep using dataclass like all other steps
        raise NotImplementedError

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        _, tables = context.get_result(RetrieveDatabasesAndTablesStep)
        for table in tables:
            if table.requires_freezing:
                # We only run it on the first client because the `ALTER TABLE (UN)FREEZE` is replicated
                await execute_with_timeout(
                    self.clients[0],
                    self.freeze_unfreeze_timeout,
                    (
                        f"ALTER TABLE {table.escaped_sql_identifier} "
                        f"{self.operation} WITH NAME {escape_sql_string(self.freeze_name.encode())}"
                        f" SETTINGS distributed_ddl_task_timeout={self.freeze_unfreeze_timeout}"
                    ).encode(),
                )


@dataclasses.dataclass
class FreezeTablesStep(FreezeUnfreezeTablesStepBase):
    """
    Creates a frozen copy of the tables that won't change while we are uploading parts of it.

    Each table is frozen separately, one after the other. This means the complete backup of all
    tables will not represent a single, globally consistent, point in time.

    The frozen copy is done using hardlink and does not cost extra disk space (ClickHouse can
    use hardlinks because parts files never change after they are created).

    This does *not* lock the table or disable writes on the live table, this just makes the backup
    not see writes done after the `ALTER TABLE FREEZE` command.

    The frozen copy is stored in a `shadow/{freeze_name}` folder inside the ClickHouse data
    directory. This directory will be scanned by the `SnapshotStep`. However we will need to write
    it in a different place when restoring the backup (see `MoveFrozenPartsStep`).
    """

    @property
    def operation(self) -> str:
        return "FREEZE"


@dataclasses.dataclass
class UnfreezeTablesStep(FreezeUnfreezeTablesStepBase):
    """
    Removes the frozen parts after we're done uploading them.

    Frozen leftovers don't immediately harm ClickHouse or cost disk space since they are
    hardlinks to the parts used by the real table. However, as ClickHouse starts mutating
    the table and replaces existing parts with new ones, these frozen parts will take disk
    space. `ALTER TABLE UNFREEZE` removes these unused parts.
    """

    @property
    def operation(self) -> str:
        return "UNFREEZE"


@dataclasses.dataclass
class MoveFrozenPartsStep(Step[None]):
    """
    Renames files in the snapshot manifest to match what we will need during recover.

    The freeze step creates hardlinks of the table data in the `shadow/` folder, then the
    snapshot steps upload these file to backup storage and remember them by their
    hash.

    Later during the restore process, we need these files to be placed in the `store/`
    folder, with a slightly different hierarchy: we need the files in the correct place to be
    able to use the `ALTER TABLE ATTACH` command and re-add the data to the empty
    tables.

    By renaming files in the snapshot manifest, we can tell the restore step to put the
    files in a different place from where they were during the backup. This doesn't cause
    problem when actually downloading files from the backup storage because the storage
    only identifies files by their hash, it doesn't care about their original, or modified, path.
    """

    disks: Disks

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        # Note: we could also do that on restore, but this way we can erase the ClickHouse `FREEZE`
        # backup name from all the snapshot entries
        # I do not like mutating an existing result, we should making this more visible
        # by returning a mutated copy and use that in other steps
        snapshot_results: List[ipc.SnapshotResult] = context.get_result(SnapshotStep)
        for snapshot_result in snapshot_results:
            assert snapshot_result.state is not None
            snapshot_result.state.files = [
                snapshot_file.copy(
                    update={
                        "relative_path": dataclasses.replace(
                            self.disks.parse_part_file_path(snapshot_file.relative_path),
                            freeze_name=None,
                            detached=False,
                        ).to_path()
                    }
                )
                for snapshot_file in snapshot_result.state.files
            ]


@dataclasses.dataclass
class ClickHouseManifestStep(Step[ClickHouseManifest]):
    """
    Extracts the ClickHouse plugin manifest from the main backup manifest.
    """

    async def run_step(self, cluster: Cluster, context: StepsContext) -> ClickHouseManifest:
        backup_manifest = context.get_result(BackupManifestStep)
        return ClickHouseManifest.from_plugin_data(backup_manifest.plugin_data)


async def run_on_every_node(
    clients: Iterable[ClickHouseClient],
    fn: Callable[[ClickHouseClient], Iterable[Awaitable[None]]],
    per_node_concurrency_limit: int,
) -> None:
    await asyncio.gather(*[gather_limited(per_node_concurrency_limit, fn(client)) for client in clients])


@dataclasses.dataclass
class RestoreReplicatedDatabasesStep(Step[None]):
    """
    Re-creates replicated databases on each client and re-create all tables in each database.

    After this step, all tables will be empty.
    """

    clients: Sequence[ClickHouseClient]
    replicated_databases_zookeeper_path: str
    replicated_database_settings: ReplicatedDatabaseSettings
    drop_databases_timeout: float
    max_concurrent_drop_databases_per_node: int
    create_databases_timeout: float
    max_concurrent_create_database_per_node: int

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        manifest = context.get_result(ClickHouseManifestStep)
        # The database must be dropped on *every* node before attempting to recreate it.
        # If we don't do that, then the recreated database on one node will recover data from
        # a node where the database wasn't recreated yet.

        def _drop_dbs(client: ClickHouseClient) -> Iterator[Awaitable[None]]:
            yield from (
                execute_with_timeout(
                    client,
                    self.drop_databases_timeout,
                    f"DROP DATABASE IF EXISTS {escape_sql_identifier(database.name)} SYNC".encode(),
                )
                for database in manifest.replicated_databases
            )

        await run_on_every_node(
            clients=self.clients, fn=_drop_dbs, per_node_concurrency_limit=self.max_concurrent_drop_databases_per_node
        )

        settings = [
            get_setting_repr(setting_name, value)
            for setting_name, value in self.replicated_database_settings.dict().items()
            if value is not None
        ]
        settings_str = ", ".join(settings)
        settings_clause = f" SETTINGS {settings_str}" if settings else ""
        create_queries = []
        for database in manifest.replicated_databases:
            database_znode_name = escape_for_file_name(database.name)
            database_path = f"{self.replicated_databases_zookeeper_path}/{database_znode_name}"
            optional_uuid_fragment = ""
            if database.uuid is not None:
                escaped_database_uuid = escape_sql_string(str(database.uuid).encode())
                optional_uuid_fragment = f" UUID {escaped_database_uuid}"
            create_queries.append(
                f"CREATE DATABASE {escape_sql_identifier(database.name)}"
                f"{optional_uuid_fragment}"
                f" ENGINE = Replicated("
                f"{escape_sql_string(database_path.encode())}, "
                f"{escape_sql_string(database.shard)}, "
                f"{escape_sql_string(database.replica)})"
                f"{settings_clause}".encode()
            )

        def _create_dbs(client: ClickHouseClient) -> Iterator[Awaitable[None]]:
            yield from (
                execute_with_timeout(client, self.create_databases_timeout, create_query) for create_query in create_queries
            )

        await run_on_every_node(
            clients=self.clients, fn=_create_dbs, per_node_concurrency_limit=self.max_concurrent_create_database_per_node
        )
        # Tables creation is not parallelized with gather since they can depend on each other
        # (although, we could use graphlib more subtly and parallelize what we can).
        session_id = secrets.token_hex()
        for query in [
            # If any table was initially created with custom global settings,
            # we need to re-enable these custom global settings when creating the table again.
            # We can enable these settings unconditionally because they are harmless
            # for tables not needing them.
            b"SET allow_experimental_geo_types=true",
            b"SET allow_experimental_object_type=true",
            b"SET allow_suspicious_low_cardinality_types=true",
            # If a table was created with flatten_nested=0, we must be careful to not re-create the
            # table with flatten_nested=1, since this would recreate the table with a different schema.
            # If a table was created with flatten_nested=1, the query in system.tables.create_table_query
            # shows the flattened fields, not the original user query with nested fields. This means that
            # when re-executing the query, we don't need to re-flatten the fields.
            # In both cases, flatten_nested=0 is the right choice when restoring a backup.
            b"SET flatten_nested=0",
        ]:
            try:
                await self.clients[0].execute(query, session_id=session_id)
            except ClickHouseClientQueryError as error:
                if error.exception_code == error.SETTING_CONSTRAINT_VIOLATION:
                    # If we can't set the option, that's fine, either it's not needed or it will fail later anyway
                    pass
                else:
                    raise

        # If any known table depends on an unknown table that was inside a non-replicated
        # database engine, then this will crash. See comment in `RetrieveReplicatedDatabasesStep`.
        for table in tables_sorted_by_dependencies(manifest.tables):
            # Materialized views creates both a table for the view itself and a table
            # with the .inner_id. prefix to store the data, we don't need to recreate
            # them manually. We will need to restore their data parts however.
            if not table.name.startswith(b".inner_id."):
                # Create on the first client and let replication do its thing
                await self.clients[0].execute(table.create_query, session_id=session_id)


DatabasesReplicas = Mapping[bytes, Sequence[DatabaseReplica]]


@dataclasses.dataclass
class ListDatabaseReplicasStep(Step[DatabasesReplicas]):
    """
    For each replicated database, returns the list of replicas.

    Each replica has a `shard_name` and a `replica_name`.
    """

    async def run_step(self, cluster: Cluster, context: StepsContext) -> DatabasesReplicas:
        manifest = context.get_result(ClickHouseManifestStep)
        server_macros = context.get_result(RetrieveMacrosStep)
        return get_databases_replicas(manifest.replicated_databases, server_macros)


@dataclasses.dataclass
class SyncDatabaseReplicasStep(Step[None]):
    zookeeper_client: ZooKeeperClient
    replicated_databases_zookeeper_path: str
    sync_timeout: float

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        databases_replicas = context.get_result(ListDatabaseReplicasStep)
        async with self.zookeeper_client.connect() as connection:
            for database_name, replicas in sorted(databases_replicas.items()):
                database_znode_name = escape_for_file_name(database_name)
                database_path = f"{self.replicated_databases_zookeeper_path}/{database_znode_name}"
                await sync_replicated_database(connection, database_path, replicas, self.sync_timeout)


@dataclasses.dataclass
class RestoreAccessEntitiesStep(Step[None]):
    """
    Restores access entities (user, roles, quotas, row_policies, settings profiles) and their grants
    to ZooKeeper. This requires using the replicated storage engine for users.

    The list of access entities to restore is read from the plugin manifest, which itself was
    filled by the `RetrieveAccessEntitiesStep` during a previous backup.

    Because of how the replicated storage engine works, recreating the entities in ZooKeeper
    is enough to have all ClickHouse servers notice the added znodes and create the entities:

    The replicated storage engine uses ZooKeeper as its main storage, each ClickHouse server
    only has an in-memory cache and uses ZooKeeper watches to detect added, modified or
    removed entities.
    """

    zookeeper_client: ZooKeeperClient
    access_entities_path: str

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        clickhouse_manifest = context.get_result(ClickHouseManifestStep)
        async with self.zookeeper_client.connect() as connection:
            for access_entity in access_entities_sorted_by_dependencies(clickhouse_manifest.access_entities):
                escaped_entity_name = escape_for_file_name(access_entity.name)
                entity_type_path = f"{self.access_entities_path}/{access_entity.type}"
                entity_uuids_path = f"{self.access_entities_path}/uuid"
                await connection.try_create(entity_type_path, b"")
                await connection.try_create(entity_uuids_path, b"")
                entity_name_path = f"{entity_type_path}/{escaped_entity_name}"
                entity_path = f"{entity_uuids_path}/{access_entity.uuid}"
                attach_query_bytes = access_entity.attach_query
                transaction = connection.transaction()
                transaction.create(entity_name_path, str(access_entity.uuid).encode())
                transaction.create(entity_path, attach_query_bytes)
                try:
                    await transaction.commit()
                except TransactionError:
                    # The only errors we can have inside the transaction are NodeExistsError.
                    # It's most likely because we're resuming a failed restore.
                    # There are odd cases where the cause and end result could be surprising:
                    # if a different entity already exists with the same name and different id,
                    # but we're not supposed to restore into a completely different
                    # ZooKeeper storage.
                    pass


@dataclasses.dataclass
class RestoreReplicaStep(Step[None]):
    """
    Restore data on all tables by using `SYSTEM RESTORE REPLICA... `.
    """

    zookeeper_client: ZooKeeperClient
    clients: Sequence[ClickHouseClient]
    disks: Disks
    restart_timeout: float
    max_concurrent_restart_per_node: int
    restore_timeout: float
    max_concurrent_restore_per_node: int

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        clickhouse_manifest = context.get_result(ClickHouseManifestStep)
        if clickhouse_manifest.version == ClickHouseBackupVersion.V1:
            return
        replicated_tables = [table for table in clickhouse_manifest.tables if table.is_replicated]
        # Before doing the restore, drop the ZooKeeper data and restart the replica.
        # Because the ZooKeeper data was removed, the restart will put the table in read-only mode,
        # this is a requirement to be allowed to run restore replica.
        async with self.zookeeper_client.connect() as connection:
            for table in replicated_tables:
                await connection.delete(f"/clickhouse/tables/{str(table.uuid)}", recursive=True)

        def _restart_replicas(client: ClickHouseClient) -> Iterator[Awaitable[None]]:
            yield from (
                execute_with_timeout(
                    client, self.restart_timeout, f"SYSTEM RESTART REPLICA {table.escaped_sql_identifier}".encode()
                )
                for table in replicated_tables
            )

        await run_on_every_node(
            clients=self.clients,
            fn=_restart_replicas,
            per_node_concurrency_limit=self.max_concurrent_restart_per_node,
        )

        def _restore_replicas(client: ClickHouseClient) -> Iterator[Awaitable[None]]:
            yield from (
                execute_with_timeout(
                    client, self.restore_timeout, f"SYSTEM RESTORE REPLICA {table.escaped_sql_identifier}".encode()
                )
                for table in replicated_tables
            )

        await run_on_every_node(
            clients=self.clients,
            fn=_restore_replicas,
            per_node_concurrency_limit=self.max_concurrent_restore_per_node,
        )


@dataclasses.dataclass
class RestoreObjectStorageFilesStep(Step[None]):
    """
    If the source and target disks are not the same, restore object storage files by copying them
    from the source to the target disk.
    """

    source_disks: Disks
    target_disks: Disks

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        clickhouse_manifest = context.get_result(ClickHouseManifestStep)
        for object_storage_files in clickhouse_manifest.object_storage_files:
            if len(object_storage_files.files) > 0:
                disk_name = object_storage_files.disk_name
                source_storage = self.source_disks.get_object_storage(disk_name=disk_name)
                if source_storage is None:
                    raise StepFailedError(f"Source disk named {disk_name!r} isn't configured as object storage")
                target_storage = self.target_disks.get_object_storage(disk_name=disk_name)
                if target_storage is None:
                    raise StepFailedError(f"Target disk named {disk_name!r} isn't configured as object storage")
                if source_storage.get_config() != target_storage.get_config():
                    paths = [file.path for file in object_storage_files.files]
                    await target_storage.copy_items_from(source_storage, paths)


@dataclasses.dataclass
class AttachMergeTreePartsStep(Step[None]):
    """
    Restore data to all tables by using `ALTER TABLE ... ATTACH`.

    Which part are restored to which servers depends on whether the tables uses
    a Replicated table engine or not, see `DistributeReplicatedPartsStep` for more
    details.
    """

    clients: Sequence[ClickHouseClient]
    disks: Disks
    attach_timeout: float
    max_concurrent_attach_per_node: int

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        backup_manifest = context.get_result(BackupManifestStep)
        clickhouse_manifest = context.get_result(ClickHouseManifestStep)
        if clickhouse_manifest.version != ClickHouseBackupVersion.V1:
            return
        tables_by_uuid = {table.uuid: table for table in clickhouse_manifest.tables}
        await asyncio.gather(
            *[
                gather_limited(
                    self.max_concurrent_attach_per_node,
                    [
                        execute_with_timeout(
                            client,
                            self.attach_timeout,
                            f"ALTER TABLE {table_identifier} ATTACH PART {escape_sql_string(part_name)}".encode(),
                        )
                        for table_identifier, part_name in list_parts_to_attach(snapshot_result, self.disks, tables_by_uuid)
                    ],
                )
                for client, snapshot_result in zip(self.clients, backup_manifest.snapshot_results)
            ]
        )


@dataclasses.dataclass
class SyncTableReplicasStep(Step[None]):
    """
    Before declaring the restoration as finished, make sure all parts of replicated tables
    are all exchanged between all nodes.
    """

    clients: Sequence[ClickHouseClient]
    sync_timeout: float
    max_concurrent_sync_per_node: int

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        manifest = context.get_result(ClickHouseManifestStep)
        if manifest.version != ClickHouseBackupVersion.V1:
            return

        def _sync_replicas(client: ClickHouseClient) -> Iterator[Awaitable[None]]:
            yield from (
                execute_with_timeout(
                    client, self.sync_timeout, f"SYSTEM SYNC REPLICA {table.escaped_sql_identifier}".encode()
                )
                for table in manifest.tables
                if table.is_replicated
            )

        await run_on_every_node(
            clients=self.clients,
            fn=_sync_replicas,
            per_node_concurrency_limit=self.max_concurrent_sync_per_node,
        )


@dataclasses.dataclass
class DeleteDanglingObjectStorageFilesStep(Step[None]):
    """
    Delete object storage files that were created before the most recent backup
    and that are not part of any backup.
    """

    disks: Disks

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        backup_manifests = context.get_result(ComputeKeptBackupsStep)
        if len(backup_manifests) < 1:
            logger.info("no backup manifest, not deleting any object storage disk file")
            # If we don't have at least one backup, we don't know which files are more recent
            # than the latest backup, so we don't do anything.
            return
        newest_backup_start_time = max((backup_manifest.start for backup_manifest in backup_manifests))
        clickhouse_manifests = [
            ClickHouseManifest.from_plugin_data(backup_manifest.plugin_data) for backup_manifest in backup_manifests
        ]
        kept_paths: dict[str, set[Path]] = {}
        for clickhouse_manifest in clickhouse_manifests:
            for object_storage_files in clickhouse_manifest.object_storage_files:
                disk_kept_paths = kept_paths.setdefault(object_storage_files.disk_name, set())
                disk_kept_paths.update((file.path for file in object_storage_files.files))
        for disk_name, disk_kept_paths in sorted(kept_paths.items()):
            disk_object_storage = self.disks.get_object_storage(disk_name=disk_name)
            if disk_object_storage is None:
                raise StepFailedError(f"Could not find object storage disk named {disk_name!r}")
            keys_to_remove = []
            logger.info("found %d object storage files to keep in disk %r", len(disk_kept_paths), disk_name)
            disk_object_storage_items = await disk_object_storage.list_items()
            for item in disk_object_storage_items:
                # We don't know if objects newer than the latest backup should be kept or not,
                # so we leave them for now. We'll delete them if necessary once there is a newer
                # backup to tell us if they are still used or not.
                if item.last_modified < newest_backup_start_time and item.key not in disk_kept_paths:
                    logger.debug("dangling object storage file in disk %r : %r", disk_name, item.key)
                    keys_to_remove.append(item.key)
            disk_available_paths = [item.key for item in disk_object_storage_items]
            for disk_kept_path in disk_kept_paths:
                if disk_kept_path not in disk_available_paths:
                    # Make sure the non-deleted files are actually in object storage
                    raise StepFailedError(f"missing object storage file in disk {disk_name!r}: {disk_kept_path!r}")
            logger.info("found %d object storage files to remove in disk %r", len(keys_to_remove), disk_name)
            for key_to_remove in keys_to_remove:
                # We should really have a batch delete operation there, but it's missing from rohmu
                logger.debug("deleting object storage file in disk %r : %r", disk_name, key_to_remove)
                await disk_object_storage.delete_item(key_to_remove)


async def execute_with_timeout(client: ClickHouseClient, timeout: float, query: bytes) -> None:
    # we use a session because we can't use the SETTINGS clause with all types of queries
    session_id = secrets.token_hex()
    await client.execute(f"SET receive_timeout={timeout}".encode(), session_id=session_id)
    await client.execute(query, session_id=session_id, timeout=timeout)
