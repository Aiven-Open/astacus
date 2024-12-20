"""Copyright (c) 2021 Aiven Ltd
See LICENSE for details.
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
    KeeperMapRow,
    KeeperMapTable,
    ReplicatedDatabase,
    Table,
    UserDefinedFunction,
)
from .parts import list_parts_to_attach
from .replication import DatabaseReplica, get_databases_replicas, get_shard_and_replica, sync_replicated_database
from astacus.common import ipc
from astacus.common.exceptions import TransientException
from astacus.common.limiter import gather_limited
from astacus.common.storage import JsonStorage
from astacus.coordinator.cluster import Cluster
from astacus.coordinator.manifest import download_backup_manifest_sync
from astacus.coordinator.plugins.base import (
    BackupManifestStep,
    ComputeKeptBackupsStep,
    SnapshotStep,
    Step,
    StepFailedError,
    StepsContext,
    SyncStep,
)
from astacus.coordinator.plugins.zookeeper import ChangeWatch, NoNodeError, TransactionError, ZooKeeperClient
from base64 import b64decode
from collections.abc import Awaitable, Callable, Iterable, Iterator, Mapping, Sequence
from datetime import timedelta
from kazoo.exceptions import ZookeeperError
from typing import Any, cast, TypeVar

import asyncio
import base64
import dataclasses
import logging
import msgspec
import os
import re
import secrets
import time
import uuid

logger = logging.getLogger(__name__)

DatabasesAndTables = tuple[Sequence[ReplicatedDatabase], Sequence[Table]]
ClickHouseVersion = tuple[int, int]

DATABASES_LIST_QUERY = b"""SELECT
    base64Encode(system.databases.name),
    system.databases.uuid
FROM system.databases
WHERE system.databases.engine == 'Replicated'
"""

TABLES_LIST_QUERY = b"""SELECT
    base64Encode(system.tables.database),
    base64Encode(system.tables.name),
    system.tables.engine,
    system.tables.uuid,
    base64Encode(system.tables.create_table_query),
    arrayZip(
        arrayMap(x -> base64Encode(x), system.tables.dependencies_database),
        arrayMap(x -> base64Encode(x), system.tables.dependencies_table))
FROM system.tables
WHERE
    system.tables.database in (SELECT name FROM system.databases WHERE engine == 'Replicated')
    AND NOT system.tables.is_temporary
ORDER BY (system.tables.database,system.tables.name)
SETTINGS show_table_uuid_in_table_create_query_if_not_nil=true
"""
_T = TypeVar("_T")


def get_setting_repr(setting_name: str, value: _T) -> str:
    escaped_value = escape_sql_string(value.encode()) if isinstance(value, str) else value
    return f"{setting_name}={escaped_value}"


@dataclasses.dataclass
class ValidateConfigStep(Step[None]):
    """Validates that we have the same number of astacus node and clickhouse nodes."""

    clickhouse: ClickHouseConfiguration

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        if len(self.clickhouse.nodes) != len(cluster.nodes):
            raise StepFailedError("Inconsistent number of nodes in the config")


@dataclasses.dataclass
class RetrieveAccessEntitiesStep(Step[Sequence[AccessEntity]]):
    """Backups access entities (user, roles, quotas, row_policies, settings profiles) and their grants
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

    async def run_step(self, cluster: Cluster, context: StepsContext) -> Sequence[AccessEntity]:
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
class RetrieveUserDefinedFunctionsStep(Step[Sequence[UserDefinedFunction]]):
    zookeeper_client: ZooKeeperClient
    replicated_user_defined_zookeeper_path: str | None

    async def run_step(self, cluster: Cluster, context: StepsContext) -> Sequence[UserDefinedFunction]:
        if self.replicated_user_defined_zookeeper_path is None:
            return []

        user_defined_functions: list[UserDefinedFunction] = []
        async with self.zookeeper_client.connect() as connection:
            change_watch = ChangeWatch()
            try:
                children = await connection.get_children(self.replicated_user_defined_zookeeper_path, watch=change_watch)
            except NoNodeError:
                # The path doesn't exist, no user defined functions to retrieve
                return []

            for child in children:
                user_defined_function_path = os.path.join(self.replicated_user_defined_zookeeper_path, child)
                user_defined_function_value = await connection.get(user_defined_function_path, watch=change_watch)
                user_defined_functions.append(UserDefinedFunction(path=child, create_query=user_defined_function_value))
            if change_watch.has_changed:
                raise TransientException("Concurrent modification during user_defined_function entities retrieval")
        return user_defined_functions


@dataclasses.dataclass
class KeeperMapTablesReadabilityStepBase(Step[None]):
    clients: Sequence[ClickHouseClient]
    _is_read_only: bool = dataclasses.field(init=False)

    def readability_statement(self, escaped_table_identifier: str) -> str:
        read_only = str(self._is_read_only).lower()
        return f"ALTER TABLE {escaped_table_identifier} MODIFY SETTING read_only={read_only}"

    async def alter_readability(self, escaped_table_identifier: str) -> None:
        statement = self.readability_statement(escaped_table_identifier).encode()
        await self.clients[0].execute(statement)

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        _, tables = context.get_result(RetrieveDatabasesAndTablesStep)
        keeper_map_table_names = [table for table in tables if table.engine == "KeeperMap"]
        privilege_update_tasks = [self.alter_readability(table.escaped_sql_identifier) for table in keeper_map_table_names]
        await asyncio.gather(*privilege_update_tasks)


@dataclasses.dataclass
class KeeperMapTablesReadOnlyStep(KeeperMapTablesReadabilityStepBase):
    _is_read_only = True


@dataclasses.dataclass
class KeeperMapTablesReadWriteStep(KeeperMapTablesReadabilityStepBase):
    _is_read_only = False


@dataclasses.dataclass
class RetrieveKeeperMapTableDataStep(Step[Sequence[KeeperMapTable]]):
    zookeeper_client: ZooKeeperClient
    keeper_map_path_prefix: str | None
    clients: Sequence[ClickHouseClient]

    async def run_step(self, cluster: Cluster, context: StepsContext) -> Sequence[KeeperMapTable]:
        if self.keeper_map_path_prefix is None:
            return []

        async with self.zookeeper_client.connect() as connection:
            change_watch = ChangeWatch()
            try:
                children = await connection.get_children(self.keeper_map_path_prefix, watch=change_watch)
            except NoNodeError:
                # The path doesn't exist, no keeper map tables to retrieve
                return []
            except ZookeeperError as e:
                raise StepFailedError("Failed to retrieve KeeperMap tables") from e

            tables = []
            for child in children:
                keeper_map_table_path = os.path.join(self.keeper_map_path_prefix, child)
                data_path = os.path.join(keeper_map_table_path, "data")
                try:
                    data = await connection.get_children_with_data(data_path)
                except NoNodeError:
                    logger.info("ZNode %s is missing, table was dropped. Skipping", data_path)
                    continue
                except ZookeeperError as e:
                    raise StepFailedError("Failed to retrieve table data") from e

                tables.append(
                    KeeperMapTable(
                        name=child,
                        data=[KeeperMapRow(key=k, value=v) for k, v in sorted(data.items())],
                    )
                )
                if change_watch.has_changed:
                    raise TransientException("Concurrent table addition / deletion during KeeperMap backup")
        return tables

    async def handle_step_failure(self, cluster: Cluster, context: StepsContext) -> None:
        try:
            await KeeperMapTablesReadOnlyStep(self.clients).run_step(cluster, context)
        except ClickHouseClientQueryError:
            logger.warning("Unable to restore write ACLs for KeeperMap tables")


@dataclasses.dataclass
class RetrieveDatabasesAndTablesStep(Step[DatabasesAndTables]):
    """Retrieves the list of all databases that use the replicated database engine and their tables.

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
        databases: dict[bytes, ReplicatedDatabase] = {}
        tables: list[Table] = []
        database_rows = await clickhouse_client.execute(DATABASES_LIST_QUERY)
        for base64_db_name, db_uuid_str in database_rows:
            assert isinstance(base64_db_name, str)
            assert isinstance(db_uuid_str, str)
            db_uuid = uuid.UUID(db_uuid_str)
            db_name = b64decode(base64_db_name)
            shard, replica = await get_shard_and_replica(clickhouse_client, db_name)
            databases[db_name] = ReplicatedDatabase(
                name=db_name,
                uuid=db_uuid,
                shard=shard,
                replica=replica,
            )
        table_rows = await clickhouse_client.execute(TABLES_LIST_QUERY)
        for (
            base64_db_name,
            base64_table_name,
            table_engine,
            table_uuid,
            base64_table_query,
            base64_dependencies,
        ) in table_rows:
            assert isinstance(base64_db_name, str)
            assert isinstance(base64_table_name, str)
            assert isinstance(table_engine, str)
            assert isinstance(base64_table_query, str)
            assert isinstance(base64_dependencies, list)
            db_name = b64decode(base64_db_name)
            if db_name not in databases:
                raise TransientException("Database created while listing tables")
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
    """Retrieves the value of all macros on each server.

    Returns a list of `Macros` objects, each item of the list matches one server.
    """

    clients: Sequence[ClickHouseClient]

    async def run_step(self, cluster: Cluster, context: StepsContext) -> Sequence[Macros]:
        return await asyncio.gather(*[fetch_server_macros(client) for client in self.clients])


@dataclasses.dataclass
class CollectObjectStorageFilesStep(Step[list[ClickHouseObjectStorageFiles]]):
    """Collects the list of files that are referenced by metadata files in the backup."""

    disks: Disks

    async def run_step(self, cluster: Cluster, context: StepsContext) -> list[ClickHouseObjectStorageFiles]:
        snapshot_results: Sequence[ipc.SnapshotResult] = context.get_result(SnapshotStep)
        object_storage_files: dict[str, set[str]] = {}
        total_size_bytes = 0
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
                        total_size_bytes += object_metadata.size_bytes
        return [
            ClickHouseObjectStorageFiles(
                disk_name=disk_name,
                files=[ClickHouseObjectStorageFile(path=path) for path in sorted(paths)],
                total_size_bytes=total_size_bytes,
            )
            for disk_name, paths in sorted(object_storage_files.items())
        ]


class CollectTieredStorageResultsStep(Step[ipc.TieredStorageResults]):
    """ClickHouse specific tiered storage result step."""

    async def run_step(self, cluster: Cluster, context: StepsContext) -> ipc.TieredStorageResults:
        object_storage_files = context.get_result(CollectObjectStorageFilesStep)
        n_objects = 0
        total_size_bytes = 0
        for object_storage_file in object_storage_files:
            n_objects += len(object_storage_file.files)
            if object_storage_file.total_size_bytes is not None:
                total_size_bytes += object_storage_file.total_size_bytes
        return ipc.TieredStorageResults(n_objects=n_objects, total_size_bytes=total_size_bytes)


@dataclasses.dataclass
class PrepareClickHouseManifestStep(Step[dict[str, Any]]):
    """Collects access entities, databases and tables from previous steps into an uploadable manifest."""

    async def run_step(self, cluster: Cluster, context: StepsContext) -> dict[str, Any]:
        databases, tables = context.get_result(RetrieveDatabasesAndTablesStep)
        user_defined_functions = context.get_result(RetrieveUserDefinedFunctionsStep)
        keeper_map_tables = context.get_result(RetrieveKeeperMapTableDataStep)
        manifest = ClickHouseManifest(
            version=ClickHouseBackupVersion.V2,
            access_entities=context.get_result(RetrieveAccessEntitiesStep),
            user_defined_functions=user_defined_functions,
            replicated_databases=databases,
            tables=tables,
            object_storage_files=context.get_result(CollectObjectStorageFilesStep),
            keeper_map_tables=keeper_map_tables,
        )
        return manifest.to_plugin_data()


@dataclasses.dataclass
class RemoveFrozenTablesStep(Step[None]):
    """Removes traces of previous backups that might have failed.
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
        versions = context.get_result(GetVersionsStep)
        to_freeze = [table for table in tables if table.requires_freezing]

        def freeze_partitions(clickhouse_client: ClickHouseClient) -> Iterable[Awaitable[None]]:
            for table in to_freeze:
                yield execute_with_timeout(
                    clickhouse_client,
                    self.freeze_unfreeze_timeout,
                    (
                        f"ALTER TABLE {table.escaped_sql_identifier} "
                        f"{self.operation} WITH NAME {escape_sql_string(self.freeze_name.encode())}"
                        f" SETTINGS distributed_ddl_task_timeout={self.freeze_unfreeze_timeout}"
                    ).encode(),
                )

        await run_partition_cmd_on_every_node(
            clients=self.clients,
            fn=freeze_partitions,
            per_node_concurrency_limit=1,
            versions=versions,
        )


@dataclasses.dataclass
class FreezeTablesStep(FreezeUnfreezeTablesStepBase):
    """Creates a frozen copy of the tables that won't change while we are uploading parts of it.

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

    async def handle_step_failure(self, cluster: Cluster, context: StepsContext) -> None:
        try:
            await KeeperMapTablesReadOnlyStep(clients=self.clients).run_step(cluster, context)
        except ClickHouseClientQueryError:
            logger.warning("Unable to restore write ACLs for KeeperMap tables")


@dataclasses.dataclass
class UnfreezeTablesStep(FreezeUnfreezeTablesStepBase):
    """Removes the frozen parts after we're done uploading them.

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
    """Renames files in the snapshot manifest to match what we will need during recover.

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
        snapshot_results: Sequence[ipc.SnapshotResult] = context.get_result(SnapshotStep)
        for snapshot_result in snapshot_results:
            assert snapshot_result.state is not None
            for snapshot_file in snapshot_result.state.files:
                snapshot_file.relative_path = msgspec.structs.replace(
                    self.disks.parse_part_file_path(snapshot_file.relative_path),
                    freeze_name=None,
                    detached=False,
                ).to_path()


@dataclasses.dataclass
class ClickHouseManifestStep(Step[ClickHouseManifest]):
    """Extracts the ClickHouse plugin manifest from the main backup manifest."""

    async def run_step(self, cluster: Cluster, context: StepsContext) -> ClickHouseManifest:
        backup_manifest = context.get_result(BackupManifestStep)
        return ClickHouseManifest.from_plugin_data(backup_manifest.plugin_data)


async def run_on_every_node(
    clients: Iterable[ClickHouseClient],
    fn: Callable[[ClickHouseClient], Iterable[Awaitable[None]]],
    per_node_concurrency_limit: int,
) -> None:
    await asyncio.gather(*[gather_limited(per_node_concurrency_limit, fn(client)) for client in clients])


async def wait_for_condition_on_every_node(
    clients: Iterable[ClickHouseClient],
    condition: Callable[[ClickHouseClient], Awaitable[bool]],
    description: str,
    timeout_seconds: float,
    recheck_every_seconds: float = 1.0,
) -> None:
    async def wait_for_condition(client: ClickHouseClient) -> None:
        start_time = time.monotonic()
        while True:
            if await condition(client):
                return
            if time.monotonic() - start_time > timeout_seconds:
                raise StepFailedError(f"Timeout while waiting for {description}")
            await asyncio.sleep(recheck_every_seconds)

    await asyncio.gather(*(wait_for_condition(client) for client in clients))


def get_restore_table_query(table: Table) -> bytes:
    # Use `ATTACH` instead of `CREATE` for materialized views for
    # proper restore in case of `SELECT` table absence
    return re.sub(b"^CREATE MATERIALIZED VIEW", b"ATTACH MATERIALIZED VIEW", table.create_query)


@dataclasses.dataclass
class RestoreReplicatedDatabasesStep(Step[None]):
    """Re-creates replicated databases on each client and re-create all tables in each database.

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
            # Upstream has introduced a similar list in the replica recovery context:
            # https://github.com/ClickHouse/ClickHouse/commit/48ed54e822f6ed6f6bdd67db3df7a4a58e550bbb
            # the two should be kept in sync until system.tables captures the query context.
            b"SET allow_experimental_inverted_index=true",
            b"SET allow_experimental_codecs=true",
            b"SET allow_experimental_live_view=true",
            b"SET allow_experimental_window_view=true",
            b"SET allow_experimental_funnel_functions=true",
            b"SET allow_experimental_nlp_functions=true",
            b"SET allow_experimental_hash_functions=true",
            b"SET allow_experimental_object_type=true",
            b"SET allow_experimental_annoy_index=true",
            b"SET allow_experimental_usearch_index=true",
            b"SET allow_experimental_bigint_types=true",
            b"SET allow_experimental_window_functions=true",
            b"SET allow_experimental_geo_types=true",
            b"SET allow_experimental_map_type=true",
            b"SET allow_suspicious_low_cardinality_types=true",
            b"SET allow_suspicious_fixed_string_types=true",
            b"SET allow_suspicious_indices=true",
            b"SET allow_suspicious_codecs=true",
            b"SET allow_hyperscan=true",
            b"SET allow_simdjson=true",
            b"SET allow_deprecated_syntax_for_merge_tree=true",
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
                if error.exception_code in (error.SETTING_CONSTRAINT_VIOLATION, error.UNKNOWN_SETTING):
                    # If we can't set the option, that's fine, either it's not needed or it will fail later anyway
                    logger.info("Could not enable experimental setting, skipped; full error: %s", error)
                else:
                    raise

        # If any known table depends on an unknown table that was inside a non-replicated
        # database engine, then this will crash. See comment in `RetrieveReplicatedDatabasesStep`.
        for table in tables_sorted_by_dependencies(manifest.tables):
            restore_table_query = get_restore_table_query(table)
            # Create on the first client and let replication do its thing
            await self.clients[0].execute(restore_table_query, session_id=session_id)


DatabasesReplicas = Mapping[bytes, Sequence[DatabaseReplica]]


@dataclasses.dataclass
class ListDatabaseReplicasStep(Step[DatabasesReplicas]):
    """For each replicated database, returns the list of replicas.

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
    """Restores access entities (user, roles, quotas, row_policies, settings profiles) and their grants
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
class RestoreUserDefinedFunctionsStep(Step[None]):
    zookeeper_client: ZooKeeperClient
    replicated_user_defined_zookeeper_path: str | None
    clients: Sequence[ClickHouseClient]
    sync_user_defined_functions_timeout: float

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        if self.replicated_user_defined_zookeeper_path is None:
            return

        clickhouse_manifest = context.get_result(ClickHouseManifestStep)
        if not clickhouse_manifest.user_defined_functions:
            return

        async with self.zookeeper_client.connect() as connection:
            for user_defined_function in clickhouse_manifest.user_defined_functions:
                path = os.path.join(self.replicated_user_defined_zookeeper_path, user_defined_function.path)
                await connection.try_create(path, user_defined_function.create_query)

        async def check_function_count(client: ClickHouseClient) -> bool:
            count = await client.execute(b"""SELECT count(*) FROM system.functions WHERE origin = 'SQLUserDefined'""")
            assert isinstance(count[0][0], str)
            return int(count[0][0]) >= len(clickhouse_manifest.user_defined_functions)

        await wait_for_condition_on_every_node(
            clients=self.clients,
            condition=check_function_count,
            description="user defined functions to be restored",
            timeout_seconds=self.sync_user_defined_functions_timeout,
        )


@dataclasses.dataclass
class RestoreKeeperMapTableDataStep(Step[None]):
    zookeeper_client: ZooKeeperClient
    keeper_map_path_prefix: str | None
    clients: Sequence[ClickHouseClient]
    sync_keeper_map_data_timeout: float

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        if self.keeper_map_path_prefix is None:
            return

        clickhouse_manifest = context.get_result(ClickHouseManifestStep)

        if not clickhouse_manifest.keeper_map_tables:
            return

        data_path = None
        row = None
        async with self.zookeeper_client.connect() as connection:
            transaction = connection.transaction()
            for table in clickhouse_manifest.keeper_map_tables:
                if not table.data:
                    continue

                data_path = os.path.join(self.keeper_map_path_prefix, table.name, "data")
                if not await connection.exists(data_path):
                    # RestoreReplicatedDatabasesStep should have created this
                    # parent znode and some other metadata znodes for the table.
                    raise StepFailedError(
                        f"KeeperMap table data path {data_path} doesn't exist in "
                        "ZooKeeper, should have been created by RestoreReplicatedDatabasesStep"
                    )
                for row in table.data:
                    row_path = os.path.join(data_path, row.key)
                    transaction.create(row_path, row.value)
            await transaction.commit()

        if data_path is not None and row is not None:
            # Wait for final restored row to be visible on all nodes.
            # Note that each ClickHouse instance maintains a single zookeeper
            # session so reading from the system table will see the same data as
            # reading from the KeeperMap table.
            async def check_row_existence(client: ClickHouseClient) -> bool:
                escaped_data_path = escape_sql_string(data_path.encode())
                escaped_name = escape_sql_string(row.key.encode())
                query = f"SELECT count(*) FROM system.zookeeper WHERE path = {escaped_data_path} AND name = {escaped_name}"
                logging.debug("Checking KeeperMap has synced with query: %s", query)
                result = await client.execute(query.encode())
                assert isinstance(result[0][0], str)
                return bool(int(result[0][0]))

            await wait_for_condition_on_every_node(
                clients=self.clients,
                condition=check_row_existence,
                description="KeeperMap table data to be restored",
                timeout_seconds=self.sync_keeper_map_data_timeout,
            )


@dataclasses.dataclass
class RestoreReplicaStep(Step[None]):
    """Restore data on all tables by using `SYSTEM RESTORE REPLICA... `."""

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
                await connection.delete(f"/clickhouse/tables/{table.uuid!s}", recursive=True)

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
class RestoreObjectStorageFilesStep(SyncStep[None]):
    """If the source and target disks are not the same, restore object storage files by copying them
    from the source to the target disk.
    """

    source_disks: Disks
    target_disks: Disks

    def run_sync_step(self, cluster: Cluster, context: StepsContext) -> None:
        clickhouse_manifest = context.get_result(ClickHouseManifestStep)
        for object_storage_files in clickhouse_manifest.object_storage_files:
            if len(object_storage_files.files) > 0:
                disk_name = object_storage_files.disk_name
                source_storage = self.source_disks.create_object_storage(disk_name=disk_name)
                if source_storage is None:
                    raise StepFailedError(f"Source disk named {disk_name!r} isn't configured as object storage")
                try:
                    target_storage = self.target_disks.create_object_storage(disk_name=disk_name)
                    if target_storage is None:
                        raise StepFailedError(f"Target disk named {disk_name!r} isn't configured as object storage")
                    try:
                        if source_storage.get_config() != target_storage.get_config():
                            paths = [file.path for file in object_storage_files.files]
                            target_storage.copy_items_from(
                                source_storage,
                                paths,
                                stats=cluster.stats,
                            )
                    finally:
                        target_storage.close()
                finally:
                    source_storage.close()


@dataclasses.dataclass
class AttachMergeTreePartsStep(Step[None]):
    """Restore data to all tables by using `ALTER TABLE ... ATTACH`.

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
    """Before declaring the restoration as finished, make sure all parts of replicated tables
    are all exchanged between all nodes.
    """

    clients: Sequence[ClickHouseClient]
    sync_timeout: float
    max_concurrent_sync_per_node: int

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        manifest = context.get_result(ClickHouseManifestStep)

        def _sync_replicas(client: ClickHouseClient) -> Iterator[Awaitable[None]]:
            yield from (
                execute_with_timeout(
                    client, self.sync_timeout, f"SYSTEM SYNC REPLICA {table.escaped_sql_identifier} LIGHTWEIGHT".encode()
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
class DeleteDanglingObjectStorageFilesStep(SyncStep[None]):
    """Delete object storage files that were created before the most recent backup
    and that are not part of any backup.
    """

    disks: Disks
    json_storage: JsonStorage
    # the longest it could be expected to take to upload a part
    file_upload_grace_period: timedelta = timedelta(hours=6)

    def run_sync_step(self, cluster: Cluster, context: StepsContext) -> None:
        backup_manifests = context.get_result(ComputeKeptBackupsStep)
        if len(backup_manifests) < 1:
            logger.info("no backup manifest, not deleting any object storage disk file")
            # If we don't have at least one backup, we don't know which files are more recent
            # than the latest backup, so we don't do anything.
            return

        # When a part is moved to the remote disk, firstly files are copied,
        # then the part is committed. This means for a very large part with
        # multiple files, the last_modified time of some files on remote storage
        # may be significantly earlier than the time the part actually appears.
        # We do not want to delete these files!
        newest_backup_start_time = max(backup_manifest.start for backup_manifest in backup_manifests)
        latest_safe_delete_time = newest_backup_start_time - self.file_upload_grace_period

        kept_paths: dict[str, set[str]] = {}
        for manifest_min in backup_manifests:
            manifest_data = download_backup_manifest_sync(self.json_storage, manifest_min.filename)
            clickhouse_manifest = ClickHouseManifest.from_plugin_data(manifest_data.plugin_data)
            for object_storage_files in clickhouse_manifest.object_storage_files:
                disk_kept_paths = kept_paths.setdefault(object_storage_files.disk_name, set())
                disk_kept_paths.update(file.path for file in object_storage_files.files)

        for disk_name, disk_kept_paths in sorted(kept_paths.items()):
            disk_object_storage = self.disks.create_object_storage(disk_name=disk_name)
            if disk_object_storage is None:
                raise StepFailedError(f"Could not find object storage disk named {disk_name!r}")
            try:
                keys_to_remove = []
                logger.info("found %d object storage files to keep in disk %r", len(disk_kept_paths), disk_name)
                disk_object_storage_items = disk_object_storage.list_items()
                for item in disk_object_storage_items:
                    if item.last_modified < latest_safe_delete_time and item.key not in disk_kept_paths:
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
                    disk_object_storage.delete_item(key_to_remove)
            finally:
                disk_object_storage.close()


@dataclasses.dataclass
class GetVersionsStep(Step[Sequence[ClickHouseVersion]]):
    """Get the version of ClickHouse running on every node."""

    clients: Sequence[ClickHouseClient]

    async def run_step(self, cluster: Cluster, context: StepsContext) -> Sequence[ClickHouseVersion]:
        return await asyncio.gather(*(get_version(client) for client in self.clients))


async def get_version(clickhouse_client: ClickHouseClient) -> ClickHouseVersion:
    rows = await clickhouse_client.execute(b"SELECT version()")
    assert isinstance(rows[0][0], str)
    major, minor = rows[0][0].split(".")[:2]
    return int(major), int(minor)


async def execute_with_timeout(client: ClickHouseClient, timeout: float, query: bytes) -> None:
    # we use a session because we can't use the SETTINGS clause with all types of queries
    session_id = secrets.token_hex()
    await client.execute(f"SET receive_timeout={timeout}".encode(), session_id=session_id)
    await client.execute(query, session_id=session_id, timeout=timeout)


CLICKHOUSE_VERSION_PARTITION_CMDS_NOT_REPLICATED = (23, 8)


async def run_partition_cmd_on_every_node(
    versions: Sequence[ClickHouseVersion],
    clients: Sequence[ClickHouseClient],
    fn: Callable[[ClickHouseClient], Iterable[Awaitable[None]]],
    per_node_concurrency_limit: int,
) -> None:
    """Run a ClickHouse partition command on every node.

    In ClickHouse 23.8, the following "partition commands" went from replicated
    to non-replicated:
    ```sql
    ALTER TABLE ... MOVE PARTITION
    ALTER TABLE ... DROP DETACHED PARTITION
    ALTER TABLE ... FREEZE
    ALTER TABLE ... FREEZE PARTITION
    ALTER TABLE ... UNFREEZE
    ALTER TABLE ... UNFREEZE PARTITION
    ALTER TABLE ... REPLACE PARTITION
    ```

    To run one of these on the cluster, use `run_partition_cmd_on_every_node`.
    The following commands were never replicated and should use
    `run_on_every_node` instead:
    ```sql
    ALTER TABLE ... ATTACH PARTITION
    ALTER TABLE ... FETCH PARTITION
    ALTER TABLE ... DROP PARTITION
    ```
    """
    assert len(versions) == len(clients)
    old_client = next(
        (client for version, client in zip(versions, clients) if version < CLICKHOUSE_VERSION_PARTITION_CMDS_NOT_REPLICATED),
        None,
    )
    if old_client is None:
        await run_on_every_node(clients, fn, per_node_concurrency_limit)
    else:
        await gather_limited(per_node_concurrency_limit, fn(old_client))
