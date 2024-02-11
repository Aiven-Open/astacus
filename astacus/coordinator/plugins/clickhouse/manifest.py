"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from astacus.common.utils import AstacusModel
from astacus.coordinator.plugins.clickhouse.client import escape_sql_identifier
from base64 import b64decode, b64encode
from collections.abc import Mapping, Sequence
from typing import Any, Optional
from uuid import UUID

import enum
import uuid


class AccessEntity(AstacusModel):
    """
    An access entity can be a user, a role, a quota, etc.
    See `RetrieveAccessEntitiesStep` for more info.
    """

    type: str
    uuid: UUID
    name: bytes
    attach_query: bytes

    @classmethod
    def from_plugin_data(cls, data: Mapping[str, Any]) -> "AccessEntity":
        return AccessEntity(
            type=data["type"],
            uuid=UUID(hex=data["uuid"]),
            name=b64decode(data["name"]),
            attach_query=b64decode(data["attach_query"]),
        )


class ReplicatedDatabase(AstacusModel):
    name: bytes
    # This is optional because of older backups without uuids
    uuid: Optional[UUID] = None
    # These contain macros, not per-server final values
    shard: bytes
    replica: bytes

    @classmethod
    def from_plugin_data(cls, data: Mapping[str, Any]) -> "ReplicatedDatabase":
        return ReplicatedDatabase(
            name=b64decode(data["name"]),
            uuid=uuid.UUID(data["uuid"]) if "uuid" in data else None,
            shard=b64decode(data["shard"]) if "shard" in data else b"{shard}",
            replica=b64decode(data["replica"]) if "replica" in data else b"{replica}",
        )


class Table(AstacusModel):
    database: bytes
    name: bytes
    engine: str
    uuid: UUID
    create_query: bytes
    # This is a list (database_name, table_name) that depends on this table,
    # *not* the list of tables that this table depends on.
    dependencies: Sequence[tuple[bytes, bytes]] = []

    @property
    def is_replicated(self) -> bool:
        return self.engine.startswith("Replicated")

    @property
    def requires_freezing(self) -> bool:
        return "MergeTree" in self.engine

    @property
    def escaped_sql_identifier(self) -> str:
        return f"{escape_sql_identifier(self.database)}.{escape_sql_identifier(self.name)}"

    @classmethod
    def from_plugin_data(cls, data: Mapping[str, Any]) -> "Table":
        dependencies = [
            (b64decode(database_name), b64decode(table_name)) for database_name, table_name in data["dependencies"]
        ]
        return Table(
            database=b64decode(data["database"]),
            name=b64decode(data["name"]),
            engine=data["engine"],
            uuid=UUID(hex=data["uuid"]),
            create_query=b64decode(data["create_query"]),
            dependencies=dependencies,
        )


class ClickHouseBackupVersion(enum.Enum):
    V1 = "v1"
    # Version 2 supports object storage disks and does not distribute parts among replicas
    V2 = "v2"


class ClickHouseObjectStorageFile(AstacusModel):
    path: str

    @classmethod
    def from_plugin_data(cls, data: dict[str, Any]) -> "ClickHouseObjectStorageFile":
        return ClickHouseObjectStorageFile(path=data["path"])


class ClickHouseObjectStorageFiles(AstacusModel):
    disk_name: str
    files: list[ClickHouseObjectStorageFile]

    @classmethod
    def from_plugin_data(cls, data: dict[str, Any]) -> "ClickHouseObjectStorageFiles":
        return ClickHouseObjectStorageFiles(
            disk_name=data["disk_name"],
            files=[ClickHouseObjectStorageFile.from_plugin_data(item) for item in data["files"]],
        )


class ClickHouseManifest(AstacusModel):
    class Config:
        use_enum_values = False

    version: ClickHouseBackupVersion
    access_entities: Sequence[AccessEntity] = []
    replicated_databases: Sequence[ReplicatedDatabase] = []
    tables: Sequence[Table] = []
    object_storage_files: list[ClickHouseObjectStorageFiles] = []

    def to_plugin_data(self) -> dict[str, Any]:
        return encode_manifest_data(self.dict())

    @classmethod
    def from_plugin_data(cls, data: Mapping[str, Any]) -> "ClickHouseManifest":
        return ClickHouseManifest(
            version=ClickHouseBackupVersion(data.get("version", ClickHouseBackupVersion.V1.value)),
            access_entities=[AccessEntity.from_plugin_data(item) for item in data["access_entities"]],
            replicated_databases=[ReplicatedDatabase.from_plugin_data(item) for item in data["replicated_databases"]],
            tables=[Table.from_plugin_data(item) for item in data["tables"]],
            object_storage_files=[
                ClickHouseObjectStorageFiles.from_plugin_data(item) for item in data.get("object_storage_files", [])
            ],
        )


def encode_manifest_data(data: Any) -> Any:
    if isinstance(data, dict):
        return {key: encode_manifest_data(value) for key, value in data.items()}
    if isinstance(data, (list, tuple)):
        return [encode_manifest_data(item) for item in data]
    if isinstance(data, bytes):
        return b64encode(data).decode()
    if isinstance(data, enum.Enum):
        return encode_manifest_data(data.value)
    if isinstance(data, UUID):
        return str(data)
    return data
