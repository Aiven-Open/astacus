"""Copyright (c) 2021 Aiven Ltd
See LICENSE for details.
"""

from astacus.common.utils import AstacusModel
from astacus.coordinator.plugins.clickhouse.client import escape_sql_identifier
from base64 import b64decode, b64encode
from collections.abc import Mapping, Sequence
from typing import Any, Self
from uuid import UUID

import enum
import uuid


class AccessEntity(AstacusModel):
    """An access entity can be a user, a role, a quota, etc.
    See `RetrieveAccessEntitiesStep` for more info.
    """

    type: str
    uuid: UUID
    name: bytes
    attach_query: bytes

    @classmethod
    def from_plugin_data(cls, data: Mapping[str, Any]) -> Self:
        return cls(
            type=data["type"],
            uuid=UUID(hex=data["uuid"]),
            name=b64decode(data["name"]),
            attach_query=b64decode(data["attach_query"]),
        )


class UserDefinedFunction(AstacusModel):
    """SQL UserDefinedFunction, stored in Zookeeper."""

    path: str
    create_query: bytes

    @classmethod
    def from_plugin_data(cls, data: Mapping[str, Any]) -> Self:
        return cls(path=data["path"], create_query=b64decode(data["create_query"]))


class KeeperMapRow(AstacusModel):
    key: str
    value: bytes

    @classmethod
    def from_plugin_data(cls, data: Mapping[str, Any]) -> Self:
        return cls(key=data["key"], value=b64decode(data["value"]))


class KeeperMapTable(AstacusModel):
    name: str
    data: Sequence[KeeperMapRow]

    @classmethod
    def from_plugin_data(cls, data: Mapping[str, Any]) -> Self:
        return cls(name=data["name"], data=[KeeperMapRow.from_plugin_data(item) for item in data["data"]])


class ReplicatedDatabase(AstacusModel):
    name: bytes
    # This is optional because of older backups without uuids
    uuid: UUID | None = None
    # These contain macros, not per-server final values
    shard: bytes
    replica: bytes

    @classmethod
    def from_plugin_data(cls, data: Mapping[str, Any]) -> Self:
        return cls(
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
    def from_plugin_data(cls, data: Mapping[str, Any]) -> Self:
        dependencies = [
            (b64decode(database_name), b64decode(table_name)) for database_name, table_name in data["dependencies"]
        ]
        return cls(
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
    def from_plugin_data(cls, data: dict[str, Any]) -> Self:
        return cls(path=data["path"])


class ClickHouseObjectStorageFiles(AstacusModel):
    disk_name: str
    files: list[ClickHouseObjectStorageFile]
    total_size_bytes: int | None = None

    @classmethod
    def from_plugin_data(cls, data: dict[str, Any]) -> Self:
        return cls(
            disk_name=data["disk_name"],
            files=[ClickHouseObjectStorageFile.from_plugin_data(item) for item in data["files"]],
            total_size_bytes=data.get("total_size_bytes"),
        )


class ClickHouseManifest(AstacusModel):
    class Config:
        use_enum_values = False

    version: ClickHouseBackupVersion
    access_entities: Sequence[AccessEntity] = []
    user_defined_functions: Sequence[UserDefinedFunction] = []
    replicated_databases: Sequence[ReplicatedDatabase] = []
    tables: Sequence[Table] = []
    object_storage_files: Sequence[ClickHouseObjectStorageFiles] = []
    keeper_map_tables: Sequence[KeeperMapTable] = []

    def to_plugin_data(self) -> dict[str, Any]:
        return encode_manifest_data(self.dict())

    @classmethod
    def from_plugin_data(cls, data: Mapping[str, Any]) -> Self:
        return cls(
            version=ClickHouseBackupVersion(data.get("version", ClickHouseBackupVersion.V1.value)),
            access_entities=[AccessEntity.from_plugin_data(item) for item in data["access_entities"]],
            user_defined_functions=[
                UserDefinedFunction.from_plugin_data(item) for item in data.get("user_defined_functions", [])
            ],
            replicated_databases=[ReplicatedDatabase.from_plugin_data(item) for item in data["replicated_databases"]],
            tables=[Table.from_plugin_data(item) for item in data["tables"]],
            object_storage_files=[
                ClickHouseObjectStorageFiles.from_plugin_data(item) for item in data.get("object_storage_files", [])
            ],
            keeper_map_tables=[KeeperMapTable.from_plugin_data(item) for item in data.get("keeper_map_tables", [])],
        )


def encode_manifest_data(data: Any) -> Any:
    if isinstance(data, dict):
        return {key: encode_manifest_data(value) for key, value in data.items()}
    if isinstance(data, list | tuple):
        return [encode_manifest_data(item) for item in data]
    if isinstance(data, bytes):
        return b64encode(data).decode()
    if isinstance(data, enum.Enum):
        return encode_manifest_data(data.value)
    if isinstance(data, UUID):
        return str(data)
    return data
