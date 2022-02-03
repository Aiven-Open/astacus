"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from astacus.common.utils import AstacusModel
from astacus.coordinator.plugins.clickhouse.client import escape_sql_identifier
from base64 import b64decode, b64encode
from typing import Any, Dict, List, Tuple
from uuid import UUID


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
    def from_plugin_data(cls, data: Dict[str, Any]) -> "AccessEntity":
        return AccessEntity(
            type=data["type"],
            uuid=UUID(hex=data["uuid"]),
            name=b64decode(data["name"]),
            attach_query=b64decode(data["attach_query"]),
        )


class ReplicatedDatabase(AstacusModel):
    name: bytes

    @classmethod
    def from_plugin_data(cls, data: Dict[str, Any]) -> "ReplicatedDatabase":
        return ReplicatedDatabase(name=b64decode(data["name"]))


class Table(AstacusModel):
    database: bytes
    name: bytes
    engine: str
    uuid: UUID
    create_query: bytes
    # This is a list (database_name, table_name) that depends on this table,
    # *not* the list of tables that this table depends on.
    dependencies: List[Tuple[bytes, bytes]] = []

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
    def from_plugin_data(cls, data: Dict[str, Any]) -> "Table":
        dependencies = [(b64decode(database_name), b64decode(table_name))
                        for database_name, table_name in data["dependencies"]]
        return Table(
            database=b64decode(data["database"]),
            name=b64decode(data["name"]),
            engine=data["engine"],
            uuid=UUID(hex=data["uuid"]),
            create_query=b64decode(data["create_query"]),
            dependencies=dependencies,
        )


class ClickHouseManifest(AstacusModel):
    access_entities: List[AccessEntity] = []
    replicated_databases: List[ReplicatedDatabase] = []
    tables: List[Table] = []

    def to_plugin_data(self) -> Dict[str, Any]:
        return encode_manifest_data(self.dict())

    @classmethod
    def from_plugin_data(cls, data: Dict[str, Any]) -> "ClickHouseManifest":
        return ClickHouseManifest(
            access_entities=[AccessEntity.from_plugin_data(item) for item in data["access_entities"]],
            replicated_databases=[ReplicatedDatabase.from_plugin_data(item) for item in data["replicated_databases"]],
            tables=[Table.from_plugin_data(item) for item in data["tables"]]
        )


def encode_manifest_data(data: Any) -> Any:
    if isinstance(data, dict):
        return {key: encode_manifest_data(value) for key, value in data.items()}
    if isinstance(data, (list, tuple)):
        return [encode_manifest_data(item) for item in data]
    if isinstance(data, bytes):
        return b64encode(data).decode()
    if isinstance(data, UUID):
        return str(data)
    return data
