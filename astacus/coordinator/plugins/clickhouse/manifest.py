"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from astacus.common.utils import AstacusModel
from astacus.coordinator.plugins.clickhouse.client import escape_sql_identifier
from typing import Any, Dict, List, Tuple
from uuid import UUID

import base64


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
            name=base64.b64decode(data["name"]),
            attach_query=base64.b64decode(data["attach_query"]),
        )


class ReplicatedDatabase(AstacusModel):
    name: str

    @classmethod
    def from_plugin_data(cls, data: Dict[str, Any]) -> "ReplicatedDatabase":
        return ReplicatedDatabase(name=data["name"])


class Table(AstacusModel):
    database: str
    name: str
    engine: str
    uuid: UUID
    create_query: str
    # This is a list (database_name, table_name) that depends on this table,
    # *not* the list of tables that this table depends on.
    dependencies: List[Tuple[str, str]] = []

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
        return Table(
            database=data["database"],
            name=data["name"],
            engine=data["engine"],
            uuid=UUID(hex=data["uuid"]),
            create_query=data["create_query"],
            dependencies=[(database_name, table_name) for database_name, table_name in data["dependencies"]],
        )


class ClickHouseManifest(AstacusModel):
    access_entities: List[AccessEntity] = []
    replicated_databases: List[ReplicatedDatabase] = []
    tables: List[Table] = []

    def to_plugin_data(self) -> Dict[str, Any]:
        return encode_bytes(self.dict())

    @classmethod
    def from_plugin_data(cls, data: Dict[str, Any]) -> "ClickHouseManifest":
        return ClickHouseManifest(
            access_entities=[AccessEntity.from_plugin_data(item) for item in data["access_entities"]],
            replicated_databases=[ReplicatedDatabase.from_plugin_data(item) for item in data["replicated_databases"]],
            tables=[Table.from_plugin_data(item) for item in data["tables"]]
        )


def encode_bytes(data: Any) -> Any:
    if isinstance(data, dict):
        return {key: encode_bytes(value) for key, value in data.items()}
    if isinstance(data, list):
        return [encode_bytes(item) for item in data]
    if isinstance(data, bytes):
        return base64.b64encode(data).decode()
    return data
