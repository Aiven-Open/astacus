"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from astacus.common.utils import AstacusModel
from astacus.coordinator.plugins.clickhouse.client import escape_sql_identifier
from typing import List, Tuple
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


class ReplicatedDatabase(AstacusModel):
    name: str


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


class ClickHouseManifest(AstacusModel):
    access_entities: List[AccessEntity] = []
    replicated_databases: List[ReplicatedDatabase] = []
    tables: List[Table] = []
