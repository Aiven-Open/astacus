"""Copyright (c) 2022 Aiven Ltd
See LICENSE for details

cassandra backup/restore plugin models

"""
from astacus.common.cassandra.schema import CassandraSchema
from astacus.common.utils import AstacusModel
from collections.abc import Sequence
from pydantic import root_validator
from typing import Optional
from uuid import UUID


class CassandraConfigurationNode(AstacusModel):
    # The configured node order has to be identified _somehow_;
    # otherwise, we cannot map the same data to same set of
    # tokens. One of these is required.
    address: Optional[str] = None
    host_id: Optional[UUID] = None
    listen_address: Optional[str] = None
    tokens: Optional[Sequence[str]] = None

    @classmethod
    @root_validator
    def ensure_identifier_provided(cls, values: dict) -> dict:
        assert any(values.get(k) for k in ["address", "host_id", "listen_address", "tokens"])
        return values


class CassandraManifestNode(AstacusModel):
    address: str
    host_id: UUID
    listen_address: str
    rack: str
    tokens: list[str]

    def matches_configuration_node(self, node: CassandraConfigurationNode) -> bool:
        for attribute in ["address", "host_id", "listen_address", "tokens"]:
            other_value = getattr(node, attribute)
            if other_value and getattr(self, attribute) != other_value:
                return False
        return True


class CassandraManifest(AstacusModel):
    cassandra_schema: CassandraSchema
    nodes: Sequence[CassandraManifestNode]
