"""Copyright (c) 2022 Aiven Ltd
See LICENSE for details

cassandra backup/restore plugin models

"""
from astacus.common.cassandra.schema import CassandraSchema
from collections.abc import Sequence
from typing import Optional
from uuid import UUID

import msgspec


class CassandraConfigurationNode(msgspec.Struct, kw_only=True, frozen=True):
    # The configured node order has to be identified _somehow_;
    # otherwise, we cannot map the same data to same set of
    # tokens. One of these is required.
    address: Optional[str] = None
    host_id: Optional[UUID] = None
    listen_address: Optional[str] = None
    tokens: Optional[Sequence[str]] = None

    def __post_init__(self) -> None:
        assert (
            self.address is not None
            or self.host_id is not None
            or self.listen_address is not None
            or self.tokens is not None
        )


class CassandraManifestNode(msgspec.Struct, kw_only=True):
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


class CassandraManifest(msgspec.Struct, kw_only=True, frozen=True):
    cassandra_schema: CassandraSchema
    nodes: Sequence[CassandraManifestNode]
