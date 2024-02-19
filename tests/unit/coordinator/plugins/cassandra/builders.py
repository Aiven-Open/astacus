"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from astacus.common.cassandra.schema import CassandraKeyspace
from collections.abc import Mapping


class CassandraKeyspaceBuilder(CassandraKeyspace):
    def with_cql_create_self(self, cql_create_self: str) -> "CassandraKeyspaceBuilder":
        self.cql_create_self = cql_create_self
        return self

    def with_network_topology_strategy_dcs(self, dcs: Mapping[str, str]) -> "CassandraKeyspaceBuilder":
        self.network_topology_strategy_dcs = dcs
        return self


def build_keyspace(name: str) -> CassandraKeyspaceBuilder:
    return CassandraKeyspaceBuilder(
        name=name,
        cql_create_self="",
        network_topology_strategy_dcs={},
        durable_writes=True,
        aggregates=[],
        functions=[],
        tables=[],
        user_types=[],
    )
