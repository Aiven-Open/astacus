"""
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""

# pylint: disable=protected-access

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Optional
from uuid import UUID

import pytest


@dataclass(frozen=True)
class RetrieveTestCase:
    name: str

    # Input
    field: Optional[str] = None
    populate_rf: int = 2
    populate_tokens: int = 7
    populate_nodes: int = 3
    # Erroneous input mode - give all nodes same address
    duplicate_address: bool = False

    # Output
    expected_error: Optional[Exception] = None

    def __str__(self):
        return self.name


@pytest.mark.parametrize(
    "case",
    [
        RetrieveTestCase(name="no matching nodes", field=None, expected_error=ValueError),
        RetrieveTestCase(name="multiple matching nodes", field="address", duplicate_address=True, expected_error=ValueError),
    ]
    + [RetrieveTestCase(name=f"match by {field}", field=field) for field in ["address", "host_id", "listen_address"]],
    ids=str,
)
def test_retrieve_manifest_from_cassandra(mocker, case):
    backup_steps = pytest.importorskip("astacus.coordinator.plugins.cassandra.model.backup_steps")
    plugin_model = pytest.importorskip("astacus.coordinator.plugins.cassandra.model")
    cassandra_schema = pytest.importorskip("astacus.common.cassandra.schema")
    mocker.patch.object(
        cassandra_schema.CassandraSchema,
        "from_cassandra_session",
        return_value=cassandra_schema.CassandraSchema(keyspaces=[]),
    )

    cassandra_nodes = [
        SimpleNamespace(
            host_id=UUID(f"1234567812345678123456781234567{node}"),
            address=f"1.2.3.{node}" if not case.duplicate_address else "127.0.0.1",
            listen_address="la" if node == 0 else None,
            broadcast_address=f"ba{node}" if node != 0 else None,
            rack="unused",
        )
        for node in range(case.populate_nodes)
    ]
    token_to_host_owner_map_items = [
        (SimpleNamespace(value=f"token{token}"), cassandra_nodes[node])
        for node in range(case.populate_nodes)
        for token in range(case.populate_tokens)
        if ((node + node * token) // case.populate_rf) % case.populate_nodes == 0
    ]

    mocked_map = SimpleNamespace(items=mocker.Mock(return_value=token_to_host_owner_map_items))
    cas = SimpleNamespace(cluster_metadata=SimpleNamespace(token_map=SimpleNamespace(token_to_host_owner=mocked_map)))

    nodes = []
    for cassandra_node in cassandra_nodes:
        cnode = plugin_model.CassandraConfigurationNode()
        # Note: 'tokens' is bit awkward to to test and unlikely to be really used so not tested here.
        if case.field:
            if case.field == "listen_address":
                setattr(cnode, case.field, cassandra_node.listen_address or cassandra_node.broadcast_address)
            else:
                setattr(cnode, case.field, getattr(cassandra_node, case.field))
        nodes.append(cnode)
    if case.expected_error:
        with pytest.raises(case.expected_error):
            backup_steps._retrieve_manifest_from_cassandra(cas, nodes)
        return
    manifest = backup_steps._retrieve_manifest_from_cassandra(cas, nodes)
    assert len(manifest.nodes) == len(nodes)
