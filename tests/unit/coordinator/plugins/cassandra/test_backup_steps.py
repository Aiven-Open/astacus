"""Copyright (c) 2022 Aiven Ltd
See LICENSE for details.
"""

from astacus.common.cassandra.schema import CassandraSchema
from astacus.coordinator.plugins.cassandra import backup_steps
from astacus.coordinator.plugins.cassandra.model import CassandraConfigurationNode
from collections.abc import Mapping
from dataclasses import dataclass
from pytest_mock import MockerFixture
from tests.unit.coordinator.plugins.cassandra.builders import build_keyspace
from unittest.mock import Mock
from uuid import UUID

import pytest


@dataclass(frozen=True)
class RetrieveTestCase:
    name: str

    # Input
    field: str | None = None
    populate_rf: int = 2
    populate_tokens: int = 7
    populate_nodes: int = 3
    # Erroneous input mode - give all nodes same address
    duplicate_address: bool = False

    # Output
    expected_error: type[Exception] | None = None

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
def test_retrieve_manifest_from_cassandra(mocker: MockerFixture, case: RetrieveTestCase) -> None:
    mocker.patch.object(CassandraSchema, "from_cassandra_session", return_value=CassandraSchema(keyspaces=[]))
    cassandra_nodes = [
        Mock(
            host_id=UUID(f"1234567812345678123456781234567{node}"),
            address=f"1.2.3.{node}" if not case.duplicate_address else "127.0.0.1",
            listen_address="la" if node == 0 else None,
            broadcast_address=f"ba{node}" if node != 0 else None,
            rack="unused",
        )
        for node in range(case.populate_nodes)
    ]
    token_to_host_owner_map_items = [
        (Mock(value=f"token{token}"), cassandra_nodes[node])
        for node in range(case.populate_nodes)
        for token in range(case.populate_tokens)
        if ((node + node * token) // case.populate_rf) % case.populate_nodes == 0
    ]

    mocked_map = Mock(items=mocker.Mock(return_value=token_to_host_owner_map_items))
    cas = Mock(cluster_metadata=Mock(token_map=Mock(token_to_host_owner=mocked_map)))

    nodes = []
    for cassandra_node in cassandra_nodes:
        cnode = CassandraConfigurationNode()
        # Note: 'tokens' is bit awkward to to test and unlikely to be really used so not tested here.
        if case.field:
            if case.field == "listen_address":
                setattr(cnode, case.field, cassandra_node.listen_address or cassandra_node.broadcast_address)
            else:
                setattr(cnode, case.field, getattr(cassandra_node, case.field))
        nodes.append(cnode)
    if case.expected_error:
        with pytest.raises(case.expected_error):
            backup_steps._retrieve_manifest_from_cassandra(cas, nodes, datacenter=None)
        return
    manifest = backup_steps._retrieve_manifest_from_cassandra(cas, nodes, datacenter=None)
    assert len(manifest.nodes) == len(nodes)


@pytest.mark.parametrize("dcs", [{}, {"other_dc": "4"}])
def test_datacenter_filtering_raises_when_dc_is_missing(dcs: Mapping[str, str]) -> None:
    keyspace = build_keyspace("empty_keyspace").with_network_topology_strategy_dcs(dcs)
    with pytest.raises(ValueError):
        backup_steps._remove_other_datacenters(keyspace, "my_dc")
    assert keyspace.network_topology_strategy_dcs == dcs


@pytest.mark.parametrize("dcs", [{"my_dc": "7"}, {"other_dc": "8", "my_dc": "7"}])
def test_datacenter_filtering_leaves_only_one_dc(dcs: Mapping[str, str]) -> None:
    keyspace = build_keyspace("my_keyspace").with_network_topology_strategy_dcs(dcs)
    backup_steps._remove_other_datacenters(keyspace, "my_dc")
    assert keyspace.network_topology_strategy_dcs == {"my_dc": "7"}
