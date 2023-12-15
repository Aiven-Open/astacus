"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Some tests of the m3 placement rewriter in common

"""
from astacus.common import m3placement
from astacus.proto import m3_placement_pb2

# What's in the (recorded, historic) placement plan
src_pnode = m3placement.M3PlacementNode(
    node_id="node-id1",
    endpoint="endpoint1",
    hostname="hostname1",
)
# What we want to replace it with
dst_pnode = m3placement.M3PlacementNode(
    node_id="node-id22",
    endpoint="endpoint22",
    hostname="hostname22",
)


def create_dummy_placement() -> m3_placement_pb2.Placement:
    placement = m3_placement_pb2.Placement()
    instance = placement.instances["node-id1"]
    instance.id = "node-id1"
    instance.endpoint = "endpoint1"
    instance.hostname = "hostname1"
    return placement


def test_rewrite_single_m3_placement_node() -> None:
    placement = create_dummy_placement()
    m3placement.rewrite_single_m3_placement_node(
        placement, src_pnode=src_pnode, dst_pnode=dst_pnode, dst_isolation_group="az22"
    )
    instance2 = placement.instances["node-id22"]
    assert instance2.endpoint == "endpoint22"
    assert instance2.hostname == "hostname22"
    assert instance2.isolation_group == "az22"


def test_rewrite_m3_placement_bytes() -> None:
    value = create_dummy_placement().SerializeToString()
    assert isinstance(value, bytes)
    expected_bytes = [b"endpoint22", b"hostname22", b"az22"]
    for expected in expected_bytes:
        assert expected not in value
    replacements = [
        m3placement.M3PlacementNodeReplacement(src_pnode=src_pnode, dst_pnode=dst_pnode, dst_isolation_group="az22")
    ]
    value = m3placement.rewrite_m3_placement_bytes(value, replacements)
    for expected in expected_bytes:
        assert expected in value
