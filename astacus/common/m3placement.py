"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

M3 placement handling code

Note that this is used only by coordinator, but it may also be
desirable to use it outside and due to that it is stand-alone module,
as opposed to part of astacus.coordinator.plugins.m3db.

"""

from astacus.common.utils import AstacusModel
from astacus.proto import m3_placement_pb2
from collections.abc import Sequence
from pydantic.v1 import validator

MAXIMUM_PROTOBUF_STR_LENGTH = 127


def non_empty_and_sane_length(value):
    if len(value) == 0:
        raise ValueError("Empty value is not supported")
    if len(value) > MAXIMUM_PROTOBUF_STR_LENGTH:
        raise ValueError("Large protobuf fields are not supported")
    return value


class M3PlacementNode(AstacusModel):
    # In Aiven-internal case, most of these are redundant fields (we
    # could derive node_id and hostname from endpoint); however, for
    # generic case, we configure all of them (and expect them to be
    # configured).

    node_id: str
    _validate_node_id = validator("node_id", allow_reuse=True)(non_empty_and_sane_length)

    endpoint: str
    _validate_endpoint = validator("endpoint", allow_reuse=True)(non_empty_and_sane_length)

    hostname: str
    _validate_hostname = validator("hostname", allow_reuse=True)(non_empty_and_sane_length)

    # isolation_group: str # redundant - it is available from generic node snapshot result as az
    # zone/weight: assumed to stay same


class M3PlacementNodeReplacement(AstacusModel):
    src_pnode: M3PlacementNode
    dst_pnode: M3PlacementNode
    dst_isolation_group: str = ""


def rewrite_single_m3_placement_node(
    placement, *, src_pnode: M3PlacementNode, dst_pnode: M3PlacementNode, dst_isolation_group=""
):
    """rewrite single m3 placement entry in-place in protobuf

    Relevant places ( see m3db
    src/cluster/generated/proto/placementpb/placement.proto which is
    copied to astacus/proto/m3_placement.proto )) :

    instance<str,Instance> map = 1, and then

    message Instance {
      string id                 = 1;
      string isolation_group    = 2;
      string endpoint           = 5;
      string hostname           = 8;
    }

    """
    instance = placement.instances[src_pnode.node_id]
    # az may or may not be set; if not, keep original
    if dst_isolation_group:
        instance.isolation_group = dst_isolation_group
    instance.endpoint = dst_pnode.endpoint
    instance.hostname = dst_pnode.hostname

    # Handle (potential) id change (bit painful as it is also map key)
    instance.id = dst_pnode.node_id
    del placement.instances[src_pnode.node_id]
    placement.instances[dst_pnode.node_id].CopyFrom(instance)


def rewrite_m3_placement_bytes(value: bytes, replacements: Sequence[M3PlacementNodeReplacement]):
    """rewrite whole binary m3 placement, with the set of node replacements"""
    placement = m3_placement_pb2.Placement()
    placement.ParseFromString(value)
    for replacement in replacements:
        rewrite_single_m3_placement_node(
            placement,
            src_pnode=replacement.src_pnode,
            dst_isolation_group=replacement.dst_isolation_group,
            dst_pnode=replacement.dst_pnode,
        )
    return placement.SerializeToString()
