"""Copyright (c) 2022 Aiven Ltd
See LICENSE for details

cassandra backup/restore plugin utilities

"""
from astacus.common import ipc
from astacus.common.cassandra.config import SNAPSHOT_NAME
from astacus.common.snapshot import SnapshotGroup
from astacus.coordinator.cluster import Cluster
from astacus.coordinator.config import CoordinatorNode
from astacus.coordinator.plugins.base import StepFailedError
from collections.abc import Sequence
from typing import Optional, Type, TypeVar

NR = TypeVar("NR", bound=ipc.NodeResult)


async def run_subop(
    cluster: Cluster,
    subop: ipc.CassandraSubOp,
    *,
    nodes: Optional[Sequence[CoordinatorNode]] = None,
    req: Optional[ipc.NodeRequest] = None,
    reqs: Optional[Sequence[ipc.NodeRequest]] = None,
    result_class: Type[NR],
) -> Sequence[NR]:
    if not req and not reqs:
        req = ipc.NodeRequest()
    start_results = await cluster.request_from_nodes(
        f"cassandra/{subop.value}",
        method="post",
        caller="Cassandra.run_subop",
        req=req,
        reqs=reqs,
        nodes=nodes,
    )
    if not start_results:
        raise StepFailedError(f"Starting of cassandra subop {subop} failed")
    return await cluster.wait_successful_results(start_results=start_results, result_class=result_class)


async def get_schema_hash(cluster: Cluster, nodes: Optional[Sequence[CoordinatorNode]] = None) -> tuple[str, str]:
    hashes = [
        x.schema_hash
        for x in await run_subop(
            cluster, ipc.CassandraSubOp.get_schema_hash, result_class=ipc.CassandraGetSchemaHashResult, nodes=nodes
        )
    ]
    if not hashes:
        return "", "Unable to retrieve schema hash at all"
    if len(set(hashes)) != 1:
        return "", f"Multiple schema hashes present: {hashes}"
    return hashes[0], ""


def snapshot_groups() -> Sequence[SnapshotGroup]:
    # first *: keyspace name; second *: table name
    return [
        SnapshotGroup(root_glob=f"data/*/*/snapshots/{SNAPSHOT_NAME}/*.db"),
        SnapshotGroup(root_glob=f"data/*/*/snapshots/{SNAPSHOT_NAME}/*.txt"),
        SnapshotGroup(root_glob=f"data/*/*/snapshots/{SNAPSHOT_NAME}/*.crc32"),
    ]


def delta_snapshot_groups() -> Sequence[SnapshotGroup]:
    # first *: keyspace name; second *: table name
    return [
        SnapshotGroup(root_glob="data/*/*/backups/*.db"),
        SnapshotGroup(root_glob="data/*/*/backups/*.txt"),
        SnapshotGroup(root_glob="data/*/*/backups/*.crc32"),
    ]
