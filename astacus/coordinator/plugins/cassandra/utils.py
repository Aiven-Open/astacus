"""Copyright (c) 2022 Aiven Ltd
See LICENSE for details

cassandra backup/restore plugin utilities

"""


from astacus.common import ipc
from astacus.coordinator.cluster import Cluster
from astacus.coordinator.config import CoordinatorNode
from astacus.coordinator.plugins.base import StepFailedError
from typing import List, Optional, Tuple, Type, TypeVar

NR = TypeVar("NR", bound=ipc.NodeResult)


async def run_subop(
    cluster: Cluster,
    subop: ipc.CassandraSubOp,
    *,
    nodes: Optional[List[CoordinatorNode]] = None,
    req: Optional[ipc.NodeRequest] = None,
    reqs: Optional[List[ipc.NodeRequest]] = None,
    result_class: Type[NR],
) -> List[NR]:
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


async def get_schema_hash(cluster: Cluster) -> Tuple[str, str]:
    hashes = [
        x.schema_hash
        for x in await run_subop(cluster, ipc.CassandraSubOp.get_schema_hash, result_class=ipc.CassandraGetSchemaHashResult)
    ]
    if not hashes:
        return "", "Unable to retrieve schema hash at all"
    if len(set(hashes)) != 1:
        return "", f"Multiple schema hashes present: {hashes}"
    return hashes[0], ""
