"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from .clear import ClearOp
from .download import DownloadOp
from .node import Node, NodeOp
from .snapshot_op import ReleaseOp, SnapshotOp, UploadOp
from .state import node_state
from astacus.common import ipc
from astacus.common.magic import StrEnum
from astacus.common.op import Op
from astacus.common.snapshot import SnapshotGroup
from astacus.node.config import CassandraAccessLevel
from astacus.node.snapshotter import Snapshotter
from astacus.starlette import get_query_param, Router
from astacus.version import __version__
from collections.abc import Sequence
from starlette.background import BackgroundTasks
from starlette.exceptions import HTTPException
from starlette.requests import Request
from typing import TypeAlias

router = Router()

READONLY_SUBOPS = {
    ipc.CassandraSubOp.get_schema_hash,
    ipc.CassandraSubOp.remove_snapshot,
    ipc.CassandraSubOp.take_snapshot,
}


class OpName(StrEnum):
    """(Long-running) operations defined in this API (for node)"""

    cassandra = "cassandra"
    clear = "clear"
    download = "download"
    snapshot = "snapshot"
    upload = "upload"
    release = "release"


def is_allowed(subop: ipc.CassandraSubOp, access_level: CassandraAccessLevel):
    match access_level:
        case CassandraAccessLevel.read:
            return subop in READONLY_SUBOPS
        case CassandraAccessLevel.write:
            return True


@router.get("/metadata")
async def metadata() -> ipc.MetadataResult:
    return ipc.MetadataResult(
        version=__version__,
        features=[feature.value for feature in ipc.NodeFeatures],
    )


@router.post("/lock")
async def lock(request: Request) -> dict:
    locker = get_query_param(request, "locker")
    ttl = int(get_query_param(request, "ttl"))
    state = node_state(request)
    with state.mutate_lock:
        if state.is_locked:
            raise HTTPException(status_code=409, detail="Already locked")
        state.lock(locker=locker, ttl=ttl)
    return {"locked": True}


@router.post("/relock")
async def relock(request: Request) -> dict:
    state = node_state(request)
    locker = get_query_param(request, "locker")
    ttl = int(get_query_param(request, "ttl"))
    with state.mutate_lock:
        if not state.is_locked:
            raise HTTPException(status_code=409, detail="Not locked")
        if state.is_locked != locker:
            raise HTTPException(status_code=403, detail="Locked by someone else")
        state.lock(locker=locker, ttl=ttl)
    return {"locked": True}


@router.post("/unlock")
async def unlock(request: Request) -> dict:
    state = node_state(request)
    locker = get_query_param(request, "locker")
    with state.mutate_lock:
        if not state.is_locked:
            raise HTTPException(status_code=409, detail="Already unlocked")
        if state.is_locked != locker:
            raise HTTPException(status_code=403, detail="Locked by someone else")
        state.unlock()
    return {"locked": False}


def create_op_result_route(route: str, op_name: OpName) -> None:
    @router.get(route + "/{op_id:int}")
    async def result_endpoint(*, request: Request, background_tasks: BackgroundTasks) -> ipc.NodeResult:
        op_id: int = request.path_params["op_id"]
        n = Node.from_request(request, background_tasks)
        op, _ = n.get_op_and_op_info(op_id=op_id, op_name=op_name)
        assert isinstance(op, NodeOp)
        return op.result


@router.post("/snapshot")
async def snapshot(body: ipc.SnapshotRequestV2, request: Request, background_tasks: BackgroundTasks) -> Op.StartResult:
    n = Node.from_request(request, background_tasks)
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    snapshotter = snapshotter_from_snapshot_req(body, n)
    return SnapshotOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=body).start(snapshotter)


create_op_result_route("/snapshot", OpName.snapshot)


@router.post("/delta/snapshot")
async def delta_snapshot(body: ipc.SnapshotRequestV2, request: Request, background_tasks: BackgroundTasks) -> Op.StartResult:
    n = Node.from_request(request, background_tasks)
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    snapshotter = delta_snapshotter_from_snapshot_req(body, n)
    return SnapshotOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=body).start(snapshotter)


create_op_result_route("/delta/snapshot", OpName.snapshot)


@router.post("/upload")
async def upload(
    body: ipc.SnapshotUploadRequestV20221129, request: Request, background_tasks: BackgroundTasks
) -> Op.StartResult:
    n = Node.from_request(request, background_tasks)
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    snapshot_ = n.get_or_create_snapshot()
    return UploadOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=body).start(snapshot_)


create_op_result_route("/upload", OpName.upload)


@router.post("/delta/upload")
async def delta_upload(
    body: ipc.SnapshotUploadRequestV20221129, request: Request, background_tasks: BackgroundTasks
) -> Op.StartResult:
    n = Node.from_request(request, background_tasks)
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    snapshot_ = n.get_or_create_delta_snapshot()
    return UploadOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=body).start(snapshot_)


create_op_result_route("/delta/upload", OpName.upload)


@router.post("/release")
async def release(body: ipc.SnapshotReleaseRequest, request: Request, background_tasks: BackgroundTasks) -> Op.StartResult:
    n = Node.from_request(request, background_tasks)
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    # Groups not needed here.
    snapshotter = n.get_snapshotter(groups=[])
    assert snapshotter
    return ReleaseOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=body).start(snapshotter)


create_op_result_route("/release", OpName.release)


@router.post("/download")
async def download(body: ipc.SnapshotDownloadRequest, request: Request, background_tasks: BackgroundTasks) -> Op.StartResult:
    n = Node.from_request(request, background_tasks)
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    snapshotter = snapshotter_from_snapshot_req(body, n)
    return DownloadOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=body).start(snapshotter)


create_op_result_route("/download", OpName.download)


@router.post("/delta/download")
async def delta_download(
    body: ipc.SnapshotDownloadRequest, request: Request, background_tasks: BackgroundTasks
) -> Op.StartResult:
    n = Node.from_request(request, background_tasks)
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    snapshotter = delta_snapshotter_from_snapshot_req(body, n)
    return DownloadOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=body).start(snapshotter)


create_op_result_route("/delta/download", OpName.download)


@router.post("/clear")
async def clear(body: ipc.SnapshotClearRequest, request: Request, background_tasks: BackgroundTasks) -> Op.StartResult:
    n = Node.from_request(request, background_tasks)
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    snapshotter = snapshotter_from_snapshot_req(body, n)
    return ClearOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=body).start(snapshotter, is_snapshot_outdated=True)


create_op_result_route("/clear", OpName.clear)


@router.post("/delta/clear")
async def delta_clear(body: ipc.SnapshotClearRequest, request: Request, background_tasks: BackgroundTasks) -> Op.StartResult:
    n = Node.from_request(request, background_tasks)
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    snapshotter = delta_snapshotter_from_snapshot_req(body, n)
    return ClearOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=body).start(snapshotter, is_snapshot_outdated=False)


create_op_result_route("/delta/clear", OpName.clear)


@router.post("/cassandra/start-cassandra")
async def cassandra_start_cassandra(
    body: ipc.CassandraStartRequest, request: Request, background_tasks: BackgroundTasks
) -> Op.StartResult:
    # pylint: disable=import-outside-toplevel
    # pylint: disable=raise-missing-from
    n = Node.from_request(request, background_tasks)
    try:
        from .cassandra import CassandraStartOp
    except ImportError:
        raise HTTPException(status_code=501, detail="Cassandra support is not installed")
    check_can_do_cassandra_subop(n, ipc.CassandraSubOp.start_cassandra)
    return CassandraStartOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=body).start()


@router.post("/cassandra/restore-sstables")
async def cassandra_restore_sstables(
    body: ipc.CassandraRestoreSSTablesRequest, request: Request, background_tasks: BackgroundTasks
) -> Op.StartResult:
    # pylint: disable=import-outside-toplevel
    # pylint: disable=raise-missing-from
    n = Node.from_request(request, background_tasks)
    try:
        from .cassandra import CassandraRestoreSSTablesOp
    except ImportError:
        raise HTTPException(status_code=501, detail="Cassandra support is not installed")
    check_can_do_cassandra_subop(n, ipc.CassandraSubOp.restore_sstables)
    return CassandraRestoreSSTablesOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=body).start()


@router.post("/cassandra/{subop:str}")
async def cassandra(body: ipc.NodeRequest, request: Request, background_tasks: BackgroundTasks) -> Op.StartResult:
    n = Node.from_request(request, background_tasks)
    subop = ipc.CassandraSubOp(request.path_params["subop"])

    # pylint: disable=import-outside-toplevel
    # pylint: disable=raise-missing-from
    try:
        from .cassandra import CassandraGetSchemaHashOp, SimpleCassandraSubOp
    except ImportError:
        raise HTTPException(status_code=501, detail="Cassandra support is not installed")
    check_can_do_cassandra_subop(n, subop)
    if subop == ipc.CassandraSubOp.get_schema_hash:
        return CassandraGetSchemaHashOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=body).start()
    return SimpleCassandraSubOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=body).start(subop=subop)


def check_can_do_cassandra_subop(n: Node, subop: ipc.CassandraSubOp) -> None:
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    if not n.config.cassandra:
        raise HTTPException(status_code=409, detail="Cassandra node configuration not found")
    if not is_allowed(subop, n.config.cassandra.access_level):
        raise HTTPException(
            status_code=403,
            detail=f"Cassandra subop {subop} is not allowed on access level {n.config.cassandra.access_level}",
        )


@router.get("/cassandra/{subop:str}/{op_id:int}")
async def cassandra_result(request: Request, background_tasks: BackgroundTasks) -> ipc.NodeResult:
    n = Node.from_request(request, background_tasks)
    op_id = request.path_params["op_id"]
    op, _ = n.get_op_and_op_info(op_id=op_id, op_name=OpName.cassandra)
    assert isinstance(op, NodeOp)
    return op.result


SnapshotReq: TypeAlias = ipc.SnapshotRequestV2 | ipc.SnapshotDownloadRequest | ipc.SnapshotClearRequest


def groups_from_snapshot_req(req: SnapshotReq) -> Sequence[SnapshotGroup]:
    # We merge the list of groups and simple root_globs
    # to handle backward compatibility if the controller is older than the nodes.
    groups = [SnapshotGroup(root_glob=root_glob) for root_glob in req.root_globs]

    if isinstance(req, ipc.SnapshotRequestV2):
        groups += [
            SnapshotGroup(
                root_glob=group.root_glob,
                excluded_names=group.excluded_names,
                embedded_file_size_max=group.embedded_file_size_max,
            )
            for group in req.groups
            if not any(existing_group.root_glob == group.root_glob for existing_group in groups)
        ]
    return groups


def snapshotter_from_snapshot_req(req: SnapshotReq, n: Node) -> Snapshotter:
    groups = groups_from_snapshot_req(req)
    return n.get_snapshotter(groups)


def delta_snapshotter_from_snapshot_req(req: SnapshotReq, n: Node) -> Snapshotter:
    groups = groups_from_snapshot_req(req)
    return n.get_delta_snapshotter(groups)
