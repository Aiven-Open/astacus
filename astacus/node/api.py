"""Copyright (c) 2020 Aiven Ltd
See LICENSE for details.
"""

from .clear import ClearOp
from .download import DownloadOp
from .node import Node
from .snapshot_op import ReleaseOp, SnapshotOp, UploadOp
from .state import node_state, NodeState
from astacus.common import ipc
from astacus.common.magic import StrEnum
from astacus.common.msgspec_glue import register_msgspec_glue, StructResponse
from astacus.common.snapshot import SnapshotGroup
from astacus.node.config import CassandraAccessLevel
from astacus.node.snapshotter import Snapshotter
from astacus.version import __version__
from collections.abc import Sequence
from fastapi import APIRouter, Body, Depends, HTTPException
from typing import Annotated, TypeAlias

register_msgspec_glue()
router = APIRouter()

READONLY_SUBOPS = {
    ipc.CassandraSubOp.get_schema_hash,
    ipc.CassandraSubOp.remove_snapshot,
    ipc.CassandraSubOp.take_snapshot,
}


class OpName(StrEnum):
    """(Long-running) operations defined in this API (for node)."""

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
def metadata() -> StructResponse:
    return StructResponse(
        ipc.MetadataResult(
            version=__version__,
            features=[feature.value for feature in ipc.NodeFeatures],
        )
    )


@router.post("/lock")
def lock(locker: str, ttl: int, state: NodeState = Depends(node_state)):
    with state.mutate_lock:
        if state.is_locked:
            raise HTTPException(status_code=409, detail="Already locked")
        state.lock(locker=locker, ttl=ttl)
    return {"locked": True}


@router.post("/relock")
def relock(locker: str, ttl: int, state: NodeState = Depends(node_state)):
    with state.mutate_lock:
        if not state.is_locked:
            raise HTTPException(status_code=409, detail="Not locked")
        if state.is_locked != locker:
            raise HTTPException(status_code=403, detail="Locked by someone else")
        state.lock(locker=locker, ttl=ttl)
    return {"locked": True}


@router.post("/unlock")
def unlock(locker: str, state: NodeState = Depends(node_state)):
    with state.mutate_lock:
        if not state.is_locked:
            raise HTTPException(status_code=409, detail="Already unlocked")
        if state.is_locked != locker:
            raise HTTPException(status_code=403, detail="Locked by someone else")
        state.unlock()
    return {"locked": False}


@router.post("/snapshot")
def snapshot(
    groups: Annotated[Sequence[ipc.SnapshotRequestGroup], Body()],
    result_url: Annotated[str, Body()] = "",
    # Accept V1 request for backward compatibility if the controller is older
    # root_globs: Annotated[Sequence[str], Body()],
    n: Node = Depends(),
):
    req = ipc.SnapshotRequestV2(
        result_url=result_url,
        groups=groups,
    )
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    snapshotter = snapshotter_from_snapshot_req(req, n)
    return SnapshotOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=req).start(snapshotter)


@router.get("/snapshot/{op_id}")
def snapshot_result(*, op_id: int, n: Node = Depends()) -> StructResponse:
    op, _ = n.get_op_and_op_info(op_id=op_id, op_name=OpName.snapshot)
    return StructResponse(op.result)


@router.post("/delta/snapshot")
def delta_snapshot(
    groups: Annotated[Sequence[ipc.SnapshotRequestGroup], Body()],
    result_url: Annotated[str, Body()] = "",
    n: Node = Depends(),
):
    req = ipc.SnapshotRequestV2(
        result_url=result_url,
        groups=groups,
    )
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    snapshotter = delta_snapshotter_from_snapshot_req(req, n)
    return SnapshotOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=req).start(snapshotter)


@router.get("/delta/snapshot/{op_id}")
def delta_snapshot_result(*, op_id: int, n: Node = Depends()) -> StructResponse:
    op, _ = n.get_op_and_op_info(op_id=op_id, op_name=OpName.snapshot)
    return StructResponse(op.result)


@router.post("/upload")
def upload(
    hashes: Annotated[Sequence[ipc.SnapshotHash], Body()],
    storage: Annotated[str, Body()],
    validate_file_hashes: Annotated[bool, Body()] = True,
    result_url: Annotated[str, Body()] = "",
    n: Node = Depends(),
):
    req = ipc.SnapshotUploadRequestV20221129(
        result_url=result_url,
        hashes=hashes,
        storage=storage,
        validate_file_hashes=validate_file_hashes,
    )
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    snapshot_ = n.get_or_create_snapshot()
    return UploadOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=req).start(snapshot_)


@router.get("/upload/{op_id}")
def upload_result(*, op_id: int, n: Node = Depends()) -> StructResponse:
    op, _ = n.get_op_and_op_info(op_id=op_id, op_name=OpName.upload)
    return StructResponse(op.result)


@router.post("/delta/upload")
def delta_upload(
    hashes: Annotated[Sequence[ipc.SnapshotHash], Body()],
    storage: Annotated[str, Body()],
    validate_file_hashes: Annotated[bool, Body()] = True,
    result_url: Annotated[str, Body()] = "",
    n: Node = Depends(),
):
    req = ipc.SnapshotUploadRequestV20221129(
        result_url=result_url,
        hashes=hashes,
        storage=storage,
        validate_file_hashes=validate_file_hashes,
    )
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    snapshot_ = n.get_or_create_delta_snapshot()
    return UploadOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=req).start(snapshot_)


@router.get("/delta/upload/{op_id}")
def delta_upload_result(*, op_id: int, n: Node = Depends()) -> StructResponse:
    op, _ = n.get_op_and_op_info(op_id=op_id, op_name=OpName.upload)
    return StructResponse(op.result)


@router.post("/release")
def release(
    hexdigests: Annotated[Sequence[str], Body()],
    result_url: Annotated[str, Body()] = "",
    n: Node = Depends(),
):
    req = ipc.SnapshotReleaseRequest(
        result_url=result_url,
        hexdigests=hexdigests,
    )
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    # Groups not needed here.
    snapshotter = n.get_snapshotter(groups=[])
    assert snapshotter
    return ReleaseOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=req).start(snapshotter)


@router.get("/release/{op_id}")
def release_result(*, op_id: int, n: Node = Depends()) -> StructResponse:
    op, _ = n.get_op_and_op_info(op_id=op_id, op_name=OpName.release)
    return StructResponse(op.result)


@router.post("/download")
def download(
    storage: Annotated[str, Body()],
    backup_name: Annotated[str, Body()],
    snapshot_index: Annotated[int, Body()],
    root_globs: Annotated[Sequence[str], Body()],
    result_url: Annotated[str, Body()] = "",
    n: Node = Depends(),
):
    req = ipc.SnapshotDownloadRequest(
        result_url=result_url,
        storage=storage,
        backup_name=backup_name,
        snapshot_index=snapshot_index,
        root_globs=root_globs,
    )
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    snapshotter = snapshotter_from_snapshot_req(req, n)
    return DownloadOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=req).start(snapshotter)


@router.get("/download/{op_id}")
def download_result(*, op_id: int, n: Node = Depends()) -> StructResponse:
    op, _ = n.get_op_and_op_info(op_id=op_id, op_name=OpName.download)
    return StructResponse(op.result)


@router.post("/delta/download")
def delta_download(
    storage: Annotated[str, Body()],
    backup_name: Annotated[str, Body()],
    snapshot_index: Annotated[int, Body()],
    root_globs: Annotated[Sequence[str], Body()],
    result_url: Annotated[str, Body()] = "",
    n: Node = Depends(),
):
    req = ipc.SnapshotDownloadRequest(
        result_url=result_url,
        storage=storage,
        backup_name=backup_name,
        snapshot_index=snapshot_index,
        root_globs=root_globs,
    )
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    snapshotter = delta_snapshotter_from_snapshot_req(req, n)
    return DownloadOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=req).start(snapshotter)


@router.get("/delta/download/{op_id}")
def delta_download_result(*, op_id: int, n: Node = Depends()) -> StructResponse:
    op, _ = n.get_op_and_op_info(op_id=op_id, op_name=OpName.download)
    return StructResponse(op.result)


@router.post("/clear")
def clear(root_globs: Annotated[Sequence[str], Body()], result_url: Annotated[str, Body()] = "", n: Node = Depends()):
    req = ipc.SnapshotClearRequest(result_url=result_url, root_globs=root_globs)
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    snapshotter = snapshotter_from_snapshot_req(req, n)
    return ClearOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=req).start(snapshotter, is_snapshot_outdated=True)


@router.get("/clear/{op_id}")
def clear_result(*, op_id: int, n: Node = Depends()) -> StructResponse:
    op, _ = n.get_op_and_op_info(op_id=op_id, op_name=OpName.clear)
    return StructResponse(op.result)


@router.post("/delta/clear")
def delta_clear(root_globs: Annotated[Sequence[str], Body()], result_url: Annotated[str, Body()] = "", n: Node = Depends()):
    req = ipc.SnapshotClearRequest(result_url=result_url, root_globs=root_globs)
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    snapshotter = delta_snapshotter_from_snapshot_req(req, n)
    return ClearOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=req).start(snapshotter, is_snapshot_outdated=False)


@router.get("/delta/clear/{op_id}")
def delta_clear_result(*, op_id: int, n: Node = Depends()) -> StructResponse:
    op, _ = n.get_op_and_op_info(op_id=op_id, op_name=OpName.clear)
    return StructResponse(op.result)


@router.post("/cassandra/start-cassandra")
def cassandra_start_cassandra(
    tokens: Annotated[Sequence[str] | None, Body()] = None,
    replace_address_first_boot: Annotated[str | None, Body()] = None,
    skip_bootstrap_streaming: Annotated[bool | None, Body()] = None,
    result_url: Annotated[str, Body()] = "",
    n: Node = Depends(),
):
    req = ipc.CassandraStartRequest(
        result_url=result_url,
        tokens=tokens,
        replace_address_first_boot=replace_address_first_boot,
        skip_bootstrap_streaming=skip_bootstrap_streaming,
    )

    try:
        from .cassandra import CassandraStartOp
    except ImportError:
        raise HTTPException(status_code=501, detail="Cassandra support is not installed")
    check_can_do_cassandra_subop(n, ipc.CassandraSubOp.start_cassandra)
    return CassandraStartOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=req).start()


@router.post("/cassandra/restore-sstables")
def cassandra_restore_sstables(
    table_glob: Annotated[str, Body()],
    keyspaces_to_skip: Annotated[Sequence[str], Body()],
    match_tables_by: Annotated[ipc.CassandraTableMatching, Body()],
    expect_empty_target: Annotated[bool, Body()],
    result_url: Annotated[str, Body()] = "",
    n: Node = Depends(),
):
    req = ipc.CassandraRestoreSSTablesRequest(
        result_url=result_url,
        table_glob=table_glob,
        keyspaces_to_skip=keyspaces_to_skip,
        match_tables_by=match_tables_by,
        expect_empty_target=expect_empty_target,
    )

    try:
        from .cassandra import CassandraRestoreSSTablesOp
    except ImportError:
        raise HTTPException(status_code=501, detail="Cassandra support is not installed")
    check_can_do_cassandra_subop(n, ipc.CassandraSubOp.restore_sstables)
    return CassandraRestoreSSTablesOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=req).start()


@router.post("/cassandra/{subop}")
def cassandra(subop: ipc.CassandraSubOp, result_url: Annotated[str, Body(embed=True)] = "", n: Node = Depends()):
    req = ipc.NodeRequest(result_url=result_url)

    try:
        from .cassandra import CassandraGetSchemaHashOp, SimpleCassandraSubOp
    except ImportError:
        raise HTTPException(status_code=501, detail="Cassandra support is not installed")
    check_can_do_cassandra_subop(n, subop)
    if subop == ipc.CassandraSubOp.get_schema_hash:
        return CassandraGetSchemaHashOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=req).start()
    return SimpleCassandraSubOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=req).start(subop=subop)


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


@router.get("/cassandra/{subop}/{op_id}")
def cassandra_result(*, subop: ipc.CassandraSubOp, op_id: int, n: Node = Depends()) -> StructResponse:
    op, _ = n.get_op_and_op_info(op_id=op_id, op_name=OpName.cassandra)
    return StructResponse(op.result)


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
