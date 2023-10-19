"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from .clear import ClearOp
from .download import DownloadOp
from .node import Node
from .snapshot import ReleaseOp, SnapshotOp, UploadOp
from .state import node_state, NodeState
from astacus.common import ipc
from astacus.common.magic import StrEnum
from astacus.common.snapshot import SnapshotGroup
from astacus.node.config import CassandraAccessLevel
from astacus.node.snapshotter import Snapshotter
from astacus.version import __version__
from enum import Enum
from fastapi import APIRouter, Depends, HTTPException
from typing import Sequence, Union

router = APIRouter()

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


class Features(Enum):
    # Added on 2022-11-29, this can be assumed to be supported everywhere after 1 or 2 years
    validate_file_hashes = "validate_file_hashes"
    # Added on 2023-06-07
    snapshot_groups = "snapshot_groups"
    # Added on 2023-10-16
    release_snapshot_files = "release_snapshot_files"


def is_allowed(subop: ipc.CassandraSubOp, access_level: CassandraAccessLevel):
    match access_level:
        case CassandraAccessLevel.read:
            return subop in READONLY_SUBOPS
        case CassandraAccessLevel.write:
            return True


@router.get("/metadata")
def metadata() -> ipc.MetadataResult:
    return ipc.MetadataResult(
        version=__version__,
        features=[feature.value for feature in Features],
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
def snapshot(req: ipc.SnapshotRequestV2, n: Node = Depends()):
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    snapshotter = snapshotter_from_snapshot_req(req, n)
    return SnapshotOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=req).start(snapshotter)


@router.get("/snapshot/{op_id}")
def snapshot_result(*, op_id: int, n: Node = Depends()):
    op, _ = n.get_op_and_op_info(op_id=op_id, op_name=OpName.snapshot)
    return op.result


@router.post("/delta/snapshot")
def delta_snapshot(req: ipc.SnapshotRequestV2, n: Node = Depends()):
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    snapshotter = delta_snapshotter_from_snapshot_req(req, n)
    return SnapshotOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=req).start(snapshotter)


@router.get("/delta/snapshot/{op_id}")
def delta_snapshot_result(*, op_id: int, n: Node = Depends()):
    op, _ = n.get_op_and_op_info(op_id=op_id, op_name=OpName.snapshot)
    return op.result


@router.post("/upload")
def upload(req: ipc.SnapshotUploadRequestV20221129, n: Node = Depends()):
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    snapshotter = n.get_snapshotter()
    assert snapshotter
    return UploadOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=req).start(snapshotter)


@router.get("/upload/{op_id}")
def upload_result(*, op_id: int, n: Node = Depends()):
    op, _ = n.get_op_and_op_info(op_id=op_id, op_name=OpName.upload)
    return op.result


@router.post("/delta/upload")
def delta_upload(req: ipc.SnapshotUploadRequestV20221129, n: Node = Depends()):
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    snapshotter = n.get_delta_snapshotter()
    assert snapshotter
    return UploadOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=req).start(snapshotter)


@router.get("/delta/upload/{op_id}")
def delta_upload_result(*, op_id: int, n: Node = Depends()):
    op, _ = n.get_op_and_op_info(op_id=op_id, op_name=OpName.upload)
    return op.result


@router.post("/release")
def release(req: ipc.SnapshotReleaseRequest, n: Node = Depends()):
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    snapshotter = n.get_snapshotter()
    assert snapshotter
    return ReleaseOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=req).start(snapshotter)


@router.get("/release/{op_id}")
def release_result(*, op_id: int, n: Node = Depends()):
    op, _ = n.get_op_and_op_info(op_id=op_id, op_name=OpName.release)
    return op.result


@router.post("/download")
def download(req: ipc.SnapshotDownloadRequest, n: Node = Depends()):
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    snapshotter = snapshotter_from_snapshot_req(req, n)
    return DownloadOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=req).start(snapshotter)


@router.get("/download/{op_id}")
def download_result(*, op_id: int, n: Node = Depends()):
    op, _ = n.get_op_and_op_info(op_id=op_id, op_name=OpName.download)
    return op.result


@router.post("/clear")
def clear(req: ipc.SnapshotClearRequest, n: Node = Depends()):
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    snapshotter = snapshotter_from_snapshot_req(req, n)
    return ClearOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=req).start(snapshotter, is_snapshot_outdated=True)


@router.get("/clear/{op_id}")
def clear_result(*, op_id: int, n: Node = Depends()):
    op, _ = n.get_op_and_op_info(op_id=op_id, op_name=OpName.clear)
    return op.result


@router.post("/delta/clear")
def delta_clear(req: ipc.SnapshotClearRequest, n: Node = Depends()):
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    snapshotter = delta_snapshotter_from_snapshot_req(req, n)
    return ClearOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=req).start(snapshotter, is_snapshot_outdated=False)


@router.get("/delta/clear/{op_id}")
def delta_clear_result(*, op_id: int, n: Node = Depends()):
    op, _ = n.get_op_and_op_info(op_id=op_id, op_name=OpName.clear)
    return op.result


@router.post("/cassandra/{subop}")
def cassandra(
    req: Union[ipc.NodeRequest, ipc.CassandraStartRequest, ipc.CassandraRestoreSSTablesRequest],
    subop: ipc.CassandraSubOp,
    n: Node = Depends(),
):
    # pylint: disable=import-outside-toplevel
    # pylint: disable=raise-missing-from
    try:
        from .cassandra import CassandraGetSchemaHashOp, CassandraRestoreSSTablesOp, CassandraStartOp, SimpleCassandraSubOp
    except ImportError:
        raise HTTPException(status_code=501, detail="Cassandra support is not installed")
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    if not n.config.cassandra:
        raise HTTPException(status_code=409, detail="Cassandra node configuration not found")
    if not is_allowed(subop, n.config.cassandra.access_level):
        raise HTTPException(
            status_code=403,
            detail=f"Cassandra subop {subop} is not allowed on access level {n.config.cassandra.access_level}",
        )

    if subop == ipc.CassandraSubOp.get_schema_hash:
        return CassandraGetSchemaHashOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=req).start()

    if subop == ipc.CassandraSubOp.start_cassandra:
        assert isinstance(req, ipc.CassandraStartRequest)
        return CassandraStartOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=req).start()

    if subop == ipc.CassandraSubOp.restore_sstables:
        assert isinstance(req, ipc.CassandraRestoreSSTablesRequest)
        return CassandraRestoreSSTablesOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=req).start()

    return SimpleCassandraSubOp(n=n, op_id=n.allocate_op_id(), stats=n.stats, req=req).start(subop=subop)


@router.get("/cassandra/{subop}/{op_id}")
def cassandra_result(*, subop: ipc.CassandraSubOp, op_id: int, n: Node = Depends()):
    op, _ = n.get_op_and_op_info(op_id=op_id, op_name=OpName.cassandra)
    return op.result


SnapshotReq = Union[ipc.SnapshotRequestV2, ipc.SnapshotDownloadRequest, ipc.SnapshotClearRequest]


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
    return n.get_or_create_snapshotter(groups)


def delta_snapshotter_from_snapshot_req(req: SnapshotReq, n: Node) -> Snapshotter:
    groups = groups_from_snapshot_req(req)
    return n.get_or_create_delta_snapshotter(groups)
