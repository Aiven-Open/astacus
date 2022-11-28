"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from .clear import ClearOp
from .download import DownloadOp
from .node import Node
from .snapshot import SnapshotOp, UploadOp
from .state import node_state, NodeState
from astacus.common import ipc
from astacus.version import __version__
from enum import Enum
from fastapi import APIRouter, Depends, HTTPException
from typing import Union

router = APIRouter()


class OpName(str, Enum):
    """(Long-running) operations defined in this API (for node)"""

    cassandra = "cassandra"
    clear = "clear"
    download = "download"
    snapshot = "snapshot"
    upload = "upload"


class Features(Enum):
    pass


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
def snapshot(req: ipc.SnapshotRequest, n: Node = Depends()):
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    return SnapshotOp(n=n, op_id=n.allocate_op_id(), stats=n.stats).start(req=req)


@router.get("/snapshot/{op_id}")
def snapshot_result(*, op_id: int, n: Node = Depends()):
    op, _ = n.get_op_and_op_info(op_id=op_id, op_name=OpName.snapshot)
    return op.result


@router.post("/upload")
def upload(req: ipc.SnapshotUploadRequest, n: Node = Depends()):
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    return UploadOp(n=n, op_id=n.allocate_op_id(), stats=n.stats).start(req=req)


@router.get("/upload/{op_id}")
def upload_result(*, op_id: int, n: Node = Depends()):
    op, _ = n.get_op_and_op_info(op_id=op_id, op_name=OpName.upload)
    return op.result


@router.post("/download")
def download(req: ipc.SnapshotDownloadRequest, n: Node = Depends()):
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    return DownloadOp(n=n, op_id=n.allocate_op_id(), stats=n.stats).start(req=req)


@router.get("/download/{op_id}")
def download_result(*, op_id: int, n: Node = Depends()):
    op, _ = n.get_op_and_op_info(op_id=op_id, op_name=OpName.download)
    return op.result


@router.post("/clear")
def clear(req: ipc.SnapshotClearRequest, n: Node = Depends()):
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    return ClearOp(n=n, op_id=n.allocate_op_id(), stats=n.stats).start(req=req)


@router.get("/clear/{op_id}")
def clear_result(*, op_id: int, n: Node = Depends()):
    op, _ = n.get_op_and_op_info(op_id=op_id, op_name=OpName.clear)
    return op.result


@router.post("/cassandra/{subop}")
def cassandra(req: Union[ipc.NodeRequest, ipc.CassandraStartRequest], subop: ipc.CassandraSubOp, n: Node = Depends()):
    # pylint: disable=import-outside-toplevel
    # pylint: disable=raise-missing-from
    try:
        from .cassandra import CassandraGetSchemaHashOp, CassandraStartOp, SimpleCassandraSubOp
    except ImportError:
        raise HTTPException(status_code=501, detail="Cassandra support is not installed")
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")

    if subop == ipc.CassandraSubOp.get_schema_hash:
        return CassandraGetSchemaHashOp(n=n, op_id=n.allocate_op_id(), stats=n.stats).start(req=req)

    if subop == ipc.CassandraSubOp.start_cassandra:
        assert isinstance(req, ipc.CassandraStartRequest)
        return CassandraStartOp(n=n, op_id=n.allocate_op_id(), stats=n.stats).start(req=req)

    return SimpleCassandraSubOp(n=n, op_id=n.allocate_op_id(), stats=n.stats).start(req=req, subop=subop)


@router.get("/cassandra/{subop}/{op_id}")
def cassandra_result(*, subop: ipc.CassandraSubOp, op_id: int, n: Node = Depends()):
    op, _ = n.get_op_and_op_info(op_id=op_id, op_name=OpName.cassandra)
    return op.result
