"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from .node import Node
from .snapshot import SnapshotOp, UploadOp
from .state import node_state, NodeState
from astacus.common.ipc import SnapshotRequest, SnapshotUploadRequest
from fastapi import APIRouter, Depends, HTTPException

import time

router = APIRouter()


@router.post("/lock")
def lock(locker: str, ttl: int, state: NodeState = Depends(node_state)):
    if state.is_locked:
        raise HTTPException(status_code=409, detail="Already locked")
    state.locked = True
    state.locker = locker
    state.locked_until = time.monotonic() + ttl
    return {"locked": True}


@router.post("/relock")
def relock(locker: str, ttl: int, state: NodeState = Depends(node_state)):
    if not state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    if state.locker != locker:
        raise HTTPException(status_code=403, detail="Locked by someone else")
    state.locked_until = time.monotonic() + ttl
    return {"locked": True}


@router.post("/unlock")
def unlock(locker: str, state: NodeState = Depends(node_state)):
    if not state.is_locked:
        raise HTTPException(status_code=409, detail="Already unlocked")
    if state.locker != locker:
        raise HTTPException(status_code=403, detail="Locked by someone else")
    state.locked = False
    return {"locked": False}


@router.post("/snapshot")
def snapshot(req: SnapshotRequest = SnapshotRequest(), n: Node = Depends()):
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    return SnapshotOp(n=n).start(req=req)


@router.get("/snapshot/{op_id}")
def snapshot_result(*, op_id: int, n: Node = Depends()):
    op, _ = n.get_op_and_op_info(op_id=op_id, op_name="snapshot")
    return op.result


@router.post("/upload")
def upload(req: SnapshotUploadRequest, n: Node = Depends()):
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    return UploadOp(n=n).start(req=req)


@router.get("/upload/{op_id}")
def upload_result(*, op_id: int, n: Node = Depends()):
    op, _ = n.get_op_and_op_info(op_id=op_id, op_name="upload")
    return op.result
