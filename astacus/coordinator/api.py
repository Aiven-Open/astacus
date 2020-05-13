"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from .coordinator import Coordinator
from .lockops import LockOps
from enum import Enum
from fastapi import APIRouter, Depends, HTTPException
from urllib.parse import urljoin

router = APIRouter()


class ErrorCode(str, Enum):
    operation_id_mismatch = "operation_id_mismatch"


@router.post("/lock")
async def lock(*, locker: str, ttl: int = 60, c: Coordinator = Depends()):
    op = LockOps(c=c, ttl=ttl, locker=locker)
    result = c.start_op(op=op, fun=op.lock)
    result["unlock-url"] = urljoin(str(c.request.url),
                                   f"../unlock?locker={locker}")
    return result


@router.get("/unlock")
def unlock(*, locker: str, c: Coordinator = Depends()):
    op = LockOps(c=c, locker=locker)
    return c.start_op(op=op, fun=op.unlock)


@router.get("/status/{op}")
def status(*, op: int, c: Coordinator = Depends()):
    if op != c.state.op:
        raise HTTPException(
            404, {
                "code": ErrorCode.operation_id_mismatch,
                "op": op,
                "message": "Unknown operation id"
            })
    return {"state": c.state.op_state}
