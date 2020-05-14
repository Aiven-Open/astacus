"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from .coordinator import Coordinator
from .lockops import LockOps
from enum import Enum
from fastapi import APIRouter, Depends, HTTPException
from urllib.parse import urljoin

import logging

router = APIRouter()

logger = logging.getLogger(__name__)


class ErrorCode(str, Enum):
    operation_id_mismatch = "operation_id_mismatch"


class OpName(str, Enum):
    lock = "lock"
    unlock = "unlock"


@router.post("/lock")
async def lock(*, locker: str, ttl: int = 60, c: Coordinator = Depends()):
    op = LockOps(c=c, ttl=ttl, locker=locker)
    result = c.start_op(op_name="lock", op=op, fun=op.lock)
    result["unlock-url"] = urljoin(str(c.request.url),
                                   f"../unlock?locker={locker}")
    return result


@router.post("/unlock")
def unlock(*, locker: str, c: Coordinator = Depends()):
    op = LockOps(c=c, locker=locker)
    return c.start_op(op_name="unlock", op=op, fun=op.unlock)


@router.get("/{op_name}/{op_id}")
def status(*, op_name: OpName, op_id: int, c: Coordinator = Depends()):
    op_info = c.state.op_info
    if op_id != op_info.op_id or op_name != op_info.op_name:
        logger.info("status for nonexistent %s.%s != %r", op_name, op_id,
                    op_info)
        raise HTTPException(
            404, {
                "code": ErrorCode.operation_id_mismatch,
                "op": op_id,
                "message": "Unknown operation id"
            })
    return {"state": op_info.op_status}
