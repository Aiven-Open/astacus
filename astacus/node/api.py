"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from .state import node_state, NodeState
from fastapi import APIRouter, Depends, HTTPException

router = APIRouter()


@router.get("/lock")
def lock(locker: str, ttl: int, state: NodeState = Depends(node_state)):
    if state.locked:
        raise HTTPException(status_code=409, detail="Already locked")
    state.locked = True
    state.locker = locker


@router.get("/unlock")
def unlock(locker: str, state: NodeState = Depends(node_state)):
    if not state.locked:
        raise HTTPException(status_code=409, detail="Already unlocked")
    if state.locker != locker:
        raise HTTPException(status_code=403, detail="Locked by someone else")

    state.locked = False
