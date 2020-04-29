"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

This state represents the state of the node.

It is persisted to disk, and stored in the app.state.
"""

from astacus.common.depends import get_or_create_state
from fastapi import Depends
from pydantic import BaseModel  # pylint: disable=no-name-in-module # ( sometimes Cython -> pylint won't work )
from starlette.requests import Request
from threading import Lock

import datetime

APP_KEY = "node_state"
APP_LOCK_KEY = "node_lock"


class NodeState(BaseModel):
    locked: bool = False
    locker: str = ''
    locked_until: datetime.datetime = None


def unlocked_node_state(request: Request) -> NodeState:
    return get_or_create_state(request=request, key=APP_KEY, factory=NodeState)


def node_lock(request: Request) -> Lock:
    """Acquire Lock to have access to NodeState

    It is intentionally not RLock, as depends are resolved in main
    asyncio event loop thread, and due to that, every request could
    acquire same RLock without issues. Plain old Lock works, but care
    should be taken in not trying to lock multiple times as that will
    deadlock.

    For longer requests combination of unlocked_node_state + node_lock
    + with (only in critical sections) in the different thread should
    be used.
    """
    return get_or_create_state(request=request, key=APP_LOCK_KEY, factory=Lock)


def node_state(state: NodeState = Depends(unlocked_node_state),
               lock: Lock = Depends(node_lock)) -> NodeState:
    with lock:
        yield state
