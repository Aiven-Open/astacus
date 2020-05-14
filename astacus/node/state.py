"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

This state represents the state of the node.

It is persisted to disk, and stored in the app.state.
"""

from astacus.common import utils
from astacus.common.op import Op
from fastapi import Depends, Request
from pydantic import BaseModel  # pylint: disable=no-name-in-module # ( sometimes Cython -> pylint won't work )
from threading import Lock

import time

APP_KEY = "node_state"
APP_LOCK_KEY = "node_lock"


class NodeState(BaseModel):
    locked: bool = False
    locker: str = ''
    locked_until: float = 0
    op_info: Op.Info = Op.Info()

    @property
    def is_locked(self):
        if self.locked:
            if time.monotonic() > self.locked_until:
                self.locked = False
        return self.locked

    @property
    def still_locked_callback(self):
        original_locker = self.locker

        def _gen():
            still_locked = self.is_locked
            while still_locked:
                if not self.is_locked or self.locker != original_locker:
                    still_locked = False
                yield still_locked
            while True:
                yield False

        gen = _gen()

        def _fun():
            return next(gen)

        return _fun


def raw_node_state(request: Request) -> NodeState:
    return utils.get_or_create_state(request=request,
                                     key=APP_KEY,
                                     factory=NodeState)


def node_lock(request: Request) -> Lock:
    """Acquire Lock to have access to NodeState

    It is intentionally not RLock, as depends are resolved in main
    asyncio event loop thread, and due to that, every request could
    acquire same RLock without issues. Plain old Lock works, but care
    should be taken in not trying to lock multiple times as that will
    deadlock.

    For longer requests combination of raw_node_state + node_lock
    + with (only in critical sections) in the different thread should
    be used.
    """
    return utils.get_or_create_state(request=request,
                                     key=APP_LOCK_KEY,
                                     factory=Lock)


def node_state(state: NodeState = Depends(raw_node_state),
               lock: Lock = Depends(node_lock)):
    with lock:
        yield state
