"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

This state represents the state of the node.

By design it cannot be persisted to disk, but e.g. op_info can be if necessary.

"""

from astacus.common import utils
from astacus.common.op import OpState
from dataclasses import dataclass
from fastapi import Request
from threading import Lock

import time

APP_KEY = "node_state"
APP_LOCK_KEY = "node_lock"


class LockEntry(utils.AstacusModel):
    locker: str = ""
    locked_until: float = 0


@dataclass
class NodeState(OpState):
    mutate_lock = Lock()
    _lock: LockEntry | None = None

    @property
    def is_locked(self):
        lock = self._lock
        if not lock:
            return False
        if time.monotonic() > lock.locked_until:
            return False
        return lock.locker

    @property
    def still_locked_callback(self):
        original_lock = self._lock
        assert original_lock

        def _gen():
            while self.is_locked == original_lock.locker:
                yield True
            while True:
                yield False

        gen = _gen()

        def _fun():
            return next(gen)

        return _fun

    def lock(self, *, locker, ttl):
        self._lock = LockEntry(locker=locker, locked_until=time.monotonic() + ttl)

    def unlock(self):
        assert self._lock
        self._lock = None


def node_state(request: Request) -> NodeState:
    return utils.get_or_create_state(state=request.app.state, key=APP_KEY, factory=NodeState)
