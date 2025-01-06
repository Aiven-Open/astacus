"""Copyright (c) 2020 Aiven Ltd
See LICENSE for details.

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
    _locks: dict[str | None, LockEntry] = {}

    @property
    def has_any_lock(self) -> bool:
        return bool(self._locks)

    def is_locked(self, lock_name: str | None) -> str | Literal[False]:
        lock = self._locks.get(lock_name)
        if not lock:
            return False
        if time.monotonic() > lock.locked_until:
            return False
        return lock.locker

    def still_locked_callback(self, lock_name: str | None) -> Callable[[], bool]:
        original_lock = self._locks.get(lock_name)
        assert original_lock

        def _gen():
            while self.is_locked(lock_name) == original_lock.locker:
                yield True
            while True:
                yield False

        gen = _gen()

        def _fun():
            return next(gen)

        return _fun

    def lock(self, *, lock_name: str | None, locker: str, ttl: float) -> None:
        lock_entry = LockEntry(locker=locker, locked_until=time.monotonic() + ttl)
        with self.mutate_lock:
            assert self._locks.get(lock_name) is None
            self._locks[lock_name] = lock_entry

    def unlock(self, *, lock_name: str | None) -> None:
        assert self.is_locked(lock_name)
        with self.mutate_lock:
            del self._locks[lock_name]


def node_state(request: Request) -> NodeState:
    return utils.get_or_create_state(state=request.app.state, key=APP_KEY, factory=NodeState)
