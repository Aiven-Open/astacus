"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Implementation of the fictious 'lock' and 'unlock' API endpoints (we
may or may not want them for debug purposes, but this is mostly all
about API design testing)

"""
from .cluster import LockResult
from .coordinator import Coordinator, CoordinatorOp
from fastapi import Depends


class LockOps(CoordinatorOp):
    def __init__(self, *, c: Coordinator = Depends(), locker: str, ttl: int = 60):
        super().__init__(c=c)
        self.locker = locker
        self.ttl = ttl

    async def lock(self) -> None:
        cluster = self.get_cluster()
        result = await cluster.request_lock(locker=self.locker, ttl=self.ttl)
        if result is not LockResult.ok:
            self.set_status_fail()
            await cluster.request_unlock(locker=self.locker)

    async def unlock(self) -> None:
        cluster = self.get_cluster()
        result = cluster.request_unlock(locker=self.locker)
        if result is not LockResult.ok:
            self.set_status_fail()
