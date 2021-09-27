"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Implementation of the fictious 'lock' and 'unlock' API endpoints (we
may or may not want them for debug purposes, but this is mostly all
about API design testing)

"""

from .coordinator import Coordinator, CoordinatorOp


class LockOps(CoordinatorOp):
    def __init__(self, *, c: Coordinator, op_id: int, locker: str, ttl: int = 60):
        super().__init__(c=c, op_id=op_id)
        self.locker = locker
        self.ttl = ttl

    async def lock(self):
        result = await self.request_lock_from_nodes(locker=self.locker, ttl=self.ttl)
        if not result:
            self.set_status_fail()
            await self.unlock()

    async def unlock(self):
        result = await self.request_unlock_from_nodes(locker=self.locker)
        if not result:
            self.set_status_fail()
