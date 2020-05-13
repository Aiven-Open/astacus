"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Implementation of the fictious 'lock' and 'unlock' API endpoints (we
may or may not want them for debug purposes, but this is mostly all
about API design testing)

"""

from .coordinator import Coordinator, Op


class LockOps(Op):
    def __init__(self, *, c: Coordinator, locker: str, ttl: int = 60):
        super().__init__(c=c)
        self.locker = locker
        self.ttl = ttl

    async def lock(self):
        for result in await self.request_from_nodes(
                f"lock?locker={self.locker}&ttl={self.ttl}",
                caller="LockOps.lock"):
            if result != {"locked": True}:
                self.set_op_state(self.State.fail)
                await self.unlock()
                return

    async def unlock(self):
        for result in await self.request_from_nodes(
                f"unlock?locker={self.locker}", caller="LockOps.unlock"):
            if result != {"locked": False}:
                self.set_op_state(self.State.fail)
