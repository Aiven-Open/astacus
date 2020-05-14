"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Implementation of the fictious 'lock' and 'unlock' API endpoints (we
may or may not want them for debug purposes, but this is mostly all
about API design testing)

"""

from .coordinator import Coordinator, CoordinatorOp

import logging

logger = logging.getLogger(__name__)


class LockOps(CoordinatorOp):
    def __init__(self, *, c: Coordinator, locker: str, ttl: int = 60):
        super().__init__(c=c)
        self.locker = locker
        self.ttl = ttl

    async def lock(self):
        results = await self.request_from_nodes(
            f"lock?locker={self.locker}&ttl={self.ttl}",
            method="post",
            caller="LockOps.lock")
        logger.debug("lock results: %r", results)
        for result in results:
            if result != {"locked": True}:
                self.set_status_fail()
                await self.unlock()
                return

    async def unlock(self):
        results = await self.request_from_nodes(f"unlock?locker={self.locker}",
                                                method="post",
                                                caller="LockOps.unlock")
        logger.debug("unlock results: %r", results)
        for result in results:
            if result != {"locked": False}:
                self.set_status_fail()
