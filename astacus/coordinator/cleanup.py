"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Database cleanup operation

"""

from astacus.common import ipc
from astacus.coordinator.coordinator import Coordinator, SteppedCoordinatorOp
from fastapi import Depends

import logging

logger = logging.getLogger(__name__)


class CleanupOp(SteppedCoordinatorOp):
    @staticmethod
    async def create(*, c: Coordinator = Depends(), req: ipc.CleanupRequest = ipc.CleanupRequest()) -> "CleanupOp":
        return CleanupOp(c=c, req=req)

    def __init__(self, *, c: Coordinator, req: ipc.CleanupRequest) -> None:
        context = c.get_operation_context()
        retention = c.config.retention.copy()
        if req.retention is not None:
            # This returns only non-defaults -> non-Nones
            for k, v in req.retention.dict().items():
                setattr(retention, k, v)
        steps = c.get_plugin().get_cleanup_steps(context=context, retention=retention, explicit_delete=req.explicit_delete)
        super().__init__(c=c, attempts=1, steps=steps)
