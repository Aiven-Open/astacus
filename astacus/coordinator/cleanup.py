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
        if req.retention is None:
            retention = ipc.Retention(
                minimum_backups=c.config.retention.minimum_backups,
                maximum_backups=c.config.retention.maximum_backups,
                keep_days=c.config.retention.keep_days,
            )
        else:
            retention = ipc.Retention(
                minimum_backups=coalesce(req.retention.minimum_backups, c.config.retention.minimum_backups),
                maximum_backups=coalesce(req.retention.maximum_backups, c.config.retention.maximum_backups),
                keep_days=coalesce(req.retention.keep_days, c.config.retention.keep_days),
            )
        steps = c.get_plugin().get_cleanup_steps(context=context, retention=retention, explicit_delete=req.explicit_delete)
        super().__init__(c=c, attempts=1, steps=steps)


def coalesce(a: int | None, b: int | None) -> int | None:
    return a if a is not None else b
