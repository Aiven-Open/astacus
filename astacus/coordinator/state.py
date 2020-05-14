"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

This state represents the state of the coordinator.

It is persisted to disk, and stored in the app.state.

"""

from astacus.common import utils
from astacus.common.op import Op
from asyncio import Lock
from fastapi import Depends, Request
from pydantic import BaseModel  # pylint: disable=no-name-in-module # ( sometimes Cython -> pylint won't work )

APP_KEY = "coordinator_state"
APP_LOCK_KEY = "coordinator_state_lock"


class CoordinatorState(BaseModel):
    op_info: Op.Info = Op.Info()


def raw_coordinator_state(request: Request) -> CoordinatorState:
    return utils.get_or_create_state(request=request,
                                     key=APP_KEY,
                                     factory=CoordinatorState)


async def coordinator_lock(request: Request) -> Lock:
    """Acquire Lock to have access to CoordinatorState """
    return utils.get_or_create_state(request=request,
                                     key=APP_LOCK_KEY,
                                     factory=Lock)


async def coordinator_state(
        state: CoordinatorState = Depends(raw_coordinator_state),
        lock: Lock = Depends(coordinator_lock)):
    async with lock:
        yield state
