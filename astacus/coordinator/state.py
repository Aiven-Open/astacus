"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

This state represents the state of the coordinator.

By design it cannot be persisted to disk, but e.g. op_info can be if necessary.

"""

from astacus.common import utils
from astacus.common.op import OpState
from asyncio import Lock
from dataclasses import dataclass
from fastapi import Depends, Request

APP_KEY = "coordinator_state"
APP_LOCK_KEY = "coordinator_state_lock"


@dataclass
class CoordinatorState(OpState):
    pass


def raw_coordinator_state(request: Request) -> CoordinatorState:
    return utils.get_or_create_state(request=request, key=APP_KEY, factory=CoordinatorState)


async def coordinator_lock(request: Request) -> Lock:
    """Acquire Lock to have access to CoordinatorState """
    return utils.get_or_create_state(request=request, key=APP_LOCK_KEY, factory=Lock)


async def coordinator_state(
    state: CoordinatorState = Depends(raw_coordinator_state), lock: Lock = Depends(coordinator_lock)
):
    async with lock:
        yield state
