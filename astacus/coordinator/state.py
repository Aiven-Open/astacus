"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

This state represents the state of the coordinator.

It is persisted to disk, and stored in the app.state.

"""

from .locker import LockerContext
from astacus.common import utils
from fastapi import Depends, Request
from pydantic import BaseModel  # pylint: disable=no-name-in-module # ( sometimes Cython -> pylint won't work )
from pydantic import Field
from threading import Lock
from typing import Optional

APP_KEY = "coordinator_state"
APP_LOCK_KEY = "coordinator_state_lock"


class FSMState(BaseModel):
    fsm_class: str = ''
    fsm_state: str = ''  # which state is the FSM in

    # astacus.coordinator.locker fsm context
    locker: Optional[LockerContext] = None


class CoordinatorState(BaseModel):
    op: int = 0
    fsm: FSMState = Field(default_factory=FSMState)


def raw_coordinator_state(request: Request) -> CoordinatorState:
    return utils.get_or_create_state(request=request,
                                     key=APP_KEY,
                                     factory=CoordinatorState)


def coordinator_lock(request: Request) -> Lock:
    """Acquire Lock to have access to CoordinatorState

    It is intentionally not RLock, as depends are resolved in main
    asyncio event loop thread, and due to that, every request could
    acquire same RLock without issues. Plain old Lock works, but care
    should be taken in not trying to lock multiple times as that will
    deadlock.

    For longer requests combination of unlocked_coordinator_state + coordinator_lock
    + with (only in critical sections) in the different thread should
    be used.
    """
    return utils.get_or_create_state(request=request,
                                     key=APP_LOCK_KEY,
                                     factory=Lock)


def coordinator_state(state: CoordinatorState = Depends(raw_coordinator_state),
                      lock: Lock = Depends(
                          coordinator_lock)) -> CoordinatorState:
    with lock:
        yield state
