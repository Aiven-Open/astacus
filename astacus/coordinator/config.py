"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from astacus.common import utils
from fastapi import Depends, Request
from pydantic import BaseModel  # pylint: disable=no-name-in-module # ( sometimes Cython -> pylint won't work )
from threading import Lock
from typing import List

APP_KEY = "coordinator_config"
APP_LOCK_KEY = "coordinator_config_lock"


class Node(BaseModel):
    url: str


class CoordinatorConfig(BaseModel):
    nodes: List[Node] = []


def raw_coordinator_config(request: Request) -> CoordinatorConfig:
    return utils.get_or_create_state(request=request,
                                     key=APP_KEY,
                                     factory=CoordinatorConfig)


def coordinator_lock(request: Request) -> Lock:
    """Acquire Lock to have access to CoordinatorConfig

    It is intentionally not RLock, as depends are resolved in main
    asyncio event loop thread, and due to that, every request could
    acquire same RLock without issues. Plain old Lock works, but care
    should be taken in not trying to lock multiple times as that will
    deadlock.

    For longer requests combination of raw_coordinator_config + coordinator_lock
    + with (only in critical sections) in the different thread should
    be used.
    """
    return utils.get_or_create_state(request=request,
                                     key=APP_LOCK_KEY,
                                     factory=Lock)


def coordinator_config(
        config: CoordinatorConfig = Depends(raw_coordinator_config),
        lock: Lock = Depends(coordinator_lock)) -> CoordinatorConfig:
    with lock:
        yield config
