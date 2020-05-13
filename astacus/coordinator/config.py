"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from astacus.common import utils
from asyncio import Lock
from fastapi import Depends, Request
from pydantic import BaseModel  # pylint: disable=no-name-in-module # ( sometimes Cython -> pylint won't work )
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


async def coordinator_lock(request: Request) -> Lock:
    """Acquire Lock to have access to CoordinatorConfig """
    return utils.get_or_create_state(request=request,
                                     key=APP_LOCK_KEY,
                                     factory=Lock)


async def coordinator_config(
        config: CoordinatorConfig = Depends(raw_coordinator_config),
        lock: Lock = Depends(coordinator_lock)):
    async with lock:
        yield config
