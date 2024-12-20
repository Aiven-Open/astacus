"""Copyright (c) 2020 Aiven Ltd
See LICENSE for details.

This state represents the state of the coordinator.

By design it cannot be persisted to disk, but e.g. op_info can be if necessary.

"""

from astacus.common import ipc, utils
from astacus.common.op import OpState
from astacus.coordinator.config import CoordinatorConfig
from dataclasses import dataclass
from fastapi import FastAPI, Request

import msgspec
import time

APP_KEY = "coordinator_state"


class CachedListResponse(msgspec.Struct, kw_only=True):
    timestamp: float = msgspec.field(default_factory=time.monotonic)
    coordinator_config: CoordinatorConfig
    list_request: ipc.ListRequest
    list_response: ipc.ListResponse


@dataclass
class CoordinatorState(OpState):
    """State of the coordinator.

    As coordinator is mixture of sync and async code ( .. lovely ..)
    no good locking strategy exists, but in-place member value
    replacement is atomic so contents should be only replaced, never
    mutated.
    """

    cached_list_response: CachedListResponse | None = None
    cached_list_running: bool = False
    shutting_down: bool = False


async def app_coordinator_state(app: FastAPI) -> CoordinatorState:
    return utils.get_or_create_state(state=app.state, key=APP_KEY, factory=CoordinatorState)


async def coordinator_state(request: Request) -> CoordinatorState:
    return await app_coordinator_state(app=request.app)
