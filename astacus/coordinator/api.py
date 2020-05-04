"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from .config import coordinator_config, CoordinatorConfig
from .locker import LockerContext, LockerFSM
from .state import coordinator_state, CoordinatorState
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request
from urllib.parse import urljoin

FSM_APP_KEY = "coordinator_fsm"

router = APIRouter()


class Coordinator:
    """ Convenience dependency which contains sub-dependencies most API endpoints need """
    def __init__(self,
                 *,
                 request: Request,
                 background_tasks: BackgroundTasks,
                 config: CoordinatorConfig = Depends(coordinator_config),
                 state: CoordinatorState = Depends(coordinator_state)):
        self.request = request
        self.background_tasks = background_tasks
        self.config = config
        self.state = state


def start_fsm(*, c: Coordinator, fsm_class, context, initial):
    """
    Start a FSM instance with given fsm_class and context,
    starting at initial state.
    """
    fsm = fsm_class(context=context, initial=initial)
    current_fsm = getattr(c.request.app.state, FSM_APP_KEY, None)
    if current_fsm and current_fsm.is_running():
        raise HTTPException(503, "An operation is already ongoing")
    if current_fsm:
        setattr(c.state.fsm, current_fsm.context_name, None)
    setattr(c.request.app.state, FSM_APP_KEY, fsm)
    c.state.fsm.fsm_class = "%s.%s" % (fsm.__class__.__module__,
                                       fsm.__class__.__qualname__)
    c.state.op += 1
    setattr(c.state.fsm, fsm.context_name, context)
    c.background_tasks.add_task(fsm.run)
    status_url = urljoin(str(c.request.url), f"../status/{c.state.op}")
    return {"status-url": status_url}


@router.post("/lock")
def lock(*, locker: str, ttl: int = 60, c: Coordinator = Depends()):
    context = LockerContext(locker=locker,
                            ttl=ttl,
                            unlocked_nodes=c.config.nodes[:])
    result = start_fsm(c=c,
                       fsm_class=LockerFSM,
                       context=context,
                       initial="lock")
    result["unlock-url"] = urljoin(str(c.request.url),
                                   f"../unlock?locker={locker}")
    return result


@router.get("/unlock")
def unlock(
        *,
        locker: str,
        c: Coordinator = Depends(),
):
    context = LockerContext(locker=locker, locked_nodes=c.config.nodes[:])
    result = start_fsm(c=c,
                       fsm_class=LockerFSM,
                       context=context,
                       initial="unlock")
    return result


@router.get("/status/{op}")
def status(
        *,
        op: int,
        c: Coordinator = Depends(),
):
    if op != c.state.op:
        raise HTTPException(404, "Unknown operation id")
    fsm = getattr(c.request.app.state, FSM_APP_KEY, None)
    if not fsm:
        return {}
    return {"state": fsm.state}
