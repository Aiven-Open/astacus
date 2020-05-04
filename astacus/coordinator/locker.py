"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Bit of a hello-world example of what can be done with transitions.

This could be equally well AsyncMachine, which would then run with
proper aiohttp sub-calls.

"""

from .fsm import FSM
from astacus.common import utils
from pydantic import BaseModel  # pylint: disable=no-name-in-module # ( sometimes Cython -> pylint won't work )
from typing import List

import logging

logger = logging.getLogger(__name__)


class LockerContext(BaseModel):
    locked_nodes: List = []
    unlocked_nodes: List = []
    locker: str = ''
    ttl: int = 0
    failing: bool = False


class LockerFSM(FSM):
    def __init__(self, *, context: LockerContext, initial=None):
        FSM.__init__(self,
                     terminal_states=["fail", "done"],
                     states=["lock", "unlock", "fail", "done"],
                     initial=initial,
                     context=context,
                     context_name="locker")
        self.add_transition("fail", "lock", "unlock")
        self.add_transition("fail", "unlock", "done")
        self.add_transition("done", "lock", "done")
        self.add_transition("done",
                            "unlock",
                            "fail",
                            conditions=["is_failing"])
        self.add_transition("done", "unlock", "done")

    def on_enter_lock(self):
        if not self._context.unlocked_nodes:
            self.done()
            return
        node = self._context.unlocked_nodes.pop()
        url = f"{node.url}/lock?locker={self._context.locker}&ttl={self._context.ttl}"
        r = utils.http_request(url, caller="LockerFSM.lock")
        if not r:
            logger.warning("Cluster lock failed on node %r", node)
            self._context.failing = True
            self.fail()
            return
        self._context.locked_nodes.append(node)
        self.run()

    def on_enter_unlock(self):
        if not self._context.locked_nodes:
            self.done()
            return
        node = self._context.locked_nodes.pop()
        url = f"{node.url}/unlock?locker={self._context.locker}"
        r = utils.http_request(url, caller="LockerFSM.unlock")
        if not r:
            self.fail()
            return
        self.run()

    def is_failing(self):
        return self._context.failing
