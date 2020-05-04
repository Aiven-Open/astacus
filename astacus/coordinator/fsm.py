"""Copyright (c) 2020 Aiven Ltd
See LICENSE for details

This is (persisted) FSM convenience class on top of
transitions.LockedMachine.

Neat things:

- snapshots its own context regularly

  - doesn't require locking (as long as it is just fired up in a
    thread, and snapshot is accessed and reference kept from another)

"""

from transitions import Machine

import copy


class FSM(Machine):
    def __init__(self, *, initial, states, terminal_states, context_name,
                 context):
        Machine.__init__(self,
                         initial=initial,
                         states=states,
                         after_state_change="_snapshot_context")
        self.terminal_states = terminal_states
        self._context = context
        self._snapshot_context()
        self._running_states = set(states) - set(terminal_states)
        self.add_transition("run", list(self._running_states), "=")
        self.context_name = context_name

    def _snapshot_context(self):
        self.context_snapshot = copy.deepcopy(self._context)

    def is_running(self):
        return self.state not in self.terminal_states
