"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from fastapi import Request
from pydantic import BaseModel  # pylint: disable=no-name-in-module # ( sometimes Cython -> pylint won't work )
from typing import List

APP_KEY = "coordinator_config"


class CoordinatorConfig(BaseModel):
    class Node(BaseModel):
        url: str

    # Which nodes are we supposed to manage
    nodes: List[Node] = []

    # How long cluster lock we acquire
    default_lock_ttl: int = 60

    # When polling for status, how often do we do it
    poll_delay_start: int = 1
    poll_delay_multiplier: int = 2
    poll_delay_max: int = 60

    # How many times can poll fail in row before we call it a day
    poll_maximum_failures: int = 5

    # Backup is attempted this many times before giving up.
    #
    # Note that values should be >1, as at least one retry makes
    # almost always sense and subsequent backup attempts are fast as
    # snapshots are incremential unless nodes have restarted.
    backup_attempts: int = 5


def coordinator_config(request: Request) -> CoordinatorConfig:
    return getattr(request.app.state, APP_KEY)
