"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from astacus.common.rohmustorage import RohmuConfig
from astacus.common.utils import AstacusModel
from fastapi import Request
from typing import List, Optional

APP_KEY = "coordinator_config"


class CoordinatorConfig(AstacusModel):
    class Node(AstacusModel):
        # What is the Astacus url of the node
        url: str

        # Availability zone the node is in; in theory this could be
        # made also just part of node config, but it feels more
        # elegant not to have probe nodes for configuration and
        # instead have required parts here in the configuration file.
        az: str = ""

    # Which nodes are we supposed to manage
    nodes: List[Node] = []

    # How long cluster lock we acquire
    default_lock_ttl: int = 60

    # When polling for status, how often do we do it
    poll_delay_start: int = 1
    poll_delay_multiplier: int = 2
    poll_delay_max: int = 60
    # How long do we wait for op to finish before giving up on it?
    poll_duration: int = 86400

    # How many times can poll fail in row before we call it a day
    poll_maximum_failures: int = 5

    # Backup is attempted this many times before giving up.
    #
    # Note that values should be >1, as at least one retry makes
    # almost always sense and subsequent backup attempts are fast as
    # snapshots are incremential unless nodes have restarted.
    backup_attempts: int = 5

    # Restore is attempted this many times before giving up.
    #
    # Note that values should be >1, as at least one retry makes
    # almost always sense and subsequent restore attempts are fast as
    # already downloaded files are not downloaded again.
    restore_attempts: int = 5

    object_storage: Optional[RohmuConfig] = None


def coordinator_config(request: Request) -> CoordinatorConfig:
    return getattr(request.app.state, APP_KEY)
