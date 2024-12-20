"""Copyright (c) 2020 Aiven Ltd
See LICENSE for details.
"""

from astacus.common import ipc
from astacus.common.rohmustorage import RohmuConfig
from astacus.common.statsd import StatsdConfig
from astacus.common.utils import AstacusModel
from collections.abc import Sequence
from fastapi import Request
from pathlib import Path

APP_KEY = "coordinator_config"


class PollConfig(AstacusModel):
    # When polling for status, how often do we do it
    delay_start: int = 1
    delay_multiplier: int = 2
    delay_max: int = 60

    # How long do we wait for op to finish before giving up on it?
    duration: int = 86400

    # How many times can poll fail in row before we call it a day
    maximum_failures: int = 5

    # Sometimes Astacus blobs can be .. big.
    # (TBD: This should be paged somehow)
    result_timeout: int = 300


class CoordinatorNode(AstacusModel):
    # What is the Astacus url of the node
    url: str

    # Availability zone the node is in; in theory this could be
    # made also just part of node config, but it feels more
    # elegant not to have probe nodes for configuration and
    # instead have required parts here in the configuration file.
    az: str = ""


class Retention(AstacusModel):
    # If set, number of backups to retain always (even beyond days)
    minimum_backups: int | None = None

    # If set, maximum number of backups to retain
    maximum_backups: int | None = None

    # Backups older than this are deleted, unless it would reduce
    # number of backups to less than minimum_backups
    keep_days: int | None = None


class CoordinatorConfig(AstacusModel):
    retention: Retention = Retention(keep_days=7, minimum_backups=3)

    poll: PollConfig = PollConfig()

    plugin: ipc.Plugin
    plugin_config: dict = {}

    # Which nodes are we supposed to manage
    nodes: Sequence[CoordinatorNode] = []

    # How long cluster lock we acquire
    #
    # Note that in busy swapping system, GIL can be locked
    # surprisingly long and e.g. 60 seconds has turned out to be
    # insufficient at times.
    default_lock_ttl: int = 600

    # Backup is attempted this many times before giving up.
    #
    # Note that values should be >1, as at least one retry makes
    # almost always sense and subsequent backup attempts are fast as
    # snapshots are incremential unless nodes have restarted.1
    backup_attempts: int = 5

    # Restore is attempted this many times before giving up.
    #
    # Note that values should be >1, as at least one retry makes
    # almost always sense and subsequent restore attempts are fast as
    # already downloaded files are not downloaded again.
    restore_attempts: int = 5

    # Optional object storage cache directory used for caching json
    # manifest fetching
    # Directory is created if it does not exist
    object_storage_cache: Path | None = None

    # These can be either globally or locally set
    object_storage: RohmuConfig | None = None
    statsd: StatsdConfig | None = None

    # How long do we cache list results unless there is (successful)
    # backup? Probably even one hour (default) is sensible enough
    list_ttl: int = 3600


def coordinator_config(request: Request) -> CoordinatorConfig:
    return getattr(request.app.state, APP_KEY)
