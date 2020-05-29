"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from astacus.common.rohmustorage import RohmuConfig
from astacus.common.statsd import StatsdConfig
from astacus.common.utils import AstacusModel
from fastapi import Request
from typing import List, Optional

APP_KEY = "node_config"


class NodeConfig(AstacusModel):
    # Which availability zone is this node in (optional)
    az: str = ""

    # Where is the root of the file hierarchy we care about
    # ( There can be others too, but probably all things we care about have at least 1 directory )
    root: str

    # list of globs, e.g. ["**/*.dat"] we want to back up from root
    root_globs: List[str]

    # Where do we hardlink things from the file hierarchy we care about
    root_link: str

    # These can be either globally or locally set
    object_storage: Optional[RohmuConfig] = None
    statsd: Optional[StatsdConfig] = None


def node_config(request: Request) -> NodeConfig:
    return getattr(request.app.state, APP_KEY)
