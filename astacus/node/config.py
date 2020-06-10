"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from astacus.common.rohmustorage import RohmuConfig
from astacus.common.statsd import StatsdConfig
from astacus.common.utils import AstacusModel
from fastapi import Request
from pathlib import Path
from pydantic import DirectoryPath
from typing import Optional

APP_KEY = "node_config"


class NodeConfig(AstacusModel):
    # Where is the root of the file hierarchy we care about
    root: DirectoryPath

    # Which availability zone is this node in (optional)
    az: str = ""

    # Where do we hardlink things from the file hierarchy we care about
    # By default, .astacus subdirectory is created in root if this is not set
    # Directory is created if it does not exist
    root_link: Optional[Path]

    # These can be either globally or locally set
    object_storage: Optional[RohmuConfig] = None
    statsd: Optional[StatsdConfig] = None


def node_config(request: Request) -> NodeConfig:
    return getattr(request.app.state, APP_KEY)
