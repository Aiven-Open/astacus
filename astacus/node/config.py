"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from fastapi import Request
from pydantic import BaseModel  # pylint: disable=no-name-in-module # ( sometimes Cython -> pylint won't work )
from typing import List

APP_KEY = "node_config"


class NodeConfig(BaseModel):
    # Where is the root of the file hierarchy we care about
    # ( There can be others too, but probably all things we care about have at least 1 directory )
    root: str

    # list of globs, e.g. ["**/*.dat"] we want to back up from root
    root_globs: List[str]

    # Where do we hardlink things from the file hierarchy we care about
    root_link: str


def node_config(request: Request) -> NodeConfig:
    return getattr(request.app.state, APP_KEY)
