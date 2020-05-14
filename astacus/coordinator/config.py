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

    nodes: List[Node] = []


def coordinator_config(request: Request) -> CoordinatorConfig:
    return getattr(request.app.state, APP_KEY)
