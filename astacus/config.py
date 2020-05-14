"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Root-level astacus configuration, which includes
- global configuration
- coordinator configuration
- node configuration
"""

from astacus.common import magic
from astacus.coordinator.config import APP_KEY as COORDINATOR_CONFIG_KEY, CoordinatorConfig
from astacus.node.config import APP_KEY as NODE_CONFIG_KEY, NodeConfig
from fastapi import Request
from pydantic import BaseModel  # pylint: disable=no-name-in-module # ( sometimes Cython -> pylint won't work )
from typing import Optional

import json

APP_KEY = "global_config"


class UvicornConfig(BaseModel):
    host: str = magic.ASTACUS_DEFAULT_HOST
    port: int = magic.ASTACUS_DEFAULT_PORT
    log_level: Optional[str] = None
    reload: bool = False


class GlobalConfig(BaseModel):
    # These have to be provided in a configuration file
    coordinator: CoordinatorConfig
    node: NodeConfig

    # These, on the other hand, have defaults
    sentry_dsn: str = ""
    uvicorn: UvicornConfig = UvicornConfig()


def global_config(request: Request) -> GlobalConfig:
    return getattr(request.app.state, APP_KEY)


def set_global_config_from_path(app, path):
    fh = open(path)
    config = GlobalConfig.parse_obj(json.load(fh))
    setattr(app.state, APP_KEY, config)
    setattr(app.state, COORDINATOR_CONFIG_KEY, config.coordinator)
    setattr(app.state, NODE_CONFIG_KEY, config.node)
    return config
