"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Root-level astacus configuration, which includes
- global configuration
- coordinator configuration
- node configuration
"""

from astacus.common import magic
from astacus.common.rohmustorage import RohmuConfig
from astacus.common.statsd import StatsdConfig
from astacus.common.utils import AstacusModel
from astacus.coordinator.config import APP_KEY as COORDINATOR_CONFIG_KEY, CoordinatorConfig
from astacus.node.config import APP_KEY as NODE_CONFIG_KEY, NodeConfig
from enum import Enum
from fastapi import Request
from typing import Optional

import yaml

APP_KEY = "global_config"


class UvicornConfig(AstacusModel):
    class HTTPMode(str, Enum):
        auto = "auto"  # default, but sometimes leads to httptools
        h11 = "h11"
        httptools = "httptools"  # crashy on Fedora 31 at least

    host: str = magic.ASTACUS_DEFAULT_HOST
    http: HTTPMode = HTTPMode.h11
    port: int = magic.ASTACUS_DEFAULT_PORT
    log_level: Optional[str] = None
    reload: bool = False


class GlobalConfig(AstacusModel):
    # These have to be provided in a configuration file
    coordinator: CoordinatorConfig
    node: NodeConfig

    # These, on the other hand, have defaults
    sentry_dsn: str = ""
    uvicorn: UvicornConfig = UvicornConfig()

    # These can be either globally or locally set
    object_storage: Optional[RohmuConfig] = None
    statsd: Optional[StatsdConfig] = None


def global_config(request: Request) -> GlobalConfig:
    return getattr(request.app.state, APP_KEY)


def set_global_config_from_path(app, path) -> GlobalConfig:
    with open(path) as fh:
        config = GlobalConfig.parse_obj(yaml.safe_load(fh))
    cconfig = config.coordinator
    nconfig = config.node
    setattr(app.state, APP_KEY, config)
    setattr(app.state, COORDINATOR_CONFIG_KEY, cconfig)
    setattr(app.state, NODE_CONFIG_KEY, nconfig)
    # Propagate keys that can be configured locally/globally
    for propagated_key in ["object_storage", "statsd"]:
        for subconfig in [cconfig, nconfig]:
            if getattr(subconfig, propagated_key) is not None:
                continue
            setattr(subconfig, propagated_key, getattr(config, propagated_key))
    return config
