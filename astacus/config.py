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
from astacus.common.utils import AstacusModel
from astacus.coordinator.config import APP_KEY as COORDINATOR_CONFIG_KEY, CoordinatorConfig
from astacus.node.config import APP_KEY as NODE_CONFIG_KEY, NodeConfig
from fastapi import Request
from typing import Optional

import yaml

APP_KEY = "global_config"


class UvicornConfig(AstacusModel):
    host: str = magic.ASTACUS_DEFAULT_HOST
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

    object_storage: Optional[RohmuConfig] = None


def global_config(request: Request) -> GlobalConfig:
    return getattr(request.app.state, APP_KEY)


def set_global_config_from_path(app, path):
    fh = open(path)
    config = GlobalConfig.parse_obj(yaml.safe_load(fh))
    cconfig = config.coordinator
    nconfig = config.node
    setattr(app.state, APP_KEY, config)
    setattr(app.state, COORDINATOR_CONFIG_KEY, cconfig)
    setattr(app.state, NODE_CONFIG_KEY, nconfig)
    # Actual object storage configuration can be either specified for
    # subcomponents directly, or just on the app level to apply to both
    if cconfig.object_storage is None:
        cconfig.object_storage = config.object_storage
    if nconfig.object_storage is None:
        nconfig.object_storage = config.object_storage
    return config
