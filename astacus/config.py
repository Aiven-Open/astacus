"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Root-level astacus configuration, which includes
- global configuration
- coordinator configuration
- node configuration
"""

from astacus.common import magic
from astacus.common.magic import StrEnum
from astacus.common.rohmustorage import RohmuConfig
from astacus.common.statsd import StatsdConfig
from astacus.common.utils import AstacusModel
from astacus.coordinator.config import APP_KEY as COORDINATOR_CONFIG_KEY, CoordinatorConfig
from astacus.node.config import APP_KEY as NODE_CONFIG_KEY, NodeConfig
from pathlib import Path
from starlette.applications import Starlette
from starlette.requests import Request

import hashlib
import io
import yaml

APP_KEY = "global_config"
APP_HASH_KEY = "global_config_hash"


class UvicornConfig(AstacusModel):
    class HTTPMode(StrEnum):
        auto = "auto"  # default, but sometimes leads to httptools
        h11 = "h11"
        httptools = "httptools"  # crashy on Fedora 31 at least

    host: str = magic.ASTACUS_DEFAULT_HOST
    http: HTTPMode = HTTPMode.h11
    port: int = magic.ASTACUS_DEFAULT_PORT
    log_level: str | None = None
    reload: bool = False


class GlobalConfig(AstacusModel):
    # These have to be provided in a configuration file
    coordinator: CoordinatorConfig
    node: NodeConfig

    # These, on the other hand, have defaults
    sentry_dsn: str = ""
    uvicorn: UvicornConfig = UvicornConfig()

    # These can be either globally or locally set
    object_storage: RohmuConfig | None = None
    statsd: StatsdConfig | None = None


def global_config(request: Request) -> GlobalConfig:
    return getattr(request.app.state, APP_KEY)


def get_config_content_and_hash(config_path: str | Path) -> tuple[str, str]:
    with open(config_path, "rb") as fh:
        config_content = fh.read()
    config_hash = hashlib.sha256(config_content).hexdigest()
    return config_content.decode(), config_hash


def set_global_config_from_path(app: Starlette, path: str | Path) -> GlobalConfig:
    config_content, config_hash = get_config_content_and_hash(path)
    with io.StringIO(config_content) as config_file:
        config = GlobalConfig.parse_obj(yaml.safe_load(config_file))
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
    setattr(app.state, APP_HASH_KEY, config_hash)
    return config
