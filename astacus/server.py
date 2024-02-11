"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

It is responsible for setting up the FastAPI app, with the sub-routers
mapped ( coordinator + node) and configured (by loading configuration
entries from both JSON file, as well as accepting configuration
entries from command line (later part TBD).

Note that 'app' may be initialized based on ASTACUS_CONFIG and SENTRY_DSN
options, or within main() which handles parameters. While not super elegant,
it allows for nice in-place-reloading.

"""

from astacus import config
from astacus.coordinator.api import router as coordinator_router
from astacus.coordinator.state import app_coordinator_state
from astacus.node.api import router as node_router
from fastapi import FastAPI
from sentry_sdk.integrations.asgi import SentryAsgiMiddleware

import logging
import os
import sentry_sdk
import subprocess
import uvicorn

logger = logging.getLogger(__name__)

app: FastAPI | None = None


def init_app():
    """Initialize the FastAPI app.

    It is stored in a global here because uvicorn we currently use is
    older than the 8/2020 version which added factory function
    support; once factory support is enabled, we could consider
    switching to using init_app as factory.
    """
    config_path = os.environ.get("ASTACUS_CONFIG")
    assert config_path
    api = FastAPI()
    api.include_router(node_router, prefix="/node", tags=["node"])
    api.include_router(coordinator_router, tags=["coordinator"])

    @api.on_event("shutdown")
    async def _shutdown_event():
        if app is not None:
            state = await app_coordinator_state(app=app)
            state.shutting_down = True

    gconfig = config.set_global_config_from_path(api, config_path)
    sentry_dsn = os.environ.get("SENTRY_DSN", gconfig.sentry_dsn)
    if sentry_dsn:
        sentry_sdk.init(dsn=sentry_dsn)  # pylint: disable=abstract-class-instantiated
        api.add_middleware(SentryAsgiMiddleware)
    global app  # pylint: disable=global-statement
    app = api
    return api


if os.environ.get("ASTACUS_CONFIG"):
    init_app()


def _systemd_notify_ready():
    if not os.environ.get("NOTIFY_SOCKET"):
        return
    try:
        from systemd import daemon  # pylint: disable=no-name-in-module,disable=import-outside-toplevel

        daemon.notify("READY=1")
    except ImportError:
        logger.warning("Running under systemd but python-systemd not available, attempting systemd notify via utility")
        subprocess.run(["systemd-notify", "--ready"], check=True)


def _run_server(args) -> bool:
    # On reload (and following init_app), the app is configured based on this
    os.environ["ASTACUS_CONFIG"] = args.config
    uconfig = init_app().state.global_config.uvicorn
    _systemd_notify_ready()
    # uvicorn log_level option overrides log levels defined in log_config.
    # This is fine, except that the list of overridden loggers depends on the version: it changes at version 0.12.
    # For now, it's safer to apply the overriding ourself to avoid surprising changes if uvicorn is upgraded.
    log_level = uconfig.log_level
    if isinstance(log_level, str):
        log_level = uvicorn.config.LOG_LEVELS[log_level]
    # We don't want debug-level info from kazoo, this leaks znode content to logs.
    kazoo_log_level = max(log_level, logging.INFO)
    uvicorn.run(
        "astacus.server:app",
        host=uconfig.host,
        port=uconfig.port,
        reload=uconfig.reload,
        log_config={
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {
                    "()": "uvicorn.logging.DefaultFormatter",
                    "fmt": "%(levelname)s\t%(name)s\t%(message)s",
                    "use_colors": None,
                },
                "access": {
                    "()": "uvicorn.logging.AccessFormatter",
                    "fmt": '%(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s',
                },
            },
            "filters": {
                "access_log_level": {
                    "()": "astacus.common.access_log.AccessLogLevelFilter",
                },
            },
            "handlers": {
                "default": {
                    "class": "logging.StreamHandler",
                    "formatter": "default",
                    "stream": "ext://sys.stderr",
                },
                "access": {
                    "class": "logging.StreamHandler",
                    "formatter": "access",
                    "level": log_level,
                    "stream": "ext://sys.stdout",
                },
            },
            "loggers": {
                "": {"handlers": ["default"], "level": log_level},
                "uvicorn.error": {"level": log_level},
                "uvicorn.access": {
                    "handlers": ["access"],
                    "filters": ["access_log_level"],
                    "level": log_level,
                    "propagate": False,
                },
                "kazoo.client": {"level": kazoo_log_level},
            },
        },
        http=uconfig.http,
    )
    return True


def create_server_parser(subparsers):
    # TBD: Add overrides for configuration file entries that may be
    # relevant to update in more human-friendly way
    server = subparsers.add_parser("server", help="Run REST server")
    server.add_argument(
        "-c",
        "--config",
        type=str,
        help="YAML configuration file to use",
        required=True,
        default=os.environ.get("ASTACUS_CONFIG"),
    )
    server.set_defaults(func=_run_server)
