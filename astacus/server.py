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
from astacus.node.api import router as node_router
from fastapi import FastAPI
from sentry_sdk.integrations.asgi import SentryAsgiMiddleware

import os
import sentry_sdk
import uvicorn  # type:ignore


def init_app():
    config_path = os.environ.get("ASTACUS_CONFIG")
    if not config_path:
        return None
    rv = FastAPI()
    rv.include_router(coordinator_router, tags=["coordinator"])
    rv.include_router(node_router, prefix="/node", tags=["node"])
    gconfig = config.set_global_config_from_path(rv, config_path)
    sentry_dsn = os.environ.get("SENTRY_DSN", gconfig.sentry_dsn)
    if sentry_dsn:
        sentry_sdk.init(dsn=sentry_dsn)
        rv = SentryAsgiMiddleware(rv)
    return rv


app = init_app()


def _run_server(args):
    os.environ["ASTACUS_CONFIG"] = args.config
    global app  # pylint: disable=global-statement
    app = init_app()
    gconfig = app.state.global_config
    uconfig = gconfig.uvicorn
    uvicorn.run(
        "astacus.server:app",
        host=uconfig.host,
        port=uconfig.port,
        reload=uconfig.reload,
        log_level=uconfig.log_level,
        http=uconfig.http
    )


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
        default=os.environ.get("ASTACUS_CONFIG")
    )
    server.set_defaults(func=_run_server)
