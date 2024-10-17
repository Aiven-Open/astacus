"""

Copyright (c) 2021 Aiven Ltd
See LICENSE for details

Dependency injection helper functions.
"""

from starlette.datastructures import URL
from starlette.requests import Request


def get_request_url(request: Request) -> URL:
    return request.url


def get_request_app_state(request: Request) -> object:
    return request.app.state
