"""

depends

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Shared depends utilities

"""

from starlette.requests import Request


def get_or_create_state(*, request: Request, key: str, factory):
    """ Get or create sub-state entry (using factory callback) """
    value = getattr(request.app.state, key, None)
    if value is None:
        value = factory()
        setattr(request.app.state, key, value)
    return value
