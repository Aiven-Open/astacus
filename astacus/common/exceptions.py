"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""


class AstacusException(Exception):
    pass


class PermanentException(AstacusException):
    pass


class ExpiredOperationException(PermanentException):
    pass


class InsufficientNodesException(PermanentException):
    pass


class InsufficientAZsException(PermanentException):
    pass
