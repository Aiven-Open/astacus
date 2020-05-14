"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""


class AstacusException(Exception):
    pass


class ExpiredOperationException(AstacusException):
    pass
