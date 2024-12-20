"""Copyright (c) 2020 Aiven Ltd
See LICENSE for details.
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


class NotFoundException(PermanentException):
    pass


class MissingSnapshotResultsException(PermanentException):
    pass


# rohmu without compression/encryption does not work; this temporary
# check is in place until that gets fixed


class CompressionOrEncryptionRequired(PermanentException):
    pass


# rohmu has given us 'some' exception we do not support; assume it is
# permanent until proven otherwise
class RohmuException(PermanentException):
    pass


class TransientException(AstacusException):
    pass


# TBD: Wrap the e.g. rohmu retryable exceptions around TransientException
