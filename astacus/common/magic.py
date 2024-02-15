"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from enum import Enum

ASTACUS_DEFAULT_HOST = "127.0.0.1"  # localhost-only, for testing
ASTACUS_DEFAULT_PORT = 5515  # random port not assigned by IANA
ASTACUS_TMPDIR = ".astacus"

# Hexdigest is 32 bytes, so something orders of magnitude more at least
DEFAULT_EMBEDDED_FILE_SIZE = 200


class StrEnum(str, Enum):
    def __str__(self) -> str:
        return str(self.value)


class LockCall(StrEnum):
    lock = "lock"
    relock = "relock"
    unlock = "unlock"


class ErrorCode(StrEnum):
    cluster_lock_unavailable = "cluster_lock_unavailable"
    operation_id_mismatch = "operation_id_mismatch"


# In storage, json files with this prefix are backup manifests
JSON_BACKUP_PREFIX = "backup-"
JSON_DELTA_PREFIX = "delta-"

JSON_STORAGE_PREFIX = "json"
HEXDIGEST_STORAGE_PREFIX = "hexdigest"
