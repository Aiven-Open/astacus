"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from enum import Enum

ASTACUS_DEFAULT_HOST = "127.0.0.1"  # localhost-only, for testing
ASTACUS_DEFAULT_PORT = 5515  # random port not assigned by IANA
ASTACUS_TMPDIR = ".astacus"

# Hexdigest is 32 bytes, so something orders of magnitude more at least
EMBEDDED_FILE_SIZE = 100


class LockCall(str, Enum):
    lock = "lock"
    relock = "relock"
    unlock = "unlock"


class ErrorCode(str, Enum):
    cluster_lock_unavailable = "cluster_lock_unavailable"
    operation_id_mismatch = "operation_id_mismatch"


# In storage, json files with this prefix are backup manifests
JSON_BACKUP_PREFIX = "backup-"
