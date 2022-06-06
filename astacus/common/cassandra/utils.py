"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details

Miscellaneous utilities related to CassandraSession

"""

_SYSTEM_KEYSPACES = {
    "system",
    # "system_auth", # While technically system keyspace, we want to restore this
    "system_distributed",
    "system_schema",
    "system_traces",
}


def is_system_keyspace(keyspace: str) -> bool:
    return keyspace in _SYSTEM_KEYSPACES
