"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details

Miscellaneous utilities related to CassandraClient

"""

_SYSTEM_KEYSPACES = {
    "system",
    "system_auth",
    "system_distributed",
    "system_schema",
    "system_traces",
}


def is_system_keyspace(keyspace):
    if not isinstance(keyspace, str):
        keyspace = keyspace.name
    return keyspace in _SYSTEM_KEYSPACES
