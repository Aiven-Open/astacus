"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from astacus.coordinator.plugins.clickhouse.macros import Macros
from astacus.coordinator.plugins.clickhouse.manifest import ReplicatedDatabase, Table
from astacus.coordinator.plugins.clickhouse.replication import DatabaseReplica, get_databases_replicas, get_tables_replicas
from uuid import UUID


def test_get_databases_replicas() -> None:
    replicated_database = [
        ReplicatedDatabase(name=b"default_db", uuid=UUID(int=0), shard=b"{all_shard}", replica=b"{my_replica}"),
        ReplicatedDatabase(name=b"sharded_db", uuid=UUID(int=1), shard=b"{my_shard}", replica=b"{my_replica}"),
    ]
    server_macros = [
        Macros.from_mapping({b"all_shard": b"all", b"my_shard": b"s1", b"my_replica": b"r1"}),
        Macros.from_mapping({b"all_shard": b"all", b"my_shard": b"s2", b"my_replica": b"r2"}),
    ]
    databases_replicas = get_databases_replicas(replicated_database, server_macros)
    assert databases_replicas == {
        b"default_db": [
            DatabaseReplica(shard_name="all", replica_name="r1"),
            DatabaseReplica(shard_name="all", replica_name="r2"),
        ],
        b"sharded_db": [
            DatabaseReplica(shard_name="s1", replica_name="r1"),
            DatabaseReplica(shard_name="s2", replica_name="r2"),
        ],
    }


def test_get_tables_replicas() -> None:
    replicated_tables = [
        Table(
            database=b"default_db",
            name=b"table_one",
            engine="ReplicatedMergeTree",
            uuid=UUID(int=3),
            create_query=b"CREATE ...",
        ),
        Table(
            database=b"sharded_db",
            name=b"table_two",
            engine="ReplicatedMergeTree",
            uuid=UUID(int=4),
            create_query=b"CREATE ...",
        ),
    ]
    databases_replicas = {
        b"default_db": [
            DatabaseReplica(shard_name="all", replica_name="r1"),
            DatabaseReplica(shard_name="all", replica_name="r2"),
        ],
        b"sharded_db": [
            DatabaseReplica(shard_name="s1", replica_name="r1"),
            DatabaseReplica(shard_name="s2", replica_name="r2"),
        ],
    }
    tables_replicas = get_tables_replicas(replicated_tables, databases_replicas)
    assert tables_replicas == {
        UUID(int=3): [
            DatabaseReplica(shard_name="all", replica_name="r1"),
            DatabaseReplica(shard_name="all", replica_name="r2"),
        ],
        UUID(int=4): [
            DatabaseReplica(shard_name="s1", replica_name="r1"),
            DatabaseReplica(shard_name="s2", replica_name="r2"),
        ],
    }
