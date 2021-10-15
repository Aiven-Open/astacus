"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from .conftest import create_clickhouse_cluster, create_zookeeper, get_clickhouse_client, Ports
from astacus.coordinator.cluster import Cluster
from astacus.coordinator.plugins.base import StepsContext
from astacus.coordinator.plugins.clickhouse.manifest import ReplicatedDatabase, Table
from astacus.coordinator.plugins.clickhouse.steps import RetrieveDatabasesAndTablesStep
from uuid import UUID

import pytest

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.order("second_to_last"),
]


@pytest.mark.asyncio
async def test_retrieve_tables(ports: Ports) -> None:
    async with create_zookeeper(ports) as zookeeper:
        # We need a "real" cluster to be able to use Replicated databases
        async with create_clickhouse_cluster(zookeeper, ports, cluster_size=1) as clickhouse_cluster:
            clickhouse = clickhouse_cluster.services[0]
            client = get_clickhouse_client(clickhouse)
            await client.execute(
                "CREATE DATABASE has_tables ENGINE = Replicated('/clickhouse/databases/has_tables', '{shard}', '{replica}')"
            )
            await client.execute(
                "CREATE DATABASE no_tables ENGINE = Replicated('/clickhouse/databases/no_tables', '{shard}', '{replica}')"
            )
            await client.execute("CREATE DATABASE not_replicated ENGINE = Atomic")
            await client.execute("CREATE TABLE has_tables.table_1 (thekey UInt32) ENGINE = MergeTree ORDER BY (thekey)")
            await client.execute("CREATE TABLE not_replicated.table_2 (thekey UInt32) ENGINE = MergeTree ORDER BY (thekey)")
            step = RetrieveDatabasesAndTablesStep(clients=[client])
            context = StepsContext()
            databases, tables = await step.run_step(Cluster(nodes=[]), context=context)
            uuid_lines = list(await client.execute("SELECT uuid FROM system.tables where name = 'table_1' "))
            table_uuid = UUID(uuid_lines[0][0])
            assert databases == [ReplicatedDatabase(name="has_tables"), ReplicatedDatabase(name="no_tables")]
            assert tables == [
                Table(
                    database="has_tables",
                    name="table_1",
                    engine="MergeTree",
                    uuid=table_uuid,
                    create_query=f"CREATE TABLE has_tables.table_1 UUID '{str(table_uuid)}' (`thekey` UInt32) "
                    "ENGINE = MergeTree ORDER BY thekey SETTINGS index_granularity = 8192",
                    dependencies=[]
                )
            ]
