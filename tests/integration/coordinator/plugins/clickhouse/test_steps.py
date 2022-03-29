"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from .conftest import create_clickhouse_cluster, get_clickhouse_client
from astacus.coordinator.cluster import Cluster
from astacus.coordinator.plugins.base import StepsContext
from astacus.coordinator.plugins.clickhouse.manifest import ReplicatedDatabase, Table
from astacus.coordinator.plugins.clickhouse.steps import RetrieveDatabasesAndTablesStep
from tests.integration.conftest import create_zookeeper, Ports
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
                (
                    "CREATE DATABASE `has_tablés` "
                    "ENGINE = Replicated('/clickhouse/databases/has_tables', '{shard}', '{replica}')"
                ).encode()
            )
            await client.execute(
                b"CREATE DATABASE no_tables "
                b"ENGINE = Replicated('/clickhouse/databases/no_tables', '{shard}', '{replica}')"
            )
            await client.execute(
                b"CREATE DATABASE `bad\x80\x00db` "
                b"ENGINE = Replicated('/clickhouse/databases/bad_db', '{shard}', '{replica}')"
            )
            await client.execute(
                (
                    "CREATE TABLE `has_tablés`.`tablé_1` (thekey UInt32) " "ENGINE = ReplicatedMergeTree ORDER BY (thekey)"
                ).encode()
            )
            step = RetrieveDatabasesAndTablesStep(clients=[client])
            context = StepsContext()
            databases, tables = await step.run_step(Cluster(nodes=[]), context=context)
            uuid_lines = list(await client.execute("SELECT uuid FROM system.tables where name = 'tablé_1'".encode()))
            table_uuid = UUID(uuid_lines[0][0])
            assert databases == [
                ReplicatedDatabase(name=b"bad\x80\x00db"),
                ReplicatedDatabase(name="has_tablés".encode()),
                ReplicatedDatabase(name=b"no_tables"),
            ]
            assert tables == [
                Table(
                    database="has_tablés".encode(),
                    name="tablé_1".encode(),
                    engine="ReplicatedMergeTree",
                    uuid=table_uuid,
                    create_query=(
                        f"CREATE TABLE `has_tablés`.`tablé_1` UUID '{str(table_uuid)}' (`thekey` UInt32) "
                        f"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{str(table_uuid)}/{{shard}}', '{{replica}}') "
                        f"ORDER BY thekey SETTINGS index_granularity = 8192"
                    ).encode(),
                    dependencies=[],
                ),
            ]
