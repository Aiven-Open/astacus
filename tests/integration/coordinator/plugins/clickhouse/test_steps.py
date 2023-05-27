"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from .conftest import ClickHouseCommand, create_clickhouse_cluster, get_clickhouse_client, MinioBucket
from astacus.coordinator.cluster import Cluster
from astacus.coordinator.plugins.base import StepsContext
from astacus.coordinator.plugins.clickhouse.manifest import ReplicatedDatabase, Table
from astacus.coordinator.plugins.clickhouse.steps import RetrieveDatabasesAndTablesStep
from base64 import b64decode
from tests.integration.conftest import create_zookeeper, Ports
from uuid import UUID

import pytest

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.order("second_to_last"),
]


@pytest.mark.asyncio
async def test_retrieve_tables(ports: Ports, clickhouse_command: ClickHouseCommand, minio_bucket: MinioBucket) -> None:
    async with create_zookeeper(ports) as zookeeper:
        # We need a "real" cluster to be able to use Replicated databases
        async with create_clickhouse_cluster(
            zookeeper, minio_bucket, ports, ["s1"], clickhouse_command
        ) as clickhouse_cluster:
            clickhouse = clickhouse_cluster.services[0]
            client = get_clickhouse_client(clickhouse)
            await client.execute(
                (
                    "CREATE DATABASE `has_tablés` "
                    "ENGINE = Replicated('/clickhouse/databases/has_tables', '{my_shard}', '{my_replica}')"
                ).encode()
            )
            await client.execute(
                b"CREATE DATABASE no_tables "
                b"ENGINE = Replicated('/clickhouse/databases/no_tables', '{my_shard}', '{my_replica}')"
            )
            await client.execute(
                b"CREATE DATABASE `bad\x80\x00db` "
                b"ENGINE = Replicated('/clickhouse/databases/bad_db', '{my_shard}', '{my_replica}')"
            )
            await client.execute(
                (
                    "CREATE TABLE `has_tablés`.`tablé_1` (thekey UInt32) ENGINE = ReplicatedMergeTree ORDER BY (thekey)"
                ).encode()
            )
            step = RetrieveDatabasesAndTablesStep(clients=[client])
            context = StepsContext()
            databases, tables = await step.run_step(Cluster(nodes=[]), context=context)
            database_uuid_lines = list(await client.execute(b"SELECT base64Encode(name),uuid FROM system.databases"))
            database_uuids = {
                b64decode(database_name): UUID(database_uuid) for database_name, database_uuid in database_uuid_lines
            }
            table_uuid_lines = list(await client.execute("SELECT uuid FROM system.tables where name = 'tablé_1'".encode()))
            table_uuid = UUID(table_uuid_lines[0][0])

            assert databases == [
                ReplicatedDatabase(
                    name=b"bad\x80\x00db",
                    uuid=database_uuids[b"bad\x80\x00db"],
                    shard=b"{my_shard}",
                    replica=b"{my_replica}",
                ),
                ReplicatedDatabase(
                    name="has_tablés".encode(),
                    uuid=database_uuids["has_tablés".encode()],
                    shard=b"{my_shard}",
                    replica=b"{my_replica}",
                ),
                ReplicatedDatabase(
                    name=b"no_tables",
                    uuid=database_uuids[b"no_tables"],
                    shard=b"{my_shard}",
                    replica=b"{my_replica}",
                ),
            ]
            zookeeper_path = (
                f"/clickhouse/tables/{str(table_uuid)}/{{my_shard}}"
                if clickhouse_cluster.expands_uuid_in_zookeeper_path
                else "/clickhouse/tables/{uuid}/{my_shard}"
            )
            assert tables == [
                Table(
                    database="has_tablés".encode(),
                    name="tablé_1".encode(),
                    engine="ReplicatedMergeTree",
                    uuid=table_uuid,
                    create_query=(
                        f"CREATE TABLE `has_tablés`.`tablé_1` UUID '{str(table_uuid)}' (`thekey` UInt32) "
                        f"ENGINE = "
                        f"ReplicatedMergeTree('{zookeeper_path}', '{{my_replica}}') "
                        f"ORDER BY thekey SETTINGS index_granularity = 8192"
                    ).encode(),
                    dependencies=[],
                ),
            ]
