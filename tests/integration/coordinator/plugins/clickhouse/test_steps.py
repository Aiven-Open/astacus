"""Copyright (c) 2021 Aiven Ltd
See LICENSE for details.
"""

from .conftest import ClickHouseCommand, create_clickhouse_cluster, get_clickhouse_client, MinioBucket
from .test_plugin import setup_cluster_users
from astacus.coordinator.cluster import Cluster
from astacus.coordinator.plugins.base import StepsContext
from astacus.coordinator.plugins.clickhouse.client import ClickHouseClient, ClickHouseClientQueryError, HttpClickHouseClient
from astacus.coordinator.plugins.clickhouse.manifest import ReplicatedDatabase, Table
from astacus.coordinator.plugins.clickhouse.steps import KeeperMapTablesReadOnlyStep, RetrieveDatabasesAndTablesStep
from base64 import b64decode
from collections.abc import AsyncIterator, Sequence
from tests.integration.conftest import create_zookeeper, Ports
from typing import cast, NamedTuple
from uuid import UUID

import pytest

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.order("second_to_last"),
]


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
            database_uuid_lines = cast(
                Sequence[tuple[str, str]],
                await client.execute(b"SELECT base64Encode(name),uuid FROM system.databases"),
            )
            database_uuids = {
                b64decode(database_name): UUID(database_uuid) for database_name, database_uuid in database_uuid_lines
            }
            table_uuid_lines: Sequence[Sequence[str]] = cast(
                list[tuple[str]],
                list(await client.execute("SELECT uuid FROM system.tables where name = 'tablé_1'".encode())),
            )
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
            assert tables == [
                Table(
                    database="has_tablés".encode(),
                    name="tablé_1".encode(),
                    engine="ReplicatedMergeTree",
                    uuid=table_uuid,
                    create_query=(
                        f"CREATE TABLE `has_tablés`.`tablé_1` UUID '{table_uuid!s}' (`thekey` UInt32) "
                        f"ENGINE = "
                        f"ReplicatedMergeTree('/clickhouse/tables/{{uuid}}/{{my_shard}}', '{{my_replica}}') "
                        f"ORDER BY thekey SETTINGS index_granularity = 8192"
                    ).encode(),
                    dependencies=[],
                ),
            ]


class KeeperMapInfo(NamedTuple):
    context: StepsContext
    clickhouse_client: ClickHouseClient
    user_client: ClickHouseClient


@pytest.fixture(name="keeper_table_context")
async def fixture_keeper_table_context(
    ports: Ports, clickhouse_command: ClickHouseCommand, minio_bucket: MinioBucket
) -> AsyncIterator[KeeperMapInfo]:
    async with (
        create_zookeeper(ports) as zookeeper,
        create_clickhouse_cluster(zookeeper, minio_bucket, ports, ["s1"], clickhouse_command) as clickhouse_cluster,
    ):
        clickhouse = clickhouse_cluster.services[0]
        admin_client = get_clickhouse_client(clickhouse)
        await setup_cluster_users([admin_client])
        for statement in [
            b"CREATE DATABASE `keeperdata` ENGINE = Replicated('/clickhouse/databases/keeperdata', '{my_shard}', '{my_replica}')",
            b"CREATE TABLE `keeperdata`.`keepertable` (thekey UInt32, thevalue UInt32) ENGINE = KeeperMap('test', 1000) PRIMARY KEY thekey",
            b"INSERT INTO `keeperdata`.`keepertable` SELECT *, materialize(1) FROM numbers(3)",
            b"CREATE USER bob IDENTIFIED WITH sha256_password BY 'secret'",
            b"GRANT INSERT, SELECT, UPDATE, DELETE ON `keeperdata`.`keepertable` TO `bob`",
        ]:
            await admin_client.execute(statement)
        user_client = HttpClickHouseClient(
            host=clickhouse.host,
            port=clickhouse.port,
            username="bob",
            password="secret",
            timeout=10,
        )
        step = RetrieveDatabasesAndTablesStep(clients=[admin_client])
        context = StepsContext()
        databases_tables_result = await step.run_step(Cluster(nodes=[]), context=context)
        context.set_result(RetrieveDatabasesAndTablesStep, databases_tables_result)
        yield KeeperMapInfo(context, admin_client, user_client)


async def test_keeper_map_table_select_only_setting_modified(keeper_table_context: KeeperMapInfo) -> None:
    steps_context, admin_client, user_client = keeper_table_context
    read_only_step = KeeperMapTablesReadOnlyStep(clients=[admin_client], allow_writes=False)
    # After the read-only step, the user should only be able to select from the table
    await read_only_step.run_step(Cluster(nodes=[]), steps_context)
    with pytest.raises(ClickHouseClientQueryError, match=".*ACCESS_DENIED.*"):
        await user_client.execute(
            b"INSERT INTO `keeperdata`.`keepertable` SETTINGS wait_for_async_insert=1  SELECT *, materialize(2) FROM numbers(3)"
        )
    with pytest.raises(ClickHouseClientQueryError, match=".*ACCESS_DENIED.*"):
        await user_client.execute(b"ALTER TABLE `keeperdata`.`keepertable` UPDATE thevalue = 3 WHERE thekey < 20")
    with pytest.raises(ClickHouseClientQueryError, match=".*ACCESS_DENIED.*"):
        await user_client.execute(b"DELETE FROM `keeperdata`.`keepertable` WHERE thekey < 20")
    read_only_row_count = cast(
        Sequence[tuple[int]], await user_client.execute(b"SELECT count() FROM `keeperdata`.`keepertable`")
    )
    assert int(read_only_row_count[0][0]) == 3
    # After the read-write step, the user should be able to write, update and delete from the table
    read_write_step = KeeperMapTablesReadOnlyStep(clients=[admin_client], allow_writes=True)
    await read_write_step.run_step(Cluster(nodes=[]), steps_context)
    await user_client.execute(b"INSERT INTO `keeperdata`.`keepertable` SELECT *, materialize(3) FROM numbers(3, 3)")
    read_write_row_count = cast(
        Sequence[tuple[int]], await user_client.execute(b"SELECT count() FROM `keeperdata`.`keepertable`")
    )
    assert int(read_write_row_count[0][0]) == 6
    await user_client.execute(b"ALTER TABLE `keeperdata`.`keepertable` UPDATE thevalue = 3 WHERE thekey < 20")
    current_values = await user_client.execute(b"SELECT thevalue FROM `keeperdata`.`keepertable` ORDER BY thekey")
    assert all(int(cast(str, val[0])) == 3 for val in current_values)
    await user_client.execute(b"DELETE FROM `keeperdata`.`keepertable` WHERE thekey < 20")
    post_delete_row_count = cast(
        Sequence[tuple[int]], await user_client.execute(b"SELECT count() FROM `keeperdata`.`keepertable`")
    )
    assert int(post_delete_row_count[0][0]) == 0
