"""
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""
from astacus.coordinator.plugins.clickhouse.replication import get_shard_and_replica
from tests.integration.conftest import create_zookeeper, Ports
from tests.integration.coordinator.plugins.clickhouse.conftest import create_clickhouse_cluster, get_clickhouse_client

import pytest

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.order("second_to_last"),
]


@pytest.mark.asyncio
async def test_get_shard_and_replica(ports: Ports) -> None:
    async with create_zookeeper(ports) as zookeeper:
        async with create_clickhouse_cluster(zookeeper, ports, cluster_size=1) as clickhouse_cluster:
            clickhouse = clickhouse_cluster.services[0]
            client = get_clickhouse_client(clickhouse)
            await client.execute(
                b"CREATE DATABASE replicated_database "
                b"ENGINE = Replicated('/clickhouse/databases/replicated_database', '{shard}', '{replica}')"
            )
            shard_and_replica = await get_shard_and_replica(client, b"replicated_database")
            assert shard_and_replica == (b"{shard}", b"{replica}")
