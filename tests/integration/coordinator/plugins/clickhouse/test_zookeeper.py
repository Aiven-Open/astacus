"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from astacus.coordinator.plugins.zookeeper import (
    KazooZooKeeperClient,
    NodeExistsError,
    NoNodeError,
    RolledBackError,
    RuntimeInconsistency,
    TransactionError,
)
from tests.integration.conftest import create_zookeeper, get_kazoo_host, Ports, Service

import dataclasses
import kazoo.client
import pytest
import secrets
import time

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.order("second_to_last"),
]


@dataclasses.dataclass
class ZNode:
    path: str
    content: bytes


@pytest.fixture(name="znode")
def fixture_znode(zookeeper: Service) -> ZNode:
    name = secrets.token_hex()
    znode = ZNode(path=f"/test/{name}", content=b"the_content")
    base_client = kazoo.client.KazooClient(hosts=[get_kazoo_host(zookeeper)])
    base_client.start()
    base_client.create(znode.path, znode.content, makepath=True)
    base_client.stop()
    base_client.close()
    return znode


@pytest.mark.asyncio
async def test_kazoo_zookeeper_client_get(zookeeper_client: KazooZooKeeperClient, znode: ZNode):
    async with zookeeper_client.connect() as connection:
        assert await connection.get(znode.path) == znode.content


@pytest.mark.asyncio
async def test_kazoo_zookeeper_client_get_missing_node_fails(zookeeper_client: KazooZooKeeperClient) -> None:
    async with zookeeper_client.connect() as connection:
        with pytest.raises(NoNodeError):
            assert await connection.get("/does/not/exist")


@pytest.mark.asyncio
async def test_kazoo_zookeeper_client_get_children(zookeeper_client: KazooZooKeeperClient) -> None:
    async with zookeeper_client.connect() as connection:
        assert await connection.get_children("/zookeeper") == ["config", "quota"]


@pytest.mark.asyncio
async def test_kazoo_zookeeper_client_get_children_of_missing_node_fails(zookeeper_client: KazooZooKeeperClient) -> None:
    async with zookeeper_client.connect() as connection:
        with pytest.raises(NoNodeError):
            assert await connection.get_children("/does/not/exists")


@pytest.mark.asyncio
async def test_kazoo_zookeeper_client_create(zookeeper_client: KazooZooKeeperClient) -> None:
    async with zookeeper_client.connect() as connection:
        assert await connection.create("/new/node", b"content")
        assert await connection.get("/new/node") == b"content"


@pytest.mark.asyncio
async def test_kazoo_zookeeper_client_create_existing_node_fails(zookeeper_client: KazooZooKeeperClient) -> None:
    async with zookeeper_client.connect() as connection:
        with pytest.raises(NodeExistsError):
            await connection.create("/zookeeper", b"content")


@pytest.mark.asyncio
async def test_kazoo_zookeeper_transaction(zookeeper_client: KazooZooKeeperClient) -> None:
    async with zookeeper_client.connect() as connection:
        transaction = connection.transaction()
        transaction.create("/transaction_1", b"content")
        transaction.create("/transaction_2", b"content")
        await transaction.commit()
        assert await connection.get("/transaction_1") == b"content"
        assert await connection.get("/transaction_2") == b"content"


@pytest.mark.asyncio
async def test_kazoo_zookeeper_failing_transaction(zookeeper_client: KazooZooKeeperClient) -> None:
    async with zookeeper_client.connect() as connection:
        await connection.create("/failing_transaction_2", b"old_content")
        transaction = connection.transaction()
        transaction.create("/failing_transaction_1", b"content")
        transaction.create("/failing_transaction_2", b"content")
        transaction.create("/failing_transaction_3", b"content")
        with pytest.raises(TransactionError) as raised:
            await transaction.commit()
        assert isinstance(raised.value.results[0], RolledBackError)
        assert isinstance(raised.value.results[1], NodeExistsError)
        assert isinstance(raised.value.results[2], RuntimeInconsistency)


@pytest.mark.asyncio
async def test_kazoo_zookeeper_client_bounded_failure_time(ports: Ports) -> None:
    async with create_zookeeper(ports) as zookeeper:
        zookeeper_client = KazooZooKeeperClient(hosts=[get_kazoo_host(zookeeper)], timeout=1)
        async with zookeeper_client.connect() as connection:
            zookeeper.process.kill()
            start_time = time.monotonic()
            with pytest.raises(Exception):
                await connection.exists("/bounded_failure_time")
            elapsed_time = time.monotonic() - start_time
            # We allow for a bit of margin
            assert elapsed_time < 10.0
