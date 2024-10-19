"""Copyright (c) 2021 Aiven Ltd
See LICENSE for details.
"""

from astacus.coordinator.plugins.zookeeper import (
    ChildrenWithData,
    KazooZooKeeperClient,
    NodeExistsError,
    NoNodeError,
    RolledBackError,
    RuntimeInconsistency,
    TransactionError,
)
from collections.abc import Callable, Iterable
from kazoo.client import KazooClient
from tests.integration.conftest import create_zookeeper, get_kazoo_host, Ports, Service

import dataclasses
import logging
import pytest
import secrets
import time

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.order("second_to_last"),
]

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class ZNode:
    path: str
    content: bytes


@pytest.fixture(name="znode")
def fixture_znode(zookeeper: Service) -> ZNode:
    name = secrets.token_hex()
    znode = ZNode(path=f"/test/{name}", content=b"the_content")
    base_client = KazooClient(hosts=[get_kazoo_host(zookeeper)])
    base_client.start()
    base_client.create(znode.path, znode.content, makepath=True)
    base_client.stop()
    base_client.close()
    return znode


async def test_kazoo_zookeeper_client_get(zookeeper_client: KazooZooKeeperClient, znode: ZNode):
    async with zookeeper_client.connect() as connection:
        assert await connection.get(znode.path) == znode.content


async def test_kazoo_zookeeper_client_get_missing_node_fails(zookeeper_client: KazooZooKeeperClient) -> None:
    async with zookeeper_client.connect() as connection:
        with pytest.raises(NoNodeError):
            assert await connection.get("/does/not/exist")


async def test_kazoo_zookeeper_client_get_children(zookeeper_client: KazooZooKeeperClient) -> None:
    async with zookeeper_client.connect() as connection:
        assert await connection.get_children("/zookeeper") == ["config", "quota"]


async def test_kazoo_zookeeper_client_get_children_of_missing_node_fails(zookeeper_client: KazooZooKeeperClient) -> None:
    async with zookeeper_client.connect() as connection:
        with pytest.raises(NoNodeError):
            assert await connection.get_children("/does/not/exists")


async def test_kazoo_zookeeper_client_try_create(zookeeper_client: KazooZooKeeperClient) -> None:
    async with zookeeper_client.connect() as connection:
        assert await connection.try_create("/new/try_create", b"new_content") is True
        assert await connection.get("/new/try_create") == b"new_content"


async def test_kazoo_zookeeper_client_try_create_failure(zookeeper_client: KazooZooKeeperClient) -> None:
    async with zookeeper_client.connect() as connection:
        await connection.create("/new/try_create_failure", b"content")
        assert await connection.try_create("/new/try_create_failure", b"new_content") is False
        assert await connection.get("/new/try_create_failure") == b"content"


async def test_kazoo_zookeeper_client_create(zookeeper_client: KazooZooKeeperClient) -> None:
    async with zookeeper_client.connect() as connection:
        await connection.create("/new/create", b"content")
        assert await connection.get("/new/create") == b"content"


async def test_kazoo_zookeeper_client_create_existing_node_fails(zookeeper_client: KazooZooKeeperClient) -> None:
    async with zookeeper_client.connect() as connection:
        with pytest.raises(NodeExistsError):
            await connection.create("/zookeeper", b"content")


async def test_kazoo_zookeeper_transaction(zookeeper_client: KazooZooKeeperClient) -> None:
    async with zookeeper_client.connect() as connection:
        transaction = connection.transaction()
        transaction.create("/transaction_1", b"content")
        transaction.create("/transaction_2", b"content")
        await transaction.commit()
        assert await connection.get("/transaction_1") == b"content"
        assert await connection.get("/transaction_2") == b"content"


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


async def test_kazoo_zookeeper_client_bounded_failure_time(ports: Ports) -> None:
    async with create_zookeeper(ports) as zookeeper:
        zookeeper_client = KazooZooKeeperClient(hosts=[get_kazoo_host(zookeeper)], user=None, timeout=1)
        async with zookeeper_client.connect() as connection:
            zookeeper.process.kill()
            start_time = time.monotonic()
            with pytest.raises(Exception):
                await connection.exists("/bounded_failure_time")
            elapsed_time = time.monotonic() - start_time
            # We allow for a bit of margin
            assert elapsed_time < 10.0


class TestGetChildrenWithData:
    @pytest.fixture(name="kazoo_client")
    def fixture_kazoo_client(self, zookeeper: Service) -> Iterable[KazooClient]:
        client = KazooClient(get_kazoo_host(zookeeper))
        client.start()
        yield client
        client.stop()

    def do_once_then_sleep(self, func: Callable[[], None]) -> Callable[[], None]:
        call_count = 0

        def wrapped() -> None:
            nonlocal call_count
            if call_count == 0:
                func()
            elif call_count == 1:
                time.sleep(2)
            call_count += 1

        return wrapped

    def test_works(self, kazoo_client: KazooClient) -> None:
        kazoo_client.create("/test_works", b"")
        kazoo_client.create("/test_works/child1", b"child1")
        kazoo_client.create("/test_works/child2", b"child2")
        kazoo_client.create("/test_works/child3", b"child3")
        assert ChildrenWithData(kazoo_client, "/test_works").get() == {
            "child1": b"child1",
            "child2": b"child2",
            "child3": b"child3",
        }

    def test_fails_if_node_doesnt_exist(self, kazoo_client: KazooClient) -> None:
        with pytest.raises(NoNodeError):
            ChildrenWithData(kazoo_client, "/test_fails_if_node_doesnt_exist").get()

    def test_fails_if_node_is_deleted(self, kazoo_client: KazooClient) -> None:
        @self.do_once_then_sleep
        def fault() -> None:
            kazoo_client.delete("/test_fails_if_node_is_deleted/child1")
            kazoo_client.delete("/test_fails_if_node_is_deleted/child2")
            kazoo_client.delete("/test_fails_if_node_is_deleted/child3")
            kazoo_client.delete("/test_fails_if_node_is_deleted")

        kazoo_client.create("/test_fails_if_node_is_deleted", b"")
        kazoo_client.create("/test_fails_if_node_is_deleted/child1", b"child1")
        kazoo_client.create("/test_fails_if_node_is_deleted/child2", b"child2")
        kazoo_client.create("/test_fails_if_node_is_deleted/child3", b"child3")
        with pytest.raises(NoNodeError):
            ChildrenWithData(kazoo_client, "/test_fails_if_node_is_deleted", get_children_fault=fault).get()

    async def test_works_with_adding_node(self, kazoo_client: KazooClient) -> None:
        @self.do_once_then_sleep
        def fault() -> None:
            kazoo_client.create("/test_works_with_adding_node/child4", b"child4")

        kazoo_client.create("/test_works_with_adding_node", b"")
        kazoo_client.create("/test_works_with_adding_node/child1", b"child1")
        kazoo_client.create("/test_works_with_adding_node/child2", b"child2")
        kazoo_client.create("/test_works_with_adding_node/child3", b"child3")
        assert ChildrenWithData(kazoo_client, "/test_works_with_adding_node", get_data_fault=fault).get() == {
            "child1": b"child1",
            "child2": b"child2",
            "child3": b"child3",
            "child4": b"child4",
        }

    async def test_works_with_removing_node(self, kazoo_client: KazooClient) -> None:
        @self.do_once_then_sleep
        def fault() -> None:
            kazoo_client.delete("/test_works_with_removing_node/child2")

        kazoo_client.create("/test_works_with_removing_node", b"")
        kazoo_client.create("/test_works_with_removing_node/child1", b"child1")
        kazoo_client.create("/test_works_with_removing_node/child2", b"child2")
        kazoo_client.create("/test_works_with_removing_node/child3", b"child3")
        assert ChildrenWithData(kazoo_client, "/test_works_with_removing_node", get_data_fault=fault).get() == {
            "child1": b"child1",
            "child3": b"child3",
        }

    async def test_works_with_changing_node(self, kazoo_client: KazooClient) -> None:
        @self.do_once_then_sleep
        def fault() -> None:
            kazoo_client.set("/test_works_with_changing_node/child2", b"child2_changed")

        kazoo_client.create("/test_works_with_changing_node", b"")
        kazoo_client.create("/test_works_with_changing_node/child1", b"child1")
        kazoo_client.create("/test_works_with_changing_node/child2", b"child2")
        kazoo_client.create("/test_works_with_changing_node/child3", b"child3")

        assert ChildrenWithData(kazoo_client, "/test_works_with_changing_node", get_data_fault=fault).get() == {
            "child2": b"child2_changed",
            "child3": b"child3",
            "child1": b"child1",
        }
