"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from astacus.coordinator.plugins.zookeeper import (
    ChangeWatch,
    FakeZooKeeperClient,
    NodeExistsError,
    NoNodeError,
    RolledBackError,
    RuntimeInconsistency,
    TransactionError,
)

import pytest

pytestmark = [pytest.mark.clickhouse]


@pytest.mark.asyncio
async def test_fake_zookeeper_client_create_includes_parents() -> None:
    client = FakeZooKeeperClient()
    async with client.connect() as connection:
        await connection.create("/path/to/some/key", b"content")
        assert await connection.get("/path") == b""
        assert await connection.get("/path/to") == b""
        assert await connection.get("/path/to/some") == b""


@pytest.mark.asyncio
async def test_fake_zookeeper_client_create_then_get() -> None:
    client = FakeZooKeeperClient()
    async with client.connect() as connection:
        await connection.create("/path/to_key", b"content")
        assert await connection.get("/path/to_key") == b"content"
        assert await connection.get("/path/to_key/") == b"content"


@pytest.mark.asyncio
async def test_fake_zookeeper_client_create_existing_node_fails() -> None:
    client = FakeZooKeeperClient()
    async with client.connect() as connection:
        await connection.create("/path/to_key", b"content")
        with pytest.raises(NodeExistsError):
            await connection.create("/path/to_key", b"content")


@pytest.mark.asyncio
async def test_fake_zookeeper_client_get_missing_node_fails() -> None:
    client = FakeZooKeeperClient()
    async with client.connect() as connection:
        with pytest.raises(NoNodeError):
            await connection.get("/path/to_key")


@pytest.mark.asyncio
async def test_fake_zookeeper_client_root_exists() -> None:
    client = FakeZooKeeperClient()
    async with client.connect() as connection:
        assert await connection.get("") == b""


@pytest.mark.asyncio
async def test_fake_zookeeper_client_get_children() -> None:
    client = FakeZooKeeperClient()
    async with client.connect() as connection:
        await connection.create("/group/of/key_1", b"content")
        await connection.create("/group/of/key_3", b"content")
        await connection.create("/group/of/key_2", b"content")
        assert await connection.get_children("/group/of") == ["key_1", "key_2", "key_3"]


@pytest.mark.asyncio
async def test_fake_zookeeper_client_get_children_of_missing_node_fails() -> None:
    client = FakeZooKeeperClient()
    async with client.connect() as connection:
        with pytest.raises(NoNodeError):
            await connection.get_children("/group/of")


@pytest.mark.asyncio
async def test_fake_zookeeper_client_get_watch() -> None:
    client = FakeZooKeeperClient()
    change_watch = ChangeWatch()
    async with client.connect() as connection:
        await connection.create("/group/of/key_1", b"content")
        await connection.get("/group/of/key_1", watch=change_watch)
        async with client.connect() as connection_2:
            await connection_2.create("/group/of/key_1/subitem", b"content")
    assert change_watch.has_changed


@pytest.mark.asyncio
async def test_fake_zookeeper_client_get_children_watch() -> None:
    client = FakeZooKeeperClient()
    change_watch = ChangeWatch()
    async with client.connect() as connection:
        await connection.create("/group/of/key_1", b"content")
        await connection.get_children("/group/of", watch=change_watch)
        async with client.connect() as connection_2:
            await connection_2.create("/group/of/key_2", b"content")
    assert change_watch.has_changed


@pytest.mark.asyncio
async def test_fake_zookeeper_transaction():
    client = FakeZooKeeperClient()
    async with client.connect() as connection:
        transaction = connection.transaction()
        transaction.create("/key_1", b"content")
        transaction.create("/key_2", b"content")
        await transaction.commit()
        assert await connection.get("/key_1") == b"content"
        assert await connection.get("/key_2") == b"content"


@pytest.mark.asyncio
async def test_fake_zookeeper_transaction_is_atomic():
    client = FakeZooKeeperClient()
    async with client.connect() as connection:
        await connection.create("/key_2", b"old content")
        transaction = connection.transaction()
        transaction.create("/key_1", b"content")
        transaction.create("/key_2", b"content")
        transaction.create("/key_3", b"content")
        with pytest.raises(TransactionError) as raised:
            await transaction.commit()
        assert not await connection.exists("/key_1")
        assert await connection.get("/key_2") == b"old content"
        # We fudge the equality a bit to simplify comparing exceptions
        expected_errors = [RolledBackError("/key_1"), NodeExistsError("/key_2"), RuntimeInconsistency("/key_3")]
        assert repr(raised.value.results) == repr(expected_errors)


@pytest.mark.asyncio
async def test_fake_zookeeper_transaction_does_not_implicitly_create_parents():
    client = FakeZooKeeperClient()
    async with client.connect() as connection:
        transaction = connection.transaction()
        transaction.create("/path/to/key_1", b"content")
        with pytest.raises(TransactionError):
            await transaction.commit()


@pytest.mark.asyncio
async def test_fake_zookeeper_transaction_generates_trigger():
    client = FakeZooKeeperClient()
    change_watch = ChangeWatch()
    async with client.connect() as connection:
        await connection.create("/key_1", b"content")
        await connection.get_children("/", watch=change_watch)
        transaction = connection.transaction()
        transaction.create("/key_2", b"content")
        await transaction.commit()
    assert change_watch.has_changed
