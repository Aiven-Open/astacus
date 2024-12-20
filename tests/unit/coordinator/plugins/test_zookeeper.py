"""Copyright (c) 2021 Aiven Ltd
See LICENSE for details.
"""

from astacus.coordinator.plugins.zookeeper import (
    ChangeWatch,
    FakeZooKeeperClient,
    NodeExistsError,
    NoNodeError,
    NotEmptyError,
    RolledBackError,
    RuntimeInconsistency,
    TransactionError,
)

import pytest

pytestmark = [pytest.mark.clickhouse]


async def test_fake_zookeeper_client_create_includes_parents() -> None:
    client = FakeZooKeeperClient()
    async with client.connect() as connection:
        await connection.create("/path/to/some/key", b"content")
        assert await connection.get("/path") == b""
        assert await connection.get("/path/to") == b""
        assert await connection.get("/path/to/some") == b""


async def test_fake_zookeeper_client_create_then_get() -> None:
    client = FakeZooKeeperClient()
    async with client.connect() as connection:
        await connection.create("/path/to_key", b"content")
        assert await connection.get("/path/to_key") == b"content"
        assert await connection.get("/path/to_key/") == b"content"


async def test_fake_zookeeper_client_create_then_set() -> None:
    client = FakeZooKeeperClient()
    async with client.connect() as connection:
        await connection.create("/path/to_key", b"content")
        await connection.set("/path/to_key", b"new content")
        assert await connection.get("/path/to_key/") == b"new content"


async def test_fake_zookeeper_client_create_existing_node_fails() -> None:
    client = FakeZooKeeperClient()
    async with client.connect() as connection:
        await connection.create("/path/to_key", b"content")
        with pytest.raises(NodeExistsError):
            await connection.create("/path/to_key", b"content")


async def test_fake_zookeeper_client_get_missing_node_fails() -> None:
    client = FakeZooKeeperClient()
    async with client.connect() as connection:
        with pytest.raises(NoNodeError):
            await connection.get("/path/to_key")


async def test_fake_zookeeper_client_set_missing_node_fails() -> None:
    client = FakeZooKeeperClient()
    async with client.connect() as connection:
        with pytest.raises(NoNodeError):
            await connection.set("/path/to_key", b"content")


async def test_fake_zookeeper_client_delete() -> None:
    client = FakeZooKeeperClient()
    async with client.connect() as connection:
        await connection.create("/path/to_key", b"content")
        await connection.delete("/path/to_key")
        assert not await connection.exists("/path/to_key")


async def test_fake_zookeeper_client_delete_fails_if_not_exist() -> None:
    client = FakeZooKeeperClient()
    async with client.connect() as connection:
        with pytest.raises(NoNodeError):
            await connection.delete("/path/to_key")


async def test_fake_zookeeper_client_delete_fails_if_has_children() -> None:
    client = FakeZooKeeperClient()
    async with client.connect() as connection:
        await connection.create("/path/to_key", b"content")
        await connection.create("/path/to_key/sub1", b"content")
        with pytest.raises(NotEmptyError):
            await connection.delete("/path/to_key")


async def test_fake_zookeeper_client_delete_recursive() -> None:
    client = FakeZooKeeperClient()
    async with client.connect() as connection:
        await connection.create("/path/to_key", b"content")
        await connection.create("/path/to_key/sub1", b"content")
        await connection.create("/path/to_key/sub2", b"content")
        await connection.delete("/path/to_key", recursive=True)
        assert not await connection.exists("/path/to_key")
        assert not await connection.exists("/path/to_key/sub1")
        assert not await connection.exists("/path/to_key/sub2")


async def test_fake_zookeeper_client_root_exists() -> None:
    client = FakeZooKeeperClient()
    async with client.connect() as connection:
        assert await connection.get("") == b""


async def test_fake_zookeeper_client_get_children() -> None:
    client = FakeZooKeeperClient()
    async with client.connect() as connection:
        await connection.create("/group/of/key_1", b"content")
        await connection.create("/group/of/key_3", b"content")
        await connection.create("/group/of/key_2", b"content")
        assert await connection.get_children("/group/of") == ["key_1", "key_2", "key_3"]


async def test_fake_zookeeper_client_get_children_of_missing_node_fails() -> None:
    client = FakeZooKeeperClient()
    async with client.connect() as connection:
        with pytest.raises(NoNodeError):
            await connection.get_children("/group/of")


async def test_fake_zookeeper_client_get_watch_on_child() -> None:
    client = FakeZooKeeperClient()
    change_watch = ChangeWatch()
    async with client.connect() as connection:
        await connection.create("/group/of/key_1", b"content")
        await connection.get("/group/of/key_1", watch=change_watch)
        async with client.connect() as connection_2:
            await connection_2.create("/group/of/key_1/subitem", b"content")
    assert change_watch.has_changed


async def test_fake_zookeeper_client_get_watch_on_set() -> None:
    client = FakeZooKeeperClient()
    change_watch = ChangeWatch()
    async with client.connect() as connection:
        await connection.create("/group/of/key_1", b"content")
        await connection.get("/group/of/key_1", watch=change_watch)
        async with client.connect() as connection_2:
            await connection_2.set("/group/of/key_1", b"new content")
    assert change_watch.has_changed


async def test_fake_zookeeper_client_get_children_watch() -> None:
    client = FakeZooKeeperClient()
    change_watch = ChangeWatch()
    async with client.connect() as connection:
        await connection.create("/group/of/key_1", b"content")
        await connection.get_children("/group/of", watch=change_watch)
        async with client.connect() as connection_2:
            await connection_2.create("/group/of/key_2", b"content")
    assert change_watch.has_changed


async def test_fake_zookeeper_transaction() -> None:
    client = FakeZooKeeperClient()
    async with client.connect() as connection:
        transaction = connection.transaction()
        transaction.create("/key_1", b"content")
        transaction.create("/key_2", b"content")
        await transaction.commit()
        assert await connection.get("/key_1") == b"content"
        assert await connection.get("/key_2") == b"content"


async def test_fake_zookeeper_transaction_is_atomic() -> None:
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


async def test_fake_zookeeper_transaction_does_not_implicitly_create_parents() -> None:
    client = FakeZooKeeperClient()
    async with client.connect() as connection:
        transaction = connection.transaction()
        transaction.create("/path/to/key_1", b"content")
        with pytest.raises(TransactionError):
            await transaction.commit()


async def test_fake_zookeeper_transaction_generates_trigger() -> None:
    client = FakeZooKeeperClient()
    change_watch = ChangeWatch()
    async with client.connect() as connection:
        await connection.create("/key_1", b"content")
        await connection.get_children("/", watch=change_watch)
        transaction = connection.transaction()
        transaction.create("/key_2", b"content")
        await transaction.commit()
    assert change_watch.has_changed
