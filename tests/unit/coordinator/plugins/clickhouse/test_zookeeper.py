"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from astacus.coordinator.plugins.clickhouse.zookeeper import ChangeWatch, FakeZooKeeperClient, NodeExistsError, NoNodeError

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
