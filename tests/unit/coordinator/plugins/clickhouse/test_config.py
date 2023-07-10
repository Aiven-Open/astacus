"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from astacus.coordinator.plugins.clickhouse.client import HttpClickHouseClient
from astacus.coordinator.plugins.clickhouse.config import ClickHouseConfiguration, ClickHouseNode
from astacus.coordinator.plugins.clickhouse.plugin import get_clickhouse_clients, get_zookeeper_client
from astacus.coordinator.plugins.zookeeper import KazooZooKeeperClient, KazooZooKeeperConnection
from astacus.coordinator.plugins.zookeeper_config import (
    ZooKeeperConfiguration,
    ZooKeeperConfigurationUser,
    ZooKeeperNode,
    ZooKeeperUser,
)
from kazoo.client import KazooClient
from typing import cast, List

import pytest

pytestmark = [pytest.mark.clickhouse]


def test_get_zookeeper_client() -> None:
    configuration = ZooKeeperConfiguration(
        nodes=[ZooKeeperNode(host="localhost", port=5555), ZooKeeperNode(host="::1", port=5556)]
    )
    client = cast(KazooZooKeeperClient, get_zookeeper_client(configuration))
    assert client.hosts == ["localhost:5555", "[::1]:5556"]


def test_get_authenticated_zookeeper_client() -> None:
    configuration = ZooKeeperConfiguration(
        nodes=[ZooKeeperNode(host="::1", port=5556)],
        user=ZooKeeperConfigurationUser(username="local-user", password="secret"),
    )
    client = get_zookeeper_client(configuration)
    assert client is not None and isinstance(client, KazooZooKeeperClient)
    assert client.user is not None and isinstance(client.user, ZooKeeperUser)
    connection = client.connect()
    assert connection is not None and isinstance(connection, KazooZooKeeperConnection)
    assert connection.client is not None and isinstance(connection.client, KazooClient)
    assert connection.client.auth_data == {("digest", "local-user:secret")}


def test_default_zookeeper_client_timeout_is_10secs() -> None:
    configuration = ZooKeeperConfiguration(nodes=[])
    client = cast(KazooZooKeeperClient, get_zookeeper_client(configuration))
    assert client.timeout == 10


def test_get_clickhouse_clients() -> None:
    configuration = ClickHouseConfiguration(
        username="user",
        password="password",
        nodes=[ClickHouseNode(host=f"n{i}.example.org", port=8123 + i) for i in range(3)],
    )
    clients = cast(List[HttpClickHouseClient], get_clickhouse_clients(configuration))
    assert [client.host for client in clients] == [node.host for node in configuration.nodes]
    assert [client.port for client in clients] == [node.port for node in configuration.nodes]
    for client in clients:
        assert client.username == configuration.username
        assert client.password == configuration.password
