"""
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""
from astacus.common.utils import build_netloc
from astacus.coordinator.plugins.zookeeper import KazooZooKeeperClient, ZooKeeperClient, ZooKeeperUser
from collections.abc import Sequence

import msgspec


class ZooKeeperNode(msgspec.Struct, kw_only=True, frozen=True):
    host: str
    port: int


class ZooKeeperConfigurationUser(msgspec.Struct, kw_only=True, frozen=True):
    username: str
    password: str  # faxme: secret = msgspec.field()

    def to_dataclass(self) -> ZooKeeperUser:
        return ZooKeeperUser(self.username, self.password)


class ZooKeeperConfiguration(msgspec.Struct, kw_only=True, frozen=True):
    nodes: Sequence[ZooKeeperNode] = msgspec.field(default_factory=list)
    user: ZooKeeperConfigurationUser | None = None


def get_zookeeper_client(configuration: ZooKeeperConfiguration) -> ZooKeeperClient:
    user = configuration.user.to_dataclass() if configuration.user else None
    return KazooZooKeeperClient(hosts=[build_netloc(node.host, node.port) for node in configuration.nodes], user=user)
