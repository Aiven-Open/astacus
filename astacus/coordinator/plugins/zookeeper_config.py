"""
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""

from astacus.common.utils import AstacusModel, build_netloc
from astacus.coordinator.plugins.zookeeper import KazooZooKeeperClient, ZooKeeperClient, ZooKeeperUser
from collections.abc import Sequence
from pydantic.v1 import SecretStr


class ZooKeeperNode(AstacusModel):
    host: str
    port: int


class ZooKeeperConfigurationUser(AstacusModel):
    username: str
    password: SecretStr

    def to_dataclass(self) -> ZooKeeperUser:
        return ZooKeeperUser(self.username, self.password.get_secret_value())


class ZooKeeperConfiguration(AstacusModel):
    nodes: Sequence[ZooKeeperNode] = []
    user: ZooKeeperConfigurationUser | None = None


def get_zookeeper_client(configuration: ZooKeeperConfiguration) -> ZooKeeperClient:
    user = configuration.user.to_dataclass() if configuration.user else None
    return KazooZooKeeperClient(hosts=[build_netloc(node.host, node.port) for node in configuration.nodes], user=user)
