"""
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""
from astacus.common.utils import AstacusModel, build_netloc
from astacus.coordinator.plugins.zookeeper import KazooZooKeeperClient, ZooKeeperClient
from typing import List


class ZooKeeperNode(AstacusModel):
    host: str
    port: int


class ZooKeeperConfiguration(AstacusModel):
    nodes: List[ZooKeeperNode] = []


def get_zookeeper_client(configuration: ZooKeeperConfiguration) -> ZooKeeperClient:
    return KazooZooKeeperClient(hosts=[build_netloc(node.host, node.port) for node in configuration.nodes])
