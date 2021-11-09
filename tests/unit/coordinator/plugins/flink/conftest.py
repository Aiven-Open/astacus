"""
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""
from astacus.coordinator.plugins.zookeeper import KazooZooKeeperClient
from docker import DockerClient
from docker.models.containers import Container
from docker.models.networks import Network
from uuid import uuid4

import logging
import pytest

logger = logging.getLogger()


@pytest.fixture(scope="session", name="docker_client")
def docker_client_fixture() -> DockerClient:
    return DockerClient.from_env()


@pytest.fixture(scope="session", name="resource_postfix")
def resource_postfix_fixture() -> str:
    return str(uuid4()).partition("-")[0]


@pytest.fixture(scope="session", name="network")
def network_fixture(docker_client: DockerClient, resource_postfix: str) -> Network:
    _network = docker_client.networks.create(name=f"network-{resource_postfix}")
    yield _network
    _network.remove()


@pytest.fixture(scope="session", name="zookeeper")
def zookeeper_fixture(docker_client: DockerClient, network: Network, resource_postfix: str) -> Container:
    zookeeper_container = docker_client.containers.run(
        image="zookeeper",
        ports={"2181/tcp": "2181/tcp"},
        network=network.name,
        name=f"zookeeper-{resource_postfix}",
        hostname="zookeeper",
        environment={"ZOOKEEPER_CLIENT_PORT": 2181},
        detach=True,
    )
    yield zookeeper_container
    zookeeper_container.remove(force=True)


@pytest.fixture(scope="session", name="zk_client")
def zk_client_fixture(zookeeper: Container) -> KazooZooKeeperClient:
    has_started = False
    zk_logs = set()
    while not has_started:
        log_line = zookeeper.logs(tail=1).decode("UTF-8").strip()
        if log_line in zk_logs:
            pass
        else:
            zk_logs.add(log_line)
            logger.info(log_line)
        if "Started" in log_line:
            has_started = True
            logging.info("ZooKeeper has started")

    client = KazooZooKeeperClient(["127.0.0.1"], 1000)
    return client
