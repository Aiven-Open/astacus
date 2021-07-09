"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details

This client code is originally from Cashew

"""

from .config import CassandraClientConfig
from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, NoHostAvailable, Session
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.query import SimpleStatement
from contextlib import contextmanager
from ssl import CERT_REQUIRED
from typing import Callable, Optional

import logging

logger = logging.getLogger(__name__)


class CassandraClient:
    def __init__(self, *, cluster: Cluster, session: Session):
        self.cluster = cluster
        self.session = session

    def execute_on_all(self, statement: str) -> None:
        # backup schema for at least a table may consist of multiple statements
        for stmt in statement.split(";\n\n"):
            self.session.execute(SimpleStatement(stmt, consistency_level=ConsistencyLevel.ALL))


@contextmanager
def client_context(
    config: CassandraClientConfig,
    map_connection_exceptions: Optional[Callable[[str], Exception]] = None,
):
    # NB: a session is needed even though it's not directly referenced, otherwise cassandra driver wont establish a
    # control connection and refreshing metadata fails
    if config.ca_cert_path:
        ssl_options = {
            "ca_certs": config.ca_cert_path,
            "cert_reqs": CERT_REQUIRED,
        }
    else:
        # TBD what the default should be otherwise
        ssl_options = {}

    try:
        with Cluster(
            connect_timeout=15,
            contact_points=config.hostnames,
            control_connection_timeout=15,
            port=config.port,
            ssl_options=ssl_options,
            auth_provider=PlainTextAuthProvider(config.username, config.password),
            load_balancing_policy=WhiteListRoundRobinPolicy(config.hostnames),
        ) as cluster, cluster.connect() as session:
            yield CassandraClient(cluster=cluster, session=session)
    except NoHostAvailable as ex:
        if isinstance(ex.errors, dict):
            error_values = list(ex.errors.values())
        elif isinstance(ex.errors, list):
            error_values = ex.errors
        else:
            raise NotImplementedError(ex.errors) from ex

        error = error_values[0]
        if map_connection_exceptions and isinstance(error, ConnectionRefusedError):
            raise map_connection_exceptions(f"Cannot connect (yet) to local Cassandra: {error}") from ex

        logger.exception("Unexpected exception while connecting to local cassandra")
        raise error from ex
