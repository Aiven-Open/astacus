"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details

This client code is originally from Cashew

"""

from .config import CassandraClientConfiguration
from astacus.common.exceptions import TransientException
from cassandra import ConsistencyLevel, metadata as cm
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, NoHostAvailable, Session
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.query import SimpleStatement
from contextlib import contextmanager
from ssl import CERT_REQUIRED
from starlette.concurrency import run_in_threadpool
from typing import Any, Callable, Generator

import logging

logger = logging.getLogger(__name__)


class NoHostAvailableException(TransientException):
    pass


class CassandraSession:
    def __init__(self, cluster: Cluster, session: Session):
        self._cluster = cluster
        self._session = session

    @property
    def cluster_metadata(self) -> cm.Metadata:
        return self._cluster.metadata

    def cluster_refresh_schema_metadata(self, *, max_schema_agreement_wait: float) -> None:
        self._cluster.refresh_schema_metadata(max_schema_agreement_wait=max_schema_agreement_wait)

    def execute(self, stmt: str) -> Any:
        return self._session.execute(SimpleStatement(stmt, consistency_level=ConsistencyLevel.ONE))

    def execute_on_all(self, statement: str) -> None:
        # backup schema for at least a table may consist of multiple statements
        for stmt in statement.split(";\n\n"):
            # TBD: Is ;\n\n valid character in something included in schema? UDF? Hopefully not.
            self._session.execute(SimpleStatement(stmt, consistency_level=ConsistencyLevel.ALL))


class CassandraClient:
    def __init__(self, config: CassandraClientConfiguration):
        self._config = config

    @contextmanager
    def connect(self) -> Generator[CassandraSession, None, None]:
        config = self._config
        hostnames = config.get_hostnames()

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
                contact_points=hostnames,
                control_connection_timeout=15,
                port=config.get_port(),
                ssl_options=ssl_options,
                auth_provider=PlainTextAuthProvider(config.username, config.password),
                load_balancing_policy=WhiteListRoundRobinPolicy(hostnames),
            ) as cluster, cluster.connect() as session:
                yield CassandraSession(cluster, session)
        except NoHostAvailable as ex:
            if isinstance(ex.errors, dict):
                error_values = list(ex.errors.values())
            elif isinstance(ex.errors, list):
                error_values = ex.errors
            else:
                raise NotImplementedError(ex.errors) from ex

            error = error_values[0]

            logger.exception("Unexpected exception while connecting to local cassandra: %r", error)
            raise NoHostAvailableException from ex

    async def run_sync(self, fun: Callable, *args: Any, **kwargs: Any) -> Any:
        def run() -> Any:
            with self.connect() as cas:
                return fun(cas, *args, **kwargs)

        return await run_in_threadpool(run)
