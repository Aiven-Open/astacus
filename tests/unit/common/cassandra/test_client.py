"""
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""

from astacus.common.cassandra.config import CassandraClientConfiguration
from pytest_mock import MockFixture

import astacus.common.cassandra.client as client_module
import pytest


def test_cassandra_session(mocker: MockFixture) -> None:
    ccluster = mocker.MagicMock()
    csession = mocker.MagicMock()
    session = client_module.CassandraSession(cluster=ccluster, session=csession)

    ccluster.metadata = 42
    assert session.cluster_metadata == 42

    session.cluster_refresh_schema_metadata(max_schema_agreement_wait=7.0)
    assert ccluster.refresh_schema_metadata.called_with(max_schema_agreement_wait=7.0)

    test_multi_statement = """
first
    part;


second!;
"""

    session.execute_on_all(test_multi_statement)
    assert len(csession.execute.mock_calls) == 2


def create_client(mocker: MockFixture, ssl: bool = False) -> client_module.CassandraClient:
    mocker.patch.object(client_module, "Cluster")
    mocker.patch.object(client_module, "WhiteListRoundRobinPolicy")

    config = CassandraClientConfiguration(
        hostnames=["none"],
        port=42,
        username="dummy",
        password="",
        ca_cert_path="/path" if ssl else None,
    )
    return client_module.CassandraClient(config)


@pytest.mark.parametrize("ssl", [False, True])
def test_cassandra_client(mocker: MockFixture, ssl: bool) -> None:
    client: client_module.CassandraClient = create_client(mocker, ssl=ssl)
    with client.connect() as session:
        assert isinstance(session, client_module.CassandraSession)


@pytest.mark.asyncio
async def test_cassandra_client_run(mocker: MockFixture):
    client = create_client(mocker)

    def test_fun(cas):
        return 42

    assert await client.run_sync(test_fun) == 42
