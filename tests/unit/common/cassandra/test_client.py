"""
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""

from astacus.common.cassandra.config import CassandraClientConfiguration

import pytest


@pytest.fixture(name="client_module", scope="session")
def fixture_client_module():
    return pytest.importorskip("astacus.common.cassandra.client", reason="Cassandra driver is not available")


def test_cassandra_session(client_module, mocker):
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


def create_client(client_module, mocker, ssl=False):
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
def test_cassandra_client(client_module, mocker, ssl):
    client: client_module.CassandraClient = create_client(client_module, mocker, ssl=ssl)
    with client.connect() as session:
        assert isinstance(session, client_module.CassandraSession)


@pytest.mark.asyncio
async def test_cassandra_client_run(client_module, mocker):
    client = create_client(client_module, mocker)

    def test_fun(cas):
        return 42

    assert await client.run_sync(test_fun) == 42
