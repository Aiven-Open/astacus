"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""

from astacus.common import ipc
from astacus.common.cassandra.config import CassandraClientConfig, SNAPSHOT_NAME
from astacus.node.config import CassandraNodeConfig
from pathlib import Path

import pytest
import subprocess


@pytest.mark.parametrize("subop", ipc.CassandraSubOp)
def test_api_cassandra_subop(app, client, mocker, subop, tmpdir):
    # pylint: disable=too-many-locals

    # Create fake snapshot files that we want to see being moved/removed/..
    root = app.state.node_config.root
    keyspace_path = root / "data" / "dummyks"
    snapshot_path = keyspace_path / "dummytable-123" / "snapshots" / SNAPSHOT_NAME
    snapshot_path.mkdir(parents=True)

    other_snapshot_path = keyspace_path / "dummytable-123" / "snapshots" / f"not{SNAPSHOT_NAME}"
    other_snapshot_path.mkdir(parents=True)

    cassandra_conf = Path(tmpdir / "cassandra.yaml")
    cassandra_conf.write_text("foo: bar")

    (snapshot_path / "asdf").write_text("foobar")
    (keyspace_path / "dummytable-234").mkdir()

    req_json = {"tokens": ["42", "7"]}
    url = f"/node/cassandra/{subop}"

    # Without lock, we shouldn't be able to do use the endpoint
    response = client.post(url, json=req_json)
    assert response.status_code == 409, response.json()

    response = client.post("/node/lock?locker=x&ttl=10")
    assert response.status_code == 200, response.json()

    subprocess_call = mocker.patch.object(subprocess, "call")
    # No Cassandra configuration in node_config yet -> should fail
    with pytest.raises(AssertionError):
        response = client.post(url, json=req_json)

    cassandra_node_config = CassandraNodeConfig(
        client=CassandraClientConfig(hostnames=["127.0.0.1"], port=42, username="dummy", password=""),
        nodetool_command=["nodetool"],
        start_command=["dummy-start"],
        stop_command=["dummy-stop"],
        config_path=cassandra_conf,
    )
    app.state.node_config.cassandra = cassandra_node_config

    # Now the app is actually correctly configured

    # Mocking this is more trouble than it is worth
    if subop == ipc.CassandraSubOp.get_schema_hash:
        return

    response = client.post(url, json=req_json)
    assert response.status_code == 200, response.json()
    status_url = response.json()["status_url"]

    if subop == ipc.CassandraSubOp.restore_snapshot:
        # The file should be moved from dummytable-123 snapshot dir to
        # dummytable-234
        assert (root / "data" / "dummyks" / "dummytable-234" / "asdf").read_text() == "foobar"

        subprocess_call.assert_any_call(cassandra_node_config.start_command)
        subprocess_call.assert_any_call(cassandra_node_config.stop_command)
    elif subop == ipc.CassandraSubOp.remove_snapshot:
        assert not snapshot_path.exists()
        assert other_snapshot_path.exists()
    elif subop == ipc.CassandraSubOp.take_snapshot:
        subprocess_call.assert_any_call(cassandra_node_config.nodetool_command + ["snapshot", "-t", SNAPSHOT_NAME])
    elif subop == ipc.CassandraSubOp.start_cassandra:
        subprocess_call.assert_any_call(cassandra_node_config.start_command)
        assert cassandra_conf.read_text() == """auto_bootstrap: false
foo: bar
initial_token: 42, 7
"""
    else:
        raise NotImplementedError(subop)

    # Ensure that besides the measurable internal effect, the API also
    # returns 200 for the status-url + the progress has completed.
    response = client.get(status_url)
    assert response.status_code == 200, response.json()
    progress = response.json()["progress"]
    assert not progress["failed"]
    assert progress["final"]
