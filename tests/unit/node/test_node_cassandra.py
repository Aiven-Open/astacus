"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from astacus.common import ipc
from astacus.common.cassandra.config import SNAPSHOT_NAME
from astacus.common.cassandra.utils import SYSTEM_KEYSPACES
from astacus.node.api import READONLY_SUBOPS
from astacus.node.config import CassandraAccessLevel, CassandraNodeConfig
from collections.abc import Callable, Sequence
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pathlib import Path
from pytest_mock import MockerFixture
from requests import Response
from tests.unit.conftest import CassandraTestConfig
from types import ModuleType

import msgspec
import pytest
import subprocess


@pytest.fixture(name="astacus_node_cassandra", autouse=True)
def fixture_astacus_node_cassandra() -> ModuleType:
    return pytest.importorskip("astacus.node.cassandra")


class CassandraTestEnv(CassandraTestConfig):
    cassandra_node_config: CassandraNodeConfig

    def __init__(self, *, app: FastAPI, client: TestClient, mocker: MockerFixture, tmp_path: Path) -> None:
        super().__init__(mocker=mocker, tmp_path=tmp_path)
        self.app = app
        self.client = client

    def lock(self) -> None:
        response = self.client.post("/node/lock?locker=x&ttl=10")
        assert response.status_code == 200, response.json()

    def post(self, *, subop: str, **kwargs) -> Response:
        url = f"/node/cassandra/{subop}"
        return self.client.post(url, **kwargs)

    def get_status(self, response: Response) -> Response:
        assert response.status_code == 200, response.json()
        status_url = response.json()["status_url"]
        return self.client.get(status_url)

    def setup_cassandra_node_config(self) -> None:
        self.cassandra_node_config = CassandraNodeConfig(
            client=self.cassandra_client_config,
            nodetool_command=["nodetool"],
            start_command=["dummy-start"],
            stop_command=["dummy-stop"],
            access_level=CassandraAccessLevel.write,
        )
        self.app.state.node_config.cassandra = self.cassandra_node_config


@pytest.fixture(name="ctenv")
def fixture_ctenv(app: FastAPI, client: TestClient, mocker: MockerFixture, tmp_path: Path) -> CassandraTestEnv:
    return CassandraTestEnv(app=app, client=client, mocker=mocker, tmp_path=tmp_path)


@pytest.mark.parametrize(
    "subop", set(ipc.CassandraSubOp) - {ipc.CassandraSubOp.get_schema_hash, ipc.CassandraSubOp.restore_sstables}
)
def test_api_cassandra_subop(
    app: FastAPI, ctenv: CassandraTestEnv, mocker: MockerFixture, subop: ipc.CassandraSubOp
) -> None:
    req_json = {"tokens": ["42", "7"]}

    # Without lock, we shouldn't be able to do use the endpoint
    response = ctenv.post(subop=subop, json=req_json)
    if response.status_code == 501:
        # If Cassandra module is missing, endpoint will return 501
        return

    assert response.status_code == 409, response.json()

    ctenv.lock()

    # No Cassandra configuration in node_config yet -> should fail
    response = ctenv.post(subop=subop, json=req_json)
    assert response.status_code == 409, response.json()

    ctenv.setup_cassandra_node_config()
    subprocess_run = mocker.patch.object(subprocess, "run")

    # Now the app is actually correctly configured

    response = ctenv.post(subop=subop, json=req_json)

    # Ensure that besides the measurable internal effect, the API also
    # returns 200 for the status-url + the progress has completed.
    status = ctenv.get_status(response)
    assert status.status_code == 200, response.json()

    progress = status.json()["progress"]
    assert not progress["failed"]
    assert progress["final"]

    if subop == ipc.CassandraSubOp.remove_snapshot:
        assert not ctenv.snapshot_path.exists()
        assert ctenv.other_snapshot_path.exists()
    elif subop == ipc.CassandraSubOp.unrestore_sstables:
        assert (ctenv.root / "data").exists()
        assert (ctenv.root / "data" / "dummyks").exists()
        assert (ctenv.root / "data" / "dummyks" / "dummytable-123").exists()
        assert {p.name for p in (ctenv.root / "data" / "dummyks" / "dummytable-123").iterdir()} == {"backups", "snapshots"}
        assert not (ctenv.root / "data" / "dummyks" / "dummytable-234").exists()
    elif subop == ipc.CassandraSubOp.start_cassandra:
        subprocess_run.assert_any_call([*ctenv.cassandra_node_config.start_command, "tempfilename"], check=True)

        assert (
            ctenv.fake_conffile.getvalue()
            == """auto_bootstrap: false
initial_token: 42, 7
listen_address: 127.0.0.1
num_tokens: 2
"""
        )
    elif subop == ipc.CassandraSubOp.stop_cassandra:
        subprocess_run.assert_any_call(ctenv.cassandra_node_config.stop_command, check=True)
    elif subop == ipc.CassandraSubOp.take_snapshot:
        subprocess_run.assert_any_call(
            [*ctenv.cassandra_node_config.nodetool_command, "snapshot", "-t", SNAPSHOT_NAME], check=True
        )
    else:
        raise NotImplementedError(subop)


@pytest.mark.parametrize("fail", [True])
def test_api_cassandra_get_schema_hash(
    ctenv: CassandraTestEnv, fail: bool, mocker: MockerFixture, astacus_node_cassandra: ModuleType
) -> None:
    # The state of API *before* these two setup steps are done is checked in the test_api_cassandra_subop
    ctenv.lock()
    ctenv.setup_cassandra_node_config()

    if not fail:
        mocker.patch.object(astacus_node_cassandra.CassandraOp, "_get_schema_hash", return_value="mockhash")

    response = ctenv.post(subop=ipc.CassandraSubOp.get_schema_hash, json={})
    status = ctenv.get_status(response)
    assert status.status_code == 200, response.json()

    progress = status.json()["progress"]
    assert not progress["failed"]
    assert progress["final"]

    schema_hash = status.json()["schema_hash"]
    if fail:
        assert not schema_hash
    else:
        assert schema_hash == "mockhash"


class TestCassandraRestoreSSTables:
    @pytest.fixture(name="make_sstables_request")
    def fixture_make_sstables_request(
        self, astacus_node_cassandra: ModuleType
    ) -> Callable[..., ipc.CassandraRestoreSSTablesRequest]:
        class DefaultedRestoreSSTablesRequest(ipc.CassandraRestoreSSTablesRequest):
            table_glob: str = astacus_node_cassandra.SNAPSHOT_GLOB
            keyspaces_to_skip: Sequence[str] = msgspec.field(default_factory=lambda: list(SYSTEM_KEYSPACES))
            match_tables_by: ipc.CassandraTableMatching = ipc.CassandraTableMatching.cfname
            expect_empty_target: bool = True

        return DefaultedRestoreSSTablesRequest

    @pytest.fixture(name="locked_ctenv", autouse=True)
    def fixture_locked_ctenv(self, ctenv: CassandraTestEnv) -> None:
        ctenv.lock()
        ctenv.setup_cassandra_node_config()

    def test_uses_glob_from_request_instead_of_default(
        self, ctenv: CassandraTestEnv, make_sstables_request: Callable[..., ipc.CassandraRestoreSSTablesRequest]
    ) -> None:
        req = make_sstables_request(table_glob=f"data/*/dummytable-123/snapshots/{SNAPSHOT_NAME}")
        self.assert_request_succeeded(ctenv, req)

        assert (ctenv.root / "data" / "dummyks" / "dummytable-234" / "asdf").read_text() == "foobar"
        assert not (ctenv.root / "data" / "dummyks" / "anothertable-789" / "data.file").exists()

    def test_skips_keyspace_if_told_to(
        self, ctenv: CassandraTestEnv, make_sstables_request: Callable[..., ipc.CassandraRestoreSSTablesRequest]
    ) -> None:
        req = make_sstables_request(keyspaces_to_skip=["dummyks"])
        self.assert_request_succeeded(ctenv, req)

        assert not (ctenv.root / "data" / "dummyks" / "dummytable-234" / "asdf").exists()
        assert (ctenv.root / "data" / "system_schema" / "tables-789" / "data.file").read_text() == "schema"

    def test_matches_tables_by_id_when_told_to(
        self, ctenv: CassandraTestEnv, make_sstables_request: Callable[..., ipc.CassandraRestoreSSTablesRequest]
    ) -> None:
        req = make_sstables_request(match_tables_by=ipc.CassandraTableMatching.cfid)
        self.assert_request_succeeded(ctenv, req)

        assert (ctenv.root / "data" / "dummyks" / "dummytable-123" / "asdf").read_text() == "foobar"
        assert not (ctenv.root / "data" / "dummyks" / "dummytable-234" / "asdf").exists()

    def test_allows_existing_files_when_told_to(
        self, ctenv: CassandraTestEnv, make_sstables_request: Callable[..., ipc.CassandraRestoreSSTablesRequest]
    ) -> None:
        req = make_sstables_request(expect_empty_target=False)
        (ctenv.root / "data" / "dummyks" / "dummytable-123" / "existing_file").write_text("exists")

        self.assert_request_succeeded(ctenv, req)

    def assert_request_succeeded(self, ctenv: CassandraTestEnv, req: ipc.CassandraRestoreSSTablesRequest) -> None:
        response = ctenv.post(subop=ipc.CassandraSubOp.restore_sstables, json=msgspec.to_builtins(req))
        status = ctenv.get_status(response)
        assert status.status_code == 200, response.json()

        progress = status.json()["progress"]
        assert not progress["failed"]
        assert progress["final"]


@pytest.mark.parametrize("dangerous_op", set(ipc.CassandraSubOp) - READONLY_SUBOPS)
def test_dangerous_ops_not_allowed_on_read_access_level(ctenv: CassandraTestEnv, dangerous_op: ipc.CassandraSubOp) -> None:
    ctenv.lock()
    ctenv.setup_cassandra_node_config()
    ctenv.cassandra_node_config.access_level = CassandraAccessLevel.read
    if dangerous_op == ipc.CassandraSubOp.restore_sstables:
        payload = {
            "table_glob": "",
            "keyspaces_to_skip": [],
            "match_tables_by": ipc.CassandraTableMatching.cfid,
            "expect_empty_target": False,
        }
    else:
        payload = {}
    response = ctenv.post(subop=dangerous_op, json=payload)
    assert response.status_code == 403, response.json()
