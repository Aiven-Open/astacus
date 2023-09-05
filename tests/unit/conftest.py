"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from astacus.common import utils
from astacus.common.cassandra.config import CassandraClientConfiguration, SNAPSHOT_NAME
from io import StringIO
from pathlib import Path

import logging
import pytest
import tempfile

logger = logging.getLogger(__name__)


@pytest.fixture(name="utils_http_request_list")
def fixture_utils_http_request_list(mocker):
    result_list = []

    def _http_request(*args, **kwargs):
        logger.info("utils_http_request_list %r %r", args, kwargs)
        assert result_list, f"Unable to serve request {args!r} {kwargs!r}"
        r = result_list.pop(0)
        if isinstance(r, dict):
            if "args" in r:
                assert list(r["args"]) == list(args)
            if "kwargs" in r:
                assert r["kwargs"] == kwargs
            if "result" in r:
                return r["result"]
            return None
        if r is None:
            return None
        raise NotImplementedError(f"Unknown item in utils_request_list: {r!r}")

    mocker.patch.object(utils, "http_request", new=_http_request)
    yield result_list
    assert not result_list, f"Unconsumed requests: {result_list!r}"


class CassandraTestConfig:
    def __init__(self, *, mocker, tmpdir):
        self.root = Path(tmpdir) / "root"

        # Create fake snapshot files that we want to see being moved/removed/..
        keyspace_path = self.root / "data" / "dummyks"
        self.snapshot_path = keyspace_path / "dummytable-123" / "snapshots" / SNAPSHOT_NAME
        self.snapshot_path.mkdir(parents=True)

        other_table_path = keyspace_path / "anothertable-345" / "snapshots" / SNAPSHOT_NAME
        other_table_path.mkdir(parents=True)
        (other_table_path / "data.file").write_text("data")

        self.backup_path = keyspace_path / "dummytable-123" / "backups"
        self.backup_path.mkdir(parents=True)

        system_schema_path = self.root / "data" / "system_schema" / "tables-789" / "snapshots" / SNAPSHOT_NAME
        system_schema_path.mkdir(parents=True)
        (system_schema_path / "data.file").write_text("schema")

        self.other_snapshot_path = keyspace_path / "dummytable-123" / "snapshots" / f"not{SNAPSHOT_NAME}"
        self.other_snapshot_path.mkdir(parents=True)

        self.cassandra_conf = Path(tmpdir / "cassandra.yaml")
        self.cassandra_conf.write_text(
            """
listen_address: 127.0.0.1
"""
        )

        (self.snapshot_path / "asdf").write_text("foobar")
        (keyspace_path / "dummytable-234").mkdir()
        (keyspace_path / "anothertable-789").mkdir()

        (self.backup_path / "incremental.backup").write_text("delta")

        named_temporary_file = mocker.patch.object(tempfile, "NamedTemporaryFile")
        self.fake_conffile = StringIO()
        named_temporary_file.return_value.__enter__.return_value = self.fake_conffile
        self.fake_conffile.name = "tempfilename"

        self.cassandra_client_config = CassandraClientConfiguration(
            hostnames=["127.0.0.1"], port=42, username="dummy", password="", config_path=self.cassandra_conf
        )


@pytest.fixture(name="cassandra_test_config")
def fixture_cassandra_test_config(mocker, tmpdir):
    yield CassandraTestConfig(mocker=mocker, tmpdir=tmpdir)
