"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from astacus.common.cassandra.config import CassandraClientConfiguration, SNAPSHOT_NAME
from io import StringIO
from pathlib import Path
from pytest_mock import MockerFixture

import logging
import py
import pytest
import tempfile

logger = logging.getLogger(__name__)


class CassandraTestConfig:
    def __init__(self, *, mocker: MockerFixture, tmpdir: py.path.local):
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
def fixture_cassandra_test_config(mocker: MockerFixture, tmpdir: py.path.local) -> CassandraTestConfig:
    return CassandraTestConfig(mocker=mocker, tmpdir=tmpdir)
