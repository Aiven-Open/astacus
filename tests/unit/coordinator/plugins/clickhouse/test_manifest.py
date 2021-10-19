"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from astacus.coordinator.plugins.clickhouse.manifest import Table

import pytest
import uuid

pytestmark = [pytest.mark.clickhouse]


@pytest.mark.parametrize(
    "engine,is_replicated,requires_freezing", [
        ("AggregatingMergeTree", False, True),
        ("Buffer", False, False),
        ("CollapsingMergeTree", False, True),
        ("COSN", False, False),
        ("Dictionary", False, False),
        ("Distributed", False, False),
        ("EmbeddedRocksDB", False, False),
        ("ExternalDistributed", False, False),
        ("File", False, False),
        ("GenerateRandom", False, False),
        ("GraphiteMergeTree", False, True),
        ("HDFS", False, False),
        ("JDBC", False, False),
        ("Join", False, False),
        ("Kafka", False, False),
        ("LiveView", False, False),
        ("Log", False, False),
        ("MaterializedView", False, False),
        ("Memory", False, False),
        ("Merge", False, False),
        ("MergeTree", False, True),
        ("MongoDB", False, False),
        ("MySQL", False, False),
        ("Null", False, False),
        ("ODBC", False, False),
        ("PostgreSQL", False, False),
        ("RabbitMQ", False, False),
        ("ReplacingMergeTree", False, True),
        ("ReplicatedAggregatingMergeTree", True, True),
        ("ReplicatedCollapsingMergeTree", True, True),
        ("ReplicatedGraphiteMergeTree", True, True),
        ("ReplicatedMergeTree", True, True),
        ("ReplicatedReplacingMergeTree", True, True),
        ("ReplicatedSummingMergeTree", True, True),
        ("ReplicatedVersionedCollapsingMergeTree", True, True),
        ("S3", False, False),
        ("Set", False, False),
        ("StripeLog", False, False),
        ("SummingMergeTree", False, True),
        ("TinyLog", False, False),
        ("URL", False, False),
        ("VersionedCollapsingMergeTree", False, True),
        ("View", False, False),
    ]
)
def test_clickhouse_table_attributes(engine: str, is_replicated: bool, requires_freezing: bool) -> None:
    table = Table(database="db", name="name", uuid=uuid.UUID(int=0), engine=engine, create_query="")
    assert table.is_replicated is is_replicated
    assert table.requires_freezing is requires_freezing


def test_table_escaped_identifier() -> None:
    table = Table(database="débé", name="na`me", uuid=uuid.UUID(int=0), engine="DontCare", create_query="")
    assert table.escaped_sql_identifier == "`débé`.`na\\`me`"
