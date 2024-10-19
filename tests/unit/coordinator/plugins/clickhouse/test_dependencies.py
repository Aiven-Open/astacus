"""Copyright (c) 2021 Aiven Ltd
See LICENSE for details.
"""

from astacus.coordinator.plugins.clickhouse.dependencies import (
    access_entities_sorted_by_dependencies,
    tables_sorted_by_dependencies,
)
from astacus.coordinator.plugins.clickhouse.manifest import AccessEntity, Table

import pytest
import uuid

pytestmark = [pytest.mark.clickhouse]


def test_tables_sorted_by_dependencies() -> None:
    t1 = Table(
        database=b"db_one",
        name=b"t1",
        engine="DontCare",
        uuid=uuid.UUID(int=0),
        create_query=b"",
        dependencies=[(b"db_two", b"t4")],
    )
    t2 = Table(
        database=b"db_one",
        name=b"t2",
        engine="DontCare",
        uuid=uuid.UUID(int=0),
        create_query=b"",
        dependencies=[],
    )
    t3 = Table(
        database=b"db_one",
        name=b"t3",
        engine="DontCare",
        uuid=uuid.UUID(int=0),
        create_query=b"",
        dependencies=[(b"db_one", b"t2")],
    )
    t4 = Table(
        database=b"db_two",
        name=b"t4",
        engine="DontCare",
        uuid=uuid.UUID(int=0),
        create_query=b"",
        dependencies=[(b"db_one", b"t2"), (b"db_one", b"t3")],
    )
    assert tables_sorted_by_dependencies([t1, t2, t3, t4]) == [t1, t4, t3, t2]
    assert tables_sorted_by_dependencies([t2, t3, t4, t1]) == [t1, t4, t3, t2]


def test_dangling_table_dependency_doesnt_crash() -> None:
    t1 = Table(
        database=b"db_one",
        name=b"t1",
        engine="DontCare",
        uuid=uuid.UUID(int=0),
        create_query=b"",
        dependencies=[(b"db_two", b"t4")],
    )
    assert tables_sorted_by_dependencies([t1]) == [t1]


def test_access_entities_sorted_by_dependencies() -> None:
    a1 = AccessEntity(
        type="P",
        uuid=uuid.UUID("00000000-abcd-0000-0000-000000000001"),
        name=b"a1",
        attach_query=b"""
            ATTACH ROW POLICY a1 TO ID('00000000-abcd-0000-0000-000000000003'));
        """,
    )
    a2 = AccessEntity(
        type="R",
        uuid=uuid.UUID("00000000-abcd-0000-0000-000000000002"),
        name=b"a2",
        attach_query=b"""
            ATTACH ROLE a2;
            ATTACH GRANT ID('00000000-abcd-0000-0000-000000000004') to a2;
        """,
    )
    a3 = AccessEntity(
        type="U",
        uuid=uuid.UUID("00000000-abcd-0000-0000-000000000003"),
        name=b"a3",
        attach_query=b"""
            ATTACH USER a3;
            ATTACH GRANT ID('00000000-abcd-0000-0000-000000000002') to a3;
            ATTACH GRANT ID('00000000-abcd-0000-0000-000000000004') to a3;
        """,
    )
    a4 = AccessEntity(
        type="R", uuid=uuid.UUID("00000000-0000-abcd-0000-000000000004"), name=b"a4", attach_query=b"ATTACH ROLE a4"
    )
    assert access_entities_sorted_by_dependencies([a1, a2, a3, a4]) == [a4, a2, a3, a1]
    assert access_entities_sorted_by_dependencies([a2, a4, a3, a1]) == [a4, a2, a3, a1]


def test_dangling_access_entities_doesnt_crash() -> None:
    # ClickHouse takes care of the cohesiveness of the access entity graph, but we can
    # imagine at least two reasons to have a partial graph :
    # - We have references to an entity from another source of users, such as an admin
    #   user hard-coded in users.xml.
    # - A future ClickHouse feature uses IDs that refer to something else than an access
    #   entity, and we accidentally pick up on that id when parsing the attach_query.
    a1 = AccessEntity(
        type="P",
        uuid=uuid.UUID("00000000-abcd-0000-0000-000000000001"),
        name=b"a1",
        attach_query=b"""
            ATTACH ROW POLICY a1 TO ID('00000000-abcd-0000-0000-000000000003'));
        """,
    )
    assert access_entities_sorted_by_dependencies([a1]) == [a1]
