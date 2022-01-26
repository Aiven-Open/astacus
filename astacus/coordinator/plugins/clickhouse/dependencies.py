"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from astacus.coordinator.plugins.clickhouse.manifest import AccessEntity, Table
from typing import List

# noinspection PyCompatibility
import graphlib
import re
import uuid


def tables_sorted_by_dependencies(tables: List[Table]) -> List[Table]:
    """
    Takes a list of `(database_name: str, table: Table)` and returns a new list,
    sorted in a valid order to create each table sequentially.

    This order is required because tables can depend on each other, for example
    tables using the Distributed table engine depends on their source tables.

    The `dependencies` attribute of each table must contain the list of
    `(database_name: str, table_name: str)` that depend on this table.
    """
    sorter = graphlib.TopologicalSorter()
    for table in tables:
        sorter.add((table.database, table.name))
        for dependency in table.dependencies:
            sorter.add(dependency, (table.database, table.name))
    sort_order = list(sorter.static_order())
    return sorted(tables, key=lambda t: sort_order.index((t.database, t.name)))


def access_entities_sorted_by_dependencies(access_entities: List[AccessEntity]) -> List[AccessEntity]:
    """
    Takes a list of `AccessEntity` and returns a new list, sorted in a valid order to
    create each entity sequentially.

    This order is required because entities can depend on each other. Most of them could
    easily be sorted by using only the entity type (roles depends on users, so all user
    should be created before all roles). However, some entity types are more complicated:
    roles can depend on other roles. This forces us to use a real topological sort to
    determine the creation order.
    """
    sorter = graphlib.TopologicalSorter()
    # Unlike tables, ClickHouse does not provide a list of dependencies between entities.
    # This means we need to parse the `attach_query` of the entity to find the uuid of
    # other entities. This is unpleasant, but the quoting format of entity names and entity
    # uuids is different enough to not risk false matches.
    clickhouse_id = re.compile(rb"ID\('([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})'\)")
    for entity in access_entities:
        sorter.add(entity.uuid)
        for uuid_bytes in clickhouse_id.findall(entity.attach_query):
            sorter.add(entity.uuid, uuid.UUID(uuid_bytes.decode()))
    sort_order = list(sorter.static_order())
    return sorted(access_entities, key=lambda e: sort_order.index(e.uuid))
