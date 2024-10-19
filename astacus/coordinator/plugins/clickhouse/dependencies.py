"""Copyright (c) 2021 Aiven Ltd
See LICENSE for details.
"""

from astacus.coordinator.plugins.clickhouse.manifest import AccessEntity, Table
from collections.abc import Callable, Hashable, Sequence
from typing import TypeVar

import graphlib
import re
import uuid

Node = TypeVar("Node")
NodeKey = TypeVar("NodeKey", bound=Hashable)


def sort_topologically(
    nodes: Sequence[Node],
    get_key: Callable[[Node], NodeKey],
    get_dependencies: Callable[[Node], Sequence[NodeKey]] = lambda x: [],
    get_dependants: Callable[[Node], Sequence[NodeKey]] = lambda x: [],
) -> list[Node]:
    """Sort elements topologically based on their dependencies."""
    sorter = graphlib.TopologicalSorter()  # type: ignore
    for element in nodes:
        element_key = get_key(element)
        sorter.add(element_key)
        for dependency in get_dependencies(element):
            sorter.add(element_key, dependency)
        for dependency in get_dependants(element):
            sorter.add(dependency, element_key)
    sort_order = list(sorter.static_order())
    return sorted(nodes, key=lambda element: sort_order.index(get_key(element)))


def tables_sorted_by_dependencies(tables: Sequence[Table]) -> Sequence[Table]:
    """Takes a list of `(database_name: str, table: Table)` and returns a new list,
    sorted in a valid order to create each table sequentially.

    This order is required because tables can depend on each other, for example
    tables using the Distributed table engine depends on their source tables.

    The `dependencies` attribute of each table must contain the list of
    `(database_name: str, table_name: str)` that depend on this table.
    """
    return sort_topologically(
        tables, get_key=lambda table: (table.database, table.name), get_dependants=lambda table: table.dependencies
    )


def access_entities_sorted_by_dependencies(access_entities: Sequence[AccessEntity]) -> Sequence[AccessEntity]:
    """Takes a list of `AccessEntity` and returns a new list, sorted in a valid order to
    create each entity sequentially.

    This order is required because entities can depend on each other. Most of them could
    easily be sorted by using only the entity type (roles depends on users, so all user
    should be created before all roles). However, some entity types are more complicated:
    roles can depend on other roles. This forces us to use a real topological sort to
    determine the creation order.
    """
    # Unlike tables, ClickHouse does not provide a list of dependencies between entities.
    # This means we need to parse the `attach_query` of the entity to find the uuid of
    # other entities. This is unpleasant, but the quoting format of entity names and entity
    # uuids is different enough to not risk false matches.
    clickhouse_id = re.compile(rb"ID\('([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})'\)")
    return sort_topologically(
        access_entities,
        get_key=lambda entity: entity.uuid,
        get_dependencies=lambda entity: [
            uuid.UUID(uuid_bytes.decode()) for uuid_bytes in clickhouse_id.findall(entity.attach_query)
        ],
    )
