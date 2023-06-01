"""
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""
from .client import ClickHouseClient, escape_sql_identifier, unescape_sql_string
from .macros import MacroExpansionError, Macros
from .manifest import ReplicatedDatabase
from .sql import chain_of, named_group, one_of, TokenType
from astacus.coordinator.plugins.base import StepFailedError
from astacus.coordinator.plugins.zookeeper import ZooKeeperConnection
from kazoo.client import EventType, WatchedEvent
from typing import Mapping, Optional, Sequence

import asyncio
import dataclasses
import re


@dataclasses.dataclass(frozen=True)
class DatabaseReplica:
    shard_name: str
    replica_name: str

    @property
    def full_name(self) -> str:
        return f"{self.shard_name}|{self.replica_name}"


async def sync_replicated_database(
    connection: ZooKeeperConnection,
    database_path: str,
    replicas: Sequence[DatabaseReplica],
    timeout_secs: float = 10.0,
) -> None:
    """Wait for all previously issued database mutation to be completed on all replicas.

    Database mutations include creating, deleting, altering tables but does not include inserting or
    removing data in these tables.
    """
    # There is a "pointer" for each database, a sequentially increasing number for each database mutation.
    # Each replica also has a pointer that tracks which mutation are applied.
    # We read the database pointer once, then wait for all replica pointers to reach that value.
    # We do not wait for them to be completely in sync, we might have more mutation after our measurement
    # of the database pointer. We just want to ensure that after this function returns, the replica has a state
    # at least as advanced as any other replica had before this function started.
    database_max_ptr_path = f"{database_path}/max_log_ptr"
    database_max_ptr_bytes = await connection.get(database_max_ptr_path)
    database_max_ptr = int(database_max_ptr_bytes)
    replica_ptrs: list[Optional[int]] = [None for _ in replicas]
    condition = asyncio.Condition()

    async def update_replica_ptr(replica_index: int, replica_ptr_path: str) -> None:
        loop = asyncio.get_running_loop()

        def watch_node(event: WatchedEvent) -> None:
            if event.type == EventType.CHANGED:
                asyncio.run_coroutine_threadsafe(update_replica_ptr(replica_index, replica_ptr_path), loop)

        replica_ptr_bytes = await connection.get(replica_ptr_path, watch_node)
        replica_ptr = int(replica_ptr_bytes)
        replica_ptrs[replica_index] = replica_ptr
        if replica_ptr >= database_max_ptr:
            async with condition:
                condition.notify()

    for index, replica in enumerate(replicas):
        replica_ptr_path = f"{database_path}/replicas/{replica.full_name}/log_ptr"
        await update_replica_ptr(index, replica_ptr_path)

    def replicas_are_synced() -> bool:
        return all(ptr is not None and ptr >= database_max_ptr for ptr in replica_ptrs)

    async with condition:
        if not await asyncio.wait_for(condition.wait_for(replicas_are_synced), timeout=timeout_secs):
            replica_names_str = ", ".join(repr(replica.full_name) for replica in replicas)
            raise TimeoutError(
                f"Timed out after {timeout_secs:.3f}s while waiting for "
                f"replicas {replica_names_str} in database {database_path!r} "
                f"to all reach mutation pointer {database_max_ptr} (last pointer values: {replica_ptrs})"
            )


async def get_shard_and_replica(clickhouse_client: ClickHouseClient, database_name: bytes) -> tuple[bytes, bytes]:
    escaped_database_name = escape_sql_identifier(database_name)
    db_rows = await clickhouse_client.execute(f"SHOW CREATE DATABASE {escaped_database_name}".encode())
    assert isinstance(db_rows[0][0], str)
    create_database_query = db_rows[0][0].encode()
    matched = re.match(
        chain_of(
            b"CREATE DATABASE",
            one_of(TokenType.RawIdentifier, TokenType.QuotedIdentifier),
            b"ENGINE",
            TokenType.Equal,
            b"Replicated",
            TokenType.OpenParenthesis,
            named_group("zookeeper_path", TokenType.String),
            TokenType.Comma,
            named_group("shard", TokenType.String),
            TokenType.Comma,
            named_group("replica", TokenType.String),
            TokenType.CloseParenthesis,
        ),
        create_database_query,
    )
    if not matched:
        raise StepFailedError(f"Could not parse CREATE DATABASE query for {database_name!r}")
    return unescape_sql_string(matched.group("shard")), unescape_sql_string(matched.group("replica"))


def get_databases_replicas(
    replicated_databases: Sequence[ReplicatedDatabase], servers_macros: Sequence[Macros]
) -> Mapping[bytes, Sequence[DatabaseReplica]]:
    """
    Get the list of replicas for each database.

    This applies macro substitution, using the macro values of each server, to the
    `shard` and `replica` values of each `ReplicatedDatabase`.
    """
    databases_replicas: dict[bytes, Sequence[DatabaseReplica]] = {}
    for database in replicated_databases:
        replicas: list[DatabaseReplica] = []
        for server_index, macros in enumerate(servers_macros, start=1):
            try:
                replicas.append(
                    DatabaseReplica(
                        shard_name=macros.expand(database.shard).decode(),
                        replica_name=macros.expand(database.replica).decode(),
                    )
                )
            except (MacroExpansionError, UnicodeDecodeError) as e:
                raise StepFailedError(f"Error in macro of server {server_index}: {e}") from e
        databases_replicas[database.name] = replicas
    return databases_replicas
