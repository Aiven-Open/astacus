"""
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""
from astacus.coordinator.plugins.zookeeper import ZooKeeperConnection
from kazoo.client import EventType, WatchedEvent
from typing import Optional, Sequence

import asyncio
import dataclasses


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
