"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from astacus.common.exceptions import TransientException
from asyncio import to_thread
from kazoo.client import EventType, KazooClient, KeeperState, TransactionRequest, WatchedEvent
from kazoo.protocol.states import ZnodeStat
from kazoo.retry import KazooRetry
from typing import AsyncIterator, Callable, Collection, Dict, List, Optional, Tuple, Type, Union

import asyncio
import contextlib
import enum
import kazoo.exceptions
import logging

logger = logging.getLogger(__name__)


Watcher = Callable[[WatchedEvent], None]


class ZooKeeperTransaction:
    def create(self, path: str, value: bytes) -> None:
        """
        Add a create operation to the transaction.
        """
        raise NotImplementedError

    async def commit(self) -> None:
        """
        Commit the transaction.
        """
        raise NotImplementedError


class ZooKeeperConnection:
    """
    A connection to a ZooKeeper cluster.

    This is a context manager, the connection is opened when entering the context manager and closed when leaving it.

    A `ZooKeeperConnection` cannot be shared between multiple threads or multiple asyncio coroutines.
    """

    async def __aenter__(self) -> "ZooKeeperConnection":
        raise NotImplementedError

    async def __aexit__(self, *exc_info) -> None:
        raise NotImplementedError

    async def get(self, path: str, watch: Optional[Watcher] = None) -> bytes:
        """
        Returns the value of the node with the specified `path`.

        Raises `NoNodeError` if the node does not exist.
        """
        raise NotImplementedError

    async def get_children(self, path: str, watch: Optional[Watcher] = None) -> List[str]:
        """
        Returns the sorted list of all children of the given `path`.

        Raises `NoNodeError` if the node does not exist.
        """
        raise NotImplementedError

    async def try_create(self, path: str, value: bytes) -> bool:
        """
        Creates the node with the specified `path` and `value`.

        Auto-creates all parent nodes if they don't exist.

        Does nothing if the node did not already exist.

        Returns `True` if the node was created
        """
        try:
            await self.create(path, value)
            return True
        except NodeExistsError:
            return False

    async def create(self, path: str, value: bytes) -> None:
        """
        Creates the node with the specified `path` and `value`.

        Auto-creates all parent nodes if they don't exist.

        Raises `NodeExistsError` if the node already exists.
        """
        raise NotImplementedError

    async def set(self, path: str, value: bytes) -> None:
        """
        Set the `value` node with the specified `path`.

        The node must already exist.

        Raises `NoNodeError` if the node already exists.
        """
        raise NotImplementedError

    async def exists(self, path: str) -> Optional[ZnodeStat]:
        """
        Check if specified node exists.
        """
        raise NotImplementedError

    def transaction(self) -> ZooKeeperTransaction:
        """
        Begin a transaction.
        """
        raise NotImplementedError


class ZooKeeperClient:
    """
    A configured client to a ZooKeeper cluster.

    This can be safely shared between multiple threads or multiple asyncio coroutines.
    """

    def connect(self) -> ZooKeeperConnection:
        raise NotImplementedError


class NoNodeError(TransientException):
    def __init__(self, path: str) -> None:
        super().__init__(path)
        self.message = f"Zookeeper node not found: {path}"


class NodeExistsError(TransientException):
    def __init__(self, path: str) -> None:
        super().__init__(path)
        self.message = f"Zookeeper node already exists: {path}"


class TransactionError(TransientException):
    def __init__(self, results: Collection[Union[bool, TransientException]]):
        super().__init__(results)
        self.message = f"Transaction failed: {results!r}"
        self.results = results


class RolledBackError(TransientException):
    def __init__(self, path: str) -> None:
        super().__init__(path)
        self.message = f"Rolled back operation on: {path}"


class RuntimeInconsistency(TransientException):
    def __init__(self, path: str) -> None:
        super().__init__(path)
        self.message = f"Runtime inconsistency on: {path}"


class KazooZooKeeperClient(ZooKeeperClient):
    def __init__(self, hosts: List[str], timeout: float = 10):
        self.hosts = hosts
        self.timeout = timeout
        self.client: Optional[KazooClient] = None
        self.lock = asyncio.Lock()

    def connect(self) -> ZooKeeperConnection:
        retry = KazooRetry(max_tries=None, deadline=self.timeout)
        client = KazooClient(hosts=self.hosts, connection_retry=retry, command_retry=retry)
        return KazooZooKeeperConnection(client)


class KazooZooKeeperTransaction(ZooKeeperTransaction):
    def __init__(self, request: TransactionRequest):
        self.request = request

    def create(self, path: str, value: bytes) -> None:
        self.request.create(path, value)

    async def commit(self) -> None:
        results = await to_thread(self.request.client.retry, self.request.commit)
        if any(isinstance(result, Exception) for result in results):
            exceptions_map: Dict[Type, Callable[[str], TransientException]] = {
                kazoo.exceptions.RolledBackError: RolledBackError,
                kazoo.exceptions.RuntimeInconsistency: RuntimeInconsistency,
                kazoo.exceptions.NoNodeError: NoNodeError,
                kazoo.exceptions.NodeExistsError: NodeExistsError,
            }
            mapped_results: Collection[Union[bool, TransientException]] = [
                exceptions_map[result.__class__](operation.path) if isinstance(result, Exception) else True
                for result, operation in zip(results, self.request.operations)
            ]
            raise TransactionError(results=mapped_results)


class KazooZooKeeperConnection(ZooKeeperConnection):
    def __init__(self, client: KazooClient):
        self.client = client

    async def __aenter__(self) -> "KazooZooKeeperConnection":
        await to_thread(self.client.start)
        return self

    async def __aexit__(self, *ext_info) -> None:
        await to_thread(self._stop_and_close)

    def _stop_and_close(self) -> None:
        self.client.stop()
        self.client.close()
        self.client = None

    async def get(self, path: str, watch: Optional[Watcher] = None) -> bytes:
        try:
            data, _ = await to_thread(self.client.retry, self.client.get, path, watch=watch)
            return data
        except kazoo.exceptions.NoNodeError as e:
            raise NoNodeError(path) from e

    async def get_children(self, path: str, watch: Optional[Watcher] = None) -> List[str]:
        try:
            return sorted(await to_thread(self.client.retry, self.client.get_children, path, watch=watch))
        except kazoo.exceptions.NoNodeError as e:
            raise NoNodeError(path) from e

    async def create(self, path: str, value: bytes) -> None:
        try:
            await to_thread(self.client.retry, self.client.create, path, value, makepath=True)
        except kazoo.exceptions.NodeExistsError as e:
            raise NodeExistsError(path) from e

    async def set(self, path: str, value: bytes) -> None:
        try:
            await to_thread(self.client.retry, self.client.set, path, value)
        except kazoo.exceptions.NoNodeError as e:
            raise NoNodeError(path) from e

    async def exists(self, path) -> Optional[ZnodeStat]:
        return await to_thread(self.client.retry, self.client.exists, path)

    def transaction(self) -> KazooZooKeeperTransaction:
        return KazooZooKeeperTransaction(request=self.client.transaction())


class FakeZooKeeperClient(ZooKeeperClient):
    Parts = Tuple[str, ...]

    def __init__(self) -> None:
        self._storage: Dict[Tuple[str, ...], bytes] = {("",): b""}
        self._lock = asyncio.Lock()
        self.connections: List["FakeZooKeeperConnection"] = []

    def connect(self) -> ZooKeeperConnection:
        return FakeZooKeeperConnection(self)

    async def inject_fault(self) -> None:
        pass

    @contextlib.asynccontextmanager
    async def get_storage(self) -> AsyncIterator[Dict[Parts, bytes]]:
        await self.inject_fault()
        async with self._lock:
            yield self._storage

    async def trigger(self, parts: Parts, event: WatchedEvent) -> None:
        all_watches = []
        async with self._lock:
            for connection in self.connections:
                all_watches.extend(connection.watches.pop(parts, []))
        # Trigger watches outside of the lock
        for watch in all_watches:
            watch(event)


class TransactionOperation(enum.Enum):
    CREATE = "create"


class FakeZooKeeperTransaction(ZooKeeperTransaction):
    def __init__(self, connection: "FakeZooKeeperConnection") -> None:
        self.connection = connection
        self.operations: List[Tuple[TransactionOperation, str, bytes]] = []
        self.committed = False

    def create(self, path: str, value: bytes) -> None:
        assert not self.committed
        self.operations.append((TransactionOperation.CREATE, path, value))

    async def commit(self) -> None:
        triggers = []
        results: List[Union[bool, TransientException]] = []
        async with self.connection.client.get_storage() as storage:
            storage_copy = storage.copy()
            for operation in self.operations:
                if operation[0] == TransactionOperation.CREATE:
                    path, value = operation[1], operation[2]
                    try:
                        triggers.append(create_locked(path, value, storage_copy, makepath=False))
                        results.append(True)
                    except (NoNodeError, NodeExistsError) as e:
                        # Anything before the first error is now RolledBack, then the first error,
                        # then anything after that is failing with RuntimeInconsistency.
                        results = [
                            *[RolledBackError(operation[1]) for operation in self.operations[: len(results)]],
                            e,
                            *[RuntimeInconsistency(operation[1]) for operation in self.operations[len(results) + 1 :]],
                        ]
                        break
            if any(result is not True for result in results):
                raise TransactionError(results=results)
            storage.clear()
            storage.update(storage_copy)
            self.committed = True
        for parts, event in triggers:
            await self.connection.client.trigger(parts, event)


class FakeZooKeeperConnection(ZooKeeperConnection):
    def __init__(self, client: FakeZooKeeperClient):
        self.client = client
        self.watches: Dict[Tuple[str, ...], List[Watcher]] = {}

    async def __aenter__(self) -> "FakeZooKeeperConnection":
        self.client.connections.append(self)
        return self

    async def __aexit__(self, *exc_info) -> None:
        self.client.connections.remove(self)

    async def get(self, path: str, watch: Optional[Watcher] = None) -> bytes:
        # We don't have the "set" command so we can ignore the watch
        assert self in self.client.connections
        parts = parse_path(path)
        async with self.client.get_storage() as storage:
            if parts not in storage:
                raise NoNodeError(path)
            if watch is not None:
                self.watches.setdefault(parts, []).append(watch)
            return storage[parts]

    async def get_children(self, path: str, watch: Optional[Watcher] = None) -> List[str]:
        # Since we have the "create" command, the watch can be triggered on the parent
        assert self in self.client.connections
        parts = parse_path(path)
        async with self.client.get_storage() as storage:
            if parts not in storage:
                raise NoNodeError(path)
            if watch is not None:
                self.watches.setdefault(parts, []).append(watch)
            return sorted([existing_parts[-1] for existing_parts in storage.keys() if existing_parts[:-1] == parts])

    async def create(self, path: str, value: bytes) -> None:
        assert self in self.client.connections
        async with self.client.get_storage() as storage:
            parent_parts, event = create_locked(path, value, storage, makepath=True)
        await self.client.trigger(parent_parts, event)

    async def set(self, path: str, value: bytes) -> None:
        assert self in self.client.connections
        parts = parse_path(path)
        event = WatchedEvent(type=EventType.CHANGED, state=KeeperState.CONNECTED, path="/".join(parts))
        async with self.client.get_storage() as storage:
            if parts not in storage:
                raise NoNodeError(path)
            storage[parts] = value
        await self.client.trigger(parts, event)

    async def exists(self, path: str) -> Optional[ZnodeStat]:
        async with self.client.get_storage() as storage:
            return parse_path(path) in storage

    def transaction(self) -> FakeZooKeeperTransaction:
        return FakeZooKeeperTransaction(connection=self)


class ChangeWatch:
    def __init__(self) -> None:
        self.has_changed = False

    def __call__(self, event: WatchedEvent) -> None:
        logger.debug("on change: %s", event)
        self.has_changed = True


def parse_path(path: str) -> Tuple[str, ...]:
    return tuple(path.rstrip("/").split("/"))


def create_locked(
    path: str, value: bytes, storage: Dict[Tuple[str, ...], bytes], *, makepath: bool
) -> Tuple[Tuple[str, ...], WatchedEvent]:
    """Low level create operation, assumes the storage is already locked."""
    parts = parse_path(path)
    if parts in storage:
        raise NodeExistsError(path)
    storage[parts] = value
    # Auto-create parents
    parent_parts = parts[:-1]
    while len(parent_parts) and parent_parts not in storage:
        if makepath:
            storage[parent_parts] = b""
            parent_parts = parent_parts[:-1]
        else:
            raise NoNodeError("/".join(parent_parts))
    event = WatchedEvent(type=EventType.CREATED, state=KeeperState.CONNECTED, path="/".join(parent_parts))
    return parent_parts, event
