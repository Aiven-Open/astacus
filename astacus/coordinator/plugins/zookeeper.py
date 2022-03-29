"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from astacus.common.exceptions import TransientException
from kazoo.client import EventType, KazooClient, KeeperState, WatchedEvent
from kazoo.protocol.states import ZnodeStat
from kazoo.retry import KazooRetry
from typing import Any, AsyncIterator, Callable, cast, Dict, List, Optional, Tuple, TypeVar

import asyncio
import contextlib
import functools
import kazoo.exceptions
import logging

logger = logging.getLogger(__name__)

try:
    # noinspection PyCompatibility
    from asyncio import to_thread  # type: ignore[attr-defined]
except ImportError:
    import contextvars

    # Only available in python >= 3.9
    T = TypeVar("T")

    async def to_thread(func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        loop = asyncio.events.get_running_loop()
        ctx = contextvars.copy_context()
        func_call = cast(Callable[[], T], functools.partial(ctx.run, func, *args, **kwargs))
        return await loop.run_in_executor(None, func_call)


Watcher = Callable[[WatchedEvent], None]


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

    async def create(self, path: str, value: bytes) -> None:
        """
        Creates the node with the specified `path` and `value`.

        Auto-creates all parent nodes if they don't exist.

        Raises `NodeExistsError` if the node already exists.
        """
        raise NotImplementedError

    async def exists(self, path: str) -> Optional[ZnodeStat]:
        """
        Check if specified node exists.
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
            return await to_thread(self.client.retry, self.client.create, path, value, makepath=True)
        except kazoo.exceptions.NodeExistsError as e:
            raise NodeExistsError(path) from e

    async def exists(self, path) -> Optional[ZnodeStat]:
        return await to_thread(self.client.retry, self.client.exists, path)


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


class FakeZooKeeperConnection(ZooKeeperConnection):
    def __init__(self, client: FakeZooKeeperClient):
        self.client = client
        self.watches: Dict[Tuple[str, ...], List[Watcher]] = {}

    async def __aenter__(self) -> "FakeZooKeeperConnection":
        self.client.connections.append(self)
        return self

    async def __aexit__(self, *exc_info) -> None:
        self.client.connections.remove(self)

    @staticmethod
    def parse_path(path: str) -> Tuple[str, ...]:
        return tuple(path.rstrip("/").split("/"))

    async def get(self, path: str, watch: Optional[Watcher] = None) -> bytes:
        # We don't have the "set" command so we can ignore the watch
        assert self in self.client.connections
        parts = self.parse_path(path)
        async with self.client.get_storage() as storage:
            if parts not in storage:
                raise NoNodeError(path)
            if watch is not None:
                self.watches.setdefault(parts, []).append(watch)
            return storage[parts]

    async def get_children(self, path: str, watch: Optional[Watcher] = None) -> List[str]:
        # Since we have the "create" command, the watch can be triggered on the parent
        assert self in self.client.connections
        parts = self.parse_path(path)
        async with self.client.get_storage() as storage:
            if parts not in storage:
                raise NoNodeError(path)
            if watch is not None:
                self.watches.setdefault(parts, []).append(watch)
            return sorted([existing_parts[-1] for existing_parts in storage.keys() if existing_parts[:-1] == parts])

    async def create(self, path: str, value: bytes) -> None:
        assert self in self.client.connections
        parts = self.parse_path(path)
        async with self.client.get_storage() as storage:
            if parts in storage:
                raise NodeExistsError(path)
            storage[parts] = value
            # Auto-create parents
            parent_parts = parts[:-1]
            while len(parent_parts) and parent_parts not in storage:
                storage[parent_parts] = b""
                parent_parts = parent_parts[:-1]
            event = WatchedEvent(type=EventType.CREATED, state=KeeperState.CONNECTED, path="/".join(parent_parts))
        await self.client.trigger(parent_parts, event)

    async def exists(self, path: str) -> Optional[ZnodeStat]:
        pass


class ChangeWatch:
    def __init__(self) -> None:
        self.has_changed = False

    def __call__(self, event: WatchedEvent) -> None:
        logger.debug("on change: %s", event)
        self.has_changed = True
