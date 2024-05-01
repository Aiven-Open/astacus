# Copyright (c) 2021 Aiven, Helsinki, Finland. https://aiven.io/
# Copyright aio-libs contributors.
# This is an heavily modified version of aiohttp.pytest_plugin
# Added:
# - no unnecessary parametrization of all async tests (we only test with one loop per test run)
# - no need to add the loop fixture to all async fixtures
# - async fixtures can have any scope, the test will run with the smallest possible scope
# Removed:
# - support for alternative async loops
# - 200ms of gc.collect after each test
from _pytest.config import Config
from _pytest.fixtures import FixtureDef, SubRequest
from _pytest.python import Function, PyCollector
from collections.abc import AsyncGenerator, Container, Iterator, Mapping, Sequence
from typing import Any, cast, Final, Self

import asyncio
import contextlib
import dataclasses
import inspect
import logging
import pytest
import threading
import warnings

LOG: Final = logging.getLogger(__name__)

FIXTURE_SCOPES: Final[Sequence[str]] = ("session", "package", "module", "class", "function")
FIXTURE_NAMES: Final[Mapping[str, str]] = {
    "function": "loop",
    "class": "class_loop",
    "module": "module_loop",
    "package": "package_loop",
    "session": "session_loop",
}


def pytest_fixture_setup(fixturedef: FixtureDef) -> None:
    """Allow fixtures to be coroutines. Run coroutine fixtures in an event loop."""
    func = fixturedef.func
    if inspect.isasyncgenfunction(func):
        is_async_generator = True
    elif asyncio.iscoroutinefunction(func):
        is_async_generator = False
    else:
        return
    loop_fixture_name = FIXTURE_NAMES[fixturedef.scope]
    fixtures_to_strip = []
    for fixture_name in "request", loop_fixture_name:
        if fixture_name not in fixturedef.argnames:
            fixturedef.argnames += (fixture_name,)
            fixtures_to_strip.append(fixture_name)

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        request = kwargs["request"]
        loop = kwargs[loop_fixture_name]
        for fixture_to_strip in fixtures_to_strip:
            del kwargs[fixture_to_strip]

        if is_async_generator:
            # for async generators, we need to advance the generator once,
            # then advance it again in a finalizer
            gen: AsyncGenerator = func(*args, **kwargs)  # type: ignore[assignment]

            def finalizer() -> Any:
                try:
                    return loop.run_until_complete(anext(gen))
                except StopAsyncIteration:
                    return None

            request.addfinalizer(finalizer)

            return loop.run_until_complete(anext(gen))
        return loop.run_until_complete(func(*args, **kwargs))

    fixturedef.func = wrapper


def pytest_pycollect_makeitem(collector: PyCollector, name: str, obj: Any) -> list[Function] | None:
    """Auto-add a "loop" fixture to all async test functions."""
    if collector.funcnamefilter(name) and asyncio.iscoroutinefunction(obj):
        functions = list(collector._genfunctions(name, obj))  # pylint: disable=protected-access
        for function in functions:
            if "loop" not in function.fixturenames:
                function.fixturenames.append("loop")
        return functions
    return None


def pytest_pyfunc_call(pyfuncitem: Function) -> bool | None:
    """Run coroutines in an event loop instead of a normal function call."""
    if asyncio.iscoroutinefunction(pyfuncitem.function):
        with _runtime_warning_context():
            fixture_info = pyfuncitem._fixtureinfo  # pylint: disable=protected-access
            test_args = {arg: pyfuncitem.funcargs[arg] for arg in fixture_info.argnames}
            loop = cast(asyncio.AbstractEventLoop, pyfuncitem.funcargs["loop"])
            loop.run_until_complete(pyfuncitem.obj(**test_args))
        return True
    return None


@dataclasses.dataclass
class AsyncScope:
    """A container to access the fixture scopes of each test function.

    See `pytest_collection_modifyitems` about why we need more than a `Set[str]`.
    """

    # This is None only if it's the root of a node graph
    parent: Self | None = None
    # This is non-empty only if parent is None
    scopes: set[str] = dataclasses.field(default_factory=set)

    @property
    def root(self) -> Self:
        return self if self.parent is None else self.parent.root

    @property
    def depth(self) -> int:
        return 0 if self.parent is None else 1 + self.parent.depth

    @property
    def connected_scopes(self) -> Container[str]:
        return self.root.scopes

    def merge(self, other: "AsyncScope") -> None:
        """Attach our own root to `other`'s root and transfer all scopes to that root."""
        self_root = self.root
        other_root = other.root
        if self_root != other_root:
            self_root.parent = other_root
            other_root.scopes.update(self_root.scopes)
            self_root.scopes.clear()


def pytest_collection_modifyitems(session: pytest.Session, config: Config, items: list[pytest.Function]) -> None:
    """Attach an `AsyncScope` to each test function.

    All `AsyncScope` that have overlapping scopes will be connected together and share
    the same root and scopes.

    This is how a single test function using two fixtures, one package-scoped and one
    module-scoped, will contaminate all other test functions in the same module that use the same
    module-scoped fixture:
    Even when that test function is not the first to run in the module, the module-scoped
    fixture will run in an async loop that lasts for the entire package.

    This matters because we want all fixtures of the same test function to run in the
    same async loop. asyncio object from popular libraries don't really like to be shared
    between different async loops.
    """
    nodes: dict[Sequence[str], AsyncScope] = {}
    for item in items:
        async_scope = AsyncScope()
        for scope_identifier in get_scope_identifiers(item):
            if scope_identifier not in nodes:
                nodes[scope_identifier] = AsyncScope(scopes={scope_identifier[0]})
            async_scope.merge(nodes[scope_identifier])
        item.async_scope = async_scope  # type: ignore[attr-defined]


def get_scope_identifiers(item: pytest.Function) -> Iterator[Sequence[str]]:
    """Enumerate all scopes of all the async fixtures required for a test function."""
    fixture_info = item._fixtureinfo  # pylint: disable=protected-access
    for fixture_name in fixture_info.initialnames:
        if fixture_name == "request":
            continue
        if fixture_name not in fixture_info.name2fixturedefs:
            continue
        fixture_def = fixture_info.name2fixturedefs[fixture_name][-1]
        if inspect.isasyncgenfunction(fixture_def.func) or inspect.iscoroutinefunction(fixture_def.func):
            if fixture_def.scope == "session":
                yield ("session",)
            elif fixture_def.scope == "package":
                yield "package", item.module.__package__
            elif fixture_def.scope == "module":
                yield "module", item.module.__name__
            elif fixture_def.scope == "class":
                if item.cls is None:
                    # You can have a fixture with a class scope outside of a class...
                    yield "class", item.name
                else:
                    yield "class", item.cls.__module__, item.cls.__qualname__


def get_loop(request: SubRequest) -> Iterator[asyncio.AbstractEventLoop]:
    """Create a new async loop or reuse one from the outermost async scope."""
    tested_function_request = request._pyfuncitem._request  # pylint: disable=protected-access
    async_scopes = tested_function_request.node.async_scope.connected_scopes
    for scope in FIXTURE_SCOPES:
        if scope == request.scope:
            # We haven't found an outer async scope, create our own
            with loop_context() as loop:
                asyncio.set_event_loop(loop)
                yield loop
                break
        if scope in async_scopes:  # or enclosing_fixture in tested_function_request.fixturenames:
            yield request.getfixturevalue(FIXTURE_NAMES[scope])
            break


class SafeEventLoop(asyncio.SelectorEventLoop):
    """Safe event loop that will only add signal handler if loop is running in main thread.

    In some cases async tests in pytest are not running in main thread.
    This can lead to randomly failing tests in case a signal handler is added to the loop.
    The SafeEventLoop will only add signal handler if it is added in main thread and otherwise does nothing.
    """

    def add_signal_handler(self, sig, callback, *args):
        if threading.current_thread() == threading.main_thread():
            super().add_signal_handler(sig, callback, *args)
        else:
            LOG.warning("Unable to add signal handler %d because loop is not running in main thread.", sig)


class SafeEventLoopPolicy(asyncio.DefaultEventLoopPolicy):
    _loop_factory = SafeEventLoop


@contextlib.contextmanager
def _runtime_warning_context() -> Iterator[None]:
    """Context manager which checks for RuntimeWarnings.

    This exists specifically to
    avoid "coroutine 'X' was never awaited" warnings being missed.

    If RuntimeWarnings occur in the context a RuntimeError is raised.
    """
    with warnings.catch_warnings(record=True) as _warnings:
        yield
        rw = ["{w.filename}:{w.lineno}:{w.message}".format(w=w) for w in _warnings if w.category == RuntimeWarning]
        if rw:
            raise RuntimeError("{} Runtime Warning{},\n{}".format(len(rw), "" if len(rw) == 1 else "s", "\n".join(rw)))


@contextlib.contextmanager
def loop_context() -> Iterator[asyncio.AbstractEventLoop]:
    asyncio.set_event_loop_policy(SafeEventLoopPolicy())
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    # Refs:
    # * https://github.com/pytest-dev/pytest-xdist/issues/620
    # * https://stackoverflow.com/a/58614689/595220
    # * https://bugs.python.org/issue35621
    # * https://github.com/python/cpython/pull/14344
    watcher = asyncio.ThreadedChildWatcher()
    watcher.attach_loop(loop)
    policy = asyncio.get_event_loop_policy()
    policy.set_child_watcher(watcher)
    try:
        yield loop
    finally:
        closed = loop.is_closed()
        if not closed:
            loop.call_soon(loop.stop)
            loop.run_forever()
            loop.close()
        asyncio.set_event_loop(None)


@pytest.fixture(scope="session", name="session_loop")
def fixture_session_loop(request: SubRequest) -> Iterator[asyncio.AbstractEventLoop]:
    yield from get_loop(request)


@pytest.fixture(scope="package", name="package_loop")
def fixture_package_loop(request: SubRequest) -> Iterator[asyncio.AbstractEventLoop]:
    yield from get_loop(request)


@pytest.fixture(scope="module", name="module_loop")
def fixture_module_loop(request: SubRequest) -> Iterator[asyncio.AbstractEventLoop]:
    yield from get_loop(request)


@pytest.fixture(scope="class", name="class_loop")
def fixture_class_loop(request: SubRequest) -> Iterator[asyncio.AbstractEventLoop]:
    yield from get_loop(request)


@pytest.fixture(name="loop")
def fixture_loop(request: SubRequest) -> Iterator[asyncio.AbstractEventLoop]:
    yield from get_loop(request)
