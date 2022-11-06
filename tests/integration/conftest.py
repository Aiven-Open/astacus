"""
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""
from astacus.common.utils import build_netloc
from astacus.coordinator.plugins.zookeeper import KazooZooKeeperClient
from pathlib import Path
from typing import AsyncIterator, Dict, Iterator, List, Optional, Union

import asyncio
import contextlib
import dataclasses
import logging
import pytest
import subprocess
import tempfile
import threading

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module", name="event_loop")
def fixture_event_loop() -> Iterator[asyncio.AbstractEventLoop]:
    # This is the same as the original `event_loop` fixture from `pytest_asyncio`
    # but with a module scope, re-declaring this fixture is their suggested way
    # of locally increasing the scope of this fixture.
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


async def get_command_path(name: str) -> Optional[Path]:
    process = await asyncio.create_subprocess_exec(
        "which", name, stderr=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE
    )
    stdout, stderr = await process.communicate()
    decoded_stderr = stderr.decode()
    if decoded_stderr:
        logger.debug("which %s: %s", name, decoded_stderr)
    if process.returncode == 0:
        return Path(stdout.decode().rstrip("\n"))
    return None


def get_zookeeper_command(*, java_path: Path, data_dir: Path, port: int) -> Optional[List[Union[str, Path]]]:
    zookeeper_jars = list(Path("/usr/share/zookeeper").glob("*.jar"))
    if zookeeper_jars:
        class_paths = [data_dir, *zookeeper_jars]
        class_path_option = ":".join(str(path) for path in class_paths)
        zookeeper_path = "org.apache.zookeeper.server.quorum.QuorumPeerMain"
        java_options = ["-Dzookeeper.admin.enableServer=false"]
        return [java_path, "-cp", class_path_option, *java_options, zookeeper_path, str(port), data_dir]
    return None


class FailPatternFoundError(Exception):
    pass


@contextlib.asynccontextmanager
async def run_process_and_wait_for_pattern(
    *,
    args: List[Union[str, Path]],
    cwd: Path,
    pattern: str,
    fail_pattern: Optional[str] = None,
    timeout: float = 10.0,
) -> AsyncIterator[asyncio.subprocess.Process]:
    # This stringification is a workaround for a bug in pydev (pydev_monkey.py:111)
    str_args = [str(arg) for arg in args]
    env: Dict[str, str] = {}
    pattern_found = asyncio.Event()
    loop = asyncio.get_running_loop()
    with subprocess.Popen(str_args, cwd=cwd, env=env, stderr=subprocess.PIPE) as process:

        def read_logs() -> None:
            assert process.stderr is not None
            while process.poll() is None:
                for line in process.stderr:
                    line = line.rstrip(b"\n")
                    decoded_line = line.rstrip(b"\n").decode(encoding="utf-8", errors="replace")
                    logger.debug("%d: %s", process.pid, decoded_line)
                    if pattern in decoded_line:
                        loop.call_soon_threadsafe(pattern_found.set)
                    if fail_pattern is not None and fail_pattern in decoded_line:
                        raise FailPatternFoundError(f"Fail pattern {fail_pattern!r} found")

        thread = threading.Thread(target=read_logs)
        thread.start()
        try:
            try:
                await asyncio.wait_for(pattern_found.wait(), timeout=timeout)
            except asyncio.TimeoutError as e:
                raise Exception(f"Pattern {pattern!r} not found after {timeout:.3f}s in output of {str_args}") from e
            yield process
        finally:
            process.kill()
            thread.join()


@dataclasses.dataclass
class Service:
    process: asyncio.subprocess.Process
    data_dir: Path
    port: int
    host: str = "localhost"
    username: Optional[str] = None
    password: Optional[str] = None


@dataclasses.dataclass
class ServiceCluster:
    services: List[Service]


class Ports:
    def __init__(self, start: int = 50000):
        self.start = start

    def allocate(self) -> int:
        allocated = self.start
        self.start += 1
        return allocated


@pytest.fixture(scope="session", name="ports")
def fixture_ports() -> Ports:
    return Ports()


@pytest.fixture(scope="module", name="zookeeper")
async def fixture_zookeeper(ports: Ports) -> AsyncIterator[Service]:
    async with create_zookeeper(ports) as zookeeper:
        yield zookeeper


def get_kazoo_host(zookeeper: Service) -> str:
    return build_netloc(zookeeper.host, zookeeper.port)


@pytest.fixture(name="zookeeper_client")
def fixture_zookeeper_client(zookeeper: Service) -> KazooZooKeeperClient:
    return KazooZooKeeperClient(hosts=[get_kazoo_host(zookeeper)], timeout=10)


@contextlib.asynccontextmanager
async def create_zookeeper(ports: Ports) -> AsyncIterator[Service]:
    java_path = await get_command_path("java")
    if java_path is None:
        pytest.skip("java installation not found")
    port = ports.allocate()
    with tempfile.TemporaryDirectory(prefix=f"zookeeper_{port}_") as data_dir_str:
        data_dir = Path(data_dir_str)
        log4j_properties = data_dir / "log4j.properties"
        log4j_properties.write_text(
            """
log4j.rootLogger=DEBUG,default
log4j.appender.default=org.apache.log4j.ConsoleAppender
log4j.appender.default.Target=System.err
log4j.appender.default.layout=org.apache.log4j.PatternLayout
log4j.appender.default.layout.ConversionPattern=[%-5p] %m%n
"""
        )
        command = get_zookeeper_command(java_path=java_path, data_dir=data_dir, port=port)
        if command is None:
            pytest.skip("zookeeper installation not found")
        async with contextlib.AsyncExitStack() as stack:
            max_attempts = 10
            for attempt in range(max_attempts):
                try:
                    process = await stack.enter_async_context(
                        run_process_and_wait_for_pattern(
                            args=command,
                            cwd=data_dir,
                            pattern="PrepRequestProcessor (sid:0) started",
                            fail_pattern="java.net.BindException: Address already in use",
                            timeout=20.0,
                        )
                    )
                    break
                except FailPatternFoundError:
                    if attempt + 1 == max_attempts:
                        raise
                    await asyncio.sleep(2.0)
            yield Service(process=process, port=port, data_dir=data_dir)
