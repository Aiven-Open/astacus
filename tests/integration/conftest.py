"""
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""

from astacus.common.utils import build_netloc
from astacus.coordinator.plugins.zookeeper import KazooZooKeeperClient
from collections.abc import AsyncIterator, Mapping, Sequence
from pathlib import Path
from types import MappingProxyType

import asyncio
import contextlib
import dataclasses
import logging
import pytest
import socket
import subprocess
import tempfile
import threading

logger = logging.getLogger(__name__)


async def get_command_path(name: str) -> Path | None:
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


def get_zookeeper_command(*, java_path: Path, data_dir: Path, port: int) -> Sequence[str | Path] | None:
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


class PatternNotFoundError(Exception):
    pass


@contextlib.asynccontextmanager
async def run_process_and_wait_for_pattern(
    *,
    args: Sequence[str | Path],
    cwd: Path,
    pattern: str,
    fail_pattern: str | None = None,
    env: Mapping[str, str] = MappingProxyType({}),
    timeout: float = 10.0,
) -> AsyncIterator[subprocess.Popen[bytes]]:
    # This stringification is a workaround for a bug in pydev (pydev_monkey.py:111)
    str_args = [str(arg) for arg in args]
    pattern_found = asyncio.Event()
    loop = asyncio.get_running_loop()
    with subprocess.Popen(str_args, cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as process:

        def read_logs() -> None:
            assert process.stdout is not None
            while process.poll() is None:
                for line in process.stdout:
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
                raise PatternNotFoundError(
                    f"Pattern {pattern!r} not found after {timeout:.3f}s in output of {str_args}"
                ) from e
            yield process
        finally:
            process.kill()
            thread.join()


@dataclasses.dataclass
class Service:
    process: subprocess.Popen[bytes] | asyncio.subprocess.Process
    data_dir: Path
    port: int
    host: str = "localhost"
    username: str | None = None
    password: str | None = None


@dataclasses.dataclass
class ServiceCluster:
    services: Sequence[Service]


class Ports:
    def __init__(self, start: int = 15000):
        self.start = start

    def allocate(self) -> int:
        while port_is_listening("127.0.0.1", self.start):
            self.start += 1
        allocated = self.start
        self.start += 1
        return allocated


def port_is_listening(hostname: str, port: int, timeout: float = 0.5) -> bool:
    try:
        connection = socket.create_connection((hostname, port), timeout)
        connection.close()
        return True
    except socket.error:
        return False


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
        # newer versions of mypy should be able to infer that this is unreachable
        assert False
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
            assert False
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
                    yield Service(process=process, port=port, data_dir=data_dir)
                    break
                except FailPatternFoundError:
                    if attempt + 1 == max_attempts:
                        raise
                    await asyncio.sleep(2.0)
