"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""
from _pytest.config import Config
from astacus.common.msgspec_glue import enc_hook
from astacus.common.utils import exponential_backoff
from contextlib import asynccontextmanager
from httpx import URL
from pathlib import Path
from tests.utils import create_rohmu_config, format_astacus_command
from types import MappingProxyType
from typing import Any, AsyncIterator, List, Mapping, Optional, Union

import asyncio
import httpx
import json
import logging
import msgspec
import os.path
import py
import pytest
import subprocess

logger = logging.getLogger(__name__)


class TestNode(msgspec.Struct):
    __test__ = False
    name: str
    url: str
    port: int

    # Where do root/link/etc for this node reside in filesystem
    path: Optional[Path] = None
    root_path: Optional[Path] = None
    db_path: Optional[Path] = None


ASTACUS_NODES = [
    # Intentionally not at default port
    TestNode(name="a1", url="http://localhost:55150", port=55150),
    TestNode(name="a2", url="http://localhost:55151", port=55151),
    TestNode(name="a3", url="http://localhost:55152", port=55152),
]

DEFAULT_PLUGIN_CONFIG = {
    "root_globs": ["*"],
}


@asynccontextmanager
async def background_process(
    program: Union[str, Path], *args: Union[str, Path], **kwargs
) -> AsyncIterator[asyncio.subprocess.Process]:
    # pylint: disable=bare-except
    proc = await asyncio.create_subprocess_exec(program, *args, **kwargs)
    try:
        yield proc
    finally:
        try:
            await asyncio.wait_for(proc.wait(), 0.1)
        except asyncio.TimeoutError:
            try:
                proc.terminate()
            except:
                # thrown exception is not well defined, unfortunately, varies
                # by event loop
                pass
            while True:
                try:
                    await asyncio.wait_for(proc.wait(), 3)
                    break
                except asyncio.TimeoutError:
                    try:
                        proc.kill()
                    except:
                        # thrown exception is not well defined, unfortunately, varies
                        # by event loop
                        break


def create_astacus_config_dict(
    *, tmpdir: Path, root_path: Path, link_path: Path, node: TestNode, plugin_config: Mapping[str, Any]
) -> Mapping[str, Any]:
    nodes = [{"url": f"{node.url}/node"} for node in ASTACUS_NODES]
    return {
        "coordinator": {"nodes": nodes, "plugin": "files", "plugin_config": dict(plugin_config)},
        "node": {
            "root": str(root_path),
            "root_link": str(link_path),
            "db_path": str(node.db_path),
        },
        "object_storage": msgspec.to_builtins(create_rohmu_config(tmpdir), enc_hook=enc_hook),
        "uvicorn": {
            "port": node.port,
            "log_level": "debug",
        },
    }


def create_astacus_config(
    *,
    tmpdir,
    node: TestNode,
    plugin_config: Mapping[str, Any] = MappingProxyType(DEFAULT_PLUGIN_CONFIG),
) -> Path:
    a = Path(tmpdir / "node" / node.name)
    node.path = a
    root_path = a / "root"
    root_path.mkdir(parents=True, exist_ok=True)
    node.root_path = root_path
    link_path = a / "link"
    link_path.mkdir(exist_ok=True)
    db_path = a / "snapshotter_db"
    db_path.mkdir(exist_ok=True)
    node.db_path = db_path
    a_conf = create_astacus_config_dict(
        tmpdir=Path(tmpdir), root_path=root_path, link_path=link_path, node=node, plugin_config=plugin_config
    )
    a_conf_path = a / "astacus.conf"
    a_conf_path.write_text(json.dumps(a_conf))
    return a_conf_path


async def wait_url_up(url: Union[URL, str]) -> None:
    async with httpx.AsyncClient() as client:
        async for _ in exponential_backoff(initial=0.1, multiplier=1.3, retries=20):
            try:
                r = await client.get(url)
                if not r.is_error:
                    logger.debug("URL %s is up", url)
                    break
            except httpx.NetworkError as ex:
                logger.debug("URL %s gave exception %r", url, ex)
        else:
            assert False, f"url {url} still not reachable"


@pytest.fixture(name="rootdir")
def fixture_rootdir(pytestconfig: Config) -> str:
    return os.path.join(os.path.dirname(__file__), "..", "..")


@asynccontextmanager
async def _astacus(*, tmpdir: py.path.local, index: int) -> AsyncIterator[TestNode]:
    node = ASTACUS_NODES[index]
    a_conf_path = create_astacus_config(tmpdir=tmpdir, node=node)
    astacus_source_root = os.path.join(os.path.dirname(__file__), "..", "..")
    cmd = format_astacus_command("server", "-c", str(a_conf_path))
    async with background_process(*cmd, env={"PYTHONPATH": astacus_source_root}) as process:
        await wait_url_up(node.url)
        yield node
    assert process.returncode == 0


@pytest.fixture(name="astacus1")
async def fixture_astacus1(tmpdir: py.path.local) -> AsyncIterator[TestNode]:
    async with _astacus(tmpdir=tmpdir, index=0) as a:
        yield a


@pytest.fixture(name="astacus2")
async def fixture_astacus2(tmpdir: py.path.local) -> AsyncIterator[TestNode]:
    async with _astacus(tmpdir=tmpdir, index=1) as a:
        yield a


@pytest.fixture(name="astacus3")
async def fixture_astacus3(tmpdir: py.path.local) -> AsyncIterator[TestNode]:
    async with _astacus(tmpdir=tmpdir, index=2) as a:
        yield a


def astacus_run(
    rootdir: str,
    astacus: TestNode,
    *args: str,
    check: bool = True,
    capture_output: bool = False,
) -> subprocess.CompletedProcess:
    cmd = format_astacus_command("--url", astacus.url, "-w", "10")
    return subprocess.run(cmd + list(args), check=check, capture_output=capture_output, env={"PYTHONPATH": rootdir})


def astacus_ls(astacus: TestNode) -> List[str]:
    assert astacus.root_path
    return sorted(str(x.relative_to(astacus.root_path)) for x in astacus.root_path.glob("**/*"))
