"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from astacus.common.utils import AstacusModel, exponential_backoff
from contextlib import asynccontextmanager
from pathlib import Path
from tests.utils import create_rohmu_config
from typing import Optional

import asyncio
import httpx
import json
import logging
import pytest

logger = logging.getLogger(__name__)


class TestNode(AstacusModel):
    name: str
    url: str
    port: int

    # Where do root/link/etc for this node reside in filesystem
    path: Optional[Path]
    root_path: Optional[Path]


ASTACUS_NODES = [
    # Intentionally not at default port
    TestNode(name="a1", url="http://localhost:55150", port=55150),
    TestNode(name="a2", url="http://localhost:55151", port=55151),
    TestNode(name="a3", url="http://localhost:55152", port=55152),
]


@asynccontextmanager
async def background_process(program, *args):
    # pylint: disable=bare-except
    proc = await asyncio.create_subprocess_exec(program, *args)
    try:
        yield
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


def create_astacus_config_dict(*, tmpdir, root_path, link_path, node):
    nodes = [{"url": f"{node.url}/node"} for node in ASTACUS_NODES]
    return {
        "coordinator": {
            "nodes": nodes
        },
        "node": {
            "root": str(root_path),
            "root_globs": ["*"],
            "root_link": str(link_path),
        },
        "object_storage": create_rohmu_config(tmpdir).dict(),
        "uvicorn": {
            "port": node.port,
            "log_level": "debug",
        }
    }


def create_astacus_config(*, tmpdir, node):
    a = Path(tmpdir / "node" / node.name)
    node.path = a
    root_path = a / "root"
    root_path.mkdir(parents=True)
    node.root_path = root_path
    link_path = a / "link"
    link_path.mkdir()
    a_conf = create_astacus_config_dict(tmpdir=tmpdir, root_path=root_path, link_path=link_path, node=node)
    a_conf_path = a / "astacus.conf"
    a_conf_path.write_text(json.dumps(a_conf))
    return a_conf_path


async def _wait_url_up(url):
    async with httpx.AsyncClient() as client:
        async for _ in exponential_backoff(initial=0.1, multiplier=1.3, retries=10):
            try:
                r = await client.get(url)
                if not r.is_error:
                    logger.debug("URL %s is up", url)
                    break
            except httpx.NetworkError as ex:
                logger.debug("URL %s gave exception %r", url, ex)
        else:
            assert False, f"url {url} still not reachable"


@asynccontextmanager
async def _astacus(*, tmpdir, index):
    node = ASTACUS_NODES[index]
    a_conf_path = create_astacus_config(tmpdir=tmpdir, node=node)
    async with background_process("astacus", "server", "-c", str(a_conf_path)):
        await _wait_url_up(node.url)
        yield node


@pytest.fixture(name="astacus1")
async def fixture_astacus1(tmpdir):
    async with _astacus(tmpdir=tmpdir, index=0) as a:
        yield a


@pytest.fixture(name="astacus2")
async def fixture_astacus2(tmpdir):
    async with _astacus(tmpdir=tmpdir, index=1) as a:
        yield a


@pytest.fixture(name="astacus3")
async def fixture_astacus3(tmpdir):
    async with _astacus(tmpdir=tmpdir, index=2) as a:
        yield a
