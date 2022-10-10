"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from typing import Awaitable, Iterable

import asyncio


class Limiter:
    def __init__(self, limit: int):
        self.semaphore = asyncio.Semaphore(limit)

    async def run(self, awaitable: Awaitable) -> None:
        async with self.semaphore:
            await awaitable


async def gather_limited(limit: int, awaitables: Iterable[Awaitable]) -> None:
    limiter = Limiter(limit)
    await asyncio.gather(*[limiter.run(awaitable) for awaitable in awaitables])
