"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from typing import Awaitable

import asyncio


class Limiter:
    def __init__(self, limit: int):
        self.semaphore = asyncio.Semaphore(limit)

    async def run(self, awaitable: Awaitable) -> None:
        async with self.semaphore:
            await awaitable
