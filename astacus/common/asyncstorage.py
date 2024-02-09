"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

"""
from astacus.common.storage import HexDigestStorage, JsonStorage
from starlette.concurrency import run_in_threadpool

import msgspec


class AsyncHexDigestStorage:
    """Subset of the HexDigestStorage API proxied async -> sync via starlette threadpool

    Note that the access is not intentionally locked; therefore even
    synchronous API can be used in parallel (at least if it is safe to
    do so) using this.

    """

    def __init__(self, storage: HexDigestStorage) -> None:
        self.storage = storage

    async def delete_hexdigest(self, hexdigest: str) -> None:
        return await run_in_threadpool(self.storage.delete_hexdigest, hexdigest)

    async def list_hexdigests(self) -> list[str]:
        return await run_in_threadpool(self.storage.list_hexdigests)


class AsyncJsonStorage:
    """Subset of the JsonStorage API proxied async -> sync via starlette threadpool

    Note that the access is not intentionally locked; therefore even
    synchronous API can be used in parallel (at least if it is safe to
    do so) using this.

    """

    def __init__(self, storage: JsonStorage) -> None:
        self.storage = storage

    async def delete_json(self, name: str) -> None:
        return await run_in_threadpool(self.storage.delete_json, name)

    async def list_jsons(self) -> list[str]:
        return await run_in_threadpool(self.storage.list_jsons)

    async def upload_json(self, name: str, data: msgspec.Struct) -> bool:
        return await run_in_threadpool(self.storage.upload_json, name, data)
