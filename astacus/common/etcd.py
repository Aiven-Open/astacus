"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Minimal etcdv3 client library on top of httpx

"""

from .utils import AstacusModel, httpx_request
from typing import Optional

import base64
import json


class KVRangeRequest(AstacusModel):
    key: str
    range_end: str = ""


def b64encode_to_str(s):
    return base64.b64encode(s).decode()


def b64encode_dict(d):
    return {b64encode_to_str(key): b64encode_to_str(value) for key, value in d}


def b64decode_dict(d):
    return {base64.b64decode(key): base64.b64decode(value) for key, value in d}


class ETCDClient:
    def __init__(self, url):
        self.url = url

    async def _request(self, url, data, *, caller):
        url = f"{self.url}/{url}"
        return await httpx_request(url, caller=caller, method="post", data=json.dumps(data))

    async def kv_deleterange(self, *, key: bytes, range_end: Optional[bytes] = None):
        data = {"key": b64encode_to_str(key)}
        if range_end:
            data["range_end"] = b64encode_to_str(range_end)
            assert key <= range_end
        result = await self._request("kv/deleterange", data, caller="ETCDClient.delete_range")
        return result

    async def kv_range(self, *, key: bytes, range_end: Optional[bytes] = None):
        data = {"key": b64encode_to_str(key)}
        if range_end:
            data["range_end"] = b64encode_to_str(range_end)
            assert key <= range_end
        result = await self._request("kv/range", data, caller="ETCDClient.kv_range")
        if not result:
            return {}
        # Most of the stuff etcd returns we do not really care about.
        #
        # So just return the (raw) {key: value} dict with b64encoded
        # keys and values. b64decode_dict can be used by caller if they care.
        return {kv["key"]: kv["value"] for kv in result["kvs"]}

    async def kv_put(self, *, key: bytes, value: bytes):
        return await self.kv_put_b64(key=b64encode_to_str(key), value=b64encode_to_str(value))

    async def kv_put_b64(self, *, key: str, value: str):
        data = {"key": key, "value": value}
        result = await self._request("kv/put", data, caller="ETCDClient.kv_put_b64")
        return result

    async def prefix_delete(self, prefix):
        assert isinstance(prefix, bytes)
        range_end = bytearray(prefix)
        range_end[-1] += 1
        return await self.kv_deleterange(key=prefix, range_end=range_end)

    async def prefix_get(self, prefix):
        assert isinstance(prefix, bytes)
        range_end = bytearray(prefix)
        range_end[-1] += 1
        return await self.kv_range(key=prefix, range_end=range_end)
