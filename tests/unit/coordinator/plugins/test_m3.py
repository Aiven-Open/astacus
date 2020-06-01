"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Test that the plugin m3 specific flow (backup + restore) works

"""
# pylint: disable=too-many-ancestors

from astacus.common.etcd import b64encode_to_str
from astacus.coordinator.config import CoordinatorConfig
from astacus.coordinator.plugins import base, m3

import pytest
import respx


class DummyM3BackupOp(m3.M3BackupOp):
    def __init__(self):
        # pylint: disable=super-init-not-called
        self.config = CoordinatorConfig.parse_obj({
            "plugin": "m3",
            "plugin_config": {
                "etcd_url": "http://dummy/etcd"
            },
        })
        self.steps = [step for step in self.steps if getattr(base.BackupOpBase, f"step_{step}", None) is None]


BACKUP_FAILS = [1, None]

key1_b64 = b64encode_to_str(b"key1")
key2_b64 = b64encode_to_str(b"key2")
value1_b64 = b64encode_to_str(b"value1")
value2_b64 = b64encode_to_str(b"value2")
expected_kvs = {
    key1_b64: value1_b64,
    key2_b64: value2_b64,
}
expected_prefixes = [{
    "etcd_key_values_b64": expected_kvs,
    "prefix_b64": b64encode_to_str(prefix)
} for prefix in DummyM3BackupOp.etcd_prefixes]

expected_plugin_data = {"etcd": {"prefixes": expected_prefixes}}


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_at", BACKUP_FAILS)
async def test_m3_backup(fail_at):
    op = DummyM3BackupOp()
    assert op.steps == ['retrieve_etcd', 'retrieve_etcd_again', 'create_m3_manifest']
    with respx.mock:
        if fail_at != 1:
            respx.post(
                "http://dummy/etcd/kv/range",
                content={"kvs": [
                    {
                        "key": key1_b64,
                        "value": value1_b64
                    },
                    {
                        "key": key2_b64,
                        "value": value2_b64
                    },
                ]}
            )
        assert await op.try_run() == (fail_at is None)
    if fail_at:
        return
    assert op.plugin_data == expected_plugin_data


class DummyM3RestoreOp(m3.M3RestoreOp):
    steps = ["backup_manifest", "restore_etcd"]

    def __init__(self):
        # pylint: disable=super-init-not-called
        self.config = CoordinatorConfig.parse_obj({
            "plugin": "m3",
            "plugin_config": {
                "etcd_url": "http://dummy/etcd"
            },
        })

    async def _download_backup_dict(self):
        return {
            "plugin": "m3",
            "plugin_data": expected_plugin_data,
            "attempt": 1,
            "snapshot_results": [],
            "start": "2020-01-01 12:00",
        }


RESTORE_FAILS = [1, 2, None]


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_at", RESTORE_FAILS)
async def test_m3_restore(fail_at):
    op = DummyM3RestoreOp()
    with respx.mock:
        if fail_at != 1:
            respx.post("http://dummy/etcd/kv/deleterange", content={"ok": True})
        if fail_at != 2:
            respx.post("http://dummy/etcd/kv/put", content={"ok": True})
        assert await op.try_run() == (fail_at is None)
