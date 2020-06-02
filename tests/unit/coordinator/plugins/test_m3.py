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

ENV = "dummyenv"

COORDINATOR_CONFIG = {
    "plugin": "m3",
    "plugin_config": {
        "etcd_url": "http://dummy/etcd",
        "environment": ENV,
    },
}


class DummyM3BackupOp(m3.M3BackupOp):
    def __init__(self):
        # pylint: disable=super-init-not-called
        self.config = CoordinatorConfig.parse_obj(COORDINATOR_CONFIG)
        self.steps = [step for step in self.steps if getattr(base.BackupOpBase, f"step_{step}", None) is None]


BACKUP_FAILS = [1, None]

KEY1_B64 = b64encode_to_str(b"key1")
KEY2_B64 = b64encode_to_str(b"key2")
VALUE1_B64 = b64encode_to_str(b"value1")
VALUE2_B64 = b64encode_to_str(b"value2")

PREFIXES = [{
    "etcd_key_values_b64": {
        KEY1_B64: VALUE1_B64,
        KEY2_B64: VALUE2_B64,
    },
    "prefix_b64": b64encode_to_str(prefix.format(env=ENV).encode())
} for prefix in DummyM3BackupOp.etcd_prefix_formats]

PLUGIN_DATA = {"etcd": {"prefixes": PREFIXES}}


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_at", BACKUP_FAILS)
async def test_m3_backup(fail_at):
    op = DummyM3BackupOp()
    assert op.steps == ['init', 'retrieve_etcd', 'retrieve_etcd_again', 'create_m3_manifest']
    with respx.mock:
        if fail_at != 1:
            respx.post(
                "http://dummy/etcd/kv/range",
                content={"kvs": [
                    {
                        "key": KEY1_B64,
                        "value": VALUE1_B64
                    },
                    {
                        "key": KEY2_B64,
                        "value": VALUE2_B64
                    },
                ]}
            )
        assert await op.try_run() == (fail_at is None)
    if fail_at:
        return
    assert op.plugin_data == PLUGIN_DATA


class DummyM3RestoreOp(m3.M3RestoreOp):
    steps = ["backup_manifest", "restore_etcd"]

    def __init__(self):
        # pylint: disable=super-init-not-called
        self.config = CoordinatorConfig.parse_obj(COORDINATOR_CONFIG)

    async def _download_backup_dict(self):
        return {
            "plugin": "m3",
            "plugin_data": PLUGIN_DATA,
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
