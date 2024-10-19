"""Copyright (c) 2022 Aiven Ltd
See LICENSE for details.
"""

from astacus.common import ipc
from astacus.coordinator.plugins.base import StepFailedError
from astacus.coordinator.plugins.cassandra import utils
from pytest_mock import MockerFixture
from unittest.mock import Mock

import pytest


@pytest.mark.parametrize("start_ok", [False, True])
async def test_run_subop(mocker: MockerFixture, start_ok: bool) -> None:
    async def request_from_nodes(*args, **kwargs):
        if start_ok:
            return 42
        return

    async def wait_successful_results(*, start_results, result_class):
        assert start_results == 42
        return 7

    cluster = Mock(request_from_nodes=request_from_nodes, wait_successful_results=wait_successful_results)
    try:
        result = await utils.run_subop(cluster=cluster, subop=ipc.CassandraSubOp.stop_cassandra, result_class=ipc.NodeResult)
    except StepFailedError:
        assert not start_ok
        return
    assert start_ok
    assert result == 7


@pytest.mark.parametrize(
    "hashes,result",
    [
        ([], ("", "Unable to retrieve schema hash at all")),
        ([1, 2], ("", "Multiple schema hashes present: [1, 2]")),
        ([1], (1, "")),
    ],
)
async def test_get_schema_hash(mocker: MockerFixture, hashes: list[int], result: tuple[str, str]) -> None:
    mocker.patch.object(utils, "run_subop", return_value=[Mock(schema_hash=hash) for hash in hashes])
    actual_result = await utils.get_schema_hash(mocker.Mock(nodes=[]))
    assert actual_result == result
