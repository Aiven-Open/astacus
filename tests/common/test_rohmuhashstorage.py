"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Test that RohmuHashStorage works as advertised.

TBD: Test with something else than local files?

"""

from astacus.common.rohmuhashstorage import RohmuConfig, RohmuHashStorage
from pathlib import Path

TEST_HEXDIGEST = "deadbeef"
TEST_DATA = b"data" * 15


def test_rohmuhashstorage(tmpdir):
    rohmu_path = Path(tmpdir) / "rohmu"
    rohmu_path.mkdir()
    tmp_path = Path(tmpdir) / "tmp"
    tmp_path.mkdir()
    config = RohmuConfig.parse_obj({
        "temporary_directory": str(tmp_path),
        "backup_target_storage": "x",
        "compression": {
            "algorithm": "zstd"
        },
        "storages": {
            "x": {
                "storage_type": "local",
                "directory": str(rohmu_path),
            }
        }
    })
    storage = RohmuHashStorage(config=config)
    storage.upload_hexdigest_bytes(TEST_HEXDIGEST, TEST_DATA)
    assert storage.download_hexdigest_bytes(TEST_HEXDIGEST) == TEST_DATA
    assert storage.list_hexdigests() == [TEST_HEXDIGEST]
    storage.delete_hexdigest(TEST_HEXDIGEST)
    assert storage.list_hexdigests() == []
