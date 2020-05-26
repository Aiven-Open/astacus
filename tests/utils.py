"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from astacus.common.rohmustorage import RohmuConfig
from pathlib import Path


def create_rohmu_config(tmpdir):
    rohmu_path = Path(tmpdir) / "rohmu"
    rohmu_path.mkdir(exist_ok=True)
    tmp_path = Path(tmpdir) / "rohmu-tmp"
    tmp_path.mkdir(exist_ok=True)
    return RohmuConfig.parse_obj({
        "temporary_directory": str(tmp_path),
        "default_storage": "x",
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
