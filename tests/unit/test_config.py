"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""
from astacus import config
from fastapi import FastAPI
from pathlib import Path

import pytest

EXAMPLE_PATH = Path(config.__file__).parent.parent / "examples"
JSON_CONFS = list(EXAMPLE_PATH.glob("astacus*.json"))
YAML_CONFS = list(EXAMPLE_PATH.glob("astacus*.yaml"))


@pytest.mark.parametrize("path", JSON_CONFS + YAML_CONFS, ids=lambda path: path.name)
def test_config_sample_load(path: Path, tmp_path: Path) -> None:
    astacus_dir = tmp_path / "astacus"
    astacus_dir.mkdir()

    (astacus_dir / "src").mkdir()  # root
    (astacus_dir / "backup").mkdir()  # object storage

    (astacus_dir / "cassandra").mkdir()  # cassandra data
    (astacus_dir / "m3").mkdir()  # m3 data

    app = FastAPI()
    rewritten_conf = tmp_path / "astacus.conf"

    conf = path.read_text()
    conf = conf.replace("/tmp/astacus", str(astacus_dir)).replace("example/cassandra", str(EXAMPLE_PATH / "cassandra"))
    rewritten_conf.write_text(conf, encoding="ascii")
    config.set_global_config_from_path(app, rewritten_conf)
