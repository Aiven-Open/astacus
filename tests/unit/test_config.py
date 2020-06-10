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


@pytest.mark.parametrize("path", JSON_CONFS + YAML_CONFS)
def test_config_sample_load(path, tmpdir):
    astacus_dir = tmpdir / "astacus"
    astacus_dir.mkdir()

    (astacus_dir / "src").mkdir()  # root
    (astacus_dir / "backup").mkdir()  # object storage

    app = FastAPI()
    rewritten_conf = tmpdir / "astacus.conf"

    conf = path.read_text()
    conf = conf.replace("/tmp/astacus", str(astacus_dir))
    rewritten_conf.write_text(conf, encoding="ascii")
    config.set_global_config_from_path(app, rewritten_conf)
