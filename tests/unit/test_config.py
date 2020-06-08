"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""
from astacus import config
from fastapi import FastAPI

import pathlib
import pytest

EXAMPLE_PATH = pathlib.Path(config.__file__).parent.parent / "examples"
JSON_CONFS = list(EXAMPLE_PATH.glob("astacus*.json"))
YAML_CONFS = list(EXAMPLE_PATH.glob("astacus*.yaml"))


@pytest.mark.parametrize("path", JSON_CONFS + YAML_CONFS)
def test_config_sample_load(path):
    app = FastAPI()
    config.set_global_config_from_path(app, path)
