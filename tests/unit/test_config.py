"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""
from astacus import config
from fastapi import FastAPI

import pathlib
import pytest


@pytest.mark.parametrize("path", list((pathlib.Path(config.__file__).parent.parent / "examples").glob("*.conf")))
def test_config_sample_load(path):
    app = FastAPI()
    config.set_global_config_from_path(app, path)
