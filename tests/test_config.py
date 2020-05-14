"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""
from astacus import config
from fastapi import FastAPI

import pathlib
import pytest


def test_config_sample_load():
    sample_path = pathlib.Path(config.__file__).parent.parent / "astacus.conf"
    if not sample_path.is_file():
        pytest.skip("no configuration file")
    app = FastAPI()
    config.set_global_config_from_path(app, sample_path)
