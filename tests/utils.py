"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""
from astacus.common.rohmustorage import RohmuConfig
from collections.abc import Sequence
from pathlib import Path
from typing import Final

import importlib
import os
import py
import re
import subprocess
import sys

# These test keys are from copied from pghoard

CONSTANT_TEST_RSA_PUBLIC_KEY: Final = """\
-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDQ9yu7rNmu0GFMYeQq9Jo2B3d9
hv5t4a+54TbbxpJlks8T27ipgsaIjqiQP7+uXNfU6UCzGFEHs9R5OELtO3Hq0Dn+
JGdxJlJ1prxVkvjCICCpiOkhc2ytmn3PWRuVf2VyeAddslEWHuXhZPptvIr593kF
lWN+9KPe+5bXS8of+wIDAQAB
-----END PUBLIC KEY-----"""

CONSTANT_TEST_RSA_PRIVATE_KEY: Final = """\
-----BEGIN PRIVATE KEY-----
MIICdwIBADANBgkqhkiG9w0BAQEFAASCAmEwggJdAgEAAoGBAND3K7us2a7QYUxh
5Cr0mjYHd32G/m3hr7nhNtvGkmWSzxPbuKmCxoiOqJA/v65c19TpQLMYUQez1Hk4
Qu07cerQOf4kZ3EmUnWmvFWS+MIgIKmI6SFzbK2afc9ZG5V/ZXJ4B12yURYe5eFk
+m28ivn3eQWVY370o977ltdLyh/7AgMBAAECgYEAkuAobRFhL+5ndTiZF1g1zCQT
aLepvbITwaL63B8GZz55LowRj5PL18/tyvYD1JqNWalZQIim67MKdOmGoRhXSF22
gUc6/SeqD27/9rsj8I+j0TrzLdTZwn88oX/gtndNutZuryCC/7KbJ8j18Jjn5qf9
ZboRKbEc7udxOb+RcYECQQD/ZLkxIvMSj0TxPUJcW4MTEsdeJHCSnQAhreIf2omi
hf4YwmuU3qnFA3ROje9jJe3LNtc0TK1kvAqfZwdpqyAdAkEA0XY4P1CPqycYvTxa
dxxWJnYA8K3g8Gs/Eo8wYKIciP+K70Q0GRP9Qlluk4vrA/wJJnTKCUl7YuAX6jDf
WdV09wJALGHXoQde0IHfTEEGEEDC9YSU6vJQMdpg1HmAS2LR+lFox+q5gWR0gk1I
YAJgcI191ovQOEF+/HuFKRBhhGZ9rQJAXOt13liNs15/sgshEq/mY997YUmxfNYG
v+P3kRa5U+kRKD14YxukARgNXrT2R+k54e5zZhVMADvrP//4RTDVVwJBAN5TV9p1
UPZXbydO8vZgPuo001KoEd9N3inq/yNcsHoF/h23Sdt/rcdfLMpCWuIYs/JAqE5K
nkMAHqg9PS372Cs=
-----END PRIVATE KEY-----"""


def create_rohmu_config(tmpdir: py.path.local, *, compression: bool = True, encryption: bool = True) -> RohmuConfig:
    x_path = Path(tmpdir) / "rohmu-x"
    x_path.mkdir(exist_ok=True)
    y_path = Path(tmpdir) / "rohmu-y"
    y_path.mkdir(exist_ok=True)
    tmp_path = Path(tmpdir) / "rohmu-tmp"
    tmp_path.mkdir(exist_ok=True)
    config = {
        "temporary_directory": str(tmp_path),
        "default_storage": "x",
        "storages": {
            "x": {
                "storage_type": "local",
                "directory": str(x_path),
            },
            "y": {
                "storage_type": "local",
                "directory": str(y_path),
            },
        },
    }
    if compression:
        config.update({"compression": {"algorithm": "zstd"}})
    if encryption:
        config.update(
            {
                "encryption_key_id": "foo",
                "encryption_keys": {
                    "foo": {"private": CONSTANT_TEST_RSA_PRIVATE_KEY, "public": CONSTANT_TEST_RSA_PUBLIC_KEY}
                },
            }
        )
    return RohmuConfig.parse_obj(config)


def parse_clickhouse_version(command_output: bytes) -> tuple[int, ...]:
    version_match = re.search(r"([\d.]+\d)", command_output.decode())
    if not version_match:
        raise ValueError(f"Unable to parse version from command output: {command_output.decode()}")
    version_tuple = tuple(int(part) for part in version_match.group(0).split(".") if part)
    return version_tuple


def get_clickhouse_version(command: Sequence[str | Path]) -> tuple[int, ...]:
    version_command_output = subprocess.check_output([*command, "--version"])
    return parse_clickhouse_version(version_command_output)


def is_cassandra_driver_importable() -> bool:
    return importlib.util.find_spec("cassandra") is not None  # type: ignore[attr-defined]


def format_astacus_command(*arg: str) -> Sequence[str]:
    # If we're gathering coverage, run subprocesses under coverage run
    if os.environ.get("COVERAGE_RUN", None):
        return [sys.executable, "-m", "coverage", "run", "-m", "astacus.main"] + list(arg)
    return [sys.executable, "-m", "astacus.main"] + list(arg)
