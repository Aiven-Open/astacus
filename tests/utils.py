"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from astacus.common.rohmustorage import RohmuConfig
from pathlib import Path

# These test keys are from copied from pghoard

CONSTANT_TEST_RSA_PUBLIC_KEY = """\
-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDQ9yu7rNmu0GFMYeQq9Jo2B3d9
hv5t4a+54TbbxpJlks8T27ipgsaIjqiQP7+uXNfU6UCzGFEHs9R5OELtO3Hq0Dn+
JGdxJlJ1prxVkvjCICCpiOkhc2ytmn3PWRuVf2VyeAddslEWHuXhZPptvIr593kF
lWN+9KPe+5bXS8of+wIDAQAB
-----END PUBLIC KEY-----"""

CONSTANT_TEST_RSA_PRIVATE_KEY = """\
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


def create_rohmu_config(tmpdir, *, compression=True, encryption=True):
    rohmu_path = Path(tmpdir) / "rohmu"
    rohmu_path.mkdir(exist_ok=True)
    tmp_path = Path(tmpdir) / "rohmu-tmp"
    tmp_path.mkdir(exist_ok=True)
    config = {
        "temporary_directory": str(tmp_path),
        "default_storage": "x",
        "storages": {
            "x": {
                "storage_type": "local",
                "directory": str(rohmu_path),
            }
        }
    }
    if compression:
        config.update({"compression": {"algorithm": "zstd"}})
    if encryption:
        config.update({
            "encryption_key_id": "foo",
            "encryption_keys": {
                "foo": {
                    "private": CONSTANT_TEST_RSA_PRIVATE_KEY,
                    "public": CONSTANT_TEST_RSA_PUBLIC_KEY
                }
            }
        })
    return RohmuConfig.parse_obj(config)
