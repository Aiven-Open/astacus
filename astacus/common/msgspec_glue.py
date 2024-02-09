"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""
from pathlib import Path
from pydantic import PydanticValueError
from pydantic.fields import ModelField
from pydantic.validators import _VALIDATORS
from typing import Any

import msgspec


class MsgSpecError(PydanticValueError):
    msg_template = "{value} is not a valid msgspec {type}"


def validate_struct(v: Any, field: ModelField) -> msgspec.Struct:
    if isinstance(v, msgspec.Struct) and isinstance(v, field.annotation):
        return v
    if isinstance(v, dict):
        return msgspec.convert(v, field.annotation)
    raise MsgSpecError(value=v, type=field.annotation)


_VALIDATORS.append((msgspec.Struct, [validate_struct]))


def dec_hook(expected_type: type, obj: Any) -> Any:
    if expected_type is Path:
        return Path(obj)
    raise NotImplementedError(expected_type)


def enc_hook(obj: Any) -> Any:
    if isinstance(obj, Path):
        return str(obj)
    raise NotImplementedError(obj)
