"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from pydantic import PydanticValueError
from pydantic.fields import ModelField
from pydantic.validators import _VALIDATORS
from starlette.responses import JSONResponse
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


def register_msgspec_glue() -> None:
    validator = (msgspec.Struct, [validate_struct])
    if validator not in _VALIDATORS:
        _VALIDATORS.append(validator)


class StructResponse(JSONResponse):
    def render(self, content: msgspec.Struct) -> bytes:
        return msgspec.json.encode(content)
