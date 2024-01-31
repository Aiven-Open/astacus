"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""
from astacus.common.json_view import (
    get_any,
    get_array,
    get_bool,
    get_float,
    get_int,
    get_object,
    get_optional_bool,
    get_optional_float,
    get_optional_int,
    get_optional_object,
    get_optional_str,
    get_str,
    iter_objects,
    JsonObjectView,
    JsonParseError,
    JsonView,
    Undefined,
)
from collections import ChainMap
from collections.abc import Sequence
from types import MappingProxyType

import pytest


def test_iter_objects_accepts_a_list_of_dicts() -> None:
    assert list(iter_objects([{}, {}])) == [{}, {}]


def test_iter_objects_accepts_a_tuple_of_dicts() -> None:
    assert list(iter_objects(({}, {}))) == [{}, {}]


def test_iter_objects_accepts_a_list_of_other_mappings() -> None:
    other_mappings: Sequence[JsonObjectView] = [ChainMap(), MappingProxyType({})]
    assert list(iter_objects(other_mappings)) == other_mappings


def test_iter_objects_rejects_a_non_dict_in_the_list() -> None:
    with pytest.raises(JsonParseError, match="Unexpected type at index 1: expected Mapping, got int"):
        list(iter_objects([{}, 123, {}]))


def test_get_array_accepts_a_list() -> None:
    assert get_array({"data": [{"key": "value"}]}, "data") == [{"key": "value"}]


def test_get_array_accepts_a_tuple() -> None:
    assert get_array({"data": ({"key": "value"},)}, "data") == ({"key": "value"},)


def test_get_array_rejects_a_non_list() -> None:
    with pytest.raises(JsonParseError, match="Unexpected type for data: expected Sequence, got int"):
        list(get_array({"data": 123}, "data"))


def test_get_array_rejects_a_missing_key() -> None:
    with pytest.raises(JsonParseError, match="Missing key in object: data"):
        list(get_array({}, "data"))


def test_get_array_accepts_a_missing_key_if_passed_a_default() -> None:
    assert list(get_array({}, "data", [{"key": "value"}])) == [{"key": "value"}]


@pytest.mark.parametrize("mapping", [{}, ChainMap(), MappingProxyType({})])
def test_get_object_accepts_any_mapping(mapping: JsonObjectView) -> None:
    assert get_object({"data": mapping}, "data") == mapping


def test_get_object_rejects_a_non_mapping() -> None:
    with pytest.raises(JsonParseError, match="Unexpected type for data: expected Mapping, got int"):
        get_object({"data": 123}, "data")


def test_get_object_rejects_a_missing_key() -> None:
    with pytest.raises(JsonParseError, match="Missing key in object: data"):
        get_object({}, "data")


def test_get_object_accepts_a_missing_key_if_passed_a_default() -> None:
    assert get_object({}, "data", {"key": "value"}) == {"key": "value"}


def test_get_str_accepts_a_str() -> None:
    assert get_str({"data": "hello"}, "data") == "hello"


def test_get_str_rejects_a_non_str() -> None:
    with pytest.raises(JsonParseError, match="Unexpected type for data: expected str, got int"):
        get_str({"data": 123}, "data")


def test_get_str_rejects_a_missing_key() -> None:
    with pytest.raises(JsonParseError, match="Missing key in object: data"):
        get_str({}, "data")


def test_get_str_accepts_a_missing_key_if_passed_a_default() -> None:
    assert get_str({}, "data", "hello") == "hello"


def test_get_int_accepts_an_int() -> None:
    assert get_int({"data": 123}, "data") == 123


def test_get_int_rejects_a_non_int() -> None:
    with pytest.raises(JsonParseError, match="Unexpected type for data: expected int, got str"):
        get_int({"data": "hello"}, "data")


def test_get_int_rejects_a_bool() -> None:
    with pytest.raises(JsonParseError, match="Unexpected type for data: expected int, got bool"):
        get_int({"data": False}, "data")


def test_get_int_rejects_a_default_bool() -> None:
    with pytest.raises(ValueError, match="Unexpected type for default value, expected int or Undefined, got bool"):
        get_int({"data": 123}, "data", default=False)


def test_get_int_rejects_a_missing_key() -> None:
    with pytest.raises(JsonParseError, match="Missing key in object: data"):
        get_int({}, "data")


def test_get_int_accepts_a_missing_key_if_passed_a_default() -> None:
    assert get_int({}, "data", 123) == 123


def test_get_float_accepts_a_float() -> None:
    assert get_float({"data": 123.0}, "data") == 123.0


def test_get_float_accepts_an_int() -> None:
    assert get_float({"data": 123}, "data") == 123.0


def test_get_float_rejects_a_non_float() -> None:
    with pytest.raises(JsonParseError, match="Unexpected type for data: expected float, got str"):
        get_float({"data": "hello"}, "data")


def test_get_float_rejects_a_missing_key() -> None:
    with pytest.raises(JsonParseError, match="Missing key in object: data"):
        get_float({}, "data")


def test_get_float_accepts_a_missing_key_if_passed_a_default_float() -> None:
    assert get_float({}, "data", 123.0) == 123.0


def test_get_float_accepts_a_missing_key_if_passed_a_default_int() -> None:
    assert get_float({}, "data", 123) == 123.0


def test_get_bool_accepts_an_bool() -> None:
    assert get_bool({"data": True}, "data") is True


def test_get_bool_rejects_a_non_bool() -> None:
    with pytest.raises(JsonParseError, match="Unexpected type for data: expected bool, got str"):
        get_bool({"data": "hello"}, "data")


def test_get_bool_rejects_a_missing_key() -> None:
    with pytest.raises(JsonParseError, match="Missing key in object: data"):
        get_bool({}, "data")


def test_get_bool_accepts_a_missing_key_if_passed_a_default() -> None:
    assert get_bool({}, "data", True) is True


@pytest.mark.parametrize("mapping", [{}, ChainMap(), MappingProxyType({})])
def test_get_optional_object_accepts_a_mapping(mapping: JsonObjectView) -> None:
    assert get_optional_object({"data": mapping}, "data") == mapping


def test_get_optional_object_accepts_a_none() -> None:
    assert get_optional_object({"data": None}, "data") is None


def test_get_optional_object_accepts_a_missing_key() -> None:
    assert get_optional_object({}, "data") is None


def test_get_optional_object_rejects_a_missing_key_if_passed_undefined() -> None:
    with pytest.raises(JsonParseError, match="Missing key in object: data"):
        get_optional_object({}, "data", Undefined.undef)


def test_get_optional_object_rejects_a_non_object() -> None:
    with pytest.raises(JsonParseError, match="Unexpected type for data: expected Mapping or None, got int"):
        get_optional_object({"data": 123}, "data")


def test_get_optional_bool_accepts_a_bool() -> None:
    assert get_optional_bool({"data": True}, "data") is True


def test_get_optional_bool_accepts_a_none() -> None:
    assert get_optional_bool({"data": None}, "data") is None


def test_get_optional_bool_accepts_a_missing_key() -> None:
    assert get_optional_bool({}, "data") is None


def test_get_optional_bool_rejects_a_missing_key_if_passed_undefined() -> None:
    with pytest.raises(JsonParseError, match="Missing key in object: data"):
        get_optional_bool({}, "data", Undefined.undef)


def test_get_optional_bool_rejects_a_non_bool() -> None:
    with pytest.raises(JsonParseError, match="Unexpected type for data: expected bool or None, got int"):
        get_optional_bool({"data": 123}, "data")


def test_get_optional_str_accepts_a_str() -> None:
    assert get_optional_str({"data": "hello"}, "data") == "hello"


def test_get_optional_str_accepts_a_none() -> None:
    assert get_optional_str({"data": None}, "data") is None


def test_get_optional_str_accepts_a_missing_key() -> None:
    assert get_optional_str({}, "data") is None


def test_get_optional_str_rejects_a_missing_key_if_passed_undefined() -> None:
    with pytest.raises(JsonParseError, match="Missing key in object: data"):
        get_optional_str({}, "data", Undefined.undef)


def test_get_optional_str_rejects_a_non_str() -> None:
    with pytest.raises(JsonParseError, match="Unexpected type for data: expected str or None, got int"):
        get_optional_str({"data": 123}, "data")


def test_get_optional_int_accepts_a_int() -> None:
    assert get_optional_int({"data": 123}, "data") == 123


def test_get_optional_int_accepts_a_none() -> None:
    assert get_optional_int({"data": None}, "data") is None


def test_get_optional_int_accepts_a_missing_key() -> None:
    assert get_optional_int({}, "data") is None


def test_get_optional_int_rejects_a_missing_key_if_passed_undefined() -> None:
    with pytest.raises(JsonParseError, match="Missing key in object: data"):
        get_optional_int({}, "data", Undefined.undef)


def test_get_optional_int_rejects_a_non_int() -> None:
    with pytest.raises(JsonParseError, match="Unexpected type for data: expected int or None, got str"):
        get_optional_int({"data": "hello"}, "data")


def test_get_optional_int_rejects_a_bool() -> None:
    with pytest.raises(JsonParseError, match="Unexpected type for data: expected int or None, got bool"):
        get_optional_int({"data": False}, "data")


def test_get_optional_int_rejects_a_default_bool() -> None:
    with pytest.raises(ValueError, match="Unexpected type for default value, expected int, None or Undefined, got bool"):
        get_optional_int({"data": 123}, "data", default=False)


def test_get_optional_float_accepts_a_float() -> None:
    assert get_optional_float({"data": 123.0}, "data") == 123.0


def test_get_optional_float_accepts_an_int() -> None:
    assert get_optional_float({"data": 123}, "data") == 123.0


def test_get_optional_float_accepts_a_none() -> None:
    assert get_optional_float({"data": None}, "data") is None


def test_get_optional_float_accepts_a_missing_key() -> None:
    assert get_optional_float({}, "data") is None


def test_get_optional_float_rejects_a_missing_key_if_passed_undefined() -> None:
    with pytest.raises(JsonParseError, match="Missing key in object: data"):
        get_optional_float({}, "data", Undefined.undef)


def test_get_optional_float_rejects_a_non_float() -> None:
    with pytest.raises(JsonParseError, match="Unexpected type for data: expected float or None, got str"):
        get_optional_float({"data": "hello"}, "data")


def test_get_any_accepts_a_defined_key() -> None:
    assert get_any({"data": 123}, "data") == 123


def test_get_any_rejects_a_missing_key() -> None:
    with pytest.raises(JsonParseError, match="Missing key in object: data"):
        get_any({}, "data")


@pytest.mark.parametrize("default", (None, 123, "hellp"))
def test_get_any_accepts_a_missing_key_if_passed_a_default(default: JsonView) -> None:
    assert get_any({}, "data", default) == default
