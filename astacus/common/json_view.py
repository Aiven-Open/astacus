"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""
from collections.abc import Iterator, Mapping, Sequence
from typing import TypeAlias

import enum

JsonScalar: TypeAlias = str | int | float | None
JsonArrayView: TypeAlias = Sequence["JsonView"]
JsonObjectView: TypeAlias = Mapping[str, "JsonView"]
JsonView: TypeAlias = JsonScalar | JsonObjectView | JsonArrayView


class JsonParseError(ValueError):
    pass


class Undefined(enum.Enum):
    undef = object()


def iter_objects(json_array_view: JsonArrayView) -> Iterator[JsonObjectView]:
    """Return an iterator of JsonObjectView, checking that all items in the list are objects."""
    for i, item in enumerate(json_array_view):
        if not isinstance(item, Mapping):
            raise JsonParseError(f"Unexpected type at index {i}: expected Mapping, got {item.__class__.__name__}")
        yield item


def get_array(
    json_object_view: JsonObjectView, key: str, default: JsonArrayView | Undefined = Undefined.undef
) -> JsonArrayView:
    """Return an iterator of JsonObjectView, the key is required unless a default is provided."""
    value = get_any(json_object_view, key, default)
    if not isinstance(value, Sequence):
        raise JsonParseError(f"Unexpected type for {key}: expected Sequence, got {value.__class__.__name__}")
    return value


def get_object(
    json_object_view: JsonObjectView, key: str, default: JsonObjectView | Undefined = Undefined.undef
) -> JsonObjectView:
    """Return an JsonObjectView, the key is required unless a default is provided."""
    value = get_any(json_object_view, key, default)
    if not isinstance(value, Mapping):
        raise JsonParseError(f"Unexpected type for {key}: expected Mapping, got {value.__class__.__name__}")
    return value


def get_str(json_object_view: JsonObjectView, key: str, default: str | Undefined = Undefined.undef) -> str:
    """Return a str, the key is required unless a default is provided."""
    value = get_any(json_object_view, key, default)
    if not isinstance(value, str):
        raise JsonParseError(f"Unexpected type for {key}: expected str, got {value.__class__.__name__}")
    return value


def get_int(json_object_view: JsonObjectView, key: str, default: int | Undefined = Undefined.undef) -> int:
    """
    Return an int, the key is required unless a default is provided.
    Despite bool being a subtype of int in Python, they are dynamically rejected to avoid confusion
    and bugs when 1 and True are both accepted in a JSON file but with a different meaning.
    """
    if isinstance(default, bool):
        raise ValueError("Unexpected type for default value, expected int or Undefined, got bool")
    value = get_any(json_object_view, key, default)
    if not isinstance(value, int) or isinstance(value, bool):
        raise JsonParseError(f"Unexpected type for {key}: expected int, got {value.__class__.__name__}")
    return value


def get_float(json_object_view: JsonObjectView, key: str, default: float | Undefined = Undefined.undef) -> float:
    """Return a float, the key is required unless a default is provided."""
    value = get_any(json_object_view, key, default)
    if isinstance(value, int):
        return float(value)
    if not isinstance(value, float):
        raise JsonParseError(f"Unexpected type for {key}: expected float, got {value.__class__.__name__}")
    return value


def get_bool(json_object_view: JsonObjectView, key: str, default: bool | Undefined = Undefined.undef) -> bool:
    """Return a bool, the key is required unless a default is provided."""
    value = get_any(json_object_view, key, default)
    if not isinstance(value, bool):
        raise JsonParseError(f"Unexpected type for {key}: expected bool, got {value.__class__.__name__}")
    return value


def get_optional_object(
    json_object_view: JsonObjectView, key: str, default: JsonObjectView | None | Undefined = None
) -> JsonObjectView | None:
    """Return a JsonObjectView or None, the key is optional unless the default is set to `Undefined.undef`."""
    value = get_any(json_object_view, key, default)
    if value is not None and not isinstance(value, Mapping):
        raise JsonParseError(f"Unexpected type for {key}: expected Mapping or None, got {value.__class__.__name__}")
    return value


def get_optional_bool(json_object_view: JsonObjectView, key: str, default: bool | None | Undefined = None) -> bool | None:
    """Return a bool or None, the key is optional unless the default is set to `Undefined.undef`."""
    value = get_any(json_object_view, key, default)
    if value is not None and not isinstance(value, bool):
        raise JsonParseError(f"Unexpected type for {key}: expected bool or None, got {value.__class__.__name__}")
    return value


def get_optional_str(json_object_view: JsonObjectView, key: str, default: str | None | Undefined = None) -> str | None:
    """Return a str or None, the key is optional unless the default is set to `Undefined.undef`."""
    value = get_any(json_object_view, key, default)
    if value is not None and not isinstance(value, str):
        raise JsonParseError(f"Unexpected type for {key}: expected str or None, got {value.__class__.__name__}")
    return value


def get_optional_int(json_object_view: JsonObjectView, key: str, default: int | None | Undefined = None) -> int | None:
    """
    Return an int or None, the key is optional unless the default is set to `Undefined.undef`.
    Despite bool being a subtype of int in Python, they are dynamically rejected to avoid confusion
    and bugs when 1 and True are both accepted in a JSON file but with a different meaning.
    """
    if isinstance(default, bool):
        raise ValueError("Unexpected type for default value, expected int, None or Undefined, got bool")
    value = get_any(json_object_view, key, default)
    if value is not None and (not isinstance(value, int) or isinstance(value, bool)):
        raise JsonParseError(f"Unexpected type for {key}: expected int or None, got {value.__class__.__name__}")
    return value


def get_optional_float(json_object_view: JsonObjectView, key: str, default: float | None | Undefined = None) -> float | None:
    """Return a float or None, the key is optional unless the default is set to `Undefined.undef`."""
    value = get_any(json_object_view, key, default)
    if isinstance(value, int):
        return float(value)
    if value is not None and not isinstance(value, float):
        raise JsonParseError(f"Unexpected type for {key}: expected float or None, got {value.__class__.__name__}")
    return value


def get_any(json_object_view: JsonObjectView, key: str, default: JsonView | Undefined = Undefined.undef) -> JsonView:
    """Return a JsonView, the key is required unless a default is provided."""
    if default is not Undefined.undef:
        return json_object_view.get(key, default)
    try:
        return json_object_view[key]
    except KeyError as e:
        raise JsonParseError(f"Missing key in object: {key}") from e
