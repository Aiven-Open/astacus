"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
import re


def escape_bytes_for_file_name(name_bytes: bytes) -> str:
    # This is based on ClickHouse's escapeForFileName which is used internally to
    # safely create both file and folders on disk and ZooKeeper node names.
    return re.sub(rb"[^a-zA-Z0-9_]", _percent_encode_byte, name_bytes).decode()


def escape_for_file_name(name: str) -> str:
    # This is based on ClickHouse's escapeForFileName which is used internally to
    # safely create both file and folders on disk and ZooKeeper node names.
    name_bytes = name.encode()
    return re.sub(rb"[^a-zA-Z0-9_]", _percent_encode_byte, name_bytes).decode()


def unescape_from_file_name_to_bytes(encoded_name: str) -> bytes:
    encoded_bytes = encoded_name.encode()
    return re.sub(rb"%([0-9A-F][0-9A-F])", _percent_decode_byte, encoded_bytes)


def unescape_from_file_name(encoded_name: str) -> str:
    encoded_bytes = encoded_name.encode()
    return re.sub(rb"%([0-9A-F][0-9A-F])", _percent_decode_byte, encoded_bytes).decode()


def _percent_encode_byte(match: re.Match) -> bytes:
    char = match.group(0)[0]
    return f"%{char:02X}".encode()


def _percent_decode_byte(match: re.Match) -> bytes:
    value = int(match.group(1), base=16)
    return value.to_bytes(1, "little")
