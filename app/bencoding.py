from itertools import chain
from typing import Any


BYTE_I = ord(b"i")
BYTE_L = ord(b"l")
BYTE_D = ord(b"d")
BYTE_E = ord(b"e")
BYTE_0 = ord(b"0")
BYTE_9 = ord(b"9")
BYTE_COL = ord(b":")


def decode_bencode(bencoded_value: bytes) -> int | bytes | list | dict:
    return _decode_bencode_impl(bencoded_value, 0)[0]


def _decode_bencode_impl(
    input: bytes, start_idx: int
) -> tuple[int | bytes | list | dict, int]:
    prefix = input[start_idx]
    if prefix == BYTE_I:
        # i123e
        end_idx = input.index(BYTE_E, start_idx + 2)
        int_value = int(input[start_idx + 1 : end_idx])
        return (int_value, end_idx + 1)
    elif BYTE_0 <= prefix <= BYTE_9:
        # 3:abc
        col_idx = input.index(BYTE_COL, start_idx + 1)
        length = int(input[start_idx:col_idx])
        end_idx = col_idx + 1 + length
        str_value = input[col_idx + 1 : end_idx]
        return (str_value, end_idx)
    elif prefix == BYTE_L:
        idx = start_idx + 1
        list_value = []
        while input[idx] != BYTE_E:
            value, next_idx = _decode_bencode_impl(input, idx)
            list_value.append(value)
            idx = next_idx
        return (list_value, idx + 1)
    elif prefix == BYTE_D:
        idx = start_idx + 1
        dict_value = {}
        while input[idx] != BYTE_E:
            key, next_idx = _decode_bencode_impl(input, idx)
            if isinstance(key, bytes):
                key = str(key, "utf-8")
            value, next_idx = _decode_bencode_impl(input, next_idx)
            dict_value[key] = value
            idx = next_idx
        return (dict_value, idx + 1)
    else:
        raise ValueError(f"Unknown prefix '{chr(prefix)}'")


def encode_bencode(value: int | bytes | str | list | dict) -> bytes:
    if isinstance(value, int):
        return f"i{value}e".encode()
    elif isinstance(value, bytes):
        return f"{len(value)}:".encode() + value
    elif isinstance(value, str):
        return f"{len(value)}:{value}".encode()
    elif isinstance(value, list):
        result = b"l"
        for item in value:
            result += encode_bencode(item)
        return result + b"e"
    elif isinstance(value, dict):
        result = b"d"
        for key, item_value in value.items():
            result += encode_bencode(key)
            result += encode_bencode(item_value)
        return result + b"e"
    else:
        raise TypeError(f"Can't bencode type {type(value)}")
