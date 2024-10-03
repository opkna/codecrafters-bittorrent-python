import json
import sys
from typing import Any

BYTE_I = ord(b"i")
BYTE_L = ord(b"l")
BYTE_D = ord(b"d")
BYTE_E = ord(b"e")
BYTE_0 = ord(b"0")
BYTE_9 = ord(b"9")
BYTE_COL = ord(b":")


# - decode_bencode(b"5:hello") -> b"hello"
# - decode_bencode(b"10:hello12345") -> b"hello12345"
def decode_bencode(bencoded_value):
    return _decode_bencode_impl(bencoded_value, 0)[0]


def _decode_bencode_impl(input: bytes, start_idx: int) -> tuple[Any, int]:
    prefix = input[start_idx]
    if prefix == BYTE_I:
        # i123e
        end_idx = input.index(BYTE_E, start_idx + 2)
        value = int(input[start_idx + 1 : end_idx])
        return (value, end_idx + 1)
    elif BYTE_0 <= prefix <= BYTE_9:
        # 3:abc
        col_idx = input.index(BYTE_COL, start_idx + 1)
        length = int(input[start_idx : col_idx])
        end_idx = col_idx + 1 + length
        value = str(input[col_idx + 1 : end_idx], "utf-8")
        return (value, end_idx)
    elif prefix == BYTE_L:
        idx = start_idx + 1
        list_value = []
        while input[idx] != BYTE_E:
            value, next_idx = _decode_bencode_impl(input, idx)
            list_value.append(value)
            idx = next_idx
        return (list_value, idx + 1)
    else:
        raise RuntimeError(f"Unknown prefix '{chr(prefix)}'")


def main():
    command = sys.argv[1]
    if command == "decode":
        bencoded_value = sys.argv[2].encode()

        # json.dumps() can't handle bytes, but bencoded "strings" need to be
        # bytestrings since they might contain non utf-8 characters.
        #
        # Let's convert them to strings for printing to the console.
        def bytes_to_str(data):
            if isinstance(data, bytes):
                return data.decode()

            raise TypeError(f"Type not serializable: {type(data)}")

        print(json.dumps(decode_bencode(bencoded_value), default=bytes_to_str))
    else:
        raise NotImplementedError(f"Unknown command {command}")


if __name__ == "__main__":
    main()
