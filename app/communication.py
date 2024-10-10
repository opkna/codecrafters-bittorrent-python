from contextlib import contextmanager
from io import BufferedRWPair
from socket import AddressFamily, SocketKind, socket
from struct import iter_unpack
from typing import Generator
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

MAX_PORT = 65535


class Address(tuple):
    _STRUCT_FORMAT = ">4sH"

    def __new__(cls, ip: str, port: int) -> "Address":
        return tuple.__new__(cls, (ip, port))

    def __repr__(self) -> str:
        return f"{self[0]}:{self[1]}"

    @classmethod
    def from_bytes_to_many(cls, data: bytes) -> Generator["Address", None, None]:
        assert len(data) % 6 == 0
        for ip, port in iter_unpack(cls._STRUCT_FORMAT, data):
            ip_str = ".".join(repr(b) for b in ip)
            yield cls(ip_str, port)

    @classmethod
    def from_str(cls, data: str) -> "Address":
        ip, port_str = data.split(":")
        port = int(port_str)
        assert all(0 <= int(p) <= 255 for p in ip.split("."))
        assert 0 <= port <= MAX_PORT
        return cls(ip, port)


def get_request(base_url: str, query: dict | None = None):
    url = base_url
    if query:
        url = f"{url}?{urlencode(query)}"

    request = Request(
        url=url,
        method="GET",
    )
    try:
        with urlopen(request) as res:
            return res.read()
    except HTTPError as err:
        print(f"Failed request to {url}: {err}")
        raise


@contextmanager
def socket_request_rw(address: Address) -> Generator[BufferedRWPair, None, None]:
    with socket(AddressFamily.AF_INET, SocketKind.SOCK_STREAM) as s:
        s.connect(address)
        with s.makefile("brw", buffering=1) as io:
            yield io
