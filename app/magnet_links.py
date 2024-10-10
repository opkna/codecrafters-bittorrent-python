from dataclasses import dataclass
from re import RegexFlag, compile
from typing import Literal, Self, TypedDict
from urllib.parse import parse_qs, urlparse

# v1: magnet:?xt=urn:btih:<info-hash>&dn=<name>&tr=<tracker-url>&x.pe=<peer-address>
# v2: magnet:?xt=urn:btmh:<tagged-info-hash>&dn=<name>&tr=<tracker-url>&x.pe=<peer-address>

XT_REGEX = compile(r"urn:(?P<hash_type>bt[im]h):(?P<hash_hex>[0-9a-f]+)", RegexFlag.I)

RawMagnetLink = TypedDict(
    "RawMagnetLink", {"xt": str, "dn": str | None, "tr": str | None, "x.pe": str | None}
)


@dataclass
class MagnetLink:
    hash_type: Literal["btih", "btmh"]
    hash: bytes
    file_name: str | None
    announce: str | None
    peer_address: str | None

    @staticmethod
    def from_raw(raw_link: RawMagnetLink) -> Self:
        xt_match = XT_REGEX.match(raw_link["xt"])
        assert xt_match
        groups = xt_match.groupdict()
        return MagnetLink(
            hash_type=groups["hash_type"],
            hash=bytes.fromhex(groups["hash_hex"]),
            file_name=raw_link.get("dn"),
            announce=raw_link.get("tr"),
            peer_address=raw_link.get("x.pe"),
        )


def parse_magnet_link(link: str):
    url = urlparse(link)
    assert url.scheme == "magnet"
    assert not any([url.netloc, url.path, url.params, url.fragment])

    query = parse_qs(url.query)
    assert all(map(lambda v: len(v) == 1, query.values()))

    raw_link: RawMagnetLink = {k: v[0] for k, v in query.items()}
    return MagnetLink.from_raw(raw_link)
