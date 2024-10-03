from struct import iter_unpack
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from urllib.parse import urlencode

from app.bencoding import decode_bencode
from app.metainfo import MetaInfoFile


def _get_request(url: str):
    request = Request(
        url=url,
        method="GET",
    )
    try:
        with urlopen(request) as res:
            return res.read()
    except HTTPError as err:
        print(f"Failed request to {url}")
        raise


def _parse_peers_ip_port(data: bytes):
    assert len(data) % 6 == 0
    result: list[tuple[str, int]] = []
    for values in iter_unpack(">BBBBH", data):
        ip = ".".join([str(s) for s in values[:4]])
        port = values[4]
        result.append((ip, port))
    return result


class PeersResponse:
    def __init__(self, data: bytes) -> None:
        values = decode_bencode(data)
        self.interval: int = values["interval"]
        self.complete: int = values["complete"]
        self.incomplete: int = values["incomplete"]
        self.min_interval: int = values["min interval"]
        self.peers = _parse_peers_ip_port(values["peers"])

    def peers_urls(self) -> list[str]:
        return [f"{p[0]}:{p[1]}" for p in self.peers]


def fetch_peers(meta_info_file: MetaInfoFile):
    query = urlencode(
        {
            "info_hash": meta_info_file.info.sha1_hash(),
            "peer_id": "00112233445566778899",
            "port": 6881,
            "uploaded": 0,
            "downloaded": 0,
            "left": meta_info_file.info.length,
            "compact": 1,
        }
    )
    url = f"{meta_info_file.announce}?{query}"
    data = _get_request(url)
    return PeersResponse(data)

    """
    info_hash: the info hash of the torrent
        20 bytes long, will need to be URL encoded
        Note: this is NOT the hexadecimal representation, which is 40 bytes long
    peer_id: a unique identifier for your client
        A string of length 20 that you get to pick. You can use something like 00112233445566778899.
    port: the port your client is listening on
        You can set this to 6881, you will not have to support this functionality during this challenge.
    uploaded: the total amount uploaded so far
        Since your client hasn't uploaded anything yet, you can set this to 0.
    downloaded: the total amount downloaded so far
        Since your client hasn't downloaded anything yet, you can set this to 0.
    left: the number of bytes left to download
        Since you client hasn't downloaded anything yet, this'll be the total length of the file (you've extracted this value from the torrent file in previous stages)
    compact: whether the peer list should use the compact representation
        For the purposes of this challenge, set this to 1.
        The compact representation is more commonly used in the wild, the non-compact representation is mostly supported for backward-compatibility.
    """
