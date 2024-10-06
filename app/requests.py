from socket import AddressFamily, SocketKind, socket
from struct import pack, unpack
from urllib.parse import urlencode

from app.bencoding import decode_bencode
from app.communication import Address, get_request
from app.metainfo import MetaInfoFile


########
# HTTP #
########
class PeersResponse:
    def __init__(self, data: bytes) -> None:
        values = decode_bencode(data)
        self.interval: int = values["interval"]
        self.complete: int = values["complete"]
        self.incomplete: int = values["incomplete"]
        self.min_interval: int = values["min interval"]
        self.addresses = list(Address.from_bytes_to_many(values["peers"]))


def fetch_peers(peer_id: str, meta_info_file: MetaInfoFile) -> PeersResponse:
    query = urlencode(
        {
            "info_hash": meta_info_file.info.get_info_hash(),
            "peer_id": peer_id,
            "port": 6881,
            "uploaded": 0,
            "downloaded": 0,
            "left": meta_info_file.info.length,
            "compact": 1,
        }
    )
    url = f"{meta_info_file.announce}?{query}"
    data = get_request(url)
    return PeersResponse(data)


###########
# Sockets #
###########
PROTOCOL_STRING = b"BitTorrent protocol"
assert len(PROTOCOL_STRING) == 19


class HandshakeResponse:
    def __init__(self, peer_id: bytes, info_hash: bytes):
        self.peer_id = peer_id
        self.info_hash = info_hash


def handshake(peer_id: str, address: Address, info_hash: bytes) -> HandshakeResponse:
    assert len(info_hash) == 20
    assert len(peer_id) == 20
    HEADER_FORMAT = "!b19sQ20s20s"
    HEADER_LENGTH = 68
    header = pack(
        HEADER_FORMAT,
        len(PROTOCOL_STRING),
        PROTOCOL_STRING,
        0,
        info_hash,
        bytes(peer_id, "ascii"),
    )
    assert len(header) == HEADER_LENGTH

    with socket(AddressFamily.AF_INET, SocketKind.SOCK_STREAM) as s:
        s.connect(address)

        send_size = s.send(header)
        assert send_size == len(header)

        msg = s.recv(2048)
        ps_length, ps, pad, recv_info_hash, recv_peer_id = unpack(
            HEADER_FORMAT, msg[:HEADER_LENGTH]
        )
        assert ps_length == len(PROTOCOL_STRING)
        assert ps == PROTOCOL_STRING

        return HandshakeResponse(recv_peer_id, recv_info_hash)
