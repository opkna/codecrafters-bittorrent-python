from abc import abstractmethod
from io import BufferedRWPair, BufferedReader, BufferedWriter
from socket import AddressFamily, SocketKind, socket
from struct import pack, unpack
from typing import ContextManager, Self, overload, override

from app.communication import Address
from app.metainfo import MetaInfo

# Protocol String
PROTO_NAME = b"BitTorrent protocol"
PROTO_NAME_LENGTH = len(PROTO_NAME)
assert PROTO_NAME_LENGTH == 19

HANDSHAKE_FORMAT = "!B19s8s20s20s"
HANDSHAKE_LENGTH = 68
PAD_8 = b"\x00" * 8


class HandshakeMessage(tuple):
    @overload
    def __new__(cls, info_hash: bytes, peer_id: bytes): ...
    @overload
    def __new__(
        cls, pnl: int, pn: bytes, pad: bytes, info_hash: bytes, peer_id: bytes
    ): ...
    def __new__(cls, *args):
        info_hash: bytes = args[-2]
        peer_id: bytes = args[-1]
        assert len(info_hash) == 20
        assert len(peer_id) == 20
        if len(args) == 2:
            return super().__new__(
                cls, (PROTO_NAME_LENGTH, PROTO_NAME, PAD_8, info_hash, peer_id)
            )
        else:
            return super().__new__(cls, args)

    @property
    def info_hash(self) -> bytes:
        return self[3]

    @property
    def peer_id(self) -> bytes:
        return self[4]

    def pack(self):
        return pack(HANDSHAKE_FORMAT, *self)

    @staticmethod
    def unpack(data: bytes) -> "HandshakeMessage":
        msg = HandshakeMessage(*unpack(HANDSHAKE_FORMAT, data))
        assert msg[0] == PROTO_NAME_LENGTH
        assert msg[1] == PROTO_NAME
        return msg


def send_handshake(io: BufferedWriter, info_hash: bytes, peer_id: bytes):
    msg = HandshakeMessage(info_hash, peer_id)
    data = msg.pack()
    send_size = io.write(data)
    assert send_size == len(data)


def recv_handshake(io: BufferedReader) -> HandshakeMessage:
    data = io.read(HANDSHAKE_LENGTH)
    if not len(data) == HANDSHAKE_LENGTH:
        raise ConnectionError("Did not get enough bytes for handshake")
    return HandshakeMessage.unpack(data)


MSG_HEADER_FORMAT = "!IB"
MSG_HEADER_LENGTH = 5


class PeerMsg:
    MSG_ID: int = 0

    def _pack_msg(self):
        payload = self._pack_payload()
        size = len(payload)
        return pack(
            f"{MSG_HEADER_FORMAT}{size}s",
            size + 1,
            self.MSG_ID,
            payload,
        )

    @abstractmethod
    def _pack_payload(self) -> bytes: ...

    @classmethod
    @abstractmethod
    def _unpack_payload(cls, data: bytes) -> Self: ...


class UnchokeMsg(PeerMsg):
    MSG_ID = 1

    @override
    def _pack_payload(self) -> bytes:
        return b""

    @override
    @classmethod
    def _unpack_payload(cls, data: bytes) -> Self:
        assert len(data) == 0
        return UnchokeMsg()


class InterestedMsg(PeerMsg):
    MSG_ID = 2

    @override
    def _pack_payload(self) -> bytes:
        return b""

    @override
    @classmethod
    def _unpack_payload(cls, data: bytes) -> Self:
        assert len(data) == 0
        return InterestedMsg()


class BitfieldMsg(PeerMsg):
    MSG_ID = 5

    @override
    def _pack_payload(self) -> bytes:
        raise NotImplementedError("BitfieldMsg._pack_payload")

    @override
    @classmethod
    def _unpack_payload(cls, data: bytes) -> Self:
        # assert len(data) == ?
        return BitfieldMsg()


class RequestMsg(PeerMsg):
    MSG_ID = 6

    def __init__(self, index: int, begin: int, length: int) -> None:
        self.index = index
        self.begin = begin
        self.length = length

    @override
    def _pack_payload(self) -> bytes:
        return pack("!3I", self.index, self.begin, self.length)

    @override
    @classmethod
    def _unpack_payload(cls, data: bytes) -> Self:
        raise NotImplementedError("RequestMsg._unpack_payload")


class PieceMsg(PeerMsg):
    MSG_ID = 7

    def __init__(self, index: int, begin: int, block: bytes) -> None:
        self.index = index
        self.begin = begin
        self.block = block

    @override
    def _pack_payload(self) -> bytes:
        raise NotImplementedError("PieceMsg._pack_payload")

    @override
    @classmethod
    def _unpack_payload(cls, data: bytes) -> Self:
        length = len(data) - 8
        return PieceMsg(*unpack(f"!II{length}s", data))


_MSG_CLASS_MAP: dict[int, PeerMsg] = {
    UnchokeMsg.MSG_ID: UnchokeMsg,
    InterestedMsg.MSG_ID: InterestedMsg,
    BitfieldMsg.MSG_ID: BitfieldMsg,
    RequestMsg.MSG_ID: RequestMsg,
    PieceMsg.MSG_ID: PieceMsg,
}


def read_msg(io: BufferedReader) -> PeerMsg:
    size: int
    id: int
    header = io.read(MSG_HEADER_LENGTH)
    if io.closed:
        raise RuntimeError(f"IO closed")
    if len(header) != MSG_HEADER_LENGTH:
        raise ConnectionError(
            f"Not egnough data, got {len(header)} of {MSG_HEADER_LENGTH}: {header}"
        )

    size, id = unpack(MSG_HEADER_FORMAT, header)
    if id not in _MSG_CLASS_MAP:
        raise RuntimeError(f"Unknown message id: {id}")
    cls = _MSG_CLASS_MAP[id]
    payload = io.read(size - 1)
    return cls._unpack_payload(payload)


def send_msg(io: BufferedWriter, msg: PeerMsg):
    data = msg._pack_msg()
    write_len = io.write(data)
    assert write_len == len(data)


class PeerConnection(ContextManager):
    _socket: socket | None
    _io: BufferedRWPair | None
    _address: Address
    _meta_info: MetaInfo
    _peer_id: bytes
    _handshake: HandshakeMessage
    _ready: bool

    def __init__(
        self,
        address: Address,
        meta_info: MetaInfo,
        peer_id: bytes,
        only_handshake=False,
    ):
        self._address = address
        self._meta_info = meta_info
        self._peer_id = peer_id
        self._socket = None
        self._io = None
        self._ready = False

        self._socket = socket(AddressFamily.AF_INET, SocketKind.SOCK_STREAM)
        self._socket.connect(address)
        self._io = self._socket.makefile("brw", buffering=1)

        # Do handshake
        send_handshake(self._io, self._meta_info.get_info_hash(), self._peer_id)
        self._handshake = recv_handshake(self._io)

        if only_handshake:
            return

        # Init communication
        msg = read_msg(self._io)
        assert isinstance(msg, BitfieldMsg)
        send_msg(self._io, InterestedMsg())
        msg = read_msg(self._io)
        assert isinstance(msg, UnchokeMsg)

        self._ready = True

    @property
    def handshake(self) -> HandshakeMessage:
        return self._handshake

    def get_blocks(self, requests: list[RequestMsg]) -> list[PieceMsg]:
        if not self._ready:
            raise RuntimeError("Can't get parts when not ready")

        for req in requests:
            send_msg(self._io, req)

        parts: list[PieceMsg] = []
        for _ in range(len(requests)):
            piece = read_msg(self._io)
            assert isinstance(piece, PieceMsg)
            parts.append(piece)

        return parts

    def close(self):
        self._ready = False
        if self._io:
            self._io.close()
            self._io = None

        if self._socket:
            self._socket.close()
            self._socket = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args, **_kwargs):
        self.close()
