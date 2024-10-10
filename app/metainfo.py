from dataclasses import dataclass
from hashlib import sha1

from app.bencoding import decode_bencode, encode_bencode


class MetaInfo:
    def __init__(self, info: dict) -> None:
        self.length: int = info["length"]
        self.name: str = str(info["name"], "utf-8")
        self.piece_length: int = info["piece length"]
        self.pieces: list[bytes] = []
        pieces_b: bytes = info["pieces"]
        assert len(pieces_b) % 20 == 0
        for i in range(0, len(pieces_b), 20):
            self.pieces.append(pieces_b[i : i + 20])

        self._info_dict = info

    def get_info_hash(self):
        return sha1(encode_bencode(self._info_dict)).digest()


@dataclass
class MetaInfoFile:
    announce: str
    info: MetaInfo

    @classmethod
    def from_file(cls, file_path: str) -> "MetaInfoFile":
        with open(file_path, "rb") as f:
            bencoded_data = f.read()

        data = decode_bencode(bencoded_data)
        return cls(str(data["announce"], "utf-8"), MetaInfo(data["info"]))
