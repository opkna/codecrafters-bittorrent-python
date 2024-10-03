from hashlib import sha1
from app.bencoding import decode_bencode, encode_bencode


class MetaInfo:
    def __init__(self, info: dict) -> None:
        self.length: int = info["length"]
        self.name: str = info["name"]
        self.piece_length: int = info["piece length"]
        self.pieces: bytes = info["pieces"]

        self._info_dict = info

    def sha1_hash(self):
        return sha1(encode_bencode(self._info_dict)).digest()


class MetaInfoFile:
    def __init__(self, file_path: str):
        with open(file_path, "rb") as f:
            bencoded_data = f.read()

        data = decode_bencode(bencoded_data)
        self.announce: str = data["announce"]
        self.info = MetaInfo(data["info"])
