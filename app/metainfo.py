from app.bencoding import decode_bencode


class MetaInfo:
    def __init__(self, info: dict) -> None:
        self.length: int = info["length"]
        self.name: str = info["name"]
        self.piece_length: int = info["piece length"]
        self.pieces: list[str] = info["pieces"]


class MetaInfoFile:
    def __init__(self, file_path: str):
        with open(file_path, "rb") as f:
            bencoded_data = f.read()

        data = decode_bencode(bencoded_data)
        self.announce: str = data["announce"]
        self.info = MetaInfo(data["info"])
