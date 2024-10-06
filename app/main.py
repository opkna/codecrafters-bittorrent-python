from json import dumps
from re import compile
from sys import argv

from app.communication import Address
from app.metainfo import MetaInfoFile
from app.bencoding import decode_bencode
from app.requests import fetch_peers, handshake

PEER_ID = "00112233445566778899"


def main():
    command = argv[1]
    if command == "decode":
        bencoded_value = argv[2].encode()

        # json.dumps() can't handle bytes, but bencoded "strings" need to be
        # bytestrings since they might contain non utf-8 characters.
        #
        # Let's convert them to strings for printing to the console.
        def bytes_to_str(data):
            if isinstance(data, bytes):
                return data.decode()
            raise TypeError(f"Type not serializable: {type(data)}")

        print(dumps(decode_bencode(bencoded_value), default=bytes_to_str))

    elif command == "info":
        meta_info_file = MetaInfoFile.from_file(argv[2])
        meta_info = meta_info_file.info

        print(f"Tracker URL: {meta_info_file.announce}")
        print(f"Length: {meta_info.length}")
        print(f"Info Hash: {meta_info.get_info_hash().hex()}")
        print(f"Piece Length: {meta_info.piece_length}")
        pieces_hex = [p.hex() for p in meta_info.pieces]
        print(f"Piece Hashes:\n{'\n'.join(pieces_hex)}")

    elif command == "peers":
        meta_info_file = MetaInfoFile.from_file(argv[2])

        peers = fetch_peers(PEER_ID, meta_info_file)
        print("\n".join(repr(a) for a in peers.addresses))

    elif command == "handshake":
        meta_info_file = MetaInfoFile.from_file(argv[2])
        address = Address.from_str(argv[3])
        info_hash = meta_info_file.info.get_info_hash()

        result = handshake(PEER_ID, address, info_hash)
        print(f"Peer ID: {result.peer_id.hex()}")
    else:
        raise NotImplementedError(f"Unknown command {command}")


if __name__ == "__main__":
    main()
