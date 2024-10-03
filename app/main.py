import json
import sys

from app.metainfo import MetaInfoFile
from app.bencoding import decode_bencode
from app.requests import fetch_peers


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

    elif command == "info":
        file_path = sys.argv[2]
        meta_info_file = MetaInfoFile(file_path)
        meta_info = meta_info_file.info
        print(f"Tracker URL: {meta_info_file.announce}")
        print(f"Length: {meta_info.length}")
        print(f"Info Hash: {meta_info.sha1_hash().hex()}")
        print(f"Piece Length: {meta_info.piece_length}")
        pieces_hex = [p.hex() for p in meta_info.pieces]
        print(f"Piece Hashes:\n{'\n'.join(pieces_hex)}")
    elif command == "peers":
        file_path = sys.argv[2]
        meta_info_file = MetaInfoFile(file_path)
        peers = fetch_peers(meta_info_file)
        print("\n".join(peers.peers_urls()))
    else:
        raise NotImplementedError(f"Unknown command {command}")


if __name__ == "__main__":
    main()
