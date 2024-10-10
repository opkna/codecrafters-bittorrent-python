from argparse import ArgumentParser
from json import dumps
from pathlib import Path
from sys import argv
from typing import Literal, TypedDict, cast

from app.bencoding import decode_bencode
from app.bittorrent_proto import PeerConnection
from app.communication import Address
from app.magnet_links import parse_magnet_link
from app.metainfo import MetaInfo, MetaInfoFile
from app.requests import download, download_piece, fetch_peers

PEER_ID = b"00112233445566778899"


class DecodeArgs(TypedDict):
    command: Literal["decode"]
    bencoded_string: str


class InfoArgs(TypedDict):
    command: Literal["info"]
    torrent_file: str


class PeersArgs(TypedDict):
    command: Literal["peers"]
    torrent_file: str


class HandshakeArgs(TypedDict):
    command: Literal["handshake"]
    torrent_file: str
    peer_ip_port: str


class DownloadPieceArgs(TypedDict):
    command: Literal["download_piece"]
    torrent_file: str
    index: int
    output: str | None


class DownloadArgs(TypedDict):
    command: Literal["download"]
    torrent_file: str
    output: str | None


class MagnetParseArgs(TypedDict):
    command: Literal["magnet_parse"]
    magnet_link: str


Args = (
    DecodeArgs
    | InfoArgs
    | PeersArgs
    | HandshakeArgs
    | DownloadPieceArgs
    | DownloadArgs
    | MagnetParseArgs
)


def parse_args(args: list[str]) -> Args:
    parser = ArgumentParser("your_bittorrent")
    cmds = parser.add_subparsers(dest="command", required=True)
    # Decode
    decode = cmds.add_parser("decode")
    decode.add_argument("bencoded_string", type=str)
    # Info
    info = cmds.add_parser("info")
    info.add_argument("torrent_file", type=str)
    # Peers
    peers = cmds.add_parser("peers")
    peers.add_argument("torrent_file", type=str)
    # Handshake
    handshake = cmds.add_parser("handshake")
    handshake.add_argument("torrent_file", type=str)
    handshake.add_argument("peer_ip_port", type=str)
    # Download piece
    download_piece = cmds.add_parser("download_piece")
    download_piece.add_argument("torrent_file", type=str)
    download_piece.add_argument("index", type=int)
    download_piece.add_argument("-o", "--output", type=str)
    # Download
    download_piece = cmds.add_parser("download")
    download_piece.add_argument("torrent_file", type=str)
    download_piece.add_argument("-o", "--output", type=str)
    # Magnet parse
    download_piece = cmds.add_parser("magnet_parse")
    download_piece.add_argument("magnet_link", type=str)

    ns = parser.parse_args(args)
    args = {k: v for k, v in ns._get_kwargs()}
    return cast(Args, args)


def get_output_file(info: MetaInfo, arg: str | None):
    if not arg:
        return Path(info.name).resolve()

    output = Path(arg).resolve()
    if output.is_dir():
        output /= info.name
    return output


def main():
    args = parse_args(argv[1:])
    if args["command"] == "decode":
        bencoded_value = args["bencoded_string"].encode()

        def bytes_to_str(data):
            if isinstance(data, bytes):
                return data.decode()
            raise TypeError(f"Type not serializable: {type(data)}")

        print(dumps(decode_bencode(bencoded_value), default=bytes_to_str))
    elif args["command"] == "info":
        meta_info_file = MetaInfoFile.from_file(args["torrent_file"])
        meta_info = meta_info_file.info

        print(f"Tracker URL: {meta_info_file.announce}")
        print(f"Length: {meta_info.length}")
        print(f"Info Hash: {meta_info.get_info_hash().hex()}")
        print(f"Piece Length: {meta_info.piece_length}")
        pieces_hex = [p.hex() for p in meta_info.pieces]
        print(f"Piece Hashes:\n{'\n'.join(pieces_hex)}")

    elif args["command"] == "peers":
        meta_info_file = MetaInfoFile.from_file(args["torrent_file"])

        peers = fetch_peers(PEER_ID, meta_info_file)
        print("\n".join(repr(a) for a in peers.addresses))

    elif args["command"] == "handshake":
        meta_info_file = MetaInfoFile.from_file(args["torrent_file"])
        address = Address.from_str(args["peer_ip_port"])

        result = PeerConnection(
            address,
            meta_info_file.info,
            PEER_ID,
            only_handshake=True,
        )
        print(f"Peer ID: {result.handshake.peer_id.hex()}")
    elif args["command"] == "download_piece":
        meta_info_file = MetaInfoFile.from_file(args["torrent_file"])
        index = args["index"]
        output_file = Path(args["output"] or f"./piece-{index}").resolve()

        peers = fetch_peers(PEER_ID, meta_info_file)
        address = peers.addresses[1]
        download_piece(
            peer_id=PEER_ID,
            addresses=peers.addresses,
            info=meta_info_file.info,
            index=index,
            output_file=output_file,
        )
    elif args["command"] == "download":
        meta_info_file = MetaInfoFile.from_file(args["torrent_file"])
        output_file = get_output_file(meta_info_file.info, args["output"])

        peers = fetch_peers(PEER_ID, meta_info_file)
        download(
            peer_id=PEER_ID,
            addresses=peers.addresses,
            info=meta_info_file.info,
            output_file=output_file,
        )
    elif args["command"] == "magnet_parse":
        magnet_link = parse_magnet_link(args["magnet_link"])
        print(f"Tracker URL: {magnet_link.announce}")
        print(f"Info Hash: {magnet_link.hash.hex()}")
    else:
        raise NotImplementedError(f"Unknown command {args['command']}")


if __name__ == "__main__":
    main()
