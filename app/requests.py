from hashlib import file_digest, sha1
from itertools import batched, chain
from multiprocessing import Process, Queue
from pathlib import Path
from shutil import copyfileobj
from time import sleep
from typing import cast

from app.bencoding import decode_bencode
from app.bittorrent_proto import PeerConnection, PieceMsg, RequestMsg
from app.communication import Address, get_request
from app.metainfo import MetaInfo, MetaInfoFile


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


def fetch_peers(peer_id: bytes, meta_info_file: MetaInfoFile) -> PeersResponse:
    data = get_request(
        meta_info_file.announce,
        {
            "info_hash": meta_info_file.info.get_info_hash(),
            "peer_id": str(peer_id, "ascii"),
            "port": 6881,
            "uploaded": 0,
            "downloaded": 0,
            "left": meta_info_file.info.length,
            "compact": 1,
        },
    )
    return PeersResponse(data)


class PieceInfo:
    def __init__(self, info: MetaInfo, peer_id: bytes, index: int, file_path: Path):
        self.peer_id = peer_id
        self.index = index
        self.file_path = file_path
        self.hash = info.pieces[index]
        self.begin = info.piece_length * index
        self.length = min(info.piece_length, info.length - self.begin)

    def get_requests(self, block_size: int):
        next_block = 0
        requests: list[RequestMsg] = []
        while next_block < self.length:
            block_length = min(block_size, self.length - next_block)
            requests.append(RequestMsg(self.index, next_block, block_length))
            next_block += block_length

        return requests


def download_piece(
    peer_id: bytes,
    addresses: list[Address],
    info: MetaInfo,
    index: int,
    output_file: Path | None,
):
    piece_file = output_file.with_name(output_file.name + f".{index}")
    piece = PieceInfo(info, peer_id, index, piece_file)
    downloader = _Downloader(addresses, info, peer_id, [piece])
    downloader.download(output_file)


def download(
    peer_id: bytes,
    addresses: list[Address],
    info: MetaInfo,
    output_file: Path | None,
):

    pieces: list[PieceInfo] = []
    for index in range(len(info.pieces)):
        piece_file = output_file.with_name(output_file.name + f".{index}")
        pieces.append(PieceInfo(info, peer_id, index, piece_file))

    downloader = _Downloader(addresses, info, peer_id, pieces)
    downloader.download(output_file)


class _Downloader:
    def __init__(
        self,
        addresses: list[Address],
        info: MetaInfo,
        peer_id: bytes,
        pieces: list[PieceInfo],
    ) -> None:
        self.addresses = addresses
        self.info = info
        self.peer_id = peer_id
        self.pieces = pieces
        self.done_pieces: list[PieceInfo | None] = [None] * len(pieces)

    def download(self, output_file: Path):
        queue_size = max(len(self.pieces), len(self.addresses))
        queue = Queue(queue_size)
        processes: list[Process] = []
        try:
            for piece in self.pieces:
                queue.put_nowait(piece)

            for address in self.addresses:
                process = self._create_process(address, queue)
                process.start()
                processes.append(process)

            while not all(self.done_pieces):
                self._check_processes(processes)
                self._check_pieces()
        finally:
            for _ in range(len(processes)):
                queue.put(None)
            for process in processes:
                process.join()
                process.close()
            queue.close()

        self._combine_file(output_file)

    def _create_process(self, address: Address, queue: Queue):
        return Process(
            name=repr(address),
            target=_start_worker,
            args=(address, self.info, self.peer_id, queue),
        )

    def _check_processes(self, processes: list[Process]):
        i = 0
        while i < len(processes):
            process = processes[i]
            if not process.is_alive():
                process.close()
                processes.pop(i)
                continue
            i += 1

        if len(processes) == 0:
            raise RuntimeError("Processes died")

    def _check_pieces(self):
        for i, piece in enumerate(self.pieces):
            if self.done_pieces[i]:
                continue
            if not self._is_piece_done(piece):
                continue

            self.done_pieces[i] = self.pieces[i]

    def _is_piece_done(self, piece: PieceInfo) -> bool:
        try:
            stat = piece.file_path.stat()
        except FileNotFoundError:
            return False

        if stat.st_size < piece.length:
            return False
        if stat.st_size > piece.length:
            raise RuntimeError("File was too big")

        with open(piece.file_path, "rb") as f:
            hash = file_digest(f, "sha1").digest()

        if hash != piece.hash:
            raise RuntimeError("Hash did not match")

        return True

    def _combine_file(self, output_file: Path):
        with open(output_file, "wb") as fo:
            for piece in self.done_pieces:
                with open(piece.file_path, "rb") as fi:
                    copyfileobj(fi, fo)
                piece.file_path.unlink()


def _start_worker(address: Address, info: MetaInfo, peer_id: bytes, queue: Queue):
    piece: PieceInfo | None = None
    try:
        with PeerConnection(address, info, peer_id) as conn:
            while True:
                piece = cast(PieceInfo | None, queue.get())
                if not piece:
                    return
                _fetch_piece(conn, piece)
                piece = None
    except (ValueError, ConnectionError):
        pass
    finally:
        if piece:
            queue.put_nowait(piece)


def _fetch_piece(conn: PeerConnection, piece: PieceInfo):
    requests = piece.get_requests(16 * 1024)
    blocks = chain.from_iterable(map(conn.get_blocks, batched(requests, 5)))
    with open(piece.file_path, "wb") as f:
        for block in blocks:
            f.write(block.block)
