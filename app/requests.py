from dataclasses import dataclass
from itertools import batched
from math import ceil
from multiprocessing import Process
from multiprocessing.connection import Connection, Pipe
from pathlib import Path
from typing import Literal, cast

from app.bencoding import decode_bencode
from app.bittorrent_proto import (
    PeerConnection,
    PieceMsg,
    RequestMsg,
)
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


###########
# Sockets #
###########


def download_piece(
    peer_id: bytes,
    addresses: list[Address],
    meta_info: MetaInfo,
    index: int,
    output_file: Path,
):
    return _Downloader(peer_id, addresses, meta_info, index).download(output_file)


@dataclass(slots=True)
class Proc:
    proc: Process
    pipe_main: Connection
    pipe_proc: Connection
    status: Literal["uninit", "ready", "working", "dead"] = "uninit"
    batch: list[RequestMsg] | None = None

    def start(self):
        assert self.proc.pid is None
        self.proc.start()
        self.status = "ready"

    def __bool__(self):
        return self.status != "dead"


class _Downloader:
    BLOCK_SIZE = 16 * 1024
    BATCH_SIZE = 1

    def __init__(
        self,
        peer_id: bytes,
        addresses: list[Address],
        meta: MetaInfo,
        index: int,
    ) -> None:
        self._peer_id = peer_id
        self._addresses = addresses
        self._meta_info = meta
        self._index = index
        self._errors: list[Exception] = []

        # Create all requests
        num_pieces = len(meta.pieces)
        if index < 0 or index >= num_pieces:
            raise ValueError(f"index out of range {index}")

        piece_begin = meta.piece_length * index
        piece_length = min(piece_begin + meta.piece_length, meta.length) - piece_begin

        self._num_blocks = ceil(piece_length / self.BLOCK_SIZE)
        requests: list[RequestMsg] = []
        self._blocks: list[PieceMsg] = []
        for i in range(self._num_blocks):
            begin = i * self.BLOCK_SIZE
            length = min(self.BLOCK_SIZE, piece_length - begin)
            requests.append(RequestMsg(index, begin, length))
        self._batches = list(batched(requests, self.BATCH_SIZE))

        # Create processes
        self._procs: list[Proc] = []
        for address in self._addresses:
            pipe_main, pipe_proc = Pipe()
            self._procs.append(
                Proc(
                    proc=Process(
                        name=repr(address),
                        target=_start_conn,
                        args=(
                            address,
                            self._meta_info,
                            self._peer_id,
                            pipe_main,
                            pipe_proc,
                        ),
                    ),
                    pipe_main=pipe_main,
                    pipe_proc=pipe_proc,
                )
            )

    def get_batch(self) -> list[RequestMsg] | None:
        if self._batches:
            return self._batches.pop(0)
        else:
            return None

    def _validate_proc(self, proc: Proc) -> bool:
        if proc.status == "dead":
            return False

        if (
            proc.proc.is_alive()
            and not proc.pipe_main.closed
            and not proc.pipe_proc.closed
        ):
            return True

        self._end_proc(proc)
        return False

    def _handle_uninit(self, proc: Proc):
        assert proc.status == "uninit"
        if len(self._batches) > 0:
            proc.start()
            self._handle_ready(proc)

    def _handle_ready(self, proc: Proc):
        assert proc.status == "ready"
        if not self._validate_proc(proc):
            return

        if batch := self.get_batch():
            proc.pipe_main.send(batch)
            proc.batch = batch
            proc.status = "working"

    def _handle_working(self, proc: Proc):
        assert proc.status == "working"
        if not self._validate_proc(proc):
            return

        try:
            result = cast(list[PieceMsg] | None | Exception, proc.pipe_main.recv())
        except (EOFError, ConnectionResetError):
            self._end_proc(proc)
            return

        if isinstance(result, Exception):
            self._errors.append(result)
            self._end_proc(proc)
            return

        if result is None:
            self._end_proc(proc)
            return

        self._blocks.extend(result)
        proc.batch = None
        proc.status = "ready"
        self._handle_ready(proc)

    def _end_proc(self, proc: Proc):
        assert proc.status != "dead"
        if proc.batch:
            self._batches.append(proc.batch)
            proc.batch = None

        # Closing pipe signals to process to exit
        if not proc.pipe_main.closed and not proc.pipe_proc.closed:
            proc.pipe_main.send(None)
        proc.pipe_main.close()
        proc.pipe_proc.close()
        if proc.proc.is_alive():
            proc.proc.join()
        proc.proc.close()
        proc.status = "dead"

    def _can_continue(self):
        if len(self._blocks) >= self._num_blocks:
            return False

        if not any(self._procs):
            return False

        return True

    def download(self, output_file: Path):
        try:
            proc_idx = 0
            while self._can_continue():
                proc = self._procs[proc_idx]
                proc_idx = (proc_idx + 1) % len(self._procs)
                if proc.status == "uninit":
                    self._handle_uninit(proc)
                elif proc.status == "ready":
                    self._handle_ready(proc)
                elif proc.status == "working":
                    self._handle_working(proc)

        finally:
            for proc in self._procs:
                if proc.status != "dead":
                    self._end_proc(proc)

        if len(self._blocks) < self._num_blocks:
            for err in self._errors:
                print(f"ERROR: {err}")
            raise RuntimeError("Failed to download all blocks")

        self._blocks.sort(key=lambda p: p.begin)
        with open(output_file, "wb") as f:
            for block in self._blocks:
                f.write(block.block)


def _start_conn(
    address: Address,
    meta_info: MetaInfo,
    peer_id: bytes,
    pipe_main: Connection,
    pipe_proc: Connection,
):
    error: Exception | None = None
    try:
        with PeerConnection(address, meta_info, peer_id) as conn:
            while True:
                requests = cast(list[RequestMsg] | None, pipe_proc.recv())
                if not requests:
                    return

                pieces = conn.get_blocks(requests)
                pipe_proc.send(pieces)
    except (EOFError, ConnectionError) as exc:
        error = exc
    finally:
        if not pipe_main.closed and not pipe_proc.closed:
            pipe_proc.send(error)
        pipe_main.close()
        pipe_proc.close()
