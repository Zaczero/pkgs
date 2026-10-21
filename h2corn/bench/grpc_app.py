"""Identical Python work served by Connect, grpc.aio, and grpclib."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import socket
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator

from bench._core import write_json
from bench.evidence import worker_identity
from examples.grpc.gen.h2corn.interop.v1.notebook_pb import (
    Edit,
    EditAck,
    GetNoteRequest,
    Note,
    NoteChunk,
    NoteRevision,
    UploadSummary,
)

SERVICE = 'h2corn.interop.v1.NotebookService'
MESSAGE_LIMIT = 2 * 1024 * 1024


class Work:
    """One shared service implementation; adapters only translate runtime APIs."""

    def __init__(self, plan: dict[str, Any]) -> None:
        self.plan = plan
        self.payload = 'x' * plan['size']
        self.byte_payload = self.payload.encode()
        self.active = 0
        self.finished = 0
        self.readers = 0
        self.consumed = 0

    @asynccontextmanager
    async def call(self) -> AsyncGenerator[None]:
        self.active += 1
        try:
            yield
        finally:
            self.active -= 1
            self.finished += 1

    async def unary(self, request: GetNoteRequest) -> Note:
        if request.note_id in {'__bench_identity', '__bench_state'}:
            return Note(
                body=json.dumps({
                    **(
                        worker_identity()
                        if request.note_id == '__bench_identity'
                        else {'pid': os.getpid()}
                    ),
                    'active': self.active,
                    'finished': self.finished,
                    'readers': self.readers,
                    'consumed': self.consumed,
                })
            )
        async with self.call():
            if len(request.note_id) != self.plan['size']:
                raise ValueError('incorrect unary payload length')
            return Note(note_id=request.note_id)

    async def download(self, request: GetNoteRequest) -> AsyncGenerator[NoteRevision]:
        self.active += 1
        try:
            for sequence in range(1, self.plan['messages'] + 1):
                yield NoteRevision(
                    note_id=request.note_id,
                    revision=sequence,
                    summary=self.payload,
                )
                if self.plan.get('hold_after_first'):
                    await asyncio.Event().wait()
        finally:
            self.active -= 1
            self.finished += 1

    async def upload(self, requests: AsyncIterator[NoteChunk]) -> UploadSummary:
        async with self.call():
            note_id = ''
            count = 0
            size = 0
            async for request in requests:
                count += 1
                if (
                    request.sequence != count
                    or request.content != self.byte_payload
                    or (note_id and request.note_id != note_id)
                ):
                    raise ValueError('incorrect upload message')
                note_id = request.note_id
                size += len(request.content)
            if count != self.plan['messages']:
                raise ValueError('incorrect upload message count')
            return UploadSummary(
                note_id=note_id, chunks_received=count, bytes_received=size
            )

    async def duplex(self, requests: AsyncIterator[Edit]) -> AsyncGenerator[EditAck]:
        queue: asyncio.Queue[EditAck | Exception | None] = asyncio.Queue(maxsize=1)

        async def read() -> None:
            self.readers += 1
            try:
                count = 0
                note_id = ''
                async for request in requests:
                    count += 1
                    if (
                        request.base_revision != count - 1
                        or request.text != self.payload
                        or (note_id and request.note_id != note_id)
                    ):
                        raise ValueError('incorrect duplex message')
                    note_id = request.note_id
                    self.consumed += 1
                    await queue.put(
                        EditAck(
                            note_id=note_id,
                            revision=count,
                            accepted_text=request.text,
                        )
                    )
                if count != self.plan['messages']:
                    raise ValueError('incorrect duplex message count')
                await queue.put(None)
            except Exception as error:
                await queue.put(error)
            finally:
                self.readers -= 1

        self.active += 1
        reader = asyncio.create_task(read())
        try:
            while (response := await queue.get()) is not None:
                if isinstance(response, Exception):
                    raise response
                yield response
        finally:
            reader.cancel()
            with suppress(asyncio.CancelledError):
                await reader
            self.active -= 1
            self.finished += 1


def connect_app(work: Work) -> Any:
    from asgi_lifespan import LifespanManager
    from starlette.applications import Starlette
    from starlette.routing import Mount

    from examples.grpc.gen.h2corn.interop.v1.notebook_connect import (
        NotebookService,
        NotebookServiceASGIApplication,
    )

    class Service(NotebookService):
        @staticmethod
        def metadata(ctx: Any) -> None:
            ctx.response_headers['x-bench-case'] = work.plan['case']
            ctx.response_trailers['x-bench-finished'] = 'yes'

        async def get_note(self, request: GetNoteRequest, ctx: Any, /) -> Note:
            self.metadata(ctx)
            return await work.unary(request)

        def list_revisions(
            self, request: GetNoteRequest, ctx: Any
        ) -> AsyncIterator[NoteRevision]:
            self.metadata(ctx)
            return work.download(request)

        async def upload_note(
            self, request: AsyncIterator[NoteChunk], ctx: Any
        ) -> UploadSummary:
            self.metadata(ctx)
            return await work.upload(request)

        def collaborate(
            self, request: AsyncIterator[Edit], ctx: Any
        ) -> AsyncIterator[EditAck]:
            self.metadata(ctx)
            return work.duplex(request)

    async def service() -> AsyncGenerator[Service]:
        yield Service()

    rpc = NotebookServiceASGIApplication(
        service(), read_max_bytes=MESSAGE_LIMIT, compressions=()
    )

    @asynccontextmanager
    async def lifespan(_app: Any) -> AsyncGenerator[None]:
        async with LifespanManager(rpc):
            yield

    return Starlette(lifespan=lifespan, routes=[Mount(rpc.path, rpc)])


async def grpcio_server(
    work: Work, endpoint: str, cert: Path | None
) -> tuple[Any, int]:
    import grpc

    from examples.grpc.gen.h2corn.interop.v1.notebook_pb_grpc import (
        NotebookServiceService,
    )

    class Service(NotebookServiceService):
        @staticmethod
        async def metadata(ctx: Any) -> None:
            await ctx.send_initial_metadata((('x-bench-case', work.plan['case']),))
            ctx.set_trailing_metadata((('x-bench-finished', 'yes'),))

        async def get_note(self, request: GetNoteRequest, context: Any) -> Note:
            await self.metadata(context)
            return await work.unary(request)

        async def list_revisions(
            self, request: GetNoteRequest, context: Any
        ) -> AsyncIterator[NoteRevision]:
            await self.metadata(context)
            responses = work.download(request)
            try:
                async for response in responses:
                    yield response
            finally:
                await responses.aclose()

        async def upload_note(
            self, request_iterator: AsyncIterator[NoteChunk], context: Any
        ) -> UploadSummary:
            await self.metadata(context)
            return await work.upload(request_iterator)

        async def collaborate(
            self, request_iterator: AsyncIterator[Edit], context: Any
        ) -> AsyncIterator[EditAck]:
            await self.metadata(context)
            responses = work.duplex(request_iterator)
            try:
                async for response in responses:
                    yield response
            finally:
                await responses.aclose()

    server = grpc.aio.server(
        options=(
            ('grpc.max_receive_message_length', MESSAGE_LIMIT),
            ('grpc.max_send_message_length', MESSAGE_LIMIT),
            ('grpc.max_concurrent_streams', 256),
        )
    )
    Service().add_to_server(server)
    if cert is None:
        port = server.add_insecure_port(endpoint)
    else:
        credentials = grpc.ssl_server_credentials([
            ((cert / 'key.pem').read_bytes(), (cert / 'cert.pem').read_bytes()),
        ])
        port = server.add_secure_port(endpoint, credentials)
    if not port:
        raise RuntimeError('grpc.aio did not allocate a listener')
    await server.start()
    return server, port


async def grpclib_server(work: Work, listener: socket.socket) -> Any:
    from grpclib.const import Cardinality, Handler
    from grpclib.encoding.base import CodecBase
    from grpclib.server import Server

    class Codec(CodecBase):
        @property
        def __content_subtype__(self) -> str:  # ruff: ignore[bad-dunder-method-name] -- grpclib's public codec protocol.
            return 'proto'

        def encode(self, message: Any, message_type: Any) -> bytes:
            return message.to_binary()

        def decode(self, data: bytes, message_type: Any) -> Any:
            return message_type.from_binary(data)

    class Service:
        async def unary(self, stream: Any) -> None:
            await stream.send_initial_metadata(
                metadata=(('x-bench-case', work.plan['case']),)
            )
            request = await stream.recv_message()
            await stream.send_message(await work.unary(request))
            await stream.send_trailing_metadata(metadata=(('x-bench-finished', 'yes'),))

        async def duplex(self, stream: Any) -> None:
            await stream.send_initial_metadata(
                metadata=(('x-bench-case', work.plan['case']),)
            )
            responses = work.duplex(aiter(stream))
            try:
                async for response in responses:
                    await stream.send_message(response)
            finally:
                await responses.aclose()
            await stream.send_trailing_metadata(metadata=(('x-bench-finished', 'yes'),))

        def __mapping__(self) -> dict[str, Any]:  # ruff: ignore[bad-dunder-method-name] -- grpclib's service registration protocol.
            return {
                f'/{SERVICE}/GetNote': Handler(
                    self.unary, Cardinality.UNARY_UNARY, GetNoteRequest, Note
                ),
                f'/{SERVICE}/Collaborate': Handler(
                    self.duplex, Cardinality.STREAM_STREAM, Edit, EditAck
                ),
            }

    server = Server([Service()], codec=Codec())
    await server.start(sock=listener)
    return server


async def serve(args: argparse.Namespace) -> None:
    plan = json.loads(args.plan.read_text())
    work = Work(plan)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, stop.set)
    listener = (
        socket.socket(fileno=args.listener_fd) if args.listener_fd is not None else None
    )
    port = listener.getsockname()[1] if listener else 0
    if args.server == 'grpcio':
        server, port = await grpcio_server(work, '127.0.0.1:0', args.cert)
        write_json(args.receipt, {**worker_identity(), 'port': port})
        try:
            await stop.wait()
        finally:
            await server.stop(5)
    elif args.server == 'grpclib':
        if listener is None:
            raise RuntimeError('grpclib requires the inherited listener')
        server = await grpclib_server(work, listener)
        write_json(args.receipt, {**worker_identity(), 'port': port})
        try:
            await stop.wait()
        finally:
            server.close()
            await server.wait_closed()
    else:
        if listener is None:
            raise RuntimeError('ASGI hosts require the inherited listener')
        if args.server == 'h2corn':
            from h2corn import Config as H2Config
            from h2corn import Server

            config = H2Config(
                bind=(f'fd://{listener.fileno()}',),
                grpc=True,
                lifespan='on',
                loop='asyncio',
                access_log=False,
                max_concurrent_streams=256,
                certfile=args.cert / 'cert.pem' if args.cert else None,
                keyfile=args.cert / 'key.pem' if args.cert else None,
            )
            host = Server(lambda: connect_app(work), config, factory=True)
            task = asyncio.create_task(host.serve())
            await host.wait_started()
            write_json(args.receipt, {**worker_identity(), 'port': port})
            try:
                await stop.wait()
            finally:
                host.shutdown()
                await task
        else:
            from hypercorn.asyncio import serve as hypercorn_serve
            from hypercorn.config import Config as HypercornConfig

            hypercorn_config = HypercornConfig()
            hypercorn_config.bind = [f'fd://{listener.fileno()}']
            hypercorn_config.accesslog = None
            hypercorn_config.h2_max_concurrent_streams = 256
            hypercorn_config.keep_alive_max_requests = 2**63 - 1
            if args.cert:
                hypercorn_config.certfile = str(args.cert / 'cert.pem')
                hypercorn_config.keyfile = str(args.cert / 'key.pem')
            write_json(args.receipt, {**worker_identity(), 'port': port})
            await hypercorn_serve(
                connect_app(work), hypercorn_config, shutdown_trigger=stop.wait
            )


def main() -> None:
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        '--server', choices=('h2corn', 'hypercorn', 'grpcio', 'grpclib'), required=True
    )
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--receipt', type=Path, required=True)
    parser.add_argument('--listener-fd', type=int)
    parser.add_argument('--cert', type=Path)
    asyncio.run(serve(parser.parse_args()))


if __name__ == '__main__':
    main()
