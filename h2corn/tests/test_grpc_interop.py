from __future__ import annotations

import asyncio
import sysconfig
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, cast

import pytest

if sysconfig.get_config_var('Py_GIL_DISABLED'):
    pytest.skip(
        'grpcio interoperability qualification targets regular CPython',
        allow_module_level=True,
    )

grpc = pytest.importorskip('grpc')
pytest.importorskip('connectrpc')

from connectrpc.code import Code  # ruff: ignore[module-import-not-at-top-of-file]
from connectrpc.errors import ConnectError  # ruff: ignore[module-import-not-at-top-of-file]
from h2corn import Config, Server  # ruff: ignore[module-import-not-at-top-of-file]
from starlette.applications import Starlette  # ruff: ignore[module-import-not-at-top-of-file]
from starlette.responses import PlainTextResponse  # ruff: ignore[module-import-not-at-top-of-file]
from starlette.routing import Mount, Route  # ruff: ignore[module-import-not-at-top-of-file]

from examples.grpc.gen.h2corn.interop.v1.notebook_connect import (  # ruff: ignore[module-import-not-at-top-of-file]
    NotebookService,
    NotebookServiceASGIApplication,
)
from examples.grpc.gen.h2corn.interop.v1.notebook_pb import (  # ruff: ignore[module-import-not-at-top-of-file]
    Edit,
    EditAck,
    GetNoteRequest,
    Note,
    NoteChunk,
    NoteRevision,
    UploadSummary,
)
from examples.grpc.gen.h2corn.interop.v1.notebook_pb_grpc import (  # ruff: ignore[module-import-not-at-top-of-file]
    NotebookServiceClient,
)
from tests._support import (  # ruff: ignore[module-import-not-at-top-of-file]
    HANG_BOUND,
    http1_request,
    running_server,
    server_port,
    stop_serving,
    wait_for_server,
)
from tests.test_tls import (  # ruff: ignore[module-import-not-at-top-of-file]
    write_mutual_tls_certs,
    write_self_signed_cert,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator, Callable
    from pathlib import Path

    from connectrpc.request import RequestContext
    from grpc import ChannelCredentials
    from starlette.requests import Request
    from starlette.types import ASGIApp

pytestmark = pytest.mark.asyncio

SERVICE_PATH = '/h2corn.interop.v1.NotebookService'


class _NotebookService(NotebookService):
    def __init__(self) -> None:
        self.revision_waiting = asyncio.Event()
        self.revision_cleaned = asyncio.Event()
        self.release_revision = asyncio.Event()
        self.deadline_remaining_ms: float | None = None
        self.collaboration_started = asyncio.Event()
        self.collaboration_cleaned = asyncio.Event()

    async def get_note(
        self,
        request: GetNoteRequest,
        ctx: RequestContext[GetNoteRequest, Note],
        /,
    ) -> Note:
        if request.note_id == 'missing':
            ctx.response_trailers['x-note-result'] = 'missing'
            raise ConnectError(Code.NOT_FOUND, 'note not found')

        ctx.response_headers['x-note-id'] = request.note_id
        ctx.response_headers['x-correlation-id'] = ctx.request_headers.get(
            'x-correlation-id',
            'absent',
        )
        ctx.response_trailers['x-note-revision'] = '3'
        return Note(
            note_id=request.note_id,
            title='Socket lifecycles',
            body='A real gRPC client reached the ASGI service.',
            author='Ada',
        )

    async def list_revisions(
        self,
        request: GetNoteRequest,
        ctx: RequestContext[GetNoteRequest, NoteRevision],
        /,
    ) -> AsyncIterator[NoteRevision]:
        if request.after_revision >= 3:
            self.deadline_remaining_ms = ctx.timeout_ms
            self.revision_waiting.set()
            try:
                await self.release_revision.wait()
                yield NoteRevision(
                    note_id=request.note_id,
                    revision=4,
                    summary='released',
                )
            finally:
                self.revision_cleaned.set()
            return

        yield NoteRevision(
            note_id=request.note_id,
            revision=2,
            summary='title updated',
        )
        yield NoteRevision(
            note_id=request.note_id,
            revision=3,
            summary='body saved',
        )

    async def upload_note(
        self,
        request: AsyncIterator[NoteChunk],
        _ctx: RequestContext[NoteChunk, UploadSummary],
        /,
    ) -> UploadSummary:
        note_id = ''
        chunks_received = 0
        bytes_received = 0
        async for chunk in request:
            if note_id and note_id != chunk.note_id:
                raise ConnectError(Code.INVALID_ARGUMENT, 'mixed note IDs')
            note_id = chunk.note_id
            chunks_received += 1
            bytes_received += len(chunk.content)
        return UploadSummary(
            note_id=note_id,
            chunks_received=chunks_received,
            bytes_received=bytes_received,
        )

    async def collaborate(
        self,
        request: AsyncIterator[Edit],
        _ctx: RequestContext[Edit, EditAck],
        /,
    ) -> AsyncIterator[EditAck]:
        try:
            async for edit in request:
                self.collaboration_started.set()
                yield EditAck(
                    note_id=edit.note_id,
                    revision=edit.base_revision + 1,
                    accepted_text=edit.text,
                )
        finally:
            self.collaboration_cleaned.set()


async def _health(_request: Request) -> PlainTextResponse:
    return PlainTextResponse('ok')


def _app(
    service: NotebookService,
    lifecycle: list[tuple[str, asyncio.AbstractEventLoop]] | None = None,
) -> Starlette:
    rpc = NotebookServiceASGIApplication(service)
    routes = [
        Route('/healthz', _health),
        Mount(rpc.path, cast('ASGIApp', rpc)),
    ]

    if lifecycle is None:
        return Starlette(routes=routes)

    @asynccontextmanager
    async def lifespan(_app: Starlette) -> AsyncGenerator[None, None]:
        lifecycle.append(('startup', asyncio.get_running_loop()))
        try:
            yield
        finally:
            lifecycle.append(('shutdown', asyncio.get_running_loop()))

    return Starlette(
        lifespan=lifespan,
        routes=routes,
    )


@asynccontextmanager
async def _running_factory_server(
    factory: Callable[[], Starlette],
    config: Config,
) -> AsyncGenerator[Server, None]:
    server = Server(factory, config, factory=True)
    serving = asyncio.create_task(server.serve())
    await wait_for_server(server, serving)
    try:
        yield server
    finally:
        await stop_serving(server, serving)


@asynccontextmanager
async def _grpc_client(
    port: int,
    *,
    credentials: ChannelCredentials | None = None,
    options: tuple[tuple[str, str], ...] = (),
) -> AsyncGenerator[NotebookServiceClient, None]:
    target = f'127.0.0.1:{port}'
    if credentials is None:
        channel = grpc.aio.insecure_channel(target)
    else:
        channel = grpc.aio.secure_channel(
            target,
            credentials,
            options=options,
        )
    try:
        await asyncio.wait_for(channel.channel_ready(), timeout=HANG_BOUND)
        yield NotebookServiceClient(channel)
    finally:
        await channel.close()


def _connect_app(
    service: _NotebookService,
    config: Config | None = None,
) -> tuple[Starlette, Config]:
    if config is None:
        config = Config(port=0, lifespan='on')
    return _app(service), config


async def test_grpc_unary_preserves_message_and_response_metadata() -> None:
    service = _NotebookService()
    app, config = _connect_app(service)

    async with (
        running_server(app, config) as server,
        _grpc_client(server_port(server)) as client,
    ):
        call = client.get_note(
            GetNoteRequest(note_id='note-17'),
            metadata=(('x-correlation-id', 'request-42'),),
        )
        note = await call
        headers = dict(await call.initial_metadata())
        trailers = dict(await call.trailing_metadata())

    assert (note.note_id, note.title, note.author) == (
        'note-17',
        'Socket lifecycles',
        'Ada',
    )
    assert headers['x-note-id'] == 'note-17'
    assert headers['x-correlation-id'] == 'request-42'
    assert trailers['x-note-revision'] == '3'


async def test_grpc_error_preserves_status_details_and_trailers() -> None:
    app, config = _connect_app(_NotebookService())

    async with (
        running_server(app, config) as server,
        _grpc_client(server_port(server)) as client,
    ):
        call = client.get_note(GetNoteRequest(note_id='missing'))
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await call

    assert error.value.code() == grpc.StatusCode.NOT_FOUND
    assert error.value.details() == 'note not found'
    assert dict(error.value.trailing_metadata())['x-note-result'] == 'missing'


async def test_grpc_server_stream_finishes_with_each_revision() -> None:
    app, config = _connect_app(_NotebookService())

    async with (
        running_server(app, config) as server,
        _grpc_client(server_port(server)) as client,
    ):
        revisions = [
            revision
            async for revision in client.list_revisions(
                GetNoteRequest(note_id='note-17')
            )
        ]

    assert [(revision.revision, revision.summary) for revision in revisions] == [
        (2, 'title updated'),
        (3, 'body saved'),
    ]


async def test_grpc_client_stream_receives_all_upload_chunks() -> None:
    app, config = _connect_app(_NotebookService())

    async def upload() -> AsyncIterator[NoteChunk]:
        yield NoteChunk(note_id='note-17', sequence=0, content=b'heading\n')
        yield NoteChunk(note_id='note-17', sequence=1, content=b'body')
        yield NoteChunk(note_id='note-17', sequence=2, content=b'\nfooter')

    async with (
        running_server(app, config) as server,
        _grpc_client(server_port(server)) as client,
    ):
        summary = await client.upload_note(upload())

    assert (summary.note_id, summary.chunks_received, summary.bytes_received) == (
        'note-17',
        3,
        len(b'heading\nbody\nfooter'),
    )


async def test_grpc_duplex_response_arrives_before_the_next_request() -> None:
    service = _NotebookService()
    app, config = _connect_app(service)
    second_request_allowed = asyncio.Event()

    async with (
        running_server(app, config) as server,
        _grpc_client(server_port(server)) as client,
    ):

        async def send_edits() -> AsyncIterator[Edit]:
            yield Edit(
                note_id='note-17',
                base_revision=1,
                text='first edit',
            )
            await second_request_allowed.wait()
            yield Edit(
                note_id='note-17',
                base_revision=2,
                text='second edit',
            )

        call = client.collaborate(send_edits())

        await asyncio.wait_for(
            service.collaboration_started.wait(),
            timeout=HANG_BOUND,
        )
        first = await asyncio.wait_for(call.read(), timeout=HANG_BOUND)
        assert first is not grpc.aio.EOF
        first = cast('EditAck', first)
        assert (first.revision, first.accepted_text) == (2, 'first edit')

        second_request_allowed.set()
        second = await asyncio.wait_for(call.read(), timeout=HANG_BOUND)
        assert second is not grpc.aio.EOF
        second = cast('EditAck', second)
        assert (second.revision, second.accepted_text) == (3, 'second edit')
        assert await asyncio.wait_for(call.read(), timeout=HANG_BOUND) is grpc.aio.EOF
        await asyncio.wait_for(
            service.collaboration_cleaned.wait(),
            timeout=HANG_BOUND,
        )


async def test_grpc_deadline_cancels_stream_and_settles_handler_cleanup() -> None:
    service = _NotebookService()
    app, config = _connect_app(service)

    async with (
        running_server(app, config) as server,
        _grpc_client(server_port(server)) as client,
    ):
        call = client.list_revisions(
            GetNoteRequest(note_id='note-17', after_revision=3),
            timeout=0.1,
        )
        await asyncio.wait_for(service.revision_waiting.wait(), timeout=HANG_BOUND)
        assert service.deadline_remaining_ms is not None
        assert service.deadline_remaining_ms > 0
        # Connect aborts after the deadline instead of promising a final status
        # once its remaining send budget is gone.
        with pytest.raises(grpc.aio.AioRpcError):
            await call.read()

        await asyncio.wait_for(
            service.revision_cleaned.wait(),
            timeout=HANG_BOUND,
        )


async def test_grpc_cancellation_closes_the_bidi_handler() -> None:
    service = _NotebookService()
    app, config = _connect_app(service)

    async with (
        running_server(app, config) as server,
        _grpc_client(server_port(server)) as client,
    ):

        async def open_stream() -> AsyncIterator[Edit]:
            yield Edit(
                note_id='note-17',
                base_revision=1,
                text='leave the stream open',
            )
            await hold_stream_open.wait()

        hold_stream_open = asyncio.Event()
        call = client.collaborate(open_stream())
        first = await asyncio.wait_for(call.read(), timeout=HANG_BOUND)
        assert first is not grpc.aio.EOF
        first = cast('EditAck', first)
        assert first.accepted_text == 'leave the stream open'

        call.cancel()
        assert await asyncio.wait_for(call.code(), timeout=HANG_BOUND) == (
            grpc.StatusCode.CANCELLED
        )
        await asyncio.wait_for(
            service.collaboration_cleaned.wait(),
            timeout=HANG_BOUND,
        )


async def test_factory_and_lifespan_use_the_serving_loop() -> None:
    factory_calls: list[tuple[asyncio.AbstractEventLoop, bool]] = []
    lifecycle: list[tuple[str, asyncio.AbstractEventLoop]] = []
    requests: list[str] = []

    class Service(_NotebookService):
        async def get_note(
            self,
            request: GetNoteRequest,
            ctx: RequestContext[GetNoteRequest, Note],
            /,
        ) -> Note:
            requests.append(request.note_id)
            return await super().get_note(request, ctx)

    def create_app() -> Starlette:
        loop = asyncio.get_running_loop()
        factory_calls.append((loop, loop.is_running()))
        return _app(Service(), lifecycle)

    async with (
        _running_factory_server(
            create_app,
            Config(port=0, lifespan='on'),
        ) as server,
        _grpc_client(server_port(server)) as client,
    ):
        note = await client.get_note(GetNoteRequest(note_id='note-17'))
        assert note.note_id == 'note-17'

    assert len(factory_calls) == 1
    factory_loop, was_running = factory_calls[0]
    assert was_running
    assert lifecycle == [
        ('startup', factory_loop),
        ('shutdown', factory_loop),
    ]
    assert requests == ['note-17']


async def test_rpc_and_ordinary_http_share_one_listener() -> None:
    from examples.grpc.app import create_app

    app = create_app()
    config = Config(port=0, lifespan='on')

    async with running_server(app, config) as server:
        port = server_port(server)
        async with _grpc_client(port) as client:
            note = await client.get_note(GetNoteRequest(note_id='welcome'))
        status, _headers, body, _trailers = await http1_request(
            port=port,
            request=(
                b'GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n'
            ),
        )

    assert (
        note.note_id,
        note.title,
        note.author,
    ) == ('welcome', 'Welcome', 'h2corn')
    assert status == 200
    assert body == b'ok'


async def test_upload_request_policy_overrides_the_global_body_cap() -> None:
    from h2corn import RequestPolicy

    policy = RequestPolicy(
        path=f'{SERVICE_PATH}/UploadNote',
        max_request_body_size=0,
        timeout_request_body_idle=0.0,
        h2_timeout_response_stall=5.0,
    )
    config = Config(
        port=0,
        lifespan='on',
        max_request_body_size=128,
        request_policies=(policy,),
    )
    app, config = _connect_app(_NotebookService(), config)

    async def upload() -> AsyncIterator[NoteChunk]:
        yield NoteChunk(
            note_id='note-17',
            sequence=0,
            content=b'x' * 4096,
        )

    async with (
        running_server(app, config) as server,
        _grpc_client(server_port(server)) as client,
    ):
        summary = await client.upload_note(upload())

    assert (summary.chunks_received, summary.bytes_received) == (1, 4096)


async def test_duplex_stream_policy_allows_a_configured_idle_gap() -> None:
    from h2corn import RequestPolicy

    service = _NotebookService()
    policy = RequestPolicy(
        path=f'{SERVICE_PATH}/Collaborate',
        max_request_body_size=4096,
        timeout_request_body_idle=0.0,
        h2_timeout_response_stall=5.0,
    )
    config = Config(
        port=0,
        lifespan='on',
        timeout_request_body_idle=0.05,
        request_policies=(policy,),
    )
    app, config = _connect_app(service, config)

    async with (
        running_server(app, config) as server,
        _grpc_client(server_port(server)) as client,
    ):
        second_request_allowed = asyncio.Event()

        async def edit_stream() -> AsyncIterator[Edit]:
            yield Edit(
                note_id='note-17',
                base_revision=1,
                text='before idle gap',
            )
            await second_request_allowed.wait()
            await asyncio.sleep(0.15)
            yield Edit(
                note_id='note-17',
                base_revision=2,
                text='after idle gap',
            )

        call = client.collaborate(edit_stream())
        first = await asyncio.wait_for(call.read(), timeout=HANG_BOUND)
        assert first is not grpc.aio.EOF
        first = cast('EditAck', first)
        assert first.accepted_text == 'before idle gap'

        second_request_allowed.set()
        second = await asyncio.wait_for(call.read(), timeout=HANG_BOUND)
        assert second is not grpc.aio.EOF
        second = cast('EditAck', second)

    assert second.accepted_text == 'after idle gap'


async def test_grpc_over_tls_uses_existing_certificate_helpers(tmp_path: Path) -> None:
    certificate, private_key = write_self_signed_cert(tmp_path)
    credentials = grpc.ssl_channel_credentials(
        root_certificates=certificate.read_bytes()
    )
    app, config = _connect_app(
        _NotebookService(),
        Config(
            port=0,
            lifespan='on',
            certfile=certificate,
            keyfile=private_key,
        ),
    )

    async with (
        running_server(app, config) as server,
        _grpc_client(
            server_port(server),
            credentials=credentials,
            options=(('grpc.ssl_target_name_override', 'localhost'),),
        ) as client,
    ):
        note = await client.get_note(GetNoteRequest(note_id='note-17'))

    assert note.author == 'Ada'


async def test_grpc_over_mutual_tls_uses_existing_certificate_helpers(
    tmp_path: Path,
) -> None:
    ca_cert, server_cert, server_key, client_cert, client_key = write_mutual_tls_certs(
        tmp_path
    )
    credentials = grpc.ssl_channel_credentials(
        root_certificates=ca_cert.read_bytes(),
        private_key=client_key.read_bytes(),
        certificate_chain=client_cert.read_bytes(),
    )
    app, config = _connect_app(
        _NotebookService(),
        Config(
            port=0,
            lifespan='on',
            certfile=server_cert,
            keyfile=server_key,
            ca_certs=ca_cert,
            cert_reqs='required',
        ),
    )

    async with (
        running_server(app, config) as server,
        _grpc_client(
            server_port(server),
            credentials=credentials,
            options=(('grpc.ssl_target_name_override', 'localhost'),),
        ) as client,
    ):
        note = await client.get_note(GetNoteRequest(note_id='note-17'))

    assert note.note_id == 'note-17'
