"""Host a Connect gRPC service and an HTTP health endpoint with h2corn."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, cast

from asgi_lifespan import LifespanManager
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from h2corn import Config, serve
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Mount, Route

from examples.grpc.gen.h2corn.interop.v1.notebook_connect import (
    NotebookService,
    NotebookServiceASGIApplication,
)
from examples.grpc.gen.h2corn.interop.v1.notebook_pb import (
    Edit,
    EditAck,
    GetNoteRequest,
    Note,
    NoteChunk,
    NoteRevision,
    UploadSummary,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator

    from connectrpc.request import RequestContext
    from starlette.requests import Request
    from starlette.types import ASGIApp


class Notebook(NotebookService):
    def __init__(self) -> None:
        self._notes = {
            'welcome': Note(
                note_id='welcome',
                title='Welcome',
                body='A Connect service hosted as an ASGI application.',
                author='h2corn',
            )
        }

    async def get_note(
        self,
        request: GetNoteRequest,
        ctx: RequestContext[GetNoteRequest, Note],
        /,
    ) -> Note:
        note = self._notes.get(request.note_id)
        if note is None:
            ctx.response_trailers['x-note-result'] = 'missing'
            raise ConnectError(Code.NOT_FOUND, 'note not found')

        ctx.response_headers['x-note-id'] = note.note_id
        ctx.response_trailers['x-note-author'] = note.author
        return note

    async def list_revisions(
        self,
        request: GetNoteRequest,
        _ctx: RequestContext[GetNoteRequest, NoteRevision],
        /,
    ) -> AsyncIterator[NoteRevision]:
        note = self._notes.get(request.note_id)
        if note is None:
            raise ConnectError(Code.NOT_FOUND, 'note not found')
        yield NoteRevision(
            note_id=note.note_id,
            revision=1,
            summary=note.title,
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
            if note_id and chunk.note_id != note_id:
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
        async for edit in request:
            yield EditAck(
                note_id=edit.note_id,
                revision=edit.base_revision + 1,
                accepted_text=edit.text,
            )


async def _health(_request: Request) -> PlainTextResponse:
    return PlainTextResponse('ok')


def create_app() -> Starlette:
    rpc = NotebookServiceASGIApplication(_notebook_service())

    @asynccontextmanager
    async def lifespan(_app: Starlette) -> AsyncGenerator[None, None]:
        async with LifespanManager(rpc):
            yield

    return Starlette(
        lifespan=lifespan,
        routes=[
            Route('/healthz', _health),
            Mount(rpc.path, cast('ASGIApp', rpc)),
        ],
    )


async def _notebook_service() -> AsyncGenerator[Notebook, None]:
    yield Notebook()


if __name__ == '__main__':
    serve(
        create_app,
        Config(
            port=8000,
            lifespan='on',
            max_request_body_size=64 * 1024 * 1024,
        ),
        factory=True,
    )
