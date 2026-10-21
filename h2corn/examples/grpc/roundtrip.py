from __future__ import annotations

import asyncio

from connectrpc.protocol import ProtocolType
from h2corn import Config, Server

from examples.grpc.app import create_app
from examples.grpc.gen.h2corn.interop.v1.notebook_connect import (
    NotebookServiceClient,
)
from examples.grpc.gen.h2corn.interop.v1.notebook_pb import GetNoteRequest


async def main() -> None:
    server = Server(
        create_app,
        Config(
            bind=('127.0.0.1:0',),
            access_log=False,
            grpc=True,
            lifespan='on',
        ),
        factory=True,
    )
    serving = asyncio.create_task(server.serve())
    try:
        await server.wait_started()
        async with NotebookServiceClient(
            f'http://{server.addresses[0]}',
            protocol=ProtocolType.GRPC,
        ) as client:
            note = await client.get_note(GetNoteRequest(note_id='welcome'))
        assert (note.note_id, note.title) == ('welcome', 'Welcome')
        print(f'native gRPC response: {note.title}')
    finally:
        server.shutdown()
        await serving


if __name__ == '__main__':
    asyncio.run(main())
