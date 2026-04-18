import asyncio
from pathlib import Path

from starlette.applications import Starlette
from starlette.responses import FileResponse, Response, StreamingResponse
from starlette.routing import Route, WebSocketRoute

FILE_RESPONSE_SIZE = 128 * 1024
FILE_RESPONSE_PATH = Path(__file__).with_name('_file_response_payload.bin')


def ensure_file_response_payload():
    if (
        FILE_RESPONSE_PATH.exists()
        and FILE_RESPONSE_PATH.stat().st_size == FILE_RESPONSE_SIZE
    ):
        return

    FILE_RESPONSE_PATH.write_bytes(b'\x00' * FILE_RESPONSE_SIZE)


ensure_file_response_payload()


async def homepage(request):
    return Response(b'Hello, World!', media_type='text/plain')


async def static_file(request):
    return FileResponse(FILE_RESPONSE_PATH, media_type='application/octet-stream')


async def streaming_post(request):
    body_len = 0
    async for chunk in request.stream():
        body_len += len(chunk)
    body_len = str(body_len).encode()

    async def chunks():
        yield b'stream-started\n'
        await asyncio.sleep(0.015)
        yield body_len
        await asyncio.sleep(0.005)
        yield b'\nstream-finished\n'

    return StreamingResponse(chunks(), media_type='text/plain')


async def websocket_endpoint(websocket):
    await websocket.accept()
    while True:
        try:
            msg = await websocket.receive_text()
            await websocket.send_text(msg)
        except Exception:
            break


app = Starlette(
    routes=[
        Route('/', homepage),
        Route('/static-file', static_file),
        Route('/streaming-post', streaming_post, methods=['POST']),
        WebSocketRoute('/ws', websocket_endpoint),
    ]
)
