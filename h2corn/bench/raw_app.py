"""The smallest correct ASGI app, for isolating the server from the framework.

The cross-server plots run Starlette, because that is what people deploy and it
is the same for every server. But it dominates the measurement: on this host a
plaintext GET costs about 111k instructions through Starlette and about 34k
through this app, so roughly two thirds of the chart value is framework.

Point `compare.py --app bench.raw_app:app` at this when measuring a change to
the server itself — a saving worth 1 % here is diluted to 0.3 % behind
Starlette, and can disappear into the noise entirely.

The routes mirror `bench_app.py`'s semantics exactly, so the same response
contracts in `_core.py` validate both. They exist so a server change can be
measured on the path it actually touches: a plaintext GET never enters the
response-budget, backpressure or sendfile code, and measuring only `/` would
report a comfortable win while a regression sat in the streaming path.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from h2corn import HTTPResponseBody, HTTPResponseStart, Receive, Scope, Send

FILE_RESPONSE_PATH = Path('bench/_file_response_payload.bin')
DOWNLOAD_CHUNK = b'x' * (16 * 1024)
DOWNLOAD_CHUNKS = 8

PLAIN_START: HTTPResponseStart = {
    'type': 'http.response.start',
    'status': 200,
    'headers': [(b'content-type', b'text/plain; charset=utf-8')],
}
OCTET_START: HTTPResponseStart = {
    'type': 'http.response.start',
    'status': 200,
    'headers': [(b'content-type', b'application/octet-stream')],
}
RESPONSE_BODY: HTTPResponseBody = {
    'type': 'http.response.body',
    'body': b'Hello, World!',
}


async def _read_body(receive: Receive) -> int:
    """Drain the request body and return its length."""
    length = 0
    more = True
    while more:
        message = await receive()
        if message['type'] == 'http.disconnect':
            break
        length += len(message.get('body', b''))
        more = message.get('more_body', False)
    return length


async def app(scope: Scope, receive: Receive, send: Send) -> None:
    if scope['type'] != 'http':
        raise RuntimeError(f'unsupported scope: {scope["type"]}')

    path = scope['path']
    if path == '/':
        await send(PLAIN_START)
        await send(RESPONSE_BODY)
        return

    if path == '/streaming-download':
        await send(OCTET_START)
        for index in range(DOWNLOAD_CHUNKS):
            await send({
                'type': 'http.response.body',
                'body': DOWNLOAD_CHUNK,
                'more_body': index + 1 < DOWNLOAD_CHUNKS,
            })
            await asyncio.sleep(0)
        return

    if path == '/static-file':
        # Prefer the zero-copy extension when the server advertises it: that is
        # the code path this scenario exists to measure. Falling back keeps the
        # app usable against a server without it.
        if 'http.response.pathsend' in scope.get('extensions', {}):
            await send(OCTET_START)
            await send({
                'type': 'http.response.pathsend',
                'path': str(FILE_RESPONSE_PATH.resolve()),
            })
            return
        await send(OCTET_START)
        await send({
            'type': 'http.response.body',
            'body': FILE_RESPONSE_PATH.read_bytes(),
        })
        return

    if path == '/streaming-post':
        body_len = str(await _read_body(receive)).encode()
        await send(PLAIN_START)
        await send({
            'type': 'http.response.body',
            'body': b'stream-started\n',
            'more_body': True,
        })
        # The sleeps mirror bench_app; they are what makes this scenario exercise
        # a response that is still open across an await, rather than one the
        # server can complete in a single batch.
        await asyncio.sleep(0.015)
        await send({'type': 'http.response.body', 'body': body_len, 'more_body': True})
        await asyncio.sleep(0.005)
        await send({'type': 'http.response.body', 'body': b'\nstream-finished\n'})
        return

    if path == '/streaming-post-fast':
        body_len = str(await _read_body(receive)).encode()
        await send(PLAIN_START)
        await send({'type': 'http.response.body', 'body': body_len})
        return

    if path == '/__bench/worker-pid':
        # Read at request time: the app module is imported before the
        # supervisor forks, so a module-level pid would name the supervisor
        # and every worker would look like the same process.
        await send(PLAIN_START)
        await send({'type': 'http.response.body', 'body': str(os.getpid()).encode()})
        return

    await send(PLAIN_START)
    await send(RESPONSE_BODY)
