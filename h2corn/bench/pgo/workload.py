"""Deterministic training inputs and the ASGI applications that consume them."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from pathlib import Path
from typing import TYPE_CHECKING

from starlette.applications import Starlette
from starlette.responses import Response, StreamingResponse
from starlette.routing import Route

if TYPE_CHECKING:
    from starlette.requests import Request

SEED = b'h2corn-pgo-v1-training'
FILE_SIZES = (65536, 131072, 2097152, 8388608)
EARLY_HINTS = (
    b'</app.css>; rel=preload; as=style',
    b'</app.js>; rel=preload; as=script',
)


def payload(index: int, size: int) -> bytes:
    key = SEED + index.to_bytes(8, 'little')
    if index % 4 == 0:
        return hashlib.shake_256(key).digest(size)
    text = json.dumps({
        'request': index,
        'message': 'A deployment-shaped response. ',
    }).encode()
    return (text * (size // len(text) + 1))[:size]


def upload_size(index: int) -> int:
    slot = index % 20
    return 0 if slot < 13 else 4096 if slot < 17 else 256 if slot < 19 else 65536


def response_size(index: int) -> int:
    slot = index % 10
    return 512 if slot < 6 else 4096 if slot < 9 else 32768


def stream_body(index: int) -> bytes:
    return b''.join(
        f'id: {event}\ndata: '.encode() + payload(index, 224).hex().encode() + b'\n\n'
        for event in range(32)
    )


async def web(request: Request) -> Response:
    index = int(request.path_params['index'])
    body = await request.body()
    if body != payload(index, upload_size(index)):
        return Response('incorrect request body', status_code=400)
    if index % 5 == 0:
        await asyncio.sleep(0.001 * (1 + index % 5))
    return Response(
        payload(index, response_size(index)), media_type='application/octet-stream'
    )


def synchronous(request: Request) -> Response:
    index = int(request.path_params['index'])
    return Response(
        payload(index, response_size(index)), media_type='application/octet-stream'
    )


async def events(request: Request) -> StreamingResponse:
    index = int(request.path_params['index'])

    async def chunks():
        for event in stream_body(index).split(b'\n\n')[:-1]:
            if index % 2:
                await asyncio.sleep(0.02)
            yield event + b'\n\n'

    return StreamingResponse(chunks(), media_type='text/event-stream')


framework = Starlette(
    routes=[
        Route('/web/{index:int}', web, methods=['GET', 'POST']),
        Route('/sync/{index:int}', synchronous),
        Route('/events/{index:int}', events),
    ]
)
completed = 0


def app(scope, receive, send):
    global completed
    if scope.get('path', '').startswith('/fail/'):
        completed += 1
        raise RuntimeError('planned training application failure')
    return application(scope, receive, send)


async def application(scope, receive, send):
    global completed
    if scope['type'] == 'lifespan':
        while True:
            message = await receive()
            if message['type'] == 'lifespan.startup':
                await send({'type': 'lifespan.startup.complete'})
            else:
                root = Path(os.environ['PGO_WORKER_OUTPUT'])
                (root / f'work-{os.getpid()}.json').write_text(
                    json.dumps({'pid': os.getpid(), 'completed': completed}),
                )
                await send({'type': 'lifespan.shutdown.complete'})
                return
    if scope['type'] == 'websocket':
        await receive()
        if scope['scheme'] == 'wss':
            assert scope['extensions']['tls']['tls_version'] == 0x0304
        if scope['path'].startswith('/deny/'):
            assert 'websocket.http.response' in scope['extensions']
            index = int(scope['path'].rsplit('/', 1)[1])
            body = payload(index, 512)
            await send({
                'type': 'websocket.http.response.start',
                'status': 403,
                'headers': [(b'x-worker', str(os.getpid()).encode())],
            })
            await send({
                'type': 'websocket.http.response.body',
                'body': body[:128],
                'more_body': True,
            })
            await send({'type': 'websocket.http.response.body', 'body': body[128:]})
            completed += 1
            return
        await send({
            'type': 'websocket.accept',
            'headers': [(b'x-worker', str(os.getpid()).encode())],
        })
        while True:
            message = await receive()
            if message['type'] == 'websocket.disconnect':
                return
            value = (
                {'text': message['text']}
                if message.get('text') is not None
                else {'bytes': message['bytes']}
            )
            await send({'type': 'websocket.send', **value})
            completed += 1
    mode, number = scope['path'].strip('/').split('/')
    index = int(number)
    if scope['scheme'] == 'https':
        assert scope['extensions']['tls']['tls_version'] in {0x0303, 0x0304}

    async def identified(message):
        if message['type'] == 'http.response.start':
            message = {
                **message,
                'headers': [
                    *message.get('headers', []),
                    (b'x-worker', str(os.getpid()).encode()),
                ],
            }
        await send(message)

    if mode == 'early-hint':
        assert 'http.response.early_hint' in scope['extensions']
        await identified({'type': 'http.response.start', 'status': 200})
        await send({'type': 'http.response.early_hint', 'links': EARLY_HINTS})
        await send({'type': 'http.response.body', 'body': payload(index, 512)})
    elif mode == 'trailers':
        assert 'http.response.trailers' in scope['extensions']
        body = payload(index, 512)
        await identified({
            'type': 'http.response.start',
            'status': 200,
            'trailers': True,
            'headers': [(b'trailer', b'x-checksum, x-finished')],
        })
        await send({'type': 'http.response.body', 'body': body})
        await send({
            'type': 'http.response.trailers',
            'headers': [(b'x-checksum', hashlib.sha256(body).hexdigest().encode())],
            'more_trailers': True,
        })
        await send({
            'type': 'http.response.trailers',
            'headers': [(b'x-finished', b'yes')],
        })
    elif mode in {'web', 'sync', 'events'}:
        await framework(scope, receive, identified)
    elif mode in {'file', 'range'}:
        size = FILE_SIZES[index % len(FILE_SIZES)]
        path = Path(os.environ['PGO_FILES']) / str(size)
        await identified({
            'type': 'http.response.start',
            'status': 200,
            'headers': [
                (b'content-length', str(size).encode()),
            ],
        })
        if mode == 'file':
            await send({'type': 'http.response.pathsend', 'path': str(path)})
        else:
            with path.with_suffix('.range').open('rb') as source:
                await send({
                    'type': 'http.response.zerocopysend',
                    'file': source,
                    'offset': 37,
                    'count': size,
                    'more_body': False,
                })
    elif mode == 'upload':
        digest = hashlib.sha256()
        while True:
            message = await receive()
            if message['type'] == 'http.disconnect':
                return
            digest.update(message.get('body', b''))
            if index % 10 == 0:
                await asyncio.sleep(0.005)
            if not message.get('more_body', False):
                break
        await identified({'type': 'http.response.start', 'status': 200})
        await send({'type': 'http.response.body', 'body': digest.hexdigest().encode()})
    elif mode == 'download':
        size = FILE_SIZES[index % len(FILE_SIZES)]
        body = payload(index, size)
        await identified({'type': 'http.response.start', 'status': 200})
        for offset in range(0, size, 65536):
            await send({
                'type': 'http.response.body',
                'body': body[offset : offset + 65536],
                'more_body': offset + 65536 < size,
            })
    elif mode.startswith('grpc-'):
        await identified({
            'type': 'http.response.start',
            'status': 200,
            'trailers': True,
            'headers': [(b'content-type', b'application/grpc')],
        })
        body = bytearray()
        while True:
            message = await receive()
            if message['type'] == 'http.disconnect':
                return
            more = message.get('more_body', False)
            data = message.get('body', b'')
            if mode == 'grpc-duplex':
                await send({
                    'type': 'http.response.body',
                    'body': data,
                    'more_body': more,
                })
            else:
                body.extend(data)
            if not more:
                break
        if mode == 'grpc-unary':
            await send({'type': 'http.response.body', 'body': bytes(body)})
        elif mode == 'grpc-upload':
            result = hashlib.sha256(body).digest()
            await send({
                'type': 'http.response.body',
                'body': b'\0' + len(result).to_bytes(4, 'big') + result,
            })
        elif mode == 'grpc-download':
            for message_index in range(16):
                await send({
                    'type': 'http.response.body',
                    'body': bytes(body),
                    'more_body': message_index != 15,
                })
        await send({
            'type': 'http.response.trailers',
            'headers': [(b'grpc-status', b'0')],
        })
    else:
        await identified({'type': 'http.response.start', 'status': 404})
        await send({'type': 'http.response.body', 'body': b'not found'})
    completed += 1
