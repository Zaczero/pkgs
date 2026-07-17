import subprocess
import sys
from typing import assert_type

import h2corn
from fastapi import FastAPI
from h2corn import (
    ASGIApp,
    ExtensionParameters,
    FrameworkASGIApp,
    HTTPExtensions,
    HTTPRequest,
    HTTPResponseBody,
    HTTPResponseStart,
    HTTPScope,
    LifespanScope,
    Receive,
    ReceiveMessage,
    Scope,
    Send,
    SendMessage,
    Server,
    WebSocketClose,
    WebSocketDisconnect,
    WebSocketExtensions,
    WebSocketReceiveBytes,
    WebSocketReceiveText,
    WebSocketScope,
)


def test_public_types_are_loaded_only_when_used() -> None:
    subprocess.run(
        [
            sys.executable,
            '-c',
            """
import sys

import h2corn

assert 'h2corn._types' not in sys.modules
assert 'HTTPRequest' in dir(h2corn)
request_type = h2corn.HTTPRequest
assert 'h2corn._types' in sys.modules
assert h2corn.HTTPRequest is request_type
""",
        ],
        check=True,
    )


async def _typed_app(scope: Scope, receive: Receive, send: Send) -> None:
    _ = scope, receive, send


def _scope_type_is_discriminated(scope: Scope) -> None:
    match scope['type']:
        case 'http':
            assert_type(scope, HTTPScope)
        case 'websocket':
            assert_type(scope, WebSocketScope)
        case 'lifespan':
            assert_type(scope, LifespanScope)


def _receive_type_is_discriminated(message: ReceiveMessage) -> None:
    if message['type'] == 'http.request':
        assert_type(message, HTTPRequest)
    elif message['type'] == 'websocket.receive':
        assert_type(message, WebSocketReceiveBytes | WebSocketReceiveText)


def _send_type_is_discriminated(message: SendMessage) -> None:
    if message['type'] == 'http.response.body':
        assert_type(message, HTTPResponseBody)


def _extension_types_expose_supported_capabilities() -> None:
    parameters: ExtensionParameters = {}
    http: HTTPExtensions = {'http.response.pathsend': parameters}
    websocket: WebSocketExtensions = {'websocket.http.response': parameters}
    assert_type(http['http.response.pathsend'], ExtensionParameters)
    assert_type(websocket['websocket.http.response'], ExtensionParameters)


def test_scope_types_are_reusable_and_framework_boundary_is_compatible() -> None:
    http_scope: HTTPScope = {
        'type': 'http',
        'asgi': {'version': '3.0', 'spec_version': '2.5'},
        'http_version': '2',
        'method': 'GET',
        'scheme': 'https',
        'path': '/',
        'raw_path': b'/',
        'query_string': b'',
        'headers': (),
        'server': ('127.0.0.1', 8000),
        'extensions': {'http.response.pathsend': {}},
    }
    websocket_scope: WebSocketScope = {
        'type': 'websocket',
        'asgi': {'version': '3.0', 'spec_version': '2.5'},
        'http_version': '1.1',
        'scheme': 'ws',
        'path': '/ws',
        'raw_path': b'/ws',
        'query_string': b'',
        'headers': (),
        'server': ('127.0.0.1', 8000),
        'subprotocols': [],
        'extensions': {'websocket.http.response': {}},
    }
    lifespan_scope: LifespanScope = {
        'type': 'lifespan',
        'asgi': {'version': '3.0', 'spec_version': '2.0'},
        'state': {},
    }

    _scope_type_is_discriminated(http_scope)
    _scope_type_is_discriminated(websocket_scope)
    _scope_type_is_discriminated(lifespan_scope)
    _receive_type_is_discriminated({'type': 'http.request'})
    _receive_type_is_discriminated({
        'type': 'websocket.receive',
        'text': 'hello',
    })
    _send_type_is_discriminated({
        'type': 'http.response.body',
        'body': b'ok',
    })
    close: WebSocketClose = {'type': 'websocket.close', 'reason': None}
    disconnect: WebSocketDisconnect = {
        'type': 'websocket.disconnect',
        'code': 1005,
        'reason': None,
    }
    response: HTTPResponseStart = {
        'type': 'http.response.start',
        'status': 200,
        'headers': ((name, value) for name, value in [(b'x-demo', b'1')]),
    }
    assert response['status'] == 200
    assert close['reason'] is None
    assert disconnect['reason'] is None
    typed_app: ASGIApp = _typed_app
    framework_app: FrameworkASGIApp = FastAPI()
    typed_server = Server(typed_app)
    framework_server = Server(framework_app)
    assert callable(typed_app)
    assert callable(framework_app)
    assert typed_server.app is typed_app
    assert framework_server.app is framework_app


def test_typeddict_runtime_required_keys_match_static_contract() -> None:
    assert HTTPRequest.__required_keys__ == frozenset({'type'})
    assert HTTPRequest.__optional_keys__ == frozenset({'body', 'more_body'})
    assert WebSocketScope.__required_keys__ >= {
        'type',
        'asgi',
        'http_version',
        'subprotocols',
    }
    assert WebSocketScope.__optional_keys__ == {'root_path', 'client', 'state'}
    assert HTTPExtensions.__required_keys__ == {'http.response.pathsend'}
    assert HTTPExtensions.__optional_keys__ == {'http.response.trailers'}
    assert WebSocketExtensions.__required_keys__ == {'websocket.http.response'}


def test_top_level_does_not_expose_typing_bootstrap_state() -> None:
    assert not hasattr(h2corn, 'TYPE_CHECKING')
    assert not hasattr(h2corn, 'Any')
