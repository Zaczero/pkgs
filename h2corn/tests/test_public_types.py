import subprocess
import sys
from collections.abc import Iterable
from typing import assert_type, get_origin, get_type_hints

import h2corn
import pytest
from fastapi import FastAPI
from h2corn import (
    Application,
    ASGIApp,
    Config,
    ExtensionParameters,
    FrameworkASGIApp,
    Headers,
    HTTPExtensions,
    HTTPRequest,
    HTTPResponseBody,
    HTTPResponseStart,
    HTTPScope,
    LifespanScope,
    Receive,
    ReceiveMessage,
    Scope,
    ScopeHeaders,
    Send,
    SendMessage,
    Server,
    TLSExtension,
    WebSocketClose,
    WebSocketDisconnect,
    WebSocketExtensions,
    WebSocketReceiveBytes,
    WebSocketReceiveText,
    WebSocketScope,
)

from tests._support import (
    h2_request,
    http1_request,
    running_server,
    server_port,
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


def _header_types_follow_the_direction_of_data_flow(
    scope_headers: ScopeHeaders,
    outbound_headers: Headers,
) -> None:
    # Taken as parameters rather than locals: an assignment narrows the
    # declared type to whatever was assigned, so a local would assert on the
    # literal's own type instead of the published alias.
    assert_type(scope_headers, ScopeHeaders)
    assert_type(outbound_headers, Headers)


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
        'headers': [(b'host', b'example.test')],
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
        'headers': [(b'host', b'example.test')],
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
    disconnect: WebSocketDisconnect = {'type': 'websocket.disconnect', 'code': 1005}
    response: HTTPResponseStart = {
        'type': 'http.response.start',
        'status': 200,
        'headers': ((name, value) for name, value in [(b'x-demo', b'1')]),
    }
    assert response['status'] == 200
    assert close['reason'] is None
    assert 'reason' not in disconnect
    typed_app: ASGIApp = _typed_app
    application: Application = _typed_app
    framework_app: FrameworkASGIApp = FastAPI()
    typed_server = Server(typed_app)
    framework_server = Server(framework_app)
    assert callable(typed_app)
    assert callable(application)
    assert callable(framework_app)
    assert typed_server.app is typed_app
    assert framework_server.app is framework_app


@pytest.mark.asyncio
@pytest.mark.parametrize('protocol', ['h1', 'h2'])
@pytest.mark.parametrize('root_path', ['', '/mounted'])
@pytest.mark.parametrize('lifespan', ['on', 'off'])
async def test_http_scope_matches_its_published_contract(
    protocol: str, root_path: str, lifespan: str
) -> None:
    """
    The scope the Rust side builds must stay inside `HTTPScope`.

    Reading `__required_keys__` back off the declaration and asserting on it
    proves nothing -- both sides of that comparison come from the same
    `NotRequired` annotations. What is worth checking is the FFI boundary:
    `root_path` is emitted only when non-empty, `client` only when present and
    `state` only when lifespan ran, and those three conditionals are exactly
    what makes the optional keys correct.
    """
    seen: dict[str, object] = {}

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            while True:
                message = await receive()
                if message['type'] == 'lifespan.startup':
                    await send({'type': 'lifespan.startup.complete'})
                elif message['type'] == 'lifespan.shutdown':
                    await send({'type': 'lifespan.shutdown.complete'})
                    return
        seen.update(scope)
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    config = Config(port=0, root_path=root_path, lifespan=lifespan)
    async with running_server(app, config) as server:
        if protocol == 'h1':
            await http1_request(
                port=server_port(server),
                request=b'GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n',
            )
        else:
            await h2_request(port=server_port(server))

    keys = set(seen)
    required = set(HTTPScope.__required_keys__)
    optional = set(HTTPScope.__optional_keys__)
    assert required <= keys, f'scope is missing required keys: {required - keys}'
    assert keys <= required | optional, (
        f'scope carries keys the published contract does not declare: '
        f'{keys - required - optional}'
    )
    assert ('root_path' in keys) == bool(root_path)
    assert ('state' in keys) == (lifespan == 'on')


def test_published_typeddicts_have_the_declared_shape() -> None:
    # Hand-written expectations, deliberately: these are an oracle independent
    # of the declarations, so a `NotRequired` added or removed by accident is
    # caught here even though the live scope test above cannot see it (a key
    # moving between required and optional stays inside the same union).
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
    assert HTTPExtensions.__optional_keys__ == {
        'http.response.trailers',
        'http.response.early_hint',
        'tls',
    }
    assert WebSocketDisconnect.__required_keys__ == {'type', 'code'}
    assert WebSocketDisconnect.__optional_keys__ == {'reason'}
    assert WebSocketExtensions.__required_keys__ == {'websocket.http.response'}
    assert get_type_hints(WebSocketDisconnect)['reason'] is str
    assert get_origin(ScopeHeaders) is list
    assert get_origin(Headers) is Iterable
    # `tls` is optional on both because the extension requires it to be absent
    # from a connection that is not TLS -- h2corn sets every one of its keys.
    assert TLSExtension.__required_keys__ == {
        'server_cert',
        'client_cert_chain',
        'client_cert_name',
        'client_cert_error',
        'tls_version',
        'cipher_suite',
    }
    assert TLSExtension.__optional_keys__ == frozenset()


def test_top_level_does_not_expose_typing_bootstrap_state() -> None:
    assert not hasattr(h2corn, 'TYPE_CHECKING')
    assert not hasattr(h2corn, 'Any')
