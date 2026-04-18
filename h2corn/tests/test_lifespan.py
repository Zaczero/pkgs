import asyncio

import pytest
from h2corn._lifespan import _serve_with_lifespan

pytestmark = pytest.mark.asyncio


async def test_lifespan_startup_failure_is_reported() -> None:
    async def app(scope, receive, send):
        assert scope['type'] == 'lifespan'
        message = await receive()
        assert message['type'] == 'lifespan.startup'
        await send({'type': 'lifespan.startup.failed', 'message': 'boom'})

    async def serve(_app):
        raise AssertionError('serve callback should not run after startup failure')

    with pytest.raises(RuntimeError, match='lifespan startup failed: boom'):
        await _serve_with_lifespan(app, serve)


async def test_lifespan_missing_protocol_is_treated_as_optional() -> None:
    served = False

    async def app(scope, _receive, _send):
        assert scope['type'] == 'lifespan'

    async def serve(_app):
        nonlocal served
        served = True

    await _serve_with_lifespan(app, serve)

    assert served is True


async def test_lifespan_state_is_copied_into_request_scopes() -> None:
    seen_states: list[dict[str, object]] = []

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            startup = await receive()
            assert startup['type'] == 'lifespan.startup'
            scope['state']['ready'] = True
            await send({'type': 'lifespan.startup.complete'})
            shutdown = await receive()
            assert shutdown['type'] == 'lifespan.shutdown'
            await send({'type': 'lifespan.shutdown.complete'})
            return

        seen_states.append(scope.get('state', {}))

    async def receive():
        raise AssertionError('request receive should not run')

    async def send(_message):
        return None

    async def serve(wrapped_app):
        first_scope = {'type': 'http'}
        await wrapped_app(first_scope, receive, send)
        assert first_scope['state'] == {'ready': True}
        first_scope['state']['ready'] = False

        second_scope = {'type': 'websocket'}
        await wrapped_app(second_scope, receive, send)
        assert second_scope['state'] == {'ready': True}

    await _serve_with_lifespan(app, serve)

    assert seen_states == [{'ready': False}, {'ready': True}]


async def test_lifespan_startup_timeout_is_reported() -> None:
    async def app(scope, receive, _send):
        assert scope['type'] == 'lifespan'
        message = await receive()
        assert message['type'] == 'lifespan.startup'
        await asyncio.sleep(0.05)

    async def serve(_app):
        raise AssertionError('serve callback should not run after startup timeout')

    with pytest.raises(RuntimeError, match='lifespan startup timed out'):
        await _serve_with_lifespan(app, serve, startup_timeout=0.01)


async def test_lifespan_shutdown_timeout_is_reported() -> None:
    async def app(scope, receive, send):
        assert scope['type'] == 'lifespan'
        startup = await receive()
        assert startup['type'] == 'lifespan.startup'
        await send({'type': 'lifespan.startup.complete'})
        shutdown = await receive()
        assert shutdown['type'] == 'lifespan.shutdown'
        await asyncio.sleep(0.05)

    async def serve(_app):
        return None

    with pytest.raises(RuntimeError, match='lifespan shutdown timed out'):
        await _serve_with_lifespan(app, serve, shutdown_timeout=0.01)
