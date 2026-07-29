"""The smallest correct ASGI app, for isolating the server from the framework.

The cross-server plots run Starlette, because that is what people deploy and it
is the same for every server. But it dominates the measurement: on this host a
plaintext GET costs about 111k instructions through Starlette and about 34k
through this app, so roughly two thirds of the published number is framework.

Point `compare.py --app bench.raw_app:app` at this when measuring a change to
the server itself — a saving worth 1 % here is diluted to 0.3 % behind
Starlette, and can disappear into the noise entirely.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from h2corn import ASGIReceiveCallable, ASGISendCallable, Scope

RESPONSE_START = {
    'type': 'http.response.start',
    'status': 200,
    'headers': [(b'content-type', b'text/plain; charset=utf-8')],
}
RESPONSE_BODY = {'type': 'http.response.body', 'body': b'Hello, World!'}


async def app(
    scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable
) -> None:
    if scope['type'] != 'http':
        raise RuntimeError(f'unsupported scope: {scope["type"]}')
    await send(RESPONSE_START)
    if scope['path'] == '/__bench/worker-pid':
        # Read at request time: the app module is imported before the
        # supervisor forks, so a module-level pid would name the supervisor
        # and every worker would look like the same process.
        await send({'type': 'http.response.body', 'body': str(os.getpid()).encode()})
        return
    await send(RESPONSE_BODY)
