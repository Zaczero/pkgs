import asyncio
from urllib.request import urlopen

from h2corn import Config, Receive, Scope, Send, Server


async def app(scope: Scope, receive: Receive, send: Send) -> None:
    assert scope['type'] == 'http'
    await send({'type': 'http.response.start', 'status': 200, 'headers': []})
    await send({'type': 'http.response.body', 'body': b'Hello, world!'})


async def main() -> None:
    server = Server(
        app,
        Config(bind=('127.0.0.1:0',), access_log=False, lifespan='off'),
    )
    serving = asyncio.create_task(server.serve())
    try:
        await server.wait_started()
        url = f'http://{server.addresses[0]}/'
        # Keep the event loop free to serve the synchronous client's request.
        with await asyncio.to_thread(urlopen, url, timeout=5) as response:  # ruff: ignore[suspicious-url-open-usage] - The URL is the local HTTP listener above.
            assert response.status == 200
            assert response.read() == b'Hello, world!'
            print(f'embedded request: {response.status}')
    finally:
        server.shutdown()
        await serving


asyncio.run(main())
