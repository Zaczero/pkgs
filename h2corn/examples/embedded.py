import asyncio
from urllib.request import urlopen

from h2corn import Config, Server
from hello import app


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
            print(f'embedded request: {response.status}')
    finally:
        server.shutdown()
        await serving


asyncio.run(main())
