import asyncio

from h2corn import Config, Server

from hello import app


async def main() -> None:
    server = Server(app, Config(bind=('127.0.0.1:8000',)))
    await server.serve()


asyncio.run(main())
