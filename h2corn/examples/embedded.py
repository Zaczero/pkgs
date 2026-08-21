import asyncio

from h2corn import Config, Server
from hello import app


async def main() -> None:
    server = Server(
        app,
        Config(bind=('127.0.0.1:0',), access_log=False, lifespan='off'),
    )
    serving = asyncio.create_task(server.serve())
    await server.wait_started()
    address = server.addresses[0]
    port = int(address.rsplit(':', 1)[1])
    try:
        reader, writer = await asyncio.open_connection('127.0.0.1', port)
        try:
            writer.write(
                b'GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n'
            )
            await writer.drain()
            head = await reader.readuntil(b'\r\n\r\n')
            status = int(head.split(b' ', 2)[1])
            headers = {
                name.lower(): value.strip()
                for line in head.split(b'\r\n')[1:]
                if line
                for name, value in (line.split(b':', 1),)
            }
            body = await reader.readexactly(int(headers[b'content-length']))
            assert status == 200
            assert b'hello from h2corn' in body
        finally:
            writer.close()
            await writer.wait_closed()
    finally:
        server.shutdown()
        await serving
    print(f'embedded request: {status}')


asyncio.run(main())
