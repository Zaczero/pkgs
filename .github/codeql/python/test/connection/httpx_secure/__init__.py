"""The query fixture's importable HTTPX Secure package."""

from anyio import connect_tcp
from anyio.abc import SocketAttribute


async def write_probe(stream):
    await stream.send(b'not-admitted')


class _AddressPolicy:
    async def _resolve(self, hostname, port, local_address, mode):
        stream = await connect_tcp(hostname, port, local_host=local_address)
        try:
            if mode == 'bad-send':
                await stream.send(b'not-admitted')
            elif mode == 'bad-helper':
                await write_probe(stream)
            elif mode == 'bad-alias':
                alias = stream
                await alias.send(b'not-admitted')
            return stream.extra(SocketAttribute.remote_address)[0]
        finally:
            await stream.aclose()


class ApplicationConnector:
    async def _resolve(self, hostname, port):
        stream = await connect_tcp(hostname, port)
        await stream.send(b'application-stream')
        await stream.aclose()
