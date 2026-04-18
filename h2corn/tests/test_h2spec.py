import asyncio
import shutil

import pytest
from h2corn import Config

from tests._support import find_free_port, running_server

pytestmark = pytest.mark.asyncio


@pytest.mark.skipif(shutil.which('h2spec') is None, reason='h2spec not found in PATH')
async def test_h2spec_conformance() -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    config = Config(port=find_free_port())
    async with running_server(app, config):
        process = await asyncio.create_subprocess_exec(
            'h2spec',
            '-h',
            '127.0.0.1',
            '-p',
            str(config.port),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()

        if process.returncode != 0:
            pytest.fail(
                f'h2spec failed with exit code {process.returncode}:\n{stdout.decode()}\n{stderr.decode()}'
            )
