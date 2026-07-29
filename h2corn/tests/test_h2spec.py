import asyncio
import shutil
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from h2corn import Config

from tests._support import running_server, server_port

pytestmark = pytest.mark.asyncio

# RFC 9113 removed RFC 7540's §5.3.1 self-dependency requirement with the
# deprecation of the dependency tree. h2spec 2.6.0 still checks it, so we
# deliberately diverge from that version for only these two named checks.
H2SPEC_RFC_9113_DIVERGENCES = frozenset({
    ('http2/5.3.1', 'Sends HEADERS frame that depends on itself'),
    ('http2/5.3.1', 'Sends PRIORITY frame that depend on itself'),
})


@pytest.mark.skipif(shutil.which('h2spec') is None, reason='h2spec not found in PATH')
async def test_h2spec_conformance(tmp_path: Path) -> None:
    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    config = Config(
        port=0,
        http1=False,
        h2_max_inbound_frame_size=16_384,
    )
    async with running_server(app, config) as server:
        report = tmp_path / 'h2spec.xml'
        process = await asyncio.create_subprocess_exec(
            'h2spec',
            '--junit-report',
            str(report),
            '-h',
            '127.0.0.1',
            '-p',
            str(server_port(server)),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()

        output = f'{stdout.decode()}\n{stderr.decode()}'
        if process.returncode != 1:
            pytest.fail(
                f'h2spec exited with {process.returncode}, expected the two RFC 9113 divergences:\n{output}'
            )
        try:
            # The fixed h2spec executable above writes this report into
            # pytest's private temporary directory.
            root = ET.parse(report).getroot()  # noqa: S314
        except (FileNotFoundError, ET.ParseError) as error:
            pytest.fail(f'h2spec did not write a parseable JUnit report: {error}\n{output}')

        failures = frozenset(
            (case.attrib['package'], case.attrib['classname'])
            for case in root.iter('testcase')
            if case.find('error') is not None or case.find('failure') is not None
        )
        assert failures == H2SPEC_RFC_9113_DIVERGENCES, output
