import asyncio
import re
import shutil
import xml.etree.ElementTree as ET
from collections import Counter
from pathlib import Path

import pytest
from h2corn import Config

from tests._support import running_server, server_port

pytestmark = pytest.mark.asyncio

# shell.nix pins the conformance tool through its pinned nixpkgs revision.
H2SPEC_VERSION = 'Version: 2.6.0 (2.6.0)'
H2SPEC_ROOT_PACKAGES = {
    'Generic tests for HTTP/2 server': 'generic',
    'Hypertext Transfer Protocol Version 2 (HTTP/2)': 'http2',
    'HPACK: Header Compression for HTTP/2': 'hpack',
}

# RFC 9113 removed RFC 7540's §5.3.1 self-dependency requirement with the
# deprecation of the dependency tree. h2spec 2.6.0 still checks it, so we
# deliberately diverge from that version for only these two named checks.
H2SPEC_RFC_9113_DIVERGENCES = frozenset({
    ('http2/5.3.1', 'Sends HEADERS frame that depends on itself'),
    ('http2/5.3.1', 'Sends PRIORITY frame that depend on itself'),
})
# h2spec 2.6.0's JUnit reporter labels pointer-valued TestError results as errors.
H2SPEC_RFC_9113_EXPECTED_ERROR_LINES = (
    'GOAWAY Frame (Error Code: PROTOCOL_ERROR)',
    'RST_STREAM Frame (Error Code: PROTOCOL_ERROR)',
    'Connection closed',
)


def _expected_h2spec_cases(output: str) -> set[tuple[str, str]]:
    package = None
    section = None
    cases: list[tuple[str, str]] = []

    for line in output.splitlines():
        if not line.strip():
            continue
        if not line[0].isspace():
            package = H2SPEC_ROOT_PACKAGES.get(line)
            section = None
            assert package is not None, f'unrecognized h2spec test group: {line!r}'
            continue

        heading = re.fullmatch(r'\s*(\d+(?:\.\d+)*)\.\s+.+', line)
        if heading is not None:
            section = heading.group(1)
            continue

        case = re.fullmatch(r'\s+\d+:\s+(.+)', line)
        assert case is not None, f'unrecognized h2spec inventory line: {line!r}'
        assert package is not None and section is not None, (
            f'h2spec case has no group or section: {line!r}'
        )
        cases.append((f'{package}/{section}', case.group(1)))

    assert cases, 'h2spec dry-run produced an empty case inventory'
    assert len(cases) == len(set(cases)), (
        'h2spec dry-run contains duplicate case identities'
    )
    return set(cases)


def _assert_h2spec_report(
    root: ET.Element,
    expected_cases: set[tuple[str, str]],
    output: str,
) -> None:
    cases = list(root.iter('testcase'))
    identities = [
        (case.attrib.get('package', ''), case.attrib.get('classname', ''))
        for case in cases
    ]
    assert all(package and name for package, name in identities), (
        f'h2spec report contains a case without its identity:\n{output}'
    )

    duplicates = [
        identity for identity, count in Counter(identities).items() if count > 1
    ]
    assert not duplicates, (
        f'h2spec report contains duplicate cases: {duplicates!r}\n{output}'
    )

    skipped = []
    unexpected_errors = []
    failures = []
    for identity, case in zip(identities, cases, strict=True):
        error = case.find('error')
        failure = case.find('failure')
        if case.find('skipped') is not None:
            skipped.append(identity)
        if error is not None:
            error_lines = (error.text or '').splitlines()
            is_reported_divergence = (
                identity in H2SPEC_RFC_9113_DIVERGENCES
                and tuple(error_lines[: len(H2SPEC_RFC_9113_EXPECTED_ERROR_LINES)])
                == H2SPEC_RFC_9113_EXPECTED_ERROR_LINES
                and any(
                    line.strip()
                    for line in error_lines[len(H2SPEC_RFC_9113_EXPECTED_ERROR_LINES) :]
                )
            )
            if not is_reported_divergence:
                unexpected_errors.append(identity)
        if error is not None or failure is not None:
            failures.append(identity)
    assert not skipped, f'h2spec skipped cases: {skipped!r}\n{output}'
    assert not unexpected_errors, (
        f'h2spec reported errors: {unexpected_errors!r}\n{output}'
    )

    actual_cases = set(identities)
    missing = expected_cases - actual_cases
    unexpected = actual_cases - expected_cases
    assert not missing and not unexpected, (
        f'h2spec case inventory differs: missing={sorted(missing)!r}, '
        f'unexpected={sorted(unexpected)!r}\n{output}'
    )
    assert Counter(failures) == Counter(H2SPEC_RFC_9113_DIVERGENCES), output


async def _h2spec_output(executable: str, *arguments: str) -> tuple[int, str]:
    process = await asyncio.create_subprocess_exec(
        executable,
        *arguments,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    return await process.wait(), f'{stdout.decode()}\n{stderr.decode()}'


async def test_h2spec_conformance(tmp_path: Path) -> None:
    executable = shutil.which('h2spec')
    if executable is None:
        pytest.skip('h2spec is optional outside the Nix development shell')

    version_status, version = await _h2spec_output(executable, '--version')
    assert version_status == 0, version
    assert version.strip() == H2SPEC_VERSION

    inventory_status, inventory_output = await _h2spec_output(executable, '--dryrun')
    assert inventory_status == 0, inventory_output
    expected_cases = _expected_h2spec_cases(inventory_output)

    async def app(scope, receive, send):
        # Keep the send window open while h2spec tests updates on an unfinished
        # request; updates racing a finished response may be ignored.
        while True:
            message = await receive()
            if message['type'] == 'http.disconnect':
                return
            if not message.get('more_body', False):
                break
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        # h2spec 2.6.0 skips its negative-window case when the response is under five bytes.
        await send({'type': 'http.response.body', 'body': b'hello'})

    config = Config(
        port=0,
        http1=False,
        h2_max_inbound_frame_size=16_384,
        lifespan='off',
    )
    async with running_server(app, config) as server:
        report = tmp_path / 'h2spec.xml'
        process = await asyncio.create_subprocess_exec(
            executable,
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
            root = ET.parse(report).getroot()  # ruff: ignore[suspicious-xml-element-tree-usage]
        except (FileNotFoundError, ET.ParseError) as error:
            pytest.fail(
                f'h2spec did not write a parseable JUnit report: {error}\n{output}'
            )

        _assert_h2spec_report(root, expected_cases, output)
