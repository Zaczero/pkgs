"""Shared test configuration.

The async test suite runs under every available event-loop policy — the
standard-library asyncio loop always, and uvloop when it is installed (the
`h2corn[uvloop]` extra, pulled in by the dev group). Each async test is
parametrized as ``[asyncio]`` / ``[uvloop]`` so both the in-process test
server and the test client exercise both loops. This is the coverage that
keeps the pump (settable ``_asyncio_future_blocking``, the eventfd
``add_reader`` doorbell, direct ``Task`` construction, the ``_enter_task``
guard) working on whichever loop a deployment picks.
"""

import asyncio
import shutil
import sys
import tempfile
from collections.abc import Callable, Iterator, Mapping
from pathlib import Path

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addini(
        'asyncio_default_fixture_loop_scope',
        help='Default event-loop scope for asynchronous fixtures.',
        default='function',
    )


def pytest_configure(config: pytest.Config) -> None:
    """Keep `ResourceWarning` fatal everywhere it can name a culprit.

    A transport releases its socket in `_call_connection_lost`, which `close()`
    only schedules once nothing is in flight. The selector loop asks `if not
    self._buffer`, and a write already handed to the kernel leaves that buffer
    empty -- so on POSIX the release is always scheduled and an unclosed socket
    means a real leak. The proactor asks `if not self._buffer and
    self._write_fut is None`, and an overlapped write stays pending until its
    completion is polled, so a transport closed in that window schedules
    nothing and `loop.close()` then discards the queues. Windows alone can end
    a test holding a socket nobody forgot to close.

    `__del__` reports that whenever the collector reaches it, so pytest blames
    whichever test was running -- a different innocent one each run. Demoting
    it there keeps it visible without letting garbage-collection timing pick a
    victim, and costs nothing on the platforms where it is a real signal.
    """
    if sys.platform == 'win32':
        config.addinivalue_line('filterwarnings', 'default::ResourceWarning')


@pytest.fixture
def captured_stderr(capfd: pytest.CaptureFixture[str]) -> pytest.CaptureFixture[str]:
    """`capfd`, drained so a test only ever reads its own output.

    pytest's file-descriptor capture is process-wide and continuous: a test that
    never requested the fixture still writes into the same buffer, so a bare
    `readouterr()` can hand back the *previous* test's lines. That turns an
    absence assertion into a coin flip -- and a test that parses every captured
    line into JSON into a crash.

    It bites under capture specifically. The server batches diagnostics when
    stderr is a regular file, which is what fd capture makes it, and graceful
    shutdown flushes that queue with a *bounded* wait. So a line normally lands
    before its own server is gone and this drain removes it -- but on a machine
    busy enough for the flush to time out, one can still arrive mid-test. If an
    absence assertion here ever fails while its subject looks correct, suspect
    the neighbour that logged the same text, not the code under test.
    """
    _ = capfd.readouterr()
    return capfd


@pytest.fixture
def unix_socket_dir() -> Iterator[Path]:
    """A short-lived directory for binding AF_UNIX sockets.

    macOS caps the AF_UNIX `sun_path` at ~104 bytes and pytest's `tmp_path`
    (under `/private/var/folders/...`) overflows it, so bind under a short
    temp root instead.
    """
    socket_dir = Path(tempfile.mkdtemp(prefix='h2c-'))
    try:
        yield socket_dir
    finally:
        shutil.rmtree(socket_dir, ignore_errors=True)


def pytest_asyncio_loop_factories(
    config: pytest.Config, item: pytest.Item
) -> Mapping[str, Callable[[], asyncio.AbstractEventLoop]]:
    """Run async tests under the stdlib loop and uvloop without global policies."""
    del config, item
    factories = {'asyncio': asyncio.new_event_loop}
    try:
        import uvloop
    except ModuleNotFoundError:
        return factories
    return {**factories, 'uvloop': uvloop.new_event_loop}
