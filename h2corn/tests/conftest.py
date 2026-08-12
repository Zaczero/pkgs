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
    socket_dir = Path(tempfile.mkdtemp(prefix='h2c-', dir='/tmp'))
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
    is_gil_enabled = getattr(sys, '_is_gil_enabled', None)
    if sys.version_info >= (3, 14) or (
        callable(is_gil_enabled) and not is_gil_enabled()
    ):
        # uvloop's CPython 3.14 compatibility layer still emits deprecations
        # under -Werror. The native asyncio loop remains the supported coverage
        # for the newest interpreter while upstream catches up.
        return factories
    try:
        import uvloop
    except ModuleNotFoundError:
        return factories
    return {**factories, 'uvloop': uvloop.new_event_loop}
