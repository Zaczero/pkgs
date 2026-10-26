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
import importlib.util
import os
import shutil
import subprocess
import sys
import sysconfig
import tempfile
from collections.abc import Callable, Iterator, Mapping
from pathlib import Path

import pytest

from tests._support import gil_is_disabled

_FREE_THREADED_BUILD = bool(sysconfig.get_config_var('Py_GIL_DISABLED'))

if _FREE_THREADED_BUILD and not gil_is_disabled():
    raise SystemExit('h2corn tests require the free-threaded interpreter GIL off')


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addini(
        'asyncio_default_fixture_loop_scope',
        help='Default event-loop scope for asynchronous fixtures.',
        default='function',
    )


@pytest.hookimpl(tryfirst=True)
def pytest_sessionstart(session: pytest.Session) -> None:
    if (
        not hasattr(session.config, 'workerinput')
        and not _FREE_THREADED_BUILD
        and importlib.util.find_spec('connectrpc') is not None
    ):
        subprocess.run(
            ['buf', 'generate'],
            cwd=Path(__file__).resolve().parents[1] / 'examples/grpc',
            env={
                key: value for key, value in os.environ.items() if key != 'LD_PRELOAD'
            },
            check=True,
        )


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


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    if _FREE_THREADED_BUILD and not gil_is_disabled():
        print(
            'h2corn tests enabled the GIL in the free-threaded interpreter',
            file=sys.stderr,
        )
        if exitstatus == pytest.ExitCode.OK:
            session.exitstatus = pytest.ExitCode.TESTS_FAILED
