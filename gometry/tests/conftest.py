from __future__ import annotations

import sys
import sysconfig
from pathlib import Path

import pytest

GOMETRY_ROOT = Path(__file__).resolve().parents[1]
TOOLS = GOMETRY_ROOT / 'tools'
if str(TOOLS) not in sys.path:
    sys.path.insert(0, str(TOOLS))

# ruff: ignore[module-import-not-at-top-of-file] - Checkout tools follow path selection.
from _gatelib import (
    prepend_tools_import_paths,
    require_runtime,
)

prepend_tools_import_paths()
require_runtime(GOMETRY_ROOT)


def _free_threaded_gil_enabled() -> bool:
    return bool(
        sysconfig.get_config_var('Py_GIL_DISABLED')
        and hasattr(sys, '_is_gil_enabled')
        and sys._is_gil_enabled()
    )


if _free_threaded_gil_enabled():
    raise SystemExit('gometry tests require the free-threaded interpreter GIL off')


# Imports can re-enable the GIL after the initial test-process check.
@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    if _free_threaded_gil_enabled():
        print(
            'Gometry tests enabled the GIL in the free-threaded interpreter',
            file=sys.stderr,
        )
        if exitstatus == pytest.ExitCode.OK:
            session.exitstatus = pytest.ExitCode.TESTS_FAILED


# The typing directory is consumed as static input by the typing gate.
collect_ignore = ['typing']
