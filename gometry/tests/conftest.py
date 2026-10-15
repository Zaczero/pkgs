from __future__ import annotations

import json
import os
import sys
import sysconfig
from pathlib import Path

import pytest

GOMETRY_ROOT = Path(__file__).resolve().parents[1]
TOOLS = GOMETRY_ROOT / 'tools'
if str(TOOLS) not in sys.path:
    sys.path.insert(0, str(TOOLS))

# Resolve checkout-local tools only after selecting the repository tools path.
from _gatelib import prepend_tools_import_paths  # noqa: E402
from runtime_artifact import require_runtime  # noqa: E402

prepend_tools_import_paths()
_producer_path = os.environ.get('GOMETRY_RUNTIME_RECEIPT')
_producer = (
    json.loads(Path(_producer_path).read_text(encoding='utf-8'))
    if _producer_path is not None
    else None
)
require_runtime(GOMETRY_ROOT, prepare_if_stale=False, build_receipt=_producer)


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
