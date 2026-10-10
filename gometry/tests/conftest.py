from __future__ import annotations

import sys
import sysconfig
from pathlib import Path

GOMETRY_ROOT = Path(__file__).resolve().parents[1]
TOOLS = GOMETRY_ROOT / 'tools'
if str(TOOLS) not in sys.path:
    sys.path.insert(0, str(TOOLS))

# Resolve checkout-local tools only after selecting the repository tools path.
from _gatelib import prepend_tools_import_paths  # noqa: E402
from runtime_artifact import require_runtime  # noqa: E402

prepend_tools_import_paths()
require_runtime(GOMETRY_ROOT, prepare_if_stale=False)
if (
    sysconfig.get_config_var('Py_GIL_DISABLED')
    and hasattr(sys, '_is_gil_enabled')
    and sys._is_gil_enabled()
):
    raise SystemExit('gometry tests require the free-threaded interpreter GIL off')

# The typing directory is consumed as static input by the typing gate.
collect_ignore = ['typing']
