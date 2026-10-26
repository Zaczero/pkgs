#!/usr/bin/env python3
"""Run the installed package's Python suite in the selected interpreter."""

import importlib
import json
import os
import platform
import subprocess
import sys
import sysconfig
from pathlib import Path

free_threaded = bool(sysconfig.get_config_var('Py_GIL_DISABLED'))
runtime = f'python-{sys.version_info.major}.{sys.version_info.minor}'
if free_threaded:
    runtime += 't'
results = Path(os.environ['GITHUB_WORKSPACE']) / 'test-results' / runtime
temporary = Path(os.environ['RUNNER_TEMP']) / 'pytest' / runtime
results.mkdir(parents=True, exist_ok=True)
temporary.parent.mkdir(parents=True, exist_ok=True)
identity = {
    'python': sys.version,
    'executable': sys.executable,
    'platform': platform.platform(),
    'package': os.environ['PACKAGE'],
    'revision': os.environ['GITHUB_SHA'],
    'free_threaded': free_threaded,
}
(results / 'runtime.json').write_text(json.dumps(identity, indent=2), encoding='utf-8')
print(json.dumps(identity), flush=True)

importlib.import_module(os.environ['IMPORT_NAME'])
if free_threaded:
    state = 'enabled' if sys._is_gil_enabled() else 'disabled'
    print(f'CPython free-threaded runtime GIL after import: {state}', flush=True)
    if os.environ['PACKAGE_TYPE'] == 'python-rust':
        assert not sys._is_gil_enabled()

arguments = [
    '--verbose',
    '--showlocals',
    f'--junitxml={results / "junit.xml"}',
    f'--basetemp={temporary}',
    '-o',
    f'junit_suite_name={runtime}',
    '-o',
    'tmp_path_retention_policy=failed',
    '-o',
    'junit_logging=all',
    '-o',
    'junit_log_passing_tests=false',
    '--timeout=0',
    '-o',
    'faulthandler_exit_on_timeout=false',
    'tests',
    '--ignore=tests/tooling',
    '--ignore=tests/test_public_docs.py',
]
if os.environ.get('FREETHREADED') == 'true':
    arguments.extend(['-n', '2'])

raise SystemExit(
    subprocess.run(
        [sys.executable, '-m', 'pytest', *arguments],
        cwd=os.environ['PACKAGE'],
        check=False,
    ).returncode
)
