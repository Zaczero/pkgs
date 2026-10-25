#!/usr/bin/env python3
"""Run the installed package's Python suite in the selected interpreter."""

import importlib
import os
import subprocess
import sys
import sysconfig

importlib.import_module(os.environ['IMPORT_NAME'])
if sysconfig.get_config_var('Py_GIL_DISABLED'):
    state = 'enabled' if sys._is_gil_enabled() else 'disabled'
    print(f'CPython free-threaded runtime GIL after import: {state}', flush=True)
    if os.environ['PACKAGE_TYPE'] == 'python-rust':
        assert not sys._is_gil_enabled()

arguments = [
    '--timeout=0',
    '-o',
    'faulthandler_timeout=0',
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
