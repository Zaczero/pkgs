"""h2corn lifecycle and workload policy for the monorepo PGO builder."""

from __future__ import annotations

import subprocess
import sys
import sysconfig
from pathlib import Path

from bench.pgo.build import generator_overlay

GENERATE_FLAGS = ('--cfg=h2corn_pgo_collect',)
PACKAGE = Path(__file__).resolve().parents[2]


def stage_generator():
    return generator_overlay(PACKAGE)


def verify_runtime(*, generator: bool) -> None:
    from h2corn import _lib

    if sys.version_info[:2] != (3, 15) or sysconfig.get_config_var('Py_GIL_DISABLED'):
        raise RuntimeError('h2corn PGO currently qualifies GIL-build CPython 3.15')
    if not getattr(sys, '_is_gil_enabled', lambda: False)():
        raise RuntimeError('h2corn PGO requires the runtime GIL')
    if (
        hasattr(_lib, '_pgo_begin') != generator
        or hasattr(_lib, '_pgo_finish') != generator
    ):
        raise RuntimeError('wheel has the wrong collection interface')
    if Path(_lib.__file__).with_name('_pgo_collect.py').exists() != generator:
        raise RuntimeError('wheel has the wrong supervisor overlay')


def collect(root: Path) -> Path:
    from bench.pgo.corpus import collect

    return collect(root)


def known_profile_diagnostic(line: str) -> bool:
    return (
        'function control flow change detected (hash mismatch)' in line
        and 'init_lib_module10ModuleExec' in line
        and 'up to 0 count discarded' in line
    )


def test(output: Path) -> None:
    subprocess.run(
        [sys.executable, str(PACKAGE / 'examples/embedded.py')], check=True, timeout=120
    )
    command = [
        sys.executable,
        '-m',
        'pytest',
        str(PACKAGE / 'tests'),
        '--ignore=' + str(PACKAGE / 'tests/test_public_docs.py'),
        '--ignore=' + str(PACKAGE / 'tests/test_stubs.py'),
        '--ignore=' + str(PACKAGE / 'tests/test_benchmark.py'),
        '--ignore=' + str(PACKAGE / 'tests/test_benchmark_sampling.py'),
        '--junitxml=' + str(output / 'pytest.xml'),
    ]
    with (output / 'pytest.log').open('w') as log:
        subprocess.run(
            command,
            cwd=PACKAGE,
            check=True,
            timeout=600,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
