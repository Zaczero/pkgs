"""Collect an instrumented Python extension from a normally exiting process."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import sysconfig
from contextlib import nullcontext
from pathlib import Path

stage_generator = nullcontext


def verify_runtime(*, generator: bool) -> None:
    if sys.implementation.name != 'cpython' or sysconfig.get_config_var(
        'Py_GIL_DISABLED'
    ):
        raise RuntimeError('PGO requires a GIL-build CPython interpreter')
    if not getattr(sys, '_is_gil_enabled', lambda: True)():
        raise RuntimeError('PGO requires the runtime GIL')


def known_profile_diagnostic(line: str) -> bool:
    return False


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def collect(
    output: Path, script: Path, image: Path, *, units: int, functions: list[str]
) -> Path:
    output.mkdir(parents=True)
    leaf = output / 'workload'
    leaf.mkdir()
    plan = {
        'script_sha256': digest(script),
        'collector_sha256': digest(Path(__file__)),
        'leaves': [
            {
                'name': 'workload',
                'weight': '100',
                'units': units,
                'expected_functions': functions,
            }
        ],
    }
    plan_bytes = (json.dumps(plan, indent=2) + '\n').encode()
    (output / 'plan.json').write_bytes(plan_bytes)
    raw = leaf / 'worker.profraw'
    log_path = leaf / 'training.log'
    with log_path.open('w') as log:
        try:
            result = subprocess.run(
                [sys.executable, str(script), 'train'],
                cwd=leaf,
                env={
                    **os.environ,
                    'LLVM_PROFILE_FILE': str(raw),
                    'PYTHONPATH': os.pathsep.join(
                        filter(
                            None,
                            (str(Path(__file__).parent), os.environ.get('PYTHONPATH')),
                        )
                    ),
                },
                stdout=subprocess.PIPE,
                stderr=log,
                text=True,
                check=True,
                timeout=120,
            )
        except subprocess.CalledProcessError as error:
            raise RuntimeError(f'training process failed; see {log_path}') from error
    if (
        digest(script) != plan['script_sha256']
        or digest(Path(__file__)) != plan['collector_sha256']
    ):
        raise RuntimeError('training source changed during collection')
    image_sha256 = digest(image)
    if json.loads(result.stdout) != {'units': units, 'image_sha256': image_sha256}:
        raise RuntimeError('training used the wrong image or completed work')
    receipt = output / 'collection.json'
    receipt.write_text(
        json.dumps(
            {
                'plan_sha256': hashlib.sha256(plan_bytes).hexdigest(),
                'leaves': [
                    {
                        **plan['leaves'][0],
                        'image_sha256': image_sha256,
                        'raw_files': [{'path': str(raw), 'sha256': digest(raw)}],
                    }
                ],
            },
            indent=2,
        )
        + '\n'
    )
    return receipt


def completed(units: int, image: Path) -> None:
    print(json.dumps({'units': units, 'image_sha256': digest(image)}))


def run_tests(output: Path, package: Path, *, ignore: tuple[str, ...] = ()) -> None:
    command = [
        sys.executable,
        '-m',
        'pytest',
        str(package / 'tests'),
        *('--ignore=' + str(package / 'tests' / name) for name in ignore),
        '--junitxml=' + str(output / 'pytest.xml'),
    ]
    with (output / 'pytest.log').open('w') as log:
        subprocess.run(
            command,
            cwd=package,
            check=True,
            timeout=600,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
