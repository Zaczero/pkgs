#!/usr/bin/env python3
"""Exercise admitted wheels in isolated interpreter environments on one runner."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path
from typing import Any


def runtime_environment(runtime: dict[str, Any]) -> dict[str, str]:
    environment = (
        Path(os.environ['RUNNER_TEMP'])
        / 'python-tests'
        / os.environ['PACKAGE']
        / runtime['python-version']
    )
    return {
        **os.environ,
        'UV_PROJECT_ENVIRONMENT': str(environment.resolve()),
        'UV_PYTHON': runtime['python-version'],
        'FREETHREADED': str(runtime['freethreaded']).lower(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('check', choices=['test', 'types', 'audit'])
    args = parser.parse_args()
    batch = json.loads(os.environ['TEST_BATCH'])
    package = os.environ['PACKAGE']
    if args.check != 'test':
        runtime = batch['typecheck'] if args.check == 'types' else batch['primary']
        environment = runtime_environment(runtime)
        if args.check == 'audit':
            command = ['uv', 'audit', '--project', package, '--locked']
        else:
            python = subprocess.check_output(
                [
                    'uv',
                    'run',
                    '--project',
                    package,
                    '--no-sync',
                    'python',
                    '-c',
                    'import sys; print(sys.executable)',
                ],
                env=environment,
                text=True,
            ).strip()
            command = ['uv', 'tool', 'run', 'pyright', '--pythonpath', python]
        return subprocess.run(
            command,
            cwd=package if args.check == 'types' else None,
            env=environment,
            check=False,
        ).returncode

    failed = False
    for runtime in batch['runtimes']:
        version = runtime['python-version']
        print(f'::group::CPython {version}', flush=True)
        environment = runtime_environment(runtime)
        commands = [
            [
                'uv',
                'sync',
                '--project',
                package,
                '--no-default-groups',
                '--group',
                runtime['dependency-group'],
                '--no-install-project',
            ],
            [
                'uv',
                'run',
                '--project',
                package,
                '--no-sync',
                'python',
                '.github/scripts/install_wheel.py',
                package,
                'dist',
            ],
            [
                'uv',
                'run',
                '--project',
                package,
                '--no-sync',
                'python',
                '.github/scripts/test_python.py',
            ],
        ]
        try:
            for command in commands:
                subprocess.run(command, env=environment, check=True)
        except subprocess.CalledProcessError as error:
            print(
                f'::error::CPython {version} failed with exit code {error.returncode}',
                flush=True,
            )
            failed = True
        finally:
            print('::endgroup::', flush=True)
    return int(failed)


if __name__ == '__main__':
    raise SystemExit(main())
