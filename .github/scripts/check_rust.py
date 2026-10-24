#!/usr/bin/env python3
"""Run the selected crates' analysis without unifying their feature sets."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


def main() -> None:
    mode = sys.argv[1]
    packages = json.loads(os.environ['RUST_CHECKS'])
    metadata = json.loads(
        subprocess.check_output(
            ['cargo', 'metadata', '--locked', '--no-deps', '--format-version', '1'],
            text=True,
        )
    )
    doctests = {
        package['name']
        for package in metadata['packages']
        if any(target['doctest'] for target in package['targets'])
    }
    failures = []
    for package in packages:
        path, crate = package['package'], package['crate']
        print(f'::group::{mode}: {crate}', flush=True)
        try:
            hook = Path(path) / 'tools/ci_before_build.sh'
            if hook.is_file():
                subprocess.run(['bash', str(hook)], check=True)
            if mode == 'clippy':
                subprocess.run(
                    ['cargo', 'clippy', '--locked', '-p', crate, '--all-targets'],
                    check=True,
                )
                if crate in doctests:
                    hook = Path(path) / 'tools/ci_before_test.sh'
                    if hook.is_file():
                        subprocess.run(['bash', str(hook)], check=True)
                    subprocess.run(
                        ['cargo', 'test', '--locked', '-p', crate, '--doc'],
                        check=True,
                    )
            elif mode == 'kani':
                subprocess.run(
                    [
                        'cargo',
                        'kani',
                        '-p',
                        crate,
                        '--lib',
                        '--bins',
                        '-j',
                        '-Z',
                        'unstable-options',
                        '--harness-timeout',
                        '180',
                        '--output-format',
                        'terse',
                    ],
                    check=True,
                )
            else:
                raise ValueError(f'unknown Rust check: {mode}')
        except subprocess.CalledProcessError:
            failures.append(crate)
            print(f'::error::{mode} failed for {crate}', flush=True)
        finally:
            print('::endgroup::', flush=True)
    if failures:
        raise SystemExit(f'{mode} failed: {", ".join(failures)}')


if __name__ == '__main__':
    main()
