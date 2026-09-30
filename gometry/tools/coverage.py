"""Combined Rust + Python coverage for the test suite.

Coverage here is a discovery tool, not a target: the value is the list of
kernel branches and facade lines the suite never reaches — candidates for
edge-case tests (or for deletion, when the branch is unreachable by design).
Do not chase the percentage.

The Rust side builds the extension under `cargo llvm-cov`'s environment in its
own `target/llvm-cov-target`, assembles it with `python/gometry` into a scratch
package there, and runs pytest against that package (with `pytest-cov`
measuring the Python facade in the same pass) and then the native Rust suite.
Every `.profraw` file aggregates into one report. The `.venv` install is never
touched, so no rebuild is needed afterwards.

Usage::

    .venv/bin/python tools/coverage.py            # text summaries
    .venv/bin/python tools/coverage.py --html     # + HTML for both sides
"""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import sys
import sysconfig
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PYTHON = ROOT / '.venv' / 'bin' / 'python'


def run(args: list[str], **kwargs) -> subprocess.CompletedProcess:
    print(f'$ {" ".join(args)}', flush=True)
    return subprocess.run(args, check=True, cwd=ROOT, **kwargs)


def main() -> int:
    html = '--html' in sys.argv[1:]
    # cargo-llvm-cov otherwise inherits the parent workspace target directory
    # and `clean --workspace` can delete sibling-package evidence. Keep the
    # whole instrumented build and every profraw file inside gometry.
    coverage_target = (ROOT / 'target' / 'llvm-cov-target').resolve()
    os.environ['CARGO_TARGET_DIR'] = str(coverage_target)
    os.environ['PYO3_PYTHON'] = str(PYTHON)
    # `show-env` asks on stdin before installing llvm-tools-preview for a new
    # toolchain; without a closed stdin it waits forever.
    exports = subprocess.run(
        ['cargo', 'llvm-cov', 'show-env', '--sh'],
        check=True,
        cwd=ROOT,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
    ).stdout
    for line in exports.splitlines():
        line = line.removeprefix('export ').strip()
        if not line:
            continue
        key, _, value = line.partition('=')
        words = shlex.split(value)
        os.environ[key] = words[0] if words else ''
    actual_target = Path(os.environ['CARGO_LLVM_COV_TARGET_DIR']).resolve()
    if actual_target != coverage_target:
        raise RuntimeError(
            f'cargo-llvm-cov escaped the package target: {actual_target}'
        )
    run(['cargo', 'llvm-cov', 'clean', '--workspace'])
    run(['cargo', 'build', '--offline', '-p', 'gometry', '--lib'])
    package = coverage_target / 'pkg'
    shutil.rmtree(package, ignore_errors=True)
    shutil.copytree(ROOT / 'python' / 'gometry', package / 'gometry')
    shutil.copy2(
        coverage_target / 'debug' / 'libgometry.so',
        package / 'gometry' / f'_lib{sysconfig.get_config_var("EXT_SUFFIX")}',
    )
    run(
        [
            str(PYTHON),
            '-m',
            'pytest',
            'tests',
            '--cov=gometry',
            '--cov-report=term-missing',
            *(['--cov-report=html:coverage-python'] if html else []),
        ],
        env={**os.environ, 'PYTHONPATH': str(package)},
    )
    # `show-env` already instruments every Cargo command. Use nextest directly;
    # cargo-llvm-cov warns that its test subcommands are not the right driver
    # inside a show-env environment intended for external tests. The embedded
    # interpreter needs the environment's site-packages for NumPy.
    site_packages = subprocess.run(
        [str(PYTHON), '-c', 'import site; print(site.getsitepackages()[0])'],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    run(
        ['cargo', 'nextest', 'run', '--offline', '-p', 'gometry', '--lib'],
        env={**os.environ, 'PYTHONPATH': site_packages},
    )
    run(['cargo', 'llvm-cov', 'report'])
    if html:
        run(['cargo', 'llvm-cov', 'report', '--html'])
        print(
            'HTML: coverage-python/index.html and target/llvm-cov-target/html/index.html'
        )
    return 0


if __name__ == '__main__':
    sys.exit(main())
