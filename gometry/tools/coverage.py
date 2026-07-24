"""Combined Rust + Python coverage for the test suite.

Coverage here is a discovery tool, not a target: the value is the list of
kernel branches and facade lines the suite never reaches — candidates for
edge-case tests (or for deletion, when the branch is unreachable by design).
Do not chase the percentage.

The Rust side instruments the extension with `cargo llvm-cov`'s environment
(separate `target/llvm-cov-target` dir, so the regular release build stays
untouched), installs the instrumented module with maturin, runs pytest (with
`pytest-cov` measuring `python/gometry` in the same pass), runs the native Rust
suite, and aggregates every `.profraw` file into one report.

Usage::

    .venv/bin/python tools/coverage.py            # text summaries
    .venv/bin/python tools/coverage.py --html     # + HTML for both sides

The installed extension is left INSTRUMENTED afterwards — rerun
`uv run --no-project --python .venv/bin/python --with maturin==1.14.1 maturin
develop --release` before benchmarking or shipping.
"""

from __future__ import annotations

import os
import shlex
import subprocess
import sys
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
    exports = subprocess.run(
        ['cargo', 'llvm-cov', 'show-env', '--sh'],
        check=True,
        cwd=ROOT,
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
    run([
        'uv',
        'run',
        '--no-project',
        f'--python={PYTHON}',
        '--with',
        'maturin==1.14.1',
        'maturin',
        'develop',
    ])
    run([
        str(PYTHON),
        '-m',
        'pytest',
        '-q',
        '--no-header',
        '--cov',
        '--cov-report=term-missing',
        *(['--cov-report=html:coverage-python'] if html else []),
    ])
    # `show-env` already instruments every Cargo command. Use nextest directly;
    # cargo-llvm-cov warns that its test subcommands are not the right driver
    # inside a show-env environment intended for external tests.
    run(['cargo', 'nextest', 'run', '-p', 'gometry'])
    run(['cargo', 'llvm-cov', 'report'])
    if html:
        run(['cargo', 'llvm-cov', 'report', '--html'])
        print(
            'HTML: coverage-python/index.html and target/llvm-cov-target/html/index.html'
        )
    print(
        '\nNOTE: the installed extension is instrumented (debug); rebuild with maturin develop --release before benchmarking.'
    )
    return 0


if __name__ == '__main__':
    sys.exit(main())
