#!/usr/bin/env python3
"""Build and run gometry's focused public-path ASan diagnostics.

The sanitizer belongs on the gometry library target, not in global RUSTFLAGS:
procedural-macro and other host crates cannot be loaded from a sanitizer ABI.
"""

from __future__ import annotations

import argparse
import os
import platform
import shutil
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

ROOT = Path(__file__).resolve().parent.parent
TESTS = (
    'tests/test_unsafe_boundaries.py',
    'tests/test_free_threading_soundness.py',
)


def _run(command: list[str], environment: dict[str, str]) -> None:
    print(f'$ {" ".join(command)}', flush=True)
    subprocess.run(command, cwd=ROOT, env=environment, check=True)


def _append_flags(environment: dict[str, str], name: str, *flags: str) -> None:
    environment[name] = ' '.join((environment.get(name, ''), *flags)).strip()


def _asan_runtime() -> Path:
    if sys.platform != 'linux' or platform.machine() != 'x86_64':
        raise SystemExit('ASan diagnostic supports Linux x86_64 only')
    clang = shutil.which('clang')
    if clang is None:
        raise SystemExit('ASan requires clang on PATH')
    result = subprocess.run(
        [clang, '-print-file-name=libclang_rt.asan-x86_64.so'],
        check=True,
        capture_output=True,
        text=True,
    )
    runtime = Path(result.stdout.strip())
    if not runtime.is_file():
        raise SystemExit(f'clang did not provide an ASan runtime: {runtime}')
    return runtime


def _extension_path(python: Path) -> Path:
    suffix = subprocess.run(
        [
            python,
            '-c',
            'import sysconfig; print(sysconfig.get_config_var("EXT_SUFFIX"))',
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return ROOT / 'python' / 'gometry' / f'_lib{suffix}'


@contextmanager
def _preserve_extension(path: Path) -> Iterator[None]:
    existed = path.is_symlink() or path.exists()
    if existed and not path.is_symlink() and not path.is_file():
        raise SystemExit(f'extension path is not a file: {path}')
    with tempfile.TemporaryDirectory(
        prefix='.gometry-asan-backup-', dir=path.parent
    ) as directory:
        backup = Path(directory) / path.name
        if existed:
            path.replace(backup)
        try:
            yield
        finally:
            if path.is_symlink() or path.exists():
                path.unlink()
            if existed:
                backup.replace(path)


ARCHIVES = (
    ('PROJ', 'proj-sys', 'libproj.a'),
    ('libtiff', 'gometry-libtiff-sys', 'libtiff.a'),
    ('SQLite', 'libsqlite3-sys', 'libsqlite3.a'),
    ('zlib', 'libz-sys', 'libz.a'),
)


def _verify_bundled_archives(target_dir: Path) -> None:
    nm = shutil.which('nm')
    if nm is None:
        raise SystemExit('bundled archive evidence requires nm on PATH')
    build_root = target_dir / 'build' / 'debug' / 'build'
    for name, package, filename in ARCHIVES:
        candidates = sorted((build_root / package).glob(f'*/out/**/{filename}'))
        if not candidates:
            raise SystemExit(f'could not locate bundled {name} archive {filename}')
        for archive in candidates:
            result = subprocess.run(
                [nm, '-u', str(archive)],
                check=False,
                capture_output=True,
                text=True,
            )
            if result.returncode:
                raise SystemExit(
                    f'nm could not inspect bundled {name} archive: {archive}'
                )
            if '__asan_' not in result.stdout:
                raise SystemExit(
                    f'bundled {name} archive lacks ASan references: {archive}'
                )
            print(f'ASan-instrumented bundled archive: {archive}', flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Build and run gometry public-path tests under AddressSanitizer.'
    )
    parser.add_argument(
        '--python',
        dest='python_path',
        required=True,
        type=Path,
        help='Python interpreter with NumPy, PyArrow, pytest, and the required plugins',
    )
    parser.add_argument(
        '--target-dir',
        type=Path,
        default=ROOT / 'target-sanitizer-address',
        help='isolated Cargo target directory (default: target-sanitizer-address)',
    )
    args = parser.parse_args()

    # Keep the venv symlink: resolving it loses the selected environment's
    # sibling maturin and site-packages in uv-managed environments.
    python = args.python_path.absolute()
    if not python.is_file():
        raise SystemExit(f'Python interpreter does not exist: {python}')
    target_dir = args.target_dir.resolve()
    runtime = _asan_runtime()

    probe = subprocess.run(
        [
            str(python),
            '-c',
            'import sys; print(getattr(sys, "_is_gil_enabled", lambda: "unknown")())',
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    print(f'Python: {python} (GIL enabled: {probe.stdout.strip()})', flush=True)

    environment = os.environ.copy()
    environment.update({
        'CC': 'clang',
        'CXX': 'clang++',
        'CARGO_TARGET_DIR': str(target_dir),
        'CARGO_BUILD_BUILD_DIR': str(target_dir / 'build'),
        'VIRTUAL_ENV': str(python.parent.parent),
        'PYTHONPATH': os.pathsep.join(
            entry
            for entry in (str(ROOT / 'python'), environment.get('PYTHONPATH', ''))
            if entry
        ),
        # Match the repository's x86_64 target configuration while keeping
        # the sanitizer off host/procedural-macro crates.
        'RUSTFLAGS': '-C target-cpu=x86-64-v2',
        'LD_PRELOAD': os.pathsep.join(
            entry
            for entry in (str(runtime), environment.get('LD_PRELOAD', ''))
            if entry
        ),
    })
    environment['PATH'] = os.pathsep.join((
        str(python.parent),
        environment.get('PATH', ''),
    ))
    _append_flags(
        environment, 'CFLAGS', '-fsanitize=address', '-fno-omit-frame-pointer'
    )
    _append_flags(
        environment, 'CXXFLAGS', '-fsanitize=address', '-fno-omit-frame-pointer'
    )
    _append_flags(environment, 'LDFLAGS', '-fsanitize=address')
    environment.setdefault(
        'ASAN_OPTIONS',
        'detect_leaks=0:halt_on_error=1:abort_on_error=1:detect_stack_use_after_return=1',
    )

    maturin = python.parent / 'maturin'
    if not maturin.is_file():
        found = shutil.which('maturin', path=environment['PATH'])
        if found is None:
            raise SystemExit('maturin is not installed in the selected environment')
        maturin = Path(found)
    extension = _extension_path(python)
    with _preserve_extension(extension):
        _run(
            [
                str(python),
                '-c',
                'import numpy, pyarrow, pytest_timeout, xdist',
            ],
            environment,
        )
        build_environment = environment.copy()
        # The runtime is needed by the developed extension, not by Cargo's host
        # compiler and procedural-macro processes.
        build_environment.pop('LD_PRELOAD', None)
        _run(
            [
                str(maturin),
                'develop',
                '--manifest-path',
                'Cargo.toml',
                '--target-dir',
                str(target_dir),
                '--skip-install',
                '--',
                '-Z',
                'sanitizer=address',
                '-C',
                'link-arg=-fsanitize=address',
            ],
            build_environment,
        )

        readelf = shutil.which('readelf')
        if readelf is None:
            raise SystemExit('ASan build evidence requires readelf on PATH')
        if not extension.is_file():
            raise SystemExit(f'ASan build did not produce the extension: {extension}')
        symbols = subprocess.run(
            [readelf, '-Ws', str(extension)],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        if '__asan_' not in symbols:
            raise SystemExit(f'extension has no ASan symbols: {extension}')
        print(f'ASan-instrumented extension: {extension}', flush=True)
        _verify_bundled_archives(target_dir)

        _run(
            [str(python), '-m', 'pytest', '-q', '--no-header', *TESTS],
            environment,
        )
    return 0


if __name__ == '__main__':
    sys.exit(main())
