#!/usr/bin/env python3
"""Produce and admit an identified public-path AddressSanitizer runtime."""

from __future__ import annotations

import argparse
import json
import os
import platform
import shlex
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

from _artifact_common import (
    MATURIN_VERSION,
    _atomic_json,
    _package_version,
    _prepared_output_hashes,
    _rust_target_triple,
    _sha256_bytes,
    _tool_output,
    sha256_file,
)
from artifact_content import inspect_wheel, python_member_hashes, source_input_digests
from quality_runtime import quality_settings
from source_artifact import (
    cargo_metadata,
    prepare_license_outputs,
    required_source_records,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

ROOT = Path(__file__).resolve().parent.parent
TESTS = (
    'tests/test_unsafe_boundaries.py',
    'tests/test_free_threading_soundness.py',
)
ARCHIVES = (
    ('PROJ', 'proj-sys', 'libproj.a'),
    ('libtiff', 'gometry-libtiff-sys', 'libtiff.a'),
    ('SQLite', 'libsqlite3-sys', 'libsqlite3.a'),
    ('zlib', 'libz-sys', 'libz.a'),
)
RUST_INSTRUMENTATION = ('-Z', 'sanitizer=address', '-C', 'link-arg=-fsanitize=address')


def _run(command: list[str], environment: dict[str, str]) -> None:
    print(f'$ {" ".join(command)}', file=sys.stderr, flush=True)
    subprocess.run(command, cwd=ROOT, env=environment, check=True)


def _append_flags(environment: dict[str, str], name: str, *flags: str) -> None:
    environment[name] = ' '.join((environment.get(name, ''), *flags)).strip()


def _asan_runtime() -> Path:
    if sys.platform != 'linux' or platform.machine() != 'x86_64':
        raise SystemExit('ASan diagnostic supports Linux x86_64 only')
    result = _tool_output(
        ['clang', '-print-file-name=libclang_rt.asan-x86_64.so'], cwd=ROOT
    )
    runtime = Path(result.strip()).resolve()
    if not runtime.is_file():
        raise SystemExit(f'clang did not provide an ASan runtime: {runtime}')
    return runtime


def _build_environment(python: Path, target_dir: Path) -> dict[str, str]:
    environment = os.environ.copy()
    for name, value in environment.items():
        if (
            name in {'RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS'}
            or (name.startswith('CARGO_TARGET_') and name.endswith('_RUSTFLAGS'))
        ) and 'sanitizer' in value:
            raise AssertionError(
                f'{name} already selects sanitizer instrumentation; '
                'ASan flags belong only on the gometry library invocation'
            )
        if name.startswith((
            'CFLAGS',
            'CXXFLAGS',
            'LDFLAGS',
            'TARGET_CFLAGS',
            'TARGET_CXXFLAGS',
        )):
            for flag in shlex.split(value):
                if flag.startswith('-fsanitize=') and {
                    'thread',
                    'memory',
                    'hwaddress',
                }.intersection(flag.removeprefix('-fsanitize=').split(',')):
                    raise AssertionError(
                        f'{name} selects instrumentation incompatible with ASan'
                    )
    environment.pop('GOMETRY_RUNTIME_RECEIPT', None)
    environment.pop('LD_PRELOAD', None)
    environment.update({
        'CC': 'clang',
        'CXX': 'clang++',
        'CARGO_TARGET_DIR': str(target_dir),
        'CARGO_BUILD_BUILD_DIR': str(target_dir / 'build'),
        'PYO3_PYTHON': str(python.resolve()),
        'NIX_DONT_SET_RPATH': '1',
    })
    target = _rust_target_triple(ROOT, environment=environment)
    if target != 'x86_64-unknown-linux-gnu':
        raise AssertionError(f'ASan diagnostic does not support target {target!r}')
    environment['CARGO_BUILD_TARGET'] = target
    environment[f'NIX_DONT_SET_RPATH_{target.replace("-", "_")}'] = '1'
    for name in ('CFLAGS', 'CXXFLAGS'):
        _append_flags(
            environment, name, '-fsanitize=address', '-fno-omit-frame-pointer'
        )
    _append_flags(environment, 'LDFLAGS', '-fsanitize=address')
    return environment


def _verify_bundled_archives(target_dir: Path) -> list[dict[str, str]]:
    records = []
    build_root = target_dir / 'build'
    for name, package, filename in ARCHIVES:
        candidates = sorted(
            path
            for path in build_root.rglob(filename)
            if package in path.parts
            or any(part.startswith(f'{package}-') for part in path.parts)
        )
        if not candidates:
            raise AssertionError(f'could not locate bundled {name} archive {filename}')
        for archive in candidates:
            symbols = _tool_output(['nm', '-u', str(archive)], cwd=ROOT)
            if '__asan_' not in symbols:
                raise AssertionError(
                    f'bundled {name} archive lacks ASan references: {archive}'
                )
            records.append({
                'name': name,
                'path': str(archive.resolve()),
                'sha256': sha256_file(archive),
            })
            print(f'ASan-instrumented bundled archive: {archive}', file=sys.stderr)
    return records


def verify_asan_image(extension: Path) -> None:
    """Require sanitizer references in the exact extension admitted for import."""
    environment = os.environ.copy()
    environment.pop('LD_PRELOAD', None)
    symbols = _tool_output(
        ['readelf', '-Ws', str(extension)], cwd=ROOT, env=environment
    )
    if '__asan_init' not in symbols or '__asan_report_' not in symbols:
        raise AssertionError(f'extension has no ASan instrumentation: {extension}')


def validate_sanitizer_wheel_tags(
    tags: Sequence[str], interpreter: Mapping[str, Any]
) -> None:
    """Admit only this interpreter's native Linux diagnostic wheel tag."""
    cache_tag = interpreter['cache_tag']
    if interpreter['implementation'] != 'CPython' or not cache_tag.startswith(
        'cpython-'
    ):
        raise AssertionError('ASan diagnostic requires a CPython wheel')
    python_tag = 'cp' + cache_tag.removeprefix('cpython-')
    abi_tag = python_tag + ('t' if interpreter['gil_disabled'] else '')
    if list(tags) != [f'{python_tag}-{abi_tag}-linux_x86_64']:
        raise AssertionError(
            'ASan wheel tags differ from the selected native interpreter'
        )


def validate_sanitizer_producer_inputs(root: Path, receipt: Mapping[str, Any]) -> None:
    """Bind a sanitizer producer to the live checkout and retained native inputs."""
    if receipt.get('kind') != 'gometry-sanitizer-runtime' or receipt.get('schema') != 1:
        raise AssertionError('ASan requires a sanitizer producer receipt')
    if receipt.get('profile') != 'dev' or receipt['build'].get('profile') != 'dev':
        raise AssertionError('ASan requires the diagnostic dev profile')
    source = receipt['source']
    for name, digest in source_input_digests(root).items():
        if source.get(name) != digest:
            raise AssertionError(f'ASan producer {name} differs from the checkout')
    if source.get('version') != _package_version(root):
        raise AssertionError('ASan producer version differs from the checkout')
    if receipt['python_members'] != python_member_hashes(root / 'python' / 'gometry'):
        raise AssertionError('ASan producer Python members differ from the checkout')
    if receipt['prepared_inputs'] != _prepared_output_hashes(root):
        raise AssertionError('ASan producer prepared inputs differ from the checkout')
    build = receipt['build']
    settings = build['settings']
    recorded_environment = {
        name: value
        for name, value in settings['environment'].items()
        if value is not None
    }
    if quality_settings(root, recorded_environment) != settings:
        raise AssertionError('ASan producer build inputs changed')
    if build['command'][-len(RUST_INSTRUMENTATION) :] != list(RUST_INSTRUMENTATION):
        raise AssertionError('ASan producer did not select library instrumentation')
    instrumentation = receipt['instrumentation']
    if instrumentation.get('sanitizer') != 'address':
        raise AssertionError('ASan producer has a different instrumentation variant')
    records = instrumentation['archives']
    if {record['name'] for record in records} != {name for name, _, _ in ARCHIVES}:
        raise AssertionError('ASan producer lacks bundled archive evidence')
    for record in [instrumentation['runtime'], *records]:
        if sha256_file(Path(record['path'])) != record['sha256']:
            raise AssertionError('ASan producer native instrumentation input changed')


def verify_asan_runtime(receipt: Mapping[str, Any]) -> None:
    """Require the recorded ASan runtime to be mapped in the consuming process."""
    runtime = Path(receipt['instrumentation']['runtime']['path']).resolve()
    mapped = {
        Path(line.rsplit(None, 1)[-1]).resolve()
        for line in Path('/proc/self/maps').read_text(encoding='utf-8').splitlines()
        if line.rsplit(None, 1)[-1].startswith('/')
    }
    if runtime not in mapped:
        raise AssertionError('the sanitizer producer ASan runtime is not loaded')


def prepare_sanitizer_runtime(
    python: Path, target_dir: Path, runtime: Path
) -> dict[str, Any]:
    """Build an explicit wheel without changing an installed or checkout extension."""
    target_dir.mkdir(parents=True, exist_ok=True)
    environment = _build_environment(python, target_dir)
    maturin = python.parent / 'maturin'
    if not maturin.is_file():
        found = shutil.which('maturin')
        if found is None:
            raise AssertionError('maturin is not installed')
        maturin = Path(found)
    version = _tool_output([str(maturin), '--version'], cwd=ROOT)
    if version.strip() != f'maturin {MATURIN_VERSION}':
        raise AssertionError(f'expected maturin {MATURIN_VERSION}, found {version!r}')
    _run(
        [str(python), '-c', 'import numpy, pyarrow, pytest_timeout, xdist'],
        environment,
    )
    lock_before = sha256_file(ROOT.parent / 'Cargo.lock')
    prepare_license_outputs(ROOT)
    prepared = _prepared_output_hashes(ROOT)
    source = source_input_digests(ROOT)
    python_members = python_member_hashes(ROOT / 'python' / 'gometry')
    metadata = cargo_metadata(ROOT.parent, locked=True)
    dependencies = {
        'lock_sha256': lock_before,
        'required_sources': required_source_records(metadata, root=ROOT.parent),
    }
    interpreter = json.loads(
        _tool_output(
            [
                str(python),
                '-c',
                f'import sys,json; sys.path.insert(0, {str(ROOT / "tools")!r}); '
                'from _artifact_common import _runtime_interpreter; '
                'print(json.dumps(_runtime_interpreter()))',
            ],
            cwd=ROOT,
            env=environment,
        )
    )
    compiler = _tool_output(['rustc', '-vV'], cwd=ROOT, env=environment)
    settings = quality_settings(ROOT, environment)
    command = [
        str(maturin),
        'build',
        '--locked',
        '--profile',
        'dev',
        '--interpreter',
        str(python),
        '--target-dir',
        str(target_dir),
        '--compatibility',
        'linux',
        '--compression-method',
        'stored',
    ]
    with tempfile.TemporaryDirectory(prefix='wheel-', dir=target_dir) as temporary:
        command.extend(['--out', temporary, '--', *RUST_INSTRUMENTATION])
        _run(command, environment)
        wheels = list(Path(temporary).glob('gometry-*.whl'))
        if len(wheels) != 1:
            raise AssertionError(f'expected one ASan wheel, found {wheels!r}')
        wheel = wheels[0]
        inspection = inspect_wheel(
            wheel,
            release=False,
            expected_python_members=python_members,
            expected_prepared_inputs=prepared,
        )
        member, payload = next(iter(inspection['extension_members'].items()))
        digest = sha256_file(wheel)
        artifact_dir = target_dir / 'artifacts' / digest
        artifact_dir.mkdir(parents=True, exist_ok=True)
        archive_path = artifact_dir / wheel.name
        shutil.copyfile(wheel, archive_path)
    package_root = target_dir / 'packages' / digest
    if not package_root.exists():
        with tempfile.TemporaryDirectory(
            prefix='package-', dir=target_dir
        ) as temporary:
            with zipfile.ZipFile(archive_path) as archive:
                archive.extractall(temporary)
            package_root.parent.mkdir(parents=True, exist_ok=True)
            Path(temporary).replace(package_root)
    extension = package_root / member
    if sha256_file(extension) != _sha256_bytes(payload):
        raise AssertionError('staged ASan extension differs from its wheel')
    verify_asan_image(extension)
    archives = _verify_bundled_archives(target_dir)
    receipt = {
        'schema': 1,
        'kind': 'gometry-sanitizer-runtime',
        'profile': 'dev',
        'package_root': str(package_root),
        'source': {**source, 'version': _package_version(ROOT), 'cargo': dependencies},
        'prepared_inputs': prepared,
        'python_members': python_members,
        'build': {
            'profile': 'dev',
            'target': environment['CARGO_BUILD_TARGET'],
            'raw_target_arch': environment['CARGO_BUILD_TARGET'].split('-', 1)[0],
            'interpreter': interpreter,
            'compiler': {'observed_at': 'before-build', 'rustc_vv': compiler},
            'cargo': {
                'version': _tool_output(
                    ['cargo', '--version'], cwd=ROOT, env=environment
                ),
                'dependencies': dependencies,
            },
            'maturin_version': MATURIN_VERSION,
            'settings': settings,
            'command': command,
        },
        'wheel': {
            'path': str(archive_path),
            'filename': archive_path.name,
            'sha256': digest,
            'generator': inspection['generator'],
            'extension_member': member,
            'extension_sha256': _sha256_bytes(payload),
        },
        'instrumentation': {
            'sanitizer': 'address',
            'runtime': {'path': str(runtime), 'sha256': sha256_file(runtime)},
            'archives': archives,
        },
    }
    if sha256_file(ROOT.parent / 'Cargo.lock') != lock_before:
        raise AssertionError('ASan production changed Cargo.lock')
    validate_sanitizer_producer_inputs(ROOT, receipt)
    _atomic_json(target_dir / 'receipt.json', receipt)
    print(f'ASan producer receipt: {target_dir / "receipt.json"}', file=sys.stderr)
    print(
        f'ASan extension: {extension} (sha256={_sha256_bytes(payload)})',
        file=sys.stderr,
    )
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Build and run gometry public-path tests under AddressSanitizer.',
        allow_abbrev=False,
    )
    parser.add_argument(
        '--python',
        dest='python_path',
        required=True,
        type=Path,
        help='Select Python with NumPy, PyArrow, pytest, and the required plugins',
    )
    parser.add_argument(
        '--target-dir',
        type=Path,
        default=ROOT / 'target-sanitizer-address',
        help='Reuse isolated ASan build state (default: target-sanitizer-address)',
    )
    args = parser.parse_args()
    python = args.python_path.absolute()
    if not python.is_file():
        parser.error(f'Python interpreter does not exist: {python}')
    target_dir = args.target_dir.resolve()
    runtime = _asan_runtime()
    receipt = prepare_sanitizer_runtime(python, target_dir, runtime)
    validate_sanitizer_producer_inputs(ROOT, receipt)
    environment = os.environ.copy()
    environment['GOMETRY_RUNTIME_RECEIPT'] = str(target_dir / 'receipt.json')
    environment['PYTHONPATH'] = os.pathsep.join(
        filter(
            None,
            (
                receipt['package_root'],
                environment.get('PYTHONPATH', ''),
            ),
        )
    )
    environment['LD_PRELOAD'] = os.pathsep.join(
        filter(
            None,
            (
                str(runtime),
                environment.get('LD_PRELOAD', ''),
            ),
        )
    )
    environment.setdefault(
        'ASAN_OPTIONS',
        'detect_leaks=0:halt_on_error=1:abort_on_error=1:detect_stack_use_after_return=1',
    )
    _run([str(python), '-m', 'pytest', '--no-header', '-n', '0', *TESTS], environment)
    return 0


if __name__ == '__main__':
    sys.exit(main())
