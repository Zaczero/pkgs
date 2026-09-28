"""Build and smoke-test the release wheel artifact.

This is intentionally heavier than the default pytest lane. There is no
``pytest -m release`` marker — run this script directly before publishing::

    .venv/bin/python tools/wheel_smoke.py
    .venv/bin/python tools/wheel_smoke.py --inspect path/to/gometry-*.whl
    .venv/bin/python tools/wheel_smoke.py --installed
    .venv/bin/python tools/wheel_smoke.py --smoke-sdist path/to/gometry-*.tar.gz
    .venv/bin/python tools/wheel_smoke.py --stage-sdist dist

Default mode builds an sdist via maturin, rebuilds the wheel from that sdist,
clean-installs it into a temporary venv, and runs a functional import/API smoke.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import re
import shlex
import shutil
import struct
import subprocess
import sys
import tarfile
import tempfile
import textwrap
import tomllib
import venv
import zipfile
from itertools import chain
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence

_SCRIPT = Path(__file__).resolve()
_ROOT_CANDIDATES = (
    _SCRIPT.parents[1],
    _SCRIPT.parents[1] / 'gometry',
    _SCRIPT.parents[2],
    _SCRIPT.parents[2] / 'gometry',
)
ROOT = next(
    (
        candidate
        for candidate in _ROOT_CANDIDATES
        if (candidate / 'Cargo.toml').is_file() and (candidate / 'native').is_dir()
    ),
    _SCRIPT.parents[1],
)
MONOREPO = ROOT.parent
LICENSE_SCRIPT = MONOREPO / '.github' / 'scripts' / 'gen_licenses.py'
README_SCRIPT = MONOREPO / '.github' / 'scripts' / 'render_pypi_readme.py'
LIBTIFF = ROOT / 'native' / 'libtiff-sys' / 'libtiff'
LIBTIFF_PIN = ROOT / 'native' / 'libtiff-sys' / 'libtiff.pin'
LIBTIFF_COMMIT = 'd01a94be176f5f6a87f7ee1c0b32e65416aa2b4d'
LIBTIFF_VERSION = '4.7.2'
REGISTRY_SOURCE = 'registry+https://github.com/rust-lang/crates.io-index'
MATURIN_VERSION = '1.14.1'
RELEASE_PROFILE = 'release-dist'
RELEASE_MATURIN_ARGS = f'--locked --profile {RELEASE_PROFILE}'
SOURCE_INPUTS_FORMAT = 'sha256-path-nul-content-nul-v5'
BUILD_INFO_SCHEMA_VERSION = 3
SOURCE_MANIFEST_NAME = 'gometry-source-manifest.json'
RUNNER_RECEIPT_NAME = 'gometry-wheel-receipt.json'
RELEASE_MANIFEST_NAME = 'gometry-release-manifest.json'
SMOKE_GRID_ENV = 'GOMETRY_SMOKE_GRID'
SMOKE_EXPECTED_SOURCE_INPUTS_ENV = 'GOMETRY_SMOKE_EXPECTED_SOURCE_INPUTS_SHA256'
SMOKE_EXPECTED_NATIVE_SOURCE_INPUTS_ENV = (
    'GOMETRY_SMOKE_EXPECTED_NATIVE_SOURCE_INPUTS_SHA256'
)
SMOKE_EXPECTED_VERSION_ENV = 'GOMETRY_SMOKE_EXPECTED_VERSION'
SMOKE_EXPECTED_TARGET_ENV = 'GOMETRY_SMOKE_EXPECTED_TARGET'
SMOKE_EXPECTED_ARCHITECTURE_ENV = 'GOMETRY_SMOKE_EXPECTED_ARCHITECTURE'
TEXT_SUFFIXES = frozenset({
    '.json',
    '.lock',
    '.md',
    '.py',
    '.pyi',
    '.rs',
    '.toml',
    '.txt',
    '.yaml',
    '.yml',
})
ABSOLUTE_BUILD_PATH = re.compile(
    r'(?i)(?:/home/[^/\s]+/|/Users/[^/\s]+/|/nix/store/|/tmp/|'
    r'/private/tmp/|/var/folders/|[a-z]:\\(?:Users|Temp)\\)'
)
PRINTABLE_STRING = re.compile(rb'[\x20-\x7e]{4,}')
# These are named Rust test fixtures, not build roots. Keep this exception
# narrower than the general temporary-path check.
NON_BUILD_PATH_PREFIXES = ('/tmp/gometry-crs-',)  # noqa: S108
SOURCE_TOOL_MEMBERS = frozenset({'tools/wheel_smoke.py'})
IDENTIFIER = re.compile(r'^(?P<python>[A-Za-z0-9]+)-(?P<platform>.+)$')
HEX_DIGEST = re.compile(r'^[0-9a-f]{64}$')
RUNNER_TARGETS = {
    'ubuntu-latest': 'x86_64-unknown-linux-gnu',
    'ubuntu-24.04-arm': 'aarch64-unknown-linux-gnu',
    'windows-latest': 'x86_64-pc-windows-msvc',
    'windows-11-arm': 'aarch64-pc-windows-msvc',
    'macos-15-intel': 'x86_64-apple-darwin',
    'macos-15': 'aarch64-apple-darwin',
}
RELEASE_ENV_VARS = (
    'GOMETRY_RELEASE_TARGET',
    'GOMETRY_RELEASE_TARGET_POLICY',
    'GOMETRY_RELEASE_RUSTFLAGS',
    'GOMETRY_RELEASE_CFLAGS',
    'GOMETRY_RELEASE_CXXFLAGS',
)


def run(cmd: list[str], *, cwd: Path = ROOT, env: dict[str, str] | None = None) -> None:
    result = subprocess.run(
        cmd,
        cwd=cwd,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.stdout:
        sys.stderr.write(result.stdout)
    if result.stderr:
        sys.stderr.write(result.stderr)
    if result.returncode:
        raise subprocess.CalledProcessError(
            result.returncode,
            cmd,
            output=result.stdout,
            stderr=result.stderr,
        )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _maturin_command() -> list[str]:
    candidates = [Path(sys.executable).with_name('maturin')]
    if executable := shutil.which('maturin'):
        candidates.append(Path(executable))
    for executable in candidates:
        if not executable.is_file():
            continue
        result = subprocess.run(
            [str(executable), '--version'],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        if (
            result.returncode == 0
            and result.stdout.strip() == f'maturin {MATURIN_VERSION}'
        ):
            return [str(executable)]
    # Keep local runs self-contained while CI can install the exact executable
    # once. `uv tool run` also avoids making a build backend part of this
    # package's runtime environment.
    return ['uv', 'tool', 'run', '--from', f'maturin=={MATURIN_VERSION}', 'maturin']


def _git_revision() -> str:
    result = subprocess.run(
        ['git', 'rev-parse', 'HEAD'],
        cwd=MONOREPO,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode or not result.stdout.strip():
        raise AssertionError(
            f'could not identify source revision: {result.stderr.strip()}'
        )
    return result.stdout.strip()


def _source_date_epoch() -> str | None:
    result = subprocess.run(
        ['git', 'show', '-s', '--format=%ct', 'HEAD'],
        cwd=MONOREPO,
        check=False,
        capture_output=True,
        text=True,
    )
    value = result.stdout.strip()
    return value if result.returncode == 0 and value.isdigit() else None


def _tool_version(command: list[str], *, env: Mapping[str, str] | None = None) -> str:
    result = subprocess.run(
        command,
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode:
        return ''
    return result.stdout.strip().splitlines()[0] if result.stdout.strip() else ''


def _package_toolchain_channel(root: Path = ROOT) -> str:
    path = root / 'rust-toolchain.toml'
    try:
        value = tomllib.loads(path.read_text(encoding='utf-8'))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise AssertionError(
            f'cannot read package Rust toolchain {path}: {error}'
        ) from error
    toolchain = value.get('toolchain')
    channel = toolchain.get('channel') if isinstance(toolchain, dict) else None
    if not isinstance(channel, str) or not channel:
        raise AssertionError(f'package Rust toolchain has no channel: {path}')
    return channel


def _target_architecture(target: str) -> str:
    architecture = target.split('-', 1)[0].lower()
    if architecture in {'x86_64', 'amd64'}:
        return 'x86_64'
    if architecture in {'aarch64', 'arm64'}:
        return 'arm64'
    return architecture


def release_target_policy(target: str) -> dict[str, Any]:
    """Return the release compiler policy for one Rust target triple.

    Rust's target-cpu is the authoritative x86 baseline. Native compilers use
    the equivalent generic ``-march`` spelling where that is meaningful;
    MSVC's x64 default and every non-x86 target retain the compiler default.
    """
    if (
        not isinstance(target, str)
        or not target
        or '-' not in target
        or '/' in target
        or '\\' in target
    ):
        raise AssertionError(f'invalid Rust target triple: {target!r}')
    architecture = _target_architecture(target)
    rustflags: list[str] = []
    cflags: list[str] = []
    cxxflags: list[str] = []
    if architecture == 'x86_64':
        rustflags = ['-C', 'target-cpu=x86-64']
        if not target.endswith('-msvc'):
            cflags = ['-march=x86-64']
            cxxflags = ['-march=x86-64']
            target_policy = 'generic-x86-64'
        else:
            target_policy = 'compiler-default-x86-64'
    elif architecture == 'arm64':
        target_policy = 'compiler-default-arm64'
    else:
        target_policy = f'compiler-default-{architecture}'
    return {
        'target': target,
        'architecture': architecture,
        'target_policy': target_policy,
        'rustflags': rustflags,
        'cflags': cflags,
        'cxxflags': cxxflags,
    }


def _flag_text(flags: Sequence[str]) -> str:
    return shlex.join(list(flags))


def _effective_release_policy(target: str | None = None) -> dict[str, Any]:
    present = [name for name in RELEASE_ENV_VARS if name in os.environ]
    if present and len(present) != len(RELEASE_ENV_VARS):
        missing = sorted(set(RELEASE_ENV_VARS) - set(present))
        raise AssertionError(
            'release identity environment is partial; missing ' + ', '.join(missing)
        )
    selected = (
        target
        or os.environ.get('GOMETRY_RELEASE_TARGET', '').strip()
        or _rust_target_triple()
    )
    policy = release_target_policy(selected)
    observed = {
        'target': os.environ.get('GOMETRY_RELEASE_TARGET'),
        'target_policy': os.environ.get('GOMETRY_RELEASE_TARGET_POLICY'),
        'rustflags': os.environ.get('GOMETRY_RELEASE_RUSTFLAGS'),
        'cflags': os.environ.get('GOMETRY_RELEASE_CFLAGS'),
        'cxxflags': os.environ.get('GOMETRY_RELEASE_CXXFLAGS'),
    }
    unsuffixed = {
        'cflags': os.environ.get('CFLAGS'),
        'cxxflags': os.environ.get('CXXFLAGS'),
    }
    expected_flags = {
        key: _flag_text(policy[key]) for key in ('rustflags', 'cflags', 'cxxflags')
    }
    if observed['target'] is not None and observed['target'] != policy['target']:
        raise AssertionError(
            f'GOMETRY_RELEASE_TARGET disagrees with target policy: '
            f'{observed["target"]!r} != {policy["target"]!r}'
        )
    if (
        observed['target_policy'] is not None
        and observed['target_policy'] != policy['target_policy']
    ):
        raise AssertionError(
            'GOMETRY_RELEASE_TARGET_POLICY disagrees with target policy: '
            f'{observed["target_policy"]!r} != {policy["target_policy"]!r}'
        )
    for key, expected in expected_flags.items():
        value = observed[key]
        if value is not None and value != expected:
            raise AssertionError(
                f'GOMETRY_RELEASE_{key.upper()} disagrees with target policy: '
                f'{value!r} != {expected!r}'
            )
    for key, value in unsuffixed.items():
        expected = expected_flags[key]
        if value is not None and value != expected:
            raise AssertionError(
                f'{key.upper()} disagrees with target policy: {value!r} != {expected!r}'
            )
    target_suffix = policy['target'].replace('-', '_')
    for key, prefix in (('cflags', 'CFLAGS'), ('cxxflags', 'CXXFLAGS')):
        value = os.environ.get(f'{prefix}_{target_suffix}')
        if value is not None and value != expected_flags[key]:
            raise AssertionError(
                f'{prefix}_{target_suffix} disagrees with target policy: '
                f'{value!r} != {expected_flags[key]!r}'
            )
    return policy


def _is_hidden_native_path(relative: str) -> bool:
    parts = PurePosixPath(relative).parts
    native_index = next(
        (
            index
            for index in range(len(parts) - 1)
            if parts[index : index + 2] == ('gometry', 'native')
        ),
        None,
    )
    return native_index is not None and any(
        part.startswith('.') for part in parts[native_index + 2 :]
    )


def _has_hidden_component(relative: str) -> bool:
    return any(part.startswith('.') for part in PurePosixPath(relative).parts)


def _source_input_records(
    root: Path,
) -> tuple[list[tuple[str, Path]], list[tuple[str, Path]]]:
    fixed = [
        ('gometry/build.rs', root / 'build.rs'),
    ]
    source_files: list[tuple[str, Path]] = []
    _collect_source_files(root / 'src', 'gometry/src', source_files)
    native_files: list[tuple[str, Path]] = []
    _collect_source_files(
        root / 'native',
        'gometry/native',
        native_files,
        skip_hidden_native=True,
    )
    return [*fixed, *source_files, *native_files], native_files


def _collect_source_files(
    root: Path,
    logical_root: str,
    files: list[tuple[str, Path]],
    *,
    skip_hidden_native: bool = False,
) -> None:
    try:
        entries = sorted(root.iterdir(), key=lambda path: path.name)
    except OSError as error:
        raise AssertionError(
            f'cannot read build input directory {root}: {error}'
        ) from error
    for path in entries:
        if path.name == 'Cargo.toml':
            continue
        logical = f'{logical_root}/{path.name}'
        if skip_hidden_native and _is_hidden_native_path(logical):
            continue
        if path.is_dir():
            _collect_source_files(
                path,
                logical,
                files,
                skip_hidden_native=skip_hidden_native,
            )
        elif path.is_file():
            files.append((logical, path))


def _digest_source_files(files: Sequence[tuple[str, Path]]) -> str:
    digest = hashlib.sha256()
    for logical, path in sorted(files, key=lambda item: item[0]):
        try:
            payload = path.read_bytes()
        except OSError as error:
            raise AssertionError(f'cannot read build input {path}: {error}') from error
        digest.update(logical.encode('utf-8'))
        digest.update(b'\0')
        digest.update(payload)
        digest.update(b'\0')
    return digest.hexdigest()


def source_input_digests(root: Path = ROOT) -> dict[str, str]:
    """Match the source and native digests emitted by ``gometry/build.rs``."""
    files, native_files = _source_input_records(root)
    return {
        'source_inputs_sha256': _digest_source_files(files),
        'native_source_inputs_sha256': _digest_source_files(native_files),
    }


def _rust_target_triple() -> str:
    configured = os.environ.get('CARGO_BUILD_TARGET', '').strip()
    if (
        configured
        and '-' in configured
        and '/' not in configured
        and '\\' not in configured
    ):
        return configured
    result = subprocess.run(
        ['rustc', '-vV'],
        check=True,
        capture_output=True,
        text=True,
    )
    host = next(
        (
            line.removeprefix('host: ').strip()
            for line in result.stdout.splitlines()
            if line.startswith('host: ')
        ),
        '',
    )
    if not host:
        raise AssertionError('rustc -vV did not report a host target')
    return host


def release_build_env(*, target: str | None = None) -> dict[str, str]:
    env = os.environ.copy()
    env.pop('RUSTC_WRAPPER', None)
    env.pop('NIX_LDFLAGS', None)
    # Cargo's environment flags have precedence over target-specific config.
    # Replace, rather than append to, inherited values so a developer's
    # x86-64-v2 setting cannot enter a distribution build and arm64 cannot
    # inherit an x86 setting from the host.
    for variable in (
        'RUSTFLAGS',
        'CARGO_ENCODED_RUSTFLAGS',
        'CFLAGS',
        'CXXFLAGS',
        'CPPFLAGS',
        'TARGET_CFLAGS',
        'TARGET_CXXFLAGS',
        'HOST_CFLAGS',
        'HOST_CXXFLAGS',
        'CMAKE_C_FLAGS',
        'CMAKE_CXX_FLAGS',
        'CMAKE_C_FLAGS_RELEASE',
        'CMAKE_CXX_FLAGS_RELEASE',
        'CMAKE_ARGS',
    ):
        env.pop(variable, None)
    env.pop('GOMETRY_RELEASE_SOURCE_COMMIT', None)
    env.pop('GOMETRY_RELEASE_PROFILE', None)
    for variable in list(env):
        if variable.startswith(('CFLAGS_', 'CXXFLAGS_')):
            env.pop(variable)
    # The developer shell may select `dev` through MATURIN_PEP517_ARGS. The
    # published wheel uses the distribution profile instead of inheriting that
    # override or falling through to maturin's unstripped local `release` profile.
    env['MATURIN_PEP517_ARGS'] = RELEASE_MATURIN_ARGS
    if source_date_epoch := _source_date_epoch():
        env['SOURCE_DATE_EPOCH'] = source_date_epoch
    # Nix's target-specific linker wrapper otherwise injects this workstation's
    # closure paths into ELF RUNPATH. Keep the unsuffixed spelling for older
    # wrappers, and set the spelling consumed by current target wrappers.
    env['NIX_DONT_SET_RPATH'] = '1'
    target = target or _rust_target_triple()
    policy = release_target_policy(target)
    env['RUSTUP_TOOLCHAIN'] = _package_toolchain_channel()
    env['GOMETRY_RELEASE_TARGET'] = policy['target']
    env['GOMETRY_RELEASE_TARGET_POLICY'] = policy['target_policy']
    for key, variable, prefix in (
        ('rustflags', 'GOMETRY_RELEASE_RUSTFLAGS', None),
        ('cflags', 'GOMETRY_RELEASE_CFLAGS', 'CFLAGS'),
        ('cxxflags', 'GOMETRY_RELEASE_CXXFLAGS', 'CXXFLAGS'),
    ):
        value = _flag_text(policy[key])
        env[variable] = value
        if prefix is None:
            if value:
                env['RUSTFLAGS'] = value
            else:
                env.pop('RUSTFLAGS', None)
        else:
            # The native build owners apply this value to CMake when the
            # unsuffixed variable is absent. The target-specific spelling is
            # consumed by cc-based dependencies such as zlib and SQLite.
            env.pop(prefix, None)
            target_variable = f'{prefix}_{target.replace("-", "_")}'
            env.pop(target_variable, None)
            if value:
                env[target_variable] = value
    env[f'NIX_DONT_SET_RPATH_{target.replace("-", "_")}'] = '1'
    env.setdefault('CARGO_TARGET_DIR', str(ROOT / 'target'))
    return env


def _source_build_env() -> dict[str, str]:
    env = release_build_env()
    # The sdist command does not build Rust code. Keeping the PEP 517 options
    # off this bootstrap step makes the distinction explicit; the final wheel
    # build receives the locked distribution profile from release_build_env.
    env.pop('MATURIN_PEP517_ARGS', None)
    return env


def _run_maturin_sdist(out_dir: Path, *, cwd: Path = ROOT) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    run(
        [*_maturin_command(), 'sdist', '--out', str(out_dir)],
        cwd=cwd,
        env=_source_build_env(),
    )


def build_wheel_from_sdist(sdist: Path, out_dir: Path, *, env: dict[str, str]) -> Path:
    sdist_version = inspect_sdist(
        sdist,
        expected_libtiff=libtiff_source_members(),
    )
    evidence = verify_sdist_dependency_sources(sdist, require_normalized=True)
    if evidence['preimage_lock_sha256'] != evidence['normalized_lock_sha256']:
        raise AssertionError(
            'wheel builds require a normalized staged sdist; '
            'use --stage-sdist before building a wheel'
        )
    run(
        [
            'uv',
            'build',
            '--wheel',
            '--python',
            sys.executable,
            '--out-dir',
            str(out_dir),
            str(sdist),
        ],
        env=env,
    )
    wheels = sorted(out_dir.glob('gometry-*.whl'))
    if len(wheels) != 1:
        raise AssertionError(f'expected one gometry wheel, found {wheels!r}')
    wheel_version = inspect_wheel(wheels[0])
    if wheel_version != sdist_version:
        raise AssertionError(
            f'sdist/wheel version mismatch: {sdist_version!r} != {wheel_version!r}'
        )
    return wheels[0]


def build_wheel(out_dir: Path) -> Path:
    stage_sdist(out_dir)
    env = release_build_env()
    sdists = sorted(out_dir.glob('gometry-*.tar.gz'))
    if len(sdists) != 1:
        raise AssertionError(f'expected one gometry sdist, found {sdists!r}')
    return build_wheel_from_sdist(sdists[0], out_dir, env=env)


def _prepared_output_hashes() -> dict[str, str]:
    paths = (
        'README.md',
        'LICENSE-APACHE.md',
        'LICENSE-MIT.md',
        'LICENSE-THIRD-PARTY.md',
        'python/gometry/_lib.pyi',
    )
    hashes: dict[str, str] = {}
    for relative in paths:
        path = ROOT / relative
        if not path.is_file():
            raise AssertionError(f'prepared source output is missing: {relative}')
        hashes[relative] = sha256_file(path)
    return hashes


def _prepared_input_identity(hashes: Mapping[str, str]) -> str:
    value = ''.join(f'{name}\0{digest}\n' for name, digest in sorted(hashes.items()))
    return _sha256_bytes(value.encode('ascii'))


def _prepare_source_outputs() -> dict[str, str]:
    if not README_SCRIPT.is_file():
        raise AssertionError(f'PyPI README renderer is missing: {README_SCRIPT}')
    if not LICENSE_SCRIPT.is_file():
        raise AssertionError(f'license generator is missing: {LICENSE_SCRIPT}')
    ref = os.environ.get('GITHUB_REF_NAME', 'main')
    run([sys.executable, str(README_SCRIPT), 'gometry', ref], cwd=MONOREPO)
    run([sys.executable, str(LICENSE_SCRIPT), 'gometry'], cwd=MONOREPO)
    return _prepared_output_hashes()


def _clear_stage_outputs(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for pattern in ('gometry-*.tar.gz', 'gometry-*.whl'):
        for path in out_dir.glob(pattern):
            path.unlink()
    for name in (SOURCE_MANIFEST_NAME, RUNNER_RECEIPT_NAME, RELEASE_MANIFEST_NAME):
        path = out_dir / name
        if path.exists():
            path.unlink()


def _single_sdist(directory: Path, *, label: str) -> Path:
    sdists = sorted(directory.glob('gometry-*.tar.gz'))
    if len(sdists) != 1:
        raise AssertionError(f'expected one {label} gometry sdist, found {sdists!r}')
    return sdists[0]


def _source_tree_files(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in root.rglob('*')
        if path.is_file()
    }


def _compare_restage_trees(bootstrap: Path, final: Path) -> None:
    before = _source_tree_files(bootstrap)
    after = _source_tree_files(final)
    if admitted_hidden := sorted(
        relative for relative in after if _is_hidden_native_path(relative)
    ):
        raise AssertionError(
            f'maturin restaging admitted hidden native files: {admitted_hidden[:20]}'
        )
    changed = sorted(
        relative
        for relative in set(before) & set(after)
        if before[relative] != after[relative]
        and relative not in {'Cargo.lock', 'PKG-INFO'}
        and not _is_hidden_native_path(relative)
    )
    # Maturin's second pass sees the extracted source as a non-git tree and
    # consequently omits hidden submodule metadata. It is not a build input;
    # every other source removal/addition is a restaging defect.
    removed = sorted(
        relative
        for relative in set(before) - set(after)
        if not _is_hidden_native_path(relative)
    )
    added = sorted(set(after) - set(before))
    if changed or removed or added:
        details = []
        if changed:
            details.append(f'changed={changed[:20]}')
        if removed:
            details.append(f'removed={removed[:20]}')
        if added:
            details.append(f'added={added[:20]}')
        raise AssertionError(
            'maturin restaging changed source inputs unexpectedly: '
            + ', '.join(details)
        )


def _source_manifest(
    *,
    sdist: Path,
    prepared_hashes: Mapping[str, str],
    evidence: Mapping[str, Any],
    source_digests: Mapping[str, str],
) -> dict[str, Any]:
    required = evidence['required_sources']
    toolchain_env = release_build_env()
    return {
        'schema': 2,
        'kind': 'gometry-source-manifest',
        'package': 'gometry',
        'commit': _git_revision(),
        'source_inputs_format': SOURCE_INPUTS_FORMAT,
        'source_inputs_sha256': source_digests['source_inputs_sha256'],
        'native_source_inputs_format': SOURCE_INPUTS_FORMAT,
        'native_source_inputs_sha256': source_digests['native_source_inputs_sha256'],
        'prepared_inputs': {
            'identity_sha256': _prepared_input_identity(prepared_hashes),
            'files': dict(sorted(prepared_hashes.items())),
        },
        'generated_output_hashes': dict(sorted(prepared_hashes.items())),
        'sdist': {
            'filename': sdist.name,
            'sha256': sha256_file(sdist),
            'version': sdist.name.removeprefix('gometry-').removesuffix('.tar.gz'),
        },
        'cargo': {
            'lock_preimage_sha256': evidence['preimage_lock_sha256'],
            'lock_normalized_sha256': evidence['normalized_lock_sha256'],
            'required_sources': required,
        },
        'toolchain': {
            'channel': _package_toolchain_channel(),
            'maturin': MATURIN_VERSION,
            'cargo': _tool_version(['cargo', '--version'], env=toolchain_env),
            'rustc': _tool_version(['rustc', '--version'], env=toolchain_env),
            'maturin_pep517_args': RELEASE_MATURIN_ARGS,
        },
    }


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )


def stage_sdist(out_dir: Path) -> Path:
    """Prepare and verify the one sdist used by every release wheel build."""
    out_dir = out_dir.resolve()
    _clear_stage_outputs(out_dir)
    final: Path | None = None
    try:
        verify_libtiff_checkout()
        prepared_hashes = _prepare_source_outputs()
        source_digests = source_input_digests()
        with tempfile.TemporaryDirectory(prefix='gometry-sdist-stage-') as temporary:
            temporary_root = Path(temporary)
            bootstrap_dir = temporary_root / 'bootstrap'
            final_dir = temporary_root / 'final'
            _run_maturin_sdist(bootstrap_dir)
            bootstrap = _single_sdist(bootstrap_dir, label='bootstrap')
            inspect_sdist(bootstrap, allow_hidden_native=True)
            with tempfile.TemporaryDirectory(
                prefix='gometry-sdist-bootstrap-'
            ) as extracted_tmp:
                extracted = Path(extracted_tmp)
                with tarfile.open(bootstrap) as archive:
                    _extract_sdist(archive, extracted)
                roots = [item for item in extracted.iterdir() if item.is_dir()]
                if len(roots) != 1:
                    raise AssertionError(
                        f'expected one extracted bootstrap root, found {roots!r}'
                    )
                root = roots[0]
                evidence = _verify_extracted_sdist(root, require_normalized=False)
                if {key: evidence[key] for key in source_digests} != dict(
                    source_digests
                ):
                    raise AssertionError(
                        'bootstrap sdist source-input identity differs from checkout'
                    )
                normalized_lock = evidence['normalized_lock_bytes']
                if not isinstance(normalized_lock, bytes):
                    raise TypeError('Cargo lock normalization did not return bytes')
                (root / 'Cargo.lock').write_bytes(normalized_lock)
                _verify_locked_sdist(root, expected_lock=normalized_lock)
                _run_maturin_sdist(final_dir, cwd=root)
                final = _single_sdist(final_dir, label='final')
                inspect_sdist(final, expected_libtiff=libtiff_source_members())
                with tempfile.TemporaryDirectory(
                    prefix='gometry-sdist-final-'
                ) as final_tmp:
                    final_extracted = Path(final_tmp)
                    with tarfile.open(final) as archive:
                        _extract_sdist(archive, final_extracted)
                    final_roots = [
                        item for item in final_extracted.iterdir() if item.is_dir()
                    ]
                    if len(final_roots) != 1:
                        raise AssertionError(
                            f'expected one extracted final root, found {final_roots!r}'
                        )
                    _compare_restage_trees(root, final_roots[0])
                    final_evidence = _verify_locked_sdist(
                        final_roots[0],
                        expected_lock=normalized_lock,
                    )
                    if (
                        final_evidence['normalized_lock_sha256']
                        != evidence['normalized_lock_sha256']
                    ):
                        raise AssertionError(
                            'final sdist Cargo.lock hash changed during restaging'
                        )
                    if {key: final_evidence[key] for key in source_digests} != dict(
                        source_digests
                    ):
                        raise AssertionError(
                            'final sdist source-input identity differs from checkout'
                        )
                    manifest = _source_manifest(
                        sdist=final,
                        prepared_hashes=prepared_hashes,
                        evidence=evidence,
                        source_digests=source_digests,
                    )
            destination = out_dir / final.name
            shutil.copyfile(final, destination)
            manifest_path = out_dir / SOURCE_MANIFEST_NAME
            _write_json(manifest_path, manifest)
            # Re-read the copied artifact and manifest so a partial destination
            # write cannot be reported as the staged source.
            validate_source_manifest(destination, manifest_path)
            return destination
    except Exception:
        if final is not None:
            (out_dir / final.name).unlink(missing_ok=True)
        (out_dir / SOURCE_MANIFEST_NAME).unlink(missing_ok=True)
        raise


def _is_text_member(name: str) -> bool:
    path = Path(name)
    return path.suffix.lower() in TEXT_SUFFIXES or path.name in {
        'METADATA',
        'PKG-INFO',
        'WHEEL',
    }


def _printable_strings(value: bytes) -> Iterator[str]:
    for match in PRINTABLE_STRING.finditer(value):
        yield match.group().decode('ascii')


def _content_leaks(
    members: dict[str, str],
    binary_members: dict[str, bytes] | None = None,
) -> list[str]:
    leaks: list[str] = []
    binary_values = (
        (name, value)
        for name, payload in (binary_members or {}).items()
        for value in _printable_strings(payload)
    )
    for name, value in chain(members.items(), binary_values):
        if any(name.endswith(f'/{member}') for member in SOURCE_TOOL_MEMBERS):
            continue
        for match in ABSOLUTE_BUILD_PATH.finditer(value):
            if any(
                value.startswith(prefix, match.start())
                for prefix in NON_BUILD_PATH_PREFIXES
            ):
                continue
            leaks.append(f'{name}: absolute build path {match.group(0)!r}')
    return leaks


def native_notice_markers() -> list[tuple[str, str, str]]:
    """Component name, license ID, and full pinned notice text."""
    spec = importlib.util.spec_from_file_location(
        'gometry_license_generator', LICENSE_SCRIPT
    )
    if spec is None or spec.loader is None:
        raise AssertionError(f'could not load license generator {LICENSE_SCRIPT}')
    generator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(generator)
    try:
        entries = generator._native_notice_texts('gometry')
    except SystemExit as error:
        raise AssertionError(str(error)) from error
    return [
        (component['name'], component['license'], text) for component, text in entries
    ]


def verify_native_notice_texts(text: str, artifact: Path) -> None:
    for component, license_id, notice in native_notice_markers():
        if (
            component not in text
            or f'Licensed under {license_id}.' not in text
            or notice not in text
        ):
            raise AssertionError(
                f'{artifact} is missing the full native notice for {component!r}'
            )


def verify_libtiff_checkout() -> None:
    """Reject an uninitialized or altered source before maturin can omit it."""
    cmake = LIBTIFF / 'CMakeLists.txt'
    version = LIBTIFF / 'VERSION'
    hint = 'git submodule update --init gometry/native/libtiff-sys/libtiff'
    if not cmake.is_file() or not version.is_file():
        raise AssertionError(f'libtiff submodule is not initialized; run `{hint}`')
    if version.read_text().strip() != LIBTIFF_VERSION:
        raise AssertionError(f'libtiff VERSION is not {LIBTIFF_VERSION}')
    if 'add_subdirectory(build)' not in cmake.read_text():
        raise AssertionError(
            'libtiff CMakeLists.txt is not the unmodified upstream file'
        )
    pin = LIBTIFF_PIN.read_text()
    required_pin = (
        'url = https://gitlab.com/libtiff/libtiff.git\n'
        f'commit = {LIBTIFF_COMMIT}\n'
        'tag = v4.7.2\n'
        'version = 4.7.2\n'
    )
    if pin != required_pin:
        raise AssertionError(
            'libtiff.pin does not describe the required upstream source'
        )
    result = subprocess.run(
        ['git', '-C', str(LIBTIFF), 'rev-parse', 'HEAD'],
        check=True,
        capture_output=True,
        text=True,
    )
    if result.stdout.strip() != LIBTIFF_COMMIT:
        raise AssertionError(f'libtiff submodule is not pinned to {LIBTIFF_COMMIT}')
    for diff_args in (('diff', '--exit-code'), ('diff', '--cached', '--exit-code')):
        dirty = subprocess.run(
            ['git', '-C', str(LIBTIFF), *diff_args],
            # A non-zero exit is the signal being read, not a failure.
            check=False,
            capture_output=True,
            text=True,
        )
        if dirty.returncode:
            raise AssertionError(
                f'libtiff submodule has tracked changes: {dirty.stdout}'
            )
    status = subprocess.run(
        ['git', '-C', str(LIBTIFF), 'status', '--porcelain', '--untracked-files=all'],
        check=True,
        capture_output=True,
        text=True,
    )
    if status.stdout:
        raise AssertionError(f'libtiff submodule has untracked files: {status.stdout}')
    license_text = (LIBTIFF / 'LICENSE.md').read_bytes()
    first_start = license_text.index(b'Copyright')
    first_end = license_text.index(b'# Lempel-Ziv')
    notices = ROOT / 'native' / 'libtiff-sys' / 'notices'
    if (notices / 'libtiff-license.md').read_bytes() != license_text[
        first_start:first_end
    ]:
        raise AssertionError(
            'libtiff license notice extract differs from upstream LICENSE.md'
        )
    if (notices / 'libtiff-lzw-license.md').read_bytes() != license_text[first_end:]:
        raise AssertionError(
            'libtiff LZW notice extract differs from upstream LICENSE.md'
        )


def inspect_sdist(
    path: Path,
    *,
    expected_libtiff: set[str] | None = None,
    allow_hidden_native: bool = False,
) -> str:
    with tarfile.open(path) as archive:
        names = archive.getnames()
        if len(names) != len(set(names)):
            raise AssertionError(f'sdist contains duplicate members: {path}')
        if not allow_hidden_native and (
            hidden := sorted(name for name in names if _is_hidden_native_path(name))
        ):
            raise AssertionError(
                f'sdist contains hidden native metadata files: {hidden[:20]}'
            )
        text_members: dict[str, str] = {}
        for member in archive.getmembers():
            if not member.isfile() or not _is_text_member(member.name):
                continue
            stream = archive.extractfile(member)
            if stream is None:
                raise AssertionError(f'sdist member could not be read: {member.name}')
            text_members[member.name] = stream.read().decode('utf-8', errors='ignore')
    inspect_libtiff_sdist_members(
        names,
        path,
        expected_libtiff,
        allow_hidden_native=allow_hidden_native,
    )
    root = f'gometry-{path.name.removeprefix("gometry-").removesuffix(".tar.gz")}'
    required = {
        f'{root}/LICENSE-APACHE.md',
        f'{root}/LICENSE-MIT.md',
        f'{root}/LICENSE-THIRD-PARTY.md',
        f'{root}/Cargo.lock',
        f'{root}/pyproject.toml',
        f'{root}/rust-toolchain.toml',
        f'{root}/gometry/rust-toolchain.toml',
        f'{root}/tools/wheel_smoke.py',
        f'{root}/gometry/native/libtiff-sys/libtiff/CMakeLists.txt',
        f'{root}/gometry/native/libtiff-sys/libtiff/libtiff/tif_lzw.c',
    }
    if missing := sorted(required - set(names)):
        raise AssertionError(f'sdist missing root release files: {missing}')
    package_info = text_members.get(f'{root}/PKG-INFO', '')
    version_line = next(
        (line for line in package_info.splitlines() if line.startswith('Version: ')),
        '',
    )
    metadata_version = version_line.removeprefix('Version: ')
    filename_version = path.name.removeprefix('gometry-').removesuffix('.tar.gz')
    if not metadata_version or metadata_version != filename_version:
        raise AssertionError(
            'sdist filename/metadata version mismatch: '
            f'{filename_version!r} != {metadata_version!r}'
        )
    pyproject = text_members[f'{root}/pyproject.toml']
    if 'patch.crates-io' in pyproject or 'vendor/' in pyproject:
        raise AssertionError('sdist still configures vendored Cargo sources')
    forbidden_configs = (
        'target.x86_64-',
        'profile.release.',
        'unstable.trim-paths',
    )
    if forbidden := [
        line.strip()
        for line in pyproject.splitlines()
        if any(token in line for token in forbidden_configs)
    ]:
        raise AssertionError(
            f'sdist Maturin config duplicates root release policy: {forbidden}'
        )
    try:
        package_toolchain = tomllib.loads(
            text_members[f'{root}/gometry/rust-toolchain.toml']
        )
    except tomllib.TOMLDecodeError as error:
        raise AssertionError('sdist package rust-toolchain.toml is invalid') from error
    channel = (
        package_toolchain.get('toolchain', {}).get('channel')
        if isinstance(package_toolchain.get('toolchain'), dict)
        else None
    )
    if channel != _package_toolchain_channel():
        raise AssertionError(
            'sdist package Rust toolchain channel does not match the checkout: '
            f'{channel!r}'
        )
    forbidden_parts = {'site', 'dev', '__pycache__'}
    suspect = [
        name
        for name in names
        if forbidden_parts.intersection(Path(name).parts)
        or Path(name).suffix in {'.pyc', '.pyo'}
        or any(
            part == 'target' or part.startswith('target-') for part in Path(name).parts
        )
    ]
    if suspect:
        raise AssertionError(
            f'sdist contains old/generated/internal paths: {suspect[:20]}'
        )
    if leaks := _content_leaks(text_members):
        raise AssertionError(f'sdist text leaks absolute build paths: {leaks[:20]}')
    verify_native_notice_texts(text_members[f'{root}/LICENSE-THIRD-PARTY.md'], path)
    return metadata_version


def libtiff_source_members() -> set[str]:
    return {
        path.relative_to(LIBTIFF).as_posix()
        for path in LIBTIFF.rglob('*')
        if path.is_file()
        and not _has_hidden_component(path.relative_to(LIBTIFF).as_posix())
    }


def inspect_libtiff_sdist(path: Path, expected: set[str] | None = None) -> None:
    with tarfile.open(path) as archive:
        inspect_libtiff_sdist_members(archive.getnames(), path, expected)


def _extract_sdist(archive: tarfile.TarFile, destination: Path) -> None:
    if sys.version_info >= (3, 12):
        archive.extractall(destination, filter='data')
        return
    for member in archive.getmembers():
        posix_path = PurePosixPath(member.name)
        windows_path = PureWindowsPath(member.name)
        if (
            posix_path.is_absolute()
            or windows_path.is_absolute()
            or windows_path.drive
            or '..' in posix_path.parts
        ):
            raise AssertionError(f'sdist contains unsafe member path: {member.name!r}')
        if not (member.isdir() or member.isfile()):
            raise AssertionError(f'sdist contains unsupported member: {member.name!r}')
        archive.extract(member, destination)


def inspect_libtiff_sdist_members(
    names: list[str],
    path: Path,
    expected: set[str] | None = None,
    *,
    allow_hidden_native: bool = False,
) -> None:
    root = f'gometry-{path.name.removeprefix("gometry-").removesuffix(".tar.gz")}'
    prefix = f'{root}/gometry/native/libtiff-sys/libtiff/'
    members = [
        name.removeprefix(prefix)
        for name in names
        if name.startswith(prefix) and not name.endswith('/')
    ]
    if len(members) != len(set(members)):
        raise AssertionError('sdist contains duplicate libtiff paths')
    for member in members:
        path = Path(member)
        if not member or path.is_absolute() or '..' in path.parts:
            raise AssertionError(f'sdist contains invalid libtiff path: {member!r}')
    actual = set(members)
    if allow_hidden_native:
        actual = {member for member in actual if not _has_hidden_component(member)}
    expected = libtiff_source_members() if expected is None else expected
    if actual != expected:
        raise AssertionError(
            'sdist libtiff file set differs from initialized submodule: '
            f'missing={sorted(expected - actual)[:20]}, '
            f'extra={sorted(actual - expected)[:20]}'
        )


def _cargo_metadata(root: Path, *, locked: bool) -> dict[str, Any]:
    command = [
        'cargo',
        'metadata',
        '--format-version=1',
    ]
    if locked:
        command.append('--locked')
    command.extend(['--manifest-path', str(root / 'gometry' / 'Cargo.toml')])
    env = os.environ.copy()
    env.pop('RUSTC_WRAPPER', None)
    env['CARGO_TERM_COLOR'] = 'never'
    result = subprocess.run(
        command,
        cwd=root,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode:
        raise AssertionError(
            f'cargo metadata {"--locked " if locked else ""}failed '
            f'(exit {result.returncode}): {result.stderr.strip()}'
        )
    try:
        value = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise AssertionError(
            f'cargo metadata returned invalid JSON: {error}'
        ) from error
    if not isinstance(value, dict):
        raise TypeError('cargo metadata returned a non-object')
    return value


def _lock_tuple(package: Mapping[str, Any]) -> tuple[str, str, str | None, str | None]:
    try:
        name = package['name']
        version = package['version']
    except KeyError as error:
        raise AssertionError(
            f'Cargo.lock package is missing {error.args[0]!r}'
        ) from error
    if not isinstance(name, str) or not isinstance(version, str):
        raise TypeError('Cargo.lock package name and version must be strings')
    source = package.get('source')
    checksum = package.get('checksum')
    if source is not None and not isinstance(source, str):
        raise AssertionError(f'Cargo.lock source for {name} must be a string')
    if checksum is not None and not isinstance(checksum, str):
        raise AssertionError(f'Cargo.lock checksum for {name} must be a string')
    return name, version, source, checksum


def _lock_tuples(lock_bytes: bytes) -> set[tuple[str, str, str | None, str | None]]:
    try:
        lock = tomllib.loads(lock_bytes.decode('utf-8'))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise AssertionError(f'Cargo.lock is not valid UTF-8 TOML: {error}') from error
    packages = lock.get('package')
    if not isinstance(packages, list):
        raise TypeError('Cargo.lock has no package array')
    return {_lock_tuple(package) for package in packages}


def _metadata_external_tuples(
    metadata: Mapping[str, Any],
    preimage: set[tuple[str, str, str | None, str | None]],
) -> set[tuple[str, str, str | None, str | None]]:
    packages = metadata.get('packages')
    if not isinstance(packages, list):
        raise TypeError('cargo metadata has no packages array')
    by_identity: dict[
        tuple[str, str, str], set[tuple[str, str, str | None, str | None]]
    ] = {}
    for record in preimage:
        name, version, source, _checksum = record
        if source is not None:
            by_identity.setdefault((name, version, source), set()).add(record)
    resolved: set[tuple[str, str, str | None, str | None]] = set()
    for package in packages:
        if not isinstance(package, dict):
            raise TypeError('cargo metadata package is not an object')
        source = package.get('source')
        if source is None:
            continue
        name = package.get('name')
        version = package.get('version')
        if (
            not isinstance(source, str)
            or not isinstance(name, str)
            or not isinstance(version, str)
        ):
            raise TypeError('cargo metadata package identity is malformed')
        candidates = by_identity.get((name, version, source), set())
        if len(candidates) != 1:
            raise AssertionError(
                'cargo metadata resolved external package absent or ambiguous in '
                f'Cargo.lock preimage: {(name, version, source)!r}'
            )
        resolved.update(candidates)
    return resolved


def _required_source_records(
    metadata: Mapping[str, Any], *, root: Path | None = None
) -> list[dict[str, Any]]:
    packages = metadata.get('packages')
    if not isinstance(packages, list):
        raise TypeError('cargo metadata has no packages array')
    expected = (
        ('geographiclib-rs', '0.2.7', REGISTRY_SOURCE, ('accurate',)),
        # H3 and PROJ are path packages in the standalone sdist; their exact
        # manifest paths and selected features attest the bundled sources.
        ('h3o', '0.10.0', None, ('std',)),
        ('proj-sys', '0.27.0', None, ('bundled_proj', 'tiff')),
    )
    records: list[dict[str, Any]] = []
    for name, version, source, features in expected:
        matches = [
            package
            for package in packages
            if isinstance(package, dict)
            and package.get('name') == name
            and (version is None or package.get('version') == version)
            and package.get('source') == source
        ]
        if len(matches) != 1:
            raise AssertionError(
                f'cargo metadata must resolve exactly one required {name} '
                f'from {source!r}; found {len(matches)}'
            )
        package = matches[0]
        if source is None and root is not None:
            manifest_path = package.get('manifest_path')
            expected_manifest = root / 'gometry' / 'native' / name / 'Cargo.toml'
            if (
                not isinstance(manifest_path, str)
                or Path(manifest_path).resolve() != expected_manifest.resolve()
            ):
                raise AssertionError(
                    f'{name} did not resolve from the bundled source path'
                )
        package_id = package.get('id')
        if not isinstance(package_id, str):
            raise TypeError(f'cargo metadata {name} has no package id')
        resolve = metadata.get('resolve')
        if not isinstance(resolve, dict) or not isinstance(resolve.get('nodes'), list):
            raise TypeError('cargo metadata resolve graph is malformed')
        nodes = [
            node
            for node in resolve['nodes']
            if isinstance(node, dict) and node.get('id') == package_id
        ]
        if len(nodes) != 1:
            raise AssertionError(
                f'cargo metadata has no unique feature node for {name}'
            )
        selected = nodes[0].get('features')
        if not isinstance(selected, list) or not all(
            isinstance(feature, str) for feature in selected
        ):
            raise AssertionError(f'cargo metadata feature list for {name} is malformed')
        selected_features = set(selected)
        if name == 'h3o':
            # Cargo metadata unions features from the workspace's implicit
            # h3o path member with the gometry dependency edge. The member's
            # default feature is only the std alias; attest the consumer edge
            # separately and normalize that workspace-only alias.
            h3o_default = package.get('features')
            if (
                not isinstance(h3o_default, dict)
                or h3o_default.get('default') != ['std']
            ):
                raise AssertionError(
                    'vendored h3o default feature must remain the std alias'
                )
            consumers = [
                candidate
                for candidate in packages
                if isinstance(candidate, dict)
                and candidate.get('name') == 'gometry'
            ]
            if root is not None:
                expected_consumer = (root / 'gometry' / 'Cargo.toml').resolve()
                consumers = [
                    candidate
                    for candidate in consumers
                    if isinstance(candidate.get('manifest_path'), str)
                    and Path(candidate['manifest_path']).resolve() == expected_consumer
                ]
            if len(consumers) != 1:
                raise AssertionError('cargo metadata has no unique gometry manifest')
            h3o_edges = [
                dependency
                for dependency in consumers[0].get('dependencies', [])
                if isinstance(dependency, dict) and dependency.get('name') == 'h3o'
            ]
            if len(h3o_edges) != 1 or h3o_edges[0].get(
                'uses_default_features'
            ) is not False or h3o_edges[0].get('features') != ['std']:
                raise AssertionError('gometry must select only the vendored h3o std feature')
            if root is not None:
                h3o_path = h3o_edges[0].get('path')
                expected_h3o = (root / 'gometry' / 'native' / 'h3o').resolve()
                if (
                    not isinstance(h3o_path, str)
                    or Path(h3o_path).resolve() != expected_h3o
                ):
                    raise AssertionError(
                        'gometry must use the bundled h3o package path'
                    )
            selected_features.discard('default')
        expected_features = set(features)
        if selected_features != expected_features:
            raise AssertionError(
                f'{name} feature selection changed: '
                f'{sorted(selected_features)} != {sorted(expected_features)}'
            )
        records.append({
            'name': name,
            'version': package.get('version'),
            'source': source,
            'features': sorted(selected_features),
        })
    return records


def _verify_extracted_sdist(root: Path, *, require_normalized: bool) -> dict[str, Any]:
    lock_path = root / 'Cargo.lock'
    try:
        preimage_bytes = lock_path.read_bytes()
    except OSError as error:
        raise AssertionError(f'cannot read extracted Cargo.lock: {error}') from error
    preimage = _lock_tuples(preimage_bytes)
    # This is deliberately the first Cargo call. The bytes and records above
    # are the authority against which Cargo's standalone-workspace pruning is
    # checked.
    metadata = _cargo_metadata(root, locked=False)
    normalized_bytes = lock_path.read_bytes()
    normalized = _lock_tuples(normalized_bytes)
    external_preimage = {record for record in preimage if record[2] is not None}
    external_normalized = {record for record in normalized if record[2] is not None}
    if unexpected := sorted(external_normalized - external_preimage):
        raise AssertionError(
            'Cargo metadata introduced external packages while normalizing Cargo.lock: '
            f'{unexpected}'
        )
    resolved = _metadata_external_tuples(metadata, preimage)
    if unexpected := sorted(resolved - external_preimage):
        raise AssertionError(
            'cargo metadata resolved external packages absent from Cargo.lock preimage: '
            f'{unexpected}'
        )
    required_sources = _required_source_records(metadata, root=root)
    if require_normalized and preimage_bytes != normalized_bytes:
        raise AssertionError(
            'sdist Cargo.lock is not normalized: '
            f'{_sha256_bytes(preimage_bytes)} != {_sha256_bytes(normalized_bytes)}'
        )
    source_digests = source_input_digests(root / 'gometry')
    return {
        'preimage_lock_bytes': preimage_bytes,
        'preimage_lock_sha256': _sha256_bytes(preimage_bytes),
        'normalized_lock_bytes': normalized_bytes,
        'normalized_lock_sha256': _sha256_bytes(normalized_bytes),
        'metadata': metadata,
        'required_sources': required_sources,
        **source_digests,
    }


def _verify_locked_sdist(root: Path, *, expected_lock: bytes) -> dict[str, Any]:
    lock_path = root / 'Cargo.lock'
    before = lock_path.read_bytes()
    if before != expected_lock:
        raise AssertionError(
            'final sdist Cargo.lock differs before locked metadata check'
        )
    metadata = _cargo_metadata(root, locked=True)
    after = lock_path.read_bytes()
    if after != before:
        raise AssertionError('cargo metadata --locked mutated Cargo.lock')
    source_digests = source_input_digests(root / 'gometry')
    return {
        'normalized_lock_bytes': after,
        'normalized_lock_sha256': _sha256_bytes(after),
        'metadata': metadata,
        'required_sources': _required_source_records(metadata, root=root),
        **source_digests,
    }


def verify_sdist_dependency_sources(
    path: Path, *, require_normalized: bool = False
) -> dict[str, Any]:
    """Verify source identities before and after standalone Cargo normalization."""
    with tempfile.TemporaryDirectory(prefix='gometry-sdist-') as tmp:
        extracted = Path(tmp)
        with tarfile.open(path) as archive:
            _extract_sdist(archive, extracted)
        roots = [item for item in extracted.iterdir() if item.is_dir()]
        if len(roots) != 1:
            raise AssertionError(f'expected one extracted sdist root, found {roots!r}')
        root = roots[0]
        evidence = _verify_extracted_sdist(root, require_normalized=require_normalized)
        normalized = evidence['normalized_lock_bytes']
        if not isinstance(normalized, bytes):
            raise TypeError('Cargo lock normalization did not return bytes')
        (root / 'Cargo.lock').write_bytes(normalized)
        locked = _verify_locked_sdist(root, expected_lock=normalized)
        return {
            **evidence,
            'locked_metadata': locked['metadata'],
        }


def _unpack_native(
    fmt: str, payload: bytes, offset: int, artifact: Path
) -> tuple[Any, ...]:
    size = struct.calcsize(fmt)
    if offset < 0 or offset + size > len(payload):
        raise AssertionError(
            f'{artifact} native extension has a truncated inspectable header'
        )
    return struct.unpack_from(fmt, payload, offset)


def _native_name(payload: bytes) -> str:
    return payload.split(b'\0', 1)[0].decode('ascii', errors='replace')


def _elf_sections(payload: bytes, artifact: Path) -> list[str]:
    if len(payload) < 16:
        raise AssertionError(f'{artifact} native ELF header is truncated')
    elf_class = payload[4]
    data_encoding = payload[5]
    byte_order = {
        (1, 1): '<',
        (1, 2): '>',
        (2, 1): '<',
        (2, 2): '>',
    }.get((elf_class, data_encoding))
    if byte_order is None:
        raise AssertionError(f'{artifact} native ELF header is unsupported')
    if elf_class == 2:
        section_offset = _unpack_native(f'{byte_order}Q', payload, 40, artifact)[0]
        section_entry_size = _unpack_native(f'{byte_order}H', payload, 58, artifact)[0]
        section_count = _unpack_native(f'{byte_order}H', payload, 60, artifact)[0]
        section_strings = _unpack_native(f'{byte_order}H', payload, 62, artifact)[0]
        section_format = f'{byte_order}IIQQQQIIQQ'
    else:
        section_offset = _unpack_native(f'{byte_order}I', payload, 32, artifact)[0]
        section_entry_size = _unpack_native(f'{byte_order}H', payload, 46, artifact)[0]
        section_count = _unpack_native(f'{byte_order}H', payload, 48, artifact)[0]
        section_strings = _unpack_native(f'{byte_order}H', payload, 50, artifact)[0]
        section_format = f'{byte_order}IIIIIIIIII'
    minimum_entry_size = struct.calcsize(section_format)
    if section_offset == 0 or section_entry_size < minimum_entry_size:
        raise AssertionError(f'{artifact} native ELF has no inspectable section table')
    first = _unpack_native(
        section_format,
        payload,
        section_offset,
        artifact,
    )
    if section_count == 0:
        section_count = first[5]
    if section_strings == 0xFFFF:
        section_strings = first[6]
    if section_strings >= section_count:
        raise AssertionError(f'{artifact} native ELF section strings are invalid')
    section_headers = [
        _unpack_native(
            section_format,
            payload,
            section_offset + index * section_entry_size,
            artifact,
        )
        for index in range(section_count)
    ]
    string_header = section_headers[section_strings]
    string_offset = string_header[4]
    string_size = string_header[5]
    if string_offset + string_size > len(payload):
        raise AssertionError(f'{artifact} native ELF strings are truncated')
    strings = payload[string_offset : string_offset + string_size]
    names: list[str] = []
    for header in section_headers:
        name_offset = header[0]
        if name_offset >= len(strings):
            raise AssertionError(f'{artifact} native ELF section name is invalid')
        names.append(_native_name(strings[name_offset:]))
    return names


def _macho_sections(payload: bytes, artifact: Path) -> list[str]:
    magic = payload[:4]
    thin = {
        b'\xce\xfa\xed\xfe': ('<', 32),
        b'\xfe\xed\xfa\xce': ('>', 32),
        b'\xcf\xfa\xed\xfe': ('<', 64),
        b'\xfe\xed\xfa\xcf': ('>', 64),
    }
    if magic in thin:
        endian, bits = thin[magic]
        header_size = 32 if bits == 64 else 28
        command_count = _unpack_native(f'{endian}I', payload, 16, artifact)[0]
        command_size = _unpack_native(f'{endian}I', payload, 20, artifact)[0]
        command_end = header_size + command_size
        if command_end > len(payload):
            raise AssertionError(f'{artifact} native Mach-O commands are truncated')
        sections: list[str] = []
        cursor = header_size
        segment_command = 0x19 if bits == 64 else 0x1
        integer = f'{endian}I'
        for _ in range(command_count):
            command, size = _unpack_native(f'{endian}II', payload, cursor, artifact)
            if size < 8 or cursor + size > command_end:
                raise AssertionError(f'{artifact} native Mach-O command is invalid')
            if command == segment_command:
                segment_name = _native_name(payload[cursor + 8 : cursor + 24])
                section_count_offset = cursor + (64 if bits == 64 else 48)
                section_start = cursor + (72 if bits == 64 else 56)
                section_size = 80 if bits == 64 else 68
                section_count = _unpack_native(
                    integer, payload, section_count_offset, artifact
                )[0]
                if segment_name == '__DWARF':
                    sections.append('__DWARF')
                for index in range(section_count):
                    section_offset = section_start + index * section_size
                    if section_offset + section_size > cursor + size:
                        raise AssertionError(
                            f'{artifact} native Mach-O sections are truncated'
                        )
                    sections.append(
                        _native_name(payload[section_offset : section_offset + 16])
                    )
            cursor += size
        return sections
    fat = {
        b'\xca\xfe\xba\xbe': ('>', 20),
        b'\xbe\xba\xfe\xca': ('<', 20),
        b'\xca\xfe\xba\xbf': ('>', 32),
        b'\xbf\xba\xfe\xca': ('<', 32),
    }
    if magic not in fat:
        raise AssertionError(f'{artifact} native Mach-O header is unsupported')
    endian, entry_size = fat[magic]
    integer = f'{endian}I'
    archive_count = _unpack_native(integer, payload, 4, artifact)[0]
    sections = []
    for index in range(archive_count):
        entry_offset = 8 + index * entry_size
        if entry_size == 20:
            slice_offset, slice_size = _unpack_native(
                f'{endian}IIIII', payload, entry_offset, artifact
            )[2:4]
        else:
            slice_offset, slice_size = _unpack_native(
                f'{endian}IIQQII', payload, entry_offset, artifact
            )[2:4]
        if slice_offset + slice_size > len(payload):
            raise AssertionError(f'{artifact} native Mach-O slice is truncated')
        sections.extend(
            _macho_sections(payload[slice_offset : slice_offset + slice_size], artifact)
        )
    return sections


def _pe_sections(payload: bytes, artifact: Path) -> list[str]:
    if len(payload) < 64:
        raise AssertionError(f'{artifact} native PE header is truncated')
    signature_offset = _unpack_native('<I', payload, 0x3C, artifact)[0]
    signature = _unpack_native('4s', payload, signature_offset, artifact)[0]
    if signature != b'PE\0\0':
        raise AssertionError(f'{artifact} native PE signature is invalid')
    coff = signature_offset + 4
    section_count = _unpack_native('<H', payload, coff + 2, artifact)[0]
    optional_size = _unpack_native('<H', payload, coff + 16, artifact)[0]
    optional = coff + 20
    optional_magic = _unpack_native('<H', payload, optional, artifact)[0]
    directory_offset = {0x10B: 96, 0x20B: 112}.get(optional_magic)
    if directory_offset is None:
        raise AssertionError(f'{artifact} native PE optional header is unsupported')
    debug_directory = _unpack_native(
        '<II',
        payload,
        optional + directory_offset + 6 * 8,
        artifact,
    )
    if debug_directory[1]:
        raise AssertionError(f'{artifact} native extension contains PE debug data')
    sections_start = optional + optional_size
    return [
        _native_name(
            _unpack_native('8s', payload, sections_start + index * 40, artifact)[0]
        )
        for index in range(section_count)
    ]


def _verify_native_debug_sections(payload: bytes, artifact: Path) -> None:
    if payload.startswith(b'\x7fELF'):
        sections = _elf_sections(payload, artifact)
        format_name = 'ELF'
        debug = lambda name: (
            name.startswith(('.debug', '.zdebug'))
            or name in {'.gnu_debugdata', '.gnu_debuglink', '.gdb_index'}
        )
    elif payload[:4] in {
        b'\xce\xfa\xed\xfe',
        b'\xfe\xed\xfa\xce',
        b'\xcf\xfa\xed\xfe',
        b'\xfe\xed\xfa\xcf',
        b'\xca\xfe\xba\xbe',
        b'\xbe\xba\xfe\xca',
        b'\xca\xfe\xba\xbf',
        b'\xbf\xba\xfe\xca',
    }:
        sections = _macho_sections(payload, artifact)
        format_name = 'Mach-O'
        debug = lambda name: (
            name == '__DWARF' or name.startswith(('__debug', '__zdebug'))
        )
    elif payload.startswith(b'MZ'):
        sections = _pe_sections(payload, artifact)
        format_name = 'PE'
        debug = lambda name: name.startswith('.debug')
    else:
        raise AssertionError(
            f'{artifact} native extension format has no inspectable debug sections'
        )
    if forbidden := sorted(name for name in sections if debug(name)):
        raise AssertionError(
            f'{artifact} native {format_name} extension contains debug sections: '
            f'{forbidden[:20]}'
        )


def inspect_wheel(path: Path) -> str:
    extension_suffixes = ('.so', '.pyd', '.dylib')
    with zipfile.ZipFile(path) as wheel:
        member_names = wheel.namelist()
        if len(member_names) != len(set(member_names)):
            raise AssertionError(f'wheel contains duplicate members: {path}')
        names = set(member_names)
        text_members = {
            name: wheel.read(name).decode('utf-8', errors='ignore')
            for name in names
            if _is_text_member(name)
        }
        binary_members = {
            name: wheel.read(name)
            for name in names
            if name.startswith('gometry/_lib') and name.endswith(extension_suffixes)
        }
    package = {name for name in names if name.startswith('gometry/')}
    dist_info = {name for name in names if '.dist-info/' in name}
    has_extension = bool(binary_members)
    required = {'gometry/_lib.pyi', 'gometry/py.typed'}
    missing = sorted(required - names)
    required_licenses = {
        'LICENSE-APACHE.md',
        'LICENSE-MIT.md',
        'LICENSE-THIRD-PARTY.md',
    }
    bundled_licenses = {Path(name).name for name in dist_info if '/licenses/' in name}
    pth_files = [name for name in names if name.endswith('.pth')]
    generated_members = sorted(
        name
        for name in names
        if '__pycache__' in Path(name).parts or name.endswith(('.pyc', '.pyo'))
    )
    if not has_extension:
        raise AssertionError('wheel does not contain gometry/_lib native extension')
    # Cargo cannot identify release-dist from PROFILE; the native artifact's
    # absence of debug sections is the inspectable publication property.
    for name, payload in binary_members.items():
        _verify_native_debug_sections(payload, Path(f'{path}!{name}'))
    if missing:
        raise AssertionError(f'wheel missing required files: {missing}')
    if missing_licenses := sorted(required_licenses - bundled_licenses):
        raise AssertionError(f'wheel missing dist-info licenses: {missing_licenses}')
    third_party = [
        name for name in dist_info if name.endswith('/licenses/LICENSE-THIRD-PARTY.md')
    ]
    if len(third_party) != 1:
        raise AssertionError(f'expected one third-party license file: {third_party!r}')
    third_party_text = text_members[third_party[0]]
    verify_native_notice_texts(third_party_text, path)
    if generated_members:
        raise AssertionError(
            f'wheel contains generated Python bytecode: {generated_members[:20]}'
        )
    if any(
        forbidden in payload
        for payload in binary_members.values()
        for forbidden in (b'x86-64-v2', b'x86_64-v2')
    ):
        raise AssertionError('wheel native extension contains an x86-64-v2 marker')
    if leaks := _content_leaks(text_members, binary_members):
        raise AssertionError(f'wheel contents leak absolute build paths: {leaks}')
    if pth_files and not package:
        raise AssertionError(f'wheel is .pth-only: {pth_files}')
    metadata_names = [name for name in dist_info if name.endswith('/METADATA')]
    if len(metadata_names) != 1:
        raise AssertionError(f'expected one wheel METADATA file: {metadata_names!r}')
    metadata = text_members[metadata_names[0]]
    version_line = next(
        (line for line in metadata.splitlines() if line.startswith('Version: ')),
        '',
    )
    metadata_version = version_line.removeprefix('Version: ')
    filename_parts = path.name.split('-', 2)
    filename_version = filename_parts[1] if len(filename_parts) == 3 else ''
    if not metadata_version or metadata_version != filename_version:
        raise AssertionError(
            'wheel filename/metadata version mismatch: '
            f'{filename_version!r} != {metadata_version!r}'
        )
    return metadata_version


def _wheel_tags(path: Path) -> list[str]:
    with zipfile.ZipFile(path) as wheel:
        tags = [
            line.removeprefix('Tag: ').strip()
            for name in wheel.namelist()
            if name.endswith('/WHEEL')
            for line in wheel.read(name).decode('utf-8').splitlines()
            if line.startswith('Tag: ')
        ]
    if not tags:
        raise AssertionError(f'wheel has no WHEEL Tag records: {path}')
    return tags


def _platform_architecture(platform_tag: str) -> str:
    normalized = platform_tag.replace('-', '_').lower()
    if normalized.endswith(('_x86_64', '_amd64')):
        return 'x86_64'
    if normalized.endswith(('_aarch64', '_arm64')):
        return 'arm64'
    if normalized.endswith(('_i686', '_x86')):
        return 'x86'
    return normalized.rsplit('_', 1)[-1]


def _wheel_architecture(path: Path) -> str:
    architectures = {
        _platform_architecture(tag.split('-', 2)[2])
        for tag in _wheel_tags(path)
        if len(tag.split('-', 2)) == 3
    }
    if len(architectures) != 1:
        raise AssertionError(
            f'wheel has no unique architecture identity: {path}: {architectures}'
        )
    return next(iter(architectures))


def _wheel_target_compatible(path: Path, target: str) -> bool:
    target = target.lower()
    for tag in _wheel_tags(path):
        parts = tag.split('-', 2)
        if len(parts) != 3:
            continue
        platform_tag = parts[2].replace('-', '_').lower()
        if platform_tag.startswith(('manylinux_', 'musllinux_')):
            if '-linux-' not in target:
                return False
            if platform_tag.startswith('musllinux_') != ('-linux-musl' in target):
                return False
        elif platform_tag.startswith('macosx_'):
            if not target.endswith('-apple-darwin'):
                return False
        elif platform_tag.startswith('win_'):
            if '-windows-' not in target:
                return False
    return True


def _platform_matches(identifier: str, wheel_platform: str) -> bool:
    identifier = identifier.replace('-', '_')
    wheel_platform = wheel_platform.replace('-', '_')
    if identifier == wheel_platform:
        return True
    family, separator, architecture = identifier.partition('_')
    return bool(
        separator
        and family in {'manylinux', 'musllinux', 'macosx'}
        and wheel_platform.startswith(f'{family}_')
        and wheel_platform.endswith(f'_{architecture}')
    )


def _wheel_matches_identifier(path: Path, identifier: str) -> bool:
    match = IDENTIFIER.fullmatch(identifier)
    if match is None:
        raise AssertionError(f'invalid cibuildwheel identifier: {identifier!r}')
    expected_python = match.group('python')
    expected_platform = match.group('platform')
    for tag in _wheel_tags(path):
        parts = tag.split('-', 2)
        if (
            len(parts) == 3
            and parts[0] == expected_python
            and _platform_matches(expected_platform, parts[2])
        ):
            return True
    return False


def _read_identifiers(path: Path) -> list[str]:
    identifiers = [
        line.strip()
        for line in path.read_text(encoding='utf-8').splitlines()
        if line.strip()
    ]
    try:
        return _read_identifiers_from_values(identifiers)
    except AssertionError as error:
        raise AssertionError(f'{error}: {path}') from error


def _match_wheels_to_identifiers(
    wheels: Sequence[Path], identifiers: Sequence[str]
) -> dict[str, Path]:
    if len(wheels) != len(identifiers):
        raise AssertionError(
            f'wheel count does not match cibuildwheel plan: '
            f'{len(wheels)} != {len(identifiers)}'
        )
    matches: dict[str, list[Path]] = {
        identifier: [
            wheel for wheel in wheels if _wheel_matches_identifier(wheel, identifier)
        ]
        for identifier in identifiers
    }
    missing = sorted(identifier for identifier, values in matches.items() if not values)
    duplicate = {
        identifier: [wheel.name for wheel in values]
        for identifier, values in matches.items()
        if len(values) > 1
    }
    if missing or duplicate:
        details = []
        if missing:
            details.append(f'missing={missing}')
        if duplicate:
            details.append(f'duplicate={duplicate}')
        raise AssertionError(
            'wheel/identifier mapping is not one-to-one: ' + ', '.join(details)
        )
    reverse: dict[Path, list[str]] = {}
    for identifier, values in matches.items():
        reverse.setdefault(values[0], []).append(identifier)
    if ambiguous := {
        wheel.name: identifiers
        for wheel, identifiers in reverse.items()
        if len(identifiers) > 1
    }:
        raise AssertionError(f'wheel matches multiple build identifiers: {ambiguous}')
    return {identifier: values[0] for identifier, values in matches.items()}


def _receipt_build_identity(wheels: Sequence[Path]) -> dict[str, Any]:
    policy = _effective_release_policy()
    for wheel in wheels:
        actual_architecture = _wheel_architecture(wheel)
        if actual_architecture != policy['architecture']:
            raise AssertionError(
                f'wheel architecture does not match release target: '
                f'{wheel.name}: {actual_architecture} != {policy["architecture"]}'
            )
        if not _wheel_target_compatible(wheel, policy['target']):
            raise AssertionError(
                f'wheel target does not match release target: '
                f'{wheel.name}: {policy["target"]}'
            )
    return {
        'target': policy['target'],
        'architecture': policy['architecture'],
        'target_policy': policy['target_policy'],
        'rustflags': list(policy['rustflags']),
        'cflags': list(policy['cflags']),
        'cxxflags': list(policy['cxxflags']),
    }


def _validate_build_identity(
    value: Any, *, field: str = 'build', strict: bool = False
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TypeError(f'{field} identity is not an object')
    expected_fields = {
        'target',
        'architecture',
        'target_policy',
        'rustflags',
        'cflags',
        'cxxflags',
    }
    if strict and set(value) != expected_fields:
        raise AssertionError(f'{field} identity fields are not exact')
    target = value.get('target')
    if not isinstance(target, str):
        raise TypeError(f'{field}.target is malformed')
    expected = release_target_policy(target)
    if (
        value.get('architecture') != expected['architecture']
        or value.get('target_policy') != expected['target_policy']
        or value.get('rustflags') != expected['rustflags']
        or value.get('cflags') != expected['cflags']
        or value.get('cxxflags') != expected['cxxflags']
    ):
        raise AssertionError(f'{field} release flags are not target policy')
    return {
        'target': expected['target'],
        'architecture': expected['architecture'],
        'target_policy': expected['target_policy'],
        'rustflags': list(expected['rustflags']),
        'cflags': list(expected['cflags']),
        'cxxflags': list(expected['cxxflags']),
    }


def _validate_runner_target(runner: str, build: Mapping[str, Any]) -> None:
    expected = RUNNER_TARGETS.get(runner)
    if expected is not None and build['target'] != expected:
        raise AssertionError(
            f'runner {runner!r} has an unexpected release target: '
            f'{build["target"]!r} != {expected!r}'
        )


def _validate_manifest_strings(value: Any, *, field: str = 'manifest') -> None:
    if isinstance(value, str):
        if ABSOLUTE_BUILD_PATH.search(value):
            raise AssertionError(f'{field} contains an absolute host path')
        return
    if isinstance(value, dict):
        for key, child in value.items():
            _validate_manifest_strings(child, field=f'{field}.{key}')
        return
    if isinstance(value, list):
        for index, child in enumerate(value):
            _validate_manifest_strings(child, field=f'{field}[{index}]')


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AssertionError(f'cannot read JSON manifest {path}: {error}') from error
    if not isinstance(value, dict):
        raise TypeError(f'JSON manifest is not an object: {path}')
    _validate_manifest_strings(value)
    return value


def _validate_source_identity(value: Any, *, field: str = 'source') -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TypeError(f'{field} identity is not an object')
    expected_fields = {
        'commit',
        'filename',
        'sha256',
        'version',
        'source_inputs_format',
        'source_inputs_sha256',
        'native_source_inputs_format',
        'native_source_inputs_sha256',
        'cargo',
        'toolchain',
    }
    if set(value) != expected_fields:
        raise AssertionError(
            f'{field} identity fields are not exact: '
            f'{sorted(set(value) ^ expected_fields)}'
        )
    commit = value.get('commit')
    if not isinstance(commit, str) or re.fullmatch(r'[0-9a-f]{40,64}', commit) is None:
        raise AssertionError(f'{field} identity has no reviewed source commit')
    filename = value.get('filename')
    digest = value.get('sha256')
    version = value.get('version')
    if (
        not isinstance(filename, str)
        or Path(filename).name != filename
        or not filename.endswith('.tar.gz')
        or not isinstance(digest, str)
        or HEX_DIGEST.fullmatch(digest) is None
        or not isinstance(version, str)
        or not version
    ):
        raise AssertionError(f'{field} sdist identity is malformed')
    for key in (
        'source_inputs_sha256',
        'native_source_inputs_sha256',
    ):
        value_digest = value.get(key)
        if (
            not isinstance(value_digest, str)
            or HEX_DIGEST.fullmatch(value_digest) is None
        ):
            raise AssertionError(f'{field} has an invalid {key}')
    if value.get('source_inputs_format') != SOURCE_INPUTS_FORMAT:
        raise AssertionError(f'{field} has an unsupported source-input format')
    if value.get('native_source_inputs_format') != SOURCE_INPUTS_FORMAT:
        raise AssertionError(f'{field} has an unsupported native source-input format')
    cargo = value.get('cargo')
    if not isinstance(cargo, dict):
        raise TypeError(f'{field} Cargo identity is missing')
    for key in ('lock_preimage_sha256', 'lock_normalized_sha256'):
        lock_digest = cargo.get(key)
        if (
            not isinstance(lock_digest, str)
            or HEX_DIGEST.fullmatch(lock_digest) is None
        ):
            raise AssertionError(f'{field}.cargo has an invalid {key}')
    required = cargo.get('required_sources')
    if (
        not isinstance(required, list)
        or not required
        or not all(isinstance(record, dict) for record in required)
    ):
        raise AssertionError(f'{field}.cargo required source identity is malformed')
    toolchain = value.get('toolchain')
    if not isinstance(toolchain, dict):
        raise TypeError(f'{field} toolchain identity is missing')
    if toolchain.get('channel') != _package_toolchain_channel():
        raise AssertionError(
            f'{field} toolchain channel does not match the checkout toolchain'
        )
    if toolchain.get('maturin') != MATURIN_VERSION:
        raise AssertionError(f'{field} maturin identity is not pinned')
    if toolchain.get('maturin_pep517_args') != RELEASE_MATURIN_ARGS:
        raise AssertionError(f'{field} Maturin release profile is not pinned')
    for key in ('cargo', 'rustc'):
        tool = toolchain.get(key)
        if not isinstance(tool, str) or not tool:
            raise AssertionError(f'{field}.toolchain has no {key} identity')
    return value


def _source_identity(manifest: Mapping[str, Any]) -> dict[str, Any]:
    sdist = manifest['sdist']
    return {
        'commit': manifest['commit'],
        'filename': sdist['filename'],
        'sha256': sdist['sha256'],
        'version': sdist['version'],
        'source_inputs_format': manifest['source_inputs_format'],
        'source_inputs_sha256': manifest['source_inputs_sha256'],
        'native_source_inputs_format': manifest['native_source_inputs_format'],
        'native_source_inputs_sha256': manifest['native_source_inputs_sha256'],
        'cargo': manifest['cargo'],
        'toolchain': manifest['toolchain'],
    }


def validate_source_manifest(sdist: Path, manifest_path: Path) -> dict[str, Any]:
    if (
        manifest_path.parent != sdist.parent
        or manifest_path.name != SOURCE_MANIFEST_NAME
    ):
        raise AssertionError('source manifest must be the sibling canonical manifest')
    manifest = _load_json(manifest_path)
    if manifest.get('schema') != 2 or manifest.get('kind') != 'gometry-source-manifest':
        raise AssertionError(f'unsupported gometry source manifest: {manifest_path}')
    if manifest.get('package') != 'gometry':
        raise AssertionError(f'source manifest is not for gometry: {manifest_path}')
    source = manifest.get('sdist')
    if not isinstance(source, dict):
        raise TypeError('source manifest has no sdist record')
    if (
        source.get('filename') != sdist.name
        or not isinstance(source.get('filename'), str)
        or Path(str(source.get('filename'))).name != source.get('filename')
    ):
        raise AssertionError(
            f'source manifest filename mismatch: {source.get("filename")!r} != {sdist.name!r}'
        )
    expected = source.get('sha256')
    if not isinstance(expected, str) or HEX_DIGEST.fullmatch(expected) is None:
        raise AssertionError('source manifest has an invalid sdist SHA-256')
    actual = sha256_file(sdist)
    if actual != expected:
        raise AssertionError(f'sdist SHA-256 mismatch: {actual} != {expected}')
    if source.get('version') != source.get('filename', '').removeprefix(
        'gometry-'
    ).removesuffix('.tar.gz'):
        raise AssertionError(
            'source manifest sdist version does not match its filename'
        )
    _validate_source_identity({
        'commit': manifest.get('commit'),
        'filename': source.get('filename'),
        'sha256': source.get('sha256'),
        'version': source.get('version'),
        'source_inputs_format': manifest.get('source_inputs_format'),
        'source_inputs_sha256': manifest.get('source_inputs_sha256'),
        'native_source_inputs_format': manifest.get('native_source_inputs_format'),
        'native_source_inputs_sha256': manifest.get('native_source_inputs_sha256'),
        'cargo': manifest.get('cargo'),
        'toolchain': manifest.get('toolchain'),
    })
    cargo = manifest.get('cargo')
    if not isinstance(cargo, dict):
        raise TypeError('source manifest has no cargo evidence')
    for key in ('lock_preimage_sha256', 'lock_normalized_sha256'):
        value = cargo.get(key)
        if not isinstance(value, str) or HEX_DIGEST.fullmatch(value) is None:
            raise AssertionError(f'source manifest has an invalid {key}')
    prepared = manifest.get('prepared_inputs')
    if not isinstance(prepared, dict):
        raise TypeError('source manifest has no prepared input identity')
    identity = prepared.get('identity_sha256')
    files = prepared.get('files')
    if not isinstance(identity, str) or HEX_DIGEST.fullmatch(identity) is None:
        raise AssertionError('source manifest has an invalid prepared input identity')
    if not isinstance(files, dict) or not files:
        raise AssertionError('source manifest has no prepared input hashes')
    for name, digest in files.items():
        if not isinstance(name, str) or not isinstance(digest, str):
            raise TypeError('source manifest prepared input hashes are malformed')
        if HEX_DIGEST.fullmatch(digest) is None:
            raise AssertionError(f'source manifest has an invalid hash for {name}')
    if _prepared_input_identity(files) != identity:
        raise AssertionError(
            'source manifest prepared input identity does not match files'
        )
    generated = manifest.get('generated_output_hashes')
    if generated != files:
        raise AssertionError(
            'source manifest generated output hashes do not match inputs'
        )
    return manifest


def verify_source_artifact(sdist: Path, manifest_path: Path) -> dict[str, Any]:
    manifest = validate_source_manifest(sdist, manifest_path)
    prepared = manifest['prepared_inputs']['files']
    with tarfile.open(sdist) as archive:
        member_names = archive.getnames()
        if len(member_names) != len(set(member_names)):
            raise AssertionError(f'source sdist contains duplicate members: {sdist}')
        roots = sorted({
            Path(member.name).parts[0]
            for member in archive.getmembers()
            if member.name and not Path(member.name).is_absolute()
        })
        if len(roots) != 1:
            raise AssertionError(f'source sdist has no unique root: {sdist}')
        package_root = Path(roots[0]) / 'gometry'
        if f'{package_root.as_posix()}/Cargo.toml' not in member_names:
            raise AssertionError('source sdist has no unique gometry package root')
        for relative, expected in prepared.items():
            member = archive.extractfile(f'{roots[0]}/{relative}')
            if member is None:
                raise AssertionError(
                    f'source sdist is missing prepared output: {relative}'
                )
            actual = _sha256_bytes(member.read())
            if actual != expected:
                raise AssertionError(
                    f'source prepared output hash mismatch: {relative}'
                )
    evidence = verify_sdist_dependency_sources(sdist, require_normalized=True)
    cargo = manifest['cargo']
    if (
        cargo['lock_normalized_sha256'] != evidence['normalized_lock_sha256']
        or cargo['required_sources'] != evidence['required_sources']
    ):
        raise AssertionError('source manifest Cargo evidence does not match the sdist')
    for key in ('source_inputs_sha256', 'native_source_inputs_sha256'):
        if manifest[key] != evidence[key]:
            raise AssertionError(
                f'source manifest {key} does not match the sdist build inputs'
            )
    return manifest


def _smoke_grid(destination: Path) -> Path:
    configured = os.environ.get(SMOKE_GRID_ENV)
    if configured:
        path = Path(configured)
        if not path.is_file():
            raise AssertionError(f'{SMOKE_GRID_ENV} does not name a file: {path}')
        return path
    archive_path = ROOT / 'native' / 'proj-sys' / 'PROJSRC' / 'proj-9.6.2.tar.gz'
    member_name = 'proj-9.6.2/data/tests/test_vgrid_pixelispoint.tif'
    destination.mkdir(parents=True, exist_ok=True)
    path = destination / Path(member_name).name
    try:
        with tarfile.open(archive_path) as archive:
            member = archive.extractfile(member_name)
            if member is None:
                raise AssertionError(
                    f'PROJ fixture is missing from {archive_path}: {member_name}'
                )
            path.write_bytes(member.read())
    except (OSError, tarfile.TarError) as error:
        raise AssertionError(
            f'could not extract valid PROJ TIFF fixture: {error}'
        ) from error
    return path


def _package_version(root: Path = ROOT) -> str:
    try:
        cargo = tomllib.loads((root / 'Cargo.toml').read_text(encoding='utf-8'))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise AssertionError(f'cannot read package Cargo.toml: {error}') from error
    package = cargo.get('package')
    version = package.get('version') if isinstance(package, dict) else None
    if not isinstance(version, str) or not version:
        raise AssertionError(
            f'package Cargo.toml has no version: {root / "Cargo.toml"}'
        )
    return version


def _smoke_environment(
    *, expected_source_manifest: Path | None = None
) -> dict[str, str]:
    if expected_source_manifest is not None:
        source_manifest = validate_source_manifest(
            _source_sdist_from_manifest(expected_source_manifest),
            expected_source_manifest,
        )
        expected_source_inputs = source_manifest['source_inputs_sha256']
        expected_native_inputs = source_manifest['native_source_inputs_sha256']
        expected_version = source_manifest['sdist']['version']
    else:
        digests = source_input_digests()
        expected_source_inputs = digests['source_inputs_sha256']
        expected_native_inputs = digests['native_source_inputs_sha256']
        expected_version = _package_version()
    policy = _effective_release_policy()
    values = {
        SMOKE_EXPECTED_SOURCE_INPUTS_ENV: expected_source_inputs,
        SMOKE_EXPECTED_NATIVE_SOURCE_INPUTS_ENV: expected_native_inputs,
        SMOKE_EXPECTED_VERSION_ENV: expected_version,
        SMOKE_EXPECTED_TARGET_ENV: policy['target'],
        SMOKE_EXPECTED_ARCHITECTURE_ENV: policy['architecture'],
    }
    env = os.environ.copy()
    env.update(values)
    return env


def smoke_installed(*, expected_source_manifest: Path | None = None) -> None:
    with tempfile.TemporaryDirectory(prefix='gometry-installed-smoke-') as tmp:
        temporary = Path(tmp)
        neutral = temporary / 'neutral'
        neutral.mkdir()
        grid = _smoke_grid(temporary / 'grid')
        env = _smoke_environment(expected_source_manifest=expected_source_manifest)
        env.pop('PYTHONPATH', None)
        env.pop('PYTHONHOME', None)
        env[SMOKE_GRID_ENV] = str(grid)
        run(
            [sys.executable, '-W', 'error', '-c', textwrap.dedent(SMOKE)],
            cwd=neutral,
            env=env,
        )


def write_runner_receipt(
    wheel_dir: Path,
    identifiers_path: Path,
    source_manifest_path: Path,
    *,
    receipt_path: Path | None = None,
    runner: str | None = None,
) -> Path:
    wheels = sorted(wheel_dir.glob('gometry-*.whl'))
    if not wheels:
        raise AssertionError(f'no gometry wheels found in {wheel_dir}')
    identifiers = _read_identifiers(identifiers_path)
    matches = _match_wheels_to_identifiers(wheels, identifiers)
    source_manifest = validate_source_manifest(
        _source_sdist_from_manifest(source_manifest_path),
        source_manifest_path,
    )
    build_identity = _receipt_build_identity(wheels)
    records = []
    for identifier in identifiers:
        wheel = matches[identifier]
        inspect_wheel(wheel)
        records.append({
            'identifier': identifier,
            'filename': wheel.name,
            'sha256': sha256_file(wheel),
        })
    if receipt_path is None:
        receipt_path = wheel_dir / RUNNER_RECEIPT_NAME
    if receipt_path.parent != wheel_dir:
        raise AssertionError('wheel receipt must be a sibling of the wheel archives')
    value = {
        'schema': 2,
        'kind': 'gometry-wheel-receipt',
        'profile': RELEASE_PROFILE,
        'runner': runner or os.environ.get('GOMETRY_BUILD_RUNNER', 'local'),
        'source': _source_identity(source_manifest),
        'build': build_identity,
        'planned_identifiers': identifiers,
        'wheels': records,
    }
    _write_json(receipt_path, value)
    validate_runner_receipt(
        receipt_path,
        wheel_dir,
        source_manifest_path,
        inspect_archives=False,
    )
    return receipt_path


def _source_sdist_from_manifest(manifest_path: Path) -> Path:
    manifest = _load_json(manifest_path)
    source = manifest.get('sdist')
    filename = source.get('filename') if isinstance(source, dict) else None
    if (
        not isinstance(filename, str)
        or Path(filename).name != filename
        or not filename.endswith('.tar.gz')
    ):
        raise TypeError(f'source manifest has no sdist filename: {manifest_path}')
    path = manifest_path.parent / filename
    if not path.is_file():
        raise AssertionError(f'source sdist named by manifest is missing: {path}')
    return path


def validate_runner_receipt(
    receipt_path: Path,
    wheel_dir: Path,
    source_manifest_path: Path,
    *,
    inspect_archives: bool = True,
) -> dict[str, Any]:
    receipt = _load_json(receipt_path)
    if receipt.get('schema') != 2 or receipt.get('kind') != 'gometry-wheel-receipt':
        raise AssertionError(f'unsupported gometry wheel receipt: {receipt_path}')
    if receipt.get('profile') != RELEASE_PROFILE:
        raise AssertionError(
            f'wheel receipt profile is not {RELEASE_PROFILE}: {receipt_path}'
        )
    runner = receipt.get('runner')
    if not isinstance(runner, str) or not runner:
        raise AssertionError(f'wheel receipt has no runner: {receipt_path}')
    source_manifest = validate_source_manifest(
        _source_sdist_from_manifest(source_manifest_path), source_manifest_path
    )
    source = receipt.get('source')
    expected_source = _source_identity(source_manifest)
    if source != expected_source:
        raise AssertionError(f'wheel receipt source identity mismatch: {receipt_path}')
    build = _validate_build_identity(
        receipt.get('build'),
        field='wheel receipt build',
        strict=True,
    )
    _validate_runner_target(runner, build)
    identifiers = receipt.get('planned_identifiers')
    wheels = receipt.get('wheels')
    if not isinstance(identifiers, list) or not all(
        isinstance(value, str) for value in identifiers
    ):
        raise AssertionError(f'wheel receipt identifiers are malformed: {receipt_path}')
    if not isinstance(wheels, list) or len(wheels) != len(identifiers):
        raise AssertionError(
            f'wheel receipt wheel set does not match plan: {receipt_path}'
        )
    planned = _read_identifiers_from_values(identifiers)
    files = sorted(wheel_dir.glob('gometry-*.whl'))
    if len(files) != len(planned):
        raise AssertionError(
            f'wheel receipt does not cover wheel directory: {receipt_path}'
        )
    actual_files = {path.name for path in wheel_dir.iterdir()}
    expected_files = {path.name for path in files} | {receipt_path.name}
    if actual_files != expected_files:
        raise AssertionError(
            f'wheel directory contains files outside its receipt: '
            f'{sorted(actual_files - expected_files)}'
        )
    matches = _match_wheels_to_identifiers(files, planned)
    listed: dict[str, dict[str, Any]] = {}
    for item in wheels:
        if not isinstance(item, dict):
            raise TypeError(f'wheel receipt contains a malformed wheel: {receipt_path}')
        identifier = item.get('identifier')
        filename = item.get('filename')
        digest = item.get('sha256')
        if (
            not isinstance(identifier, str)
            or not isinstance(filename, str)
            or not isinstance(digest, str)
            or HEX_DIGEST.fullmatch(digest) is None
        ):
            raise AssertionError(
                f'wheel receipt contains invalid wheel identity: {receipt_path}'
            )
        if identifier in listed:
            raise AssertionError(
                f'wheel receipt contains duplicate identifier: {identifier}'
            )
        listed[identifier] = item
    if set(listed) != set(planned):
        raise AssertionError(
            f'wheel receipt identifiers do not match plan: {receipt_path}'
        )
    for identifier, wheel in matches.items():
        item = listed[identifier]
        if item['filename'] != wheel.name:
            raise AssertionError(f'wheel receipt filename mismatch for {identifier}')
        if item['sha256'] != sha256_file(wheel):
            raise AssertionError(f'wheel SHA-256 mismatch for {wheel.name}')
        if _wheel_architecture(wheel) != build['architecture']:
            raise AssertionError(
                f'wheel receipt architecture mismatch for {wheel.name}'
            )
        if not _wheel_target_compatible(wheel, build['target']):
            raise AssertionError(f'wheel receipt target mismatch for {wheel.name}')
        if inspect_archives:
            inspect_wheel(wheel)
    return receipt


def _read_identifiers_from_values(values: Sequence[str]) -> list[str]:
    result = list(values)
    if not result or len(result) != len(set(result)):
        raise AssertionError('cibuildwheel identifier plan is empty or duplicated')
    for identifier in result:
        if IDENTIFIER.fullmatch(identifier) is None:
            raise AssertionError(f'invalid cibuildwheel identifier: {identifier!r}')
    return result


def _receipt_paths(runners_dir: Path) -> list[Path]:
    return sorted(runners_dir.rglob(RUNNER_RECEIPT_NAME))


def _clear_directory(directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    for child in directory.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()


def assemble_release(
    output_dir: Path,
    source_dir: Path,
    runners_dir: Path,
    expected_runners: Sequence[str],
) -> Path:
    """Validate every runner receipt and assemble the publishable set."""
    expected = list(expected_runners)
    if not expected or len(expected) != len(set(expected)):
        raise AssertionError('expected gometry build runners are empty or duplicated')
    _clear_directory(output_dir)
    source_manifest_path = source_dir / SOURCE_MANIFEST_NAME
    sdist = _source_sdist_from_manifest(source_manifest_path)
    source_entries = {path.name for path in source_dir.iterdir()}
    if source_entries != {SOURCE_MANIFEST_NAME, sdist.name}:
        raise AssertionError(
            f'gometry source artifact contains unexpected files: '
            f'{sorted(source_entries - {SOURCE_MANIFEST_NAME, sdist.name})}'
        )
    source_manifest = verify_source_artifact(sdist, source_manifest_path)
    expected_directories = {f'gometry-wheels-{runner}' for runner in expected}
    actual_directories = {path.name for path in runners_dir.iterdir() if path.is_dir()}
    if actual_directories != expected_directories:
        raise AssertionError(
            f'gometry runner artifact directories mismatch: '
            f'missing={sorted(expected_directories - actual_directories)}, '
            f'extra={sorted(actual_directories - expected_directories)}'
        )
    unexpected_files = {path.name for path in runners_dir.iterdir() if path.is_file()}
    if unexpected_files:
        raise AssertionError(
            f'gometry runner artifact root contains files: {sorted(unexpected_files)}'
        )
    receipts = _receipt_paths(runners_dir)
    if len(receipts) != len(expected):
        raise AssertionError(
            f'runner receipt count does not match expected runners: '
            f'{len(receipts)} != {len(expected)}'
        )
    by_runner: dict[str, tuple[Path, dict[str, Any]]] = {}
    for receipt_path in receipts:
        receipt = validate_runner_receipt(
            receipt_path,
            receipt_path.parent,
            source_manifest_path,
        )
        runner = receipt['runner']
        if receipt_path.parent.name != f'gometry-wheels-{runner}':
            raise AssertionError(
                f'runner receipt directory does not identify {runner}: {receipt_path}'
            )
        if runner in by_runner:
            raise AssertionError(f'duplicate gometry runner receipt: {runner}')
        by_runner[runner] = (receipt_path, receipt)
    actual_runners = set(by_runner)
    if actual_runners != set(expected):
        raise AssertionError(
            f'gometry runner set mismatch: missing={sorted(set(expected) - actual_runners)}, '
            f'extra={sorted(actual_runners - set(expected))}'
        )

    wheels: list[dict[str, Any]] = []
    copied_names = {sdist.name}
    shutil.copyfile(sdist, output_dir / sdist.name)
    for runner in expected:
        receipt_path, receipt = by_runner[runner]
        for item in receipt['wheels']:
            filename = item['filename']
            if filename in copied_names:
                raise AssertionError(
                    f'duplicate release distribution filename: {filename}'
                )
            source_wheel = receipt_path.parent / filename
            if not source_wheel.is_file():
                raise AssertionError(
                    f'wheel named by receipt is missing: {source_wheel}'
                )
            shutil.copyfile(source_wheel, output_dir / filename)
            copied_names.add(filename)
            wheels.append({
                'identifier': item['identifier'],
                'filename': filename,
                'sha256': item['sha256'],
                'runner': runner,
                'target': receipt['build']['target'],
                'architecture': receipt['build']['architecture'],
                'target_policy': receipt['build']['target_policy'],
                'rustflags': receipt['build']['rustflags'],
                'cflags': receipt['build']['cflags'],
                'cxxflags': receipt['build']['cxxflags'],
            })
    manifest = {
        'schema': 2,
        'kind': 'gometry-release-manifest',
        'package': 'gometry',
        'profile': RELEASE_PROFILE,
        'source': _source_identity(source_manifest),
        'expected_runners': expected,
        'wheels': wheels,
    }
    manifest_path = output_dir / RELEASE_MANIFEST_NAME
    _write_json(manifest_path, manifest)
    validate_release_manifest(manifest_path, output_dir, expected_runners=expected)
    return manifest_path


def validate_release_manifest(
    manifest_path: Path,
    distribution_dir: Path,
    *,
    expected_runners: Sequence[str] | None = None,
) -> dict[str, Any]:
    if (
        manifest_path.parent != distribution_dir
        or manifest_path.name != RELEASE_MANIFEST_NAME
    ):
        raise AssertionError('release manifest must be the sibling canonical manifest')
    manifest = _load_json(manifest_path)
    if (
        manifest.get('schema') != 2
        or manifest.get('kind') != 'gometry-release-manifest'
    ):
        raise AssertionError(f'unsupported gometry release manifest: {manifest_path}')
    if manifest.get('package') != 'gometry':
        raise AssertionError(f'release manifest is not for gometry: {manifest_path}')
    if manifest.get('profile') != RELEASE_PROFILE:
        raise AssertionError(
            f'release manifest profile is not {RELEASE_PROFILE}: {manifest_path}'
        )
    runners = manifest.get('expected_runners')
    if not isinstance(runners, list) or not all(
        isinstance(runner, str) and runner for runner in runners
    ):
        raise AssertionError('release manifest expected runners are malformed')
    if len(runners) != len(set(runners)):
        raise AssertionError('release manifest expected runners are duplicated')
    if expected_runners is not None and set(runners) != set(expected_runners):
        raise AssertionError(
            'release manifest expected runner set does not match resolve output'
        )
    source = _validate_source_identity(
        manifest.get('source'),
        field='release source',
    )
    source_filename = source['filename']
    source_digest = source['sha256']
    wheels = manifest.get('wheels')
    if not isinstance(wheels, list) or not wheels:
        raise AssertionError('release manifest has no wheels')
    filenames = {source_filename}
    seen_identifiers: set[str] = set()
    seen_runners: set[str] = set()
    for item in wheels:
        if not isinstance(item, dict):
            raise TypeError('release manifest wheel record is malformed')
        filename = item.get('filename')
        digest = item.get('sha256')
        identifier = item.get('identifier')
        runner = item.get('runner')
        if (
            not isinstance(filename, str)
            or not filename.endswith('.whl')
            or Path(filename).name != filename
            or not isinstance(digest, str)
            or HEX_DIGEST.fullmatch(digest) is None
            or not isinstance(identifier, str)
            or not isinstance(runner, str)
        ):
            raise AssertionError('release manifest wheel identity is malformed')
        if filename in filenames:
            raise AssertionError(f'duplicate release manifest filename: {filename}')
        if identifier in seen_identifiers:
            raise AssertionError(f'duplicate release manifest identifier: {identifier}')
        if runner not in runners:
            raise AssertionError(
                f'release manifest names an unexpected runner: {runner}'
            )
        build = _validate_build_identity(item, field=f'release wheel {filename}')
        _validate_runner_target(runner, build)
        if build['target'] != item.get('target'):
            raise AssertionError(
                f'release wheel target identity is malformed: {filename}'
            )
        filenames.add(filename)
        seen_identifiers.add(identifier)
        seen_runners.add(runner)
        path = distribution_dir / filename
        if not path.is_file():
            raise AssertionError(f'release wheel is missing: {path}')
        if sha256_file(path) != digest:
            raise AssertionError(f'release wheel SHA-256 mismatch: {filename}')
        if not _wheel_matches_identifier(path, identifier):
            raise AssertionError(
                f'release wheel does not match its build identifier: {filename} / {identifier}'
            )
        if _wheel_architecture(path) != build['architecture']:
            raise AssertionError(f'release wheel architecture mismatch: {filename}')
        if not _wheel_target_compatible(path, build['target']):
            raise AssertionError(f'release wheel target mismatch: {filename}')
        inspect_wheel(path)
    if seen_runners != set(runners):
        raise AssertionError(
            f'release manifest does not cover every runner: '
            f'missing={sorted(set(runners) - seen_runners)}'
        )
    sdist = distribution_dir / source_filename
    if not sdist.is_file():
        raise AssertionError(f'release sdist is missing: {sdist}')
    if sha256_file(sdist) != source_digest:
        raise AssertionError(f'release sdist SHA-256 mismatch: {source_filename}')
    sdist_version = inspect_sdist(
        sdist,
        expected_libtiff=libtiff_source_members(),
    )
    if sdist_version != source['version']:
        raise AssertionError(
            'release manifest sdist version does not match its archive'
        )
    evidence = verify_sdist_dependency_sources(sdist, require_normalized=True)
    if (
        evidence['source_inputs_sha256'] != source['source_inputs_sha256']
        or evidence['native_source_inputs_sha256']
        != source['native_source_inputs_sha256']
        or evidence['normalized_lock_sha256']
        != source['cargo']['lock_normalized_sha256']
        or evidence['required_sources'] != source['cargo']['required_sources']
    ):
        raise AssertionError(
            'release manifest source identity does not match its sdist'
        )
    actual_files = {path.name for path in distribution_dir.iterdir()}
    expected_files = filenames | {manifest_path.name}
    if actual_files != expected_files:
        raise AssertionError(
            f'release distribution directory mismatch: '
            f'missing={sorted(expected_files - actual_files)}, '
            f'extra={sorted(actual_files - expected_files)}'
        )
    return manifest


SMOKE = r"""
import importlib.metadata
import json
import os
import platform
from pathlib import Path
import sys
import sysconfig

import numpy as np
import gometry as gm
import gometry._lib as lib

distribution_version = importlib.metadata.version('gometry')
metadata = importlib.metadata.metadata('gometry')
assert metadata['Name'].lower() == 'gometry'
assert gm.__version__ == distribution_version
assert distribution_version and distribution_version[0].isdigit()
assert distribution_version == os.environ['GOMETRY_SMOKE_EXPECTED_VERSION']

build_info = getattr(lib, '__build_info__', None)
assert isinstance(build_info, dict)
expected_build_info_fields = {
    'schema_version',
    'source_inputs_format',
    'source_inputs_sha256',
    'native_source_inputs_format',
    'native_source_inputs_sha256',
    'profile_family',
    'opt_level',
    'debug_assertions',
    'target',
    'target_arch',
    'target_features',
}
assert set(build_info) == expected_build_info_fields, set(build_info)
expected_build_info = {
    'schema_version': 3,
    'source_inputs_format': 'sha256-path-nul-content-nul-v5',
    'source_inputs_sha256': os.environ['GOMETRY_SMOKE_EXPECTED_SOURCE_INPUTS_SHA256'],
    'native_source_inputs_format': 'sha256-path-nul-content-nul-v5',
    'native_source_inputs_sha256': os.environ[
        'GOMETRY_SMOKE_EXPECTED_NATIVE_SOURCE_INPUTS_SHA256'
    ],
    'profile_family': 'release',
    'opt_level': '3',
    'debug_assertions': False,
    'target': os.environ['GOMETRY_SMOKE_EXPECTED_TARGET'],
    'target_arch': os.environ['GOMETRY_SMOKE_EXPECTED_ARCHITECTURE'],
}
for key, expected in expected_build_info.items():
    assert build_info.get(key) == expected, (key, build_info.get(key), expected)
target_features = build_info.get('target_features')
assert isinstance(target_features, str)
if expected_build_info['target_arch'] == 'x86_64':
    forbidden_features = {
        'avx', 'avx2', 'bmi1', 'bmi2', 'fma', 'popcnt', 'sse4.1', 'sse4.2'
    }
    assert not forbidden_features.intersection(
        set(target_features.split(','))
    ), target_features

machine = platform.machine().lower()
machine_architecture = (
    'x86_64' if machine in {'x86_64', 'amd64'} else
    'arm64' if machine in {'aarch64', 'arm64'} else machine
)
assert machine_architecture == expected_build_info['target_arch']
extension_suffix = sysconfig.get_config_var('EXT_SUFFIX')
assert isinstance(extension_suffix, str) and extension_suffix
assert str(Path(lib.__file__)).endswith(extension_suffix)

if sysconfig.get_config_var('Py_GIL_DISABLED'):
    assert not sys._is_gil_enabled(), 'gometry re-enabled the GIL'

optional = {'pyarrow', 'pandas', 'polars', 'geopandas', 'lonboard'}
loaded_optional = sorted(optional & set(sys.modules))
assert loaded_optional == [], loaded_optional

point = gm.Point(21.0, 52.0, crs=4326)
other = gm.Point(22.0, 52.0, crs=4326)
projected = point.to_crs(3857)
assert projected.crs == 'EPSG:3857'
assert point.buffer(1000.0).area > 0
assert gm.distance(point, other) > 0
info = gm.CRS(4326).info
assert info['authority'] == 'EPSG' and info['code'] == '4326'
assert gm.crs_grid('null')['available'] is True
transformed = gm.crs_transform(4326, 3857, 21.0, 52.0)
assert np.all(np.isfinite(transformed))

wkb = point.to_wkb(include_srid=True)
assert gm.from_wkb(wkb).crs == 'EPSG:4326'
assert gm.from_wkt(point.to_wkt(), crs=4326).crs == 'EPSG:4326'
geojson = json.loads(point.to_geojson())
assert gm.from_geojson(geojson).to_wkt() == point.to_wkt()

grid = Path(os.environ['GOMETRY_SMOKE_GRID'])
assert grid.is_file()
gm.crs_configure(search_paths=[grid.parent])
operation = (
    '+proj=pipeline +step +proj=unitconvert +xy_in=deg +xy_out=rad '
    f'+step +proj=vgridshift +grids={grid.name} +multiplier=1'
)
grid_result = gm.crs_apply(operation, 4.5, 52.5, 0.0)
np.testing.assert_allclose(grid_result, (np.deg2rad(4.5), np.deg2rad(52.5), 11.5))

typed_empty = gm.GeometryArray([gm.Point()])
assert typed_empty[0].is_empty and typed_empty.is_empty.tolist() == [True]
missing = gm.GeometryArray([point, None])
assert missing.is_missing.tolist() == [False, True]
assert missing[1] is None

mask = gm.contains_xy(gm.box(20, 51, 22, 53), [21.0, 30.0], [52.0, 52.0])
assert isinstance(mask, np.ndarray)
assert mask.flags.writeable is False
np.testing.assert_array_equal(mask, [True, False])
coords = point.coords
assert coords.x.flags.writeable is False and coords.y.flags.writeable is False
assert np.asarray(coords).flags.writeable is False
bounds = gm.GeometryArray([point]).bounds
assert bounds.flags.writeable is False
"""


def smoke_install(wheel: Path, *, expected_source_manifest: Path | None = None) -> None:
    with tempfile.TemporaryDirectory(prefix='gometry-wheel-smoke-') as tmp:
        temporary = Path(tmp)
        venv_dir = temporary / 'venv'
        venv.EnvBuilder(with_pip=True).create(venv_dir)
        python = venv_dir / ('Scripts/python.exe' if os.name == 'nt' else 'bin/python')
        run([str(python), '-m', 'pip', 'install', str(wheel)])
        neutral = temporary / 'neutral'
        neutral.mkdir()
        grid = _smoke_grid(temporary / 'grid')
        env = _smoke_environment(expected_source_manifest=expected_source_manifest)
        env.pop('PYTHONPATH', None)
        env.pop('PYTHONHOME', None)
        env[SMOKE_GRID_ENV] = str(grid)
        run(
            [str(python), '-W', 'error', '-c', textwrap.dedent(SMOKE)],
            cwd=neutral,
            env=env,
        )


def inspect_artifact(path: Path) -> None:
    if path.name.endswith('.whl'):
        inspect_wheel(path)
    elif path.name.endswith('.tar.gz'):
        inspect_sdist(path)
        verify_sdist_dependency_sources(path)
    else:
        raise AssertionError(f'unsupported release artifact: {path}')


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        '--inspect',
        nargs='+',
        type=Path,
        metavar='ARTIFACT',
        help='inspect existing wheel/sdist artifacts without rebuilding',
    )
    mode.add_argument(
        '--installed',
        action='store_true',
        help='run the functional smoke against the active installed wheel',
    )
    mode.add_argument(
        '--smoke-wheel',
        type=Path,
        metavar='WHEEL',
        help='inspect, clean-install, and functionally smoke one existing wheel',
    )
    mode.add_argument(
        '--smoke-sdist',
        type=Path,
        metavar='SDIST',
        help='inspect an sdist, rebuild its wheel, and smoke the clean install',
    )
    mode.add_argument(
        '--inspect-libtiff-sdist',
        type=Path,
        metavar='SDIST',
        help='inspect representative libtiff members without requiring generated licenses',
    )
    mode.add_argument(
        '--stage-sdist',
        type=Path,
        metavar='DIRECTORY',
        help='stage one source-verified sdist and its sibling manifest',
    )
    mode.add_argument(
        '--verify-source',
        nargs=2,
        type=Path,
        metavar=('SDIST', 'MANIFEST'),
        help='verify a staged sdist against its source manifest',
    )
    mode.add_argument(
        '--write-wheel-receipt',
        nargs=3,
        type=Path,
        metavar=('WHEEL_DIR', 'IDENTIFIERS', 'SOURCE_MANIFEST'),
        help='validate planned wheels and write one runner receipt',
    )
    mode.add_argument(
        '--assemble-release',
        nargs=3,
        type=Path,
        metavar=('OUTPUT_DIR', 'SOURCE_DIR', 'RUNNERS_DIR'),
        help='validate runner receipts and assemble release distributions',
    )
    mode.add_argument(
        '--verify-release',
        type=Path,
        metavar='DIRECTORY',
        help='verify a complete gometry release distribution directory',
    )
    parser.add_argument(
        '--expected-runners',
        type=Path,
        metavar='JSON',
        help='JSON array of expected runners for --assemble-release',
    )
    parser.add_argument(
        '--expected-source-manifest',
        type=Path,
        metavar='JSON',
        help='staged source manifest whose identity the installed smoke must load',
    )
    args = parser.parse_args()
    if args.installed:
        expected_source_manifest = (
            args.expected_source_manifest.resolve()
            if args.expected_source_manifest is not None
            else None
        )
        smoke_installed(expected_source_manifest=expected_source_manifest)
        print('installed wheel smoke OK')
        return 0
    if args.inspect:
        for path in args.inspect:
            path = path.resolve()
            if path.name.endswith('.tar.gz'):
                inspect_sdist(path, expected_libtiff=libtiff_source_members())
                verify_sdist_dependency_sources(path)
                print(f'artifact inspection OK: {path.name}')
                continue
            inspect_artifact(path)
            print(f'artifact inspection OK: {path.name}')
        return 0
    if args.smoke_wheel:
        wheel = args.smoke_wheel.resolve()
        inspect_wheel(wheel)
        expected_source_manifest = (
            args.expected_source_manifest.resolve()
            if args.expected_source_manifest is not None
            else None
        )
        smoke_install(wheel, expected_source_manifest=expected_source_manifest)
        print(f'wheel smoke OK: {wheel.name}')
        return 0
    if args.smoke_sdist:
        sdist = args.smoke_sdist.resolve()
        with tempfile.TemporaryDirectory(prefix='gometry-sdist-smoke-') as tmp:
            wheel = build_wheel_from_sdist(
                sdist,
                Path(tmp),
                env=release_build_env(),
            )
            smoke_install(wheel)
        print(f'sdist rebuild smoke OK: {sdist.name}')
        return 0
    if args.inspect_libtiff_sdist:
        sdist = args.inspect_libtiff_sdist.resolve()
        inspect_libtiff_sdist(sdist)
        print(f'libtiff sdist inspection OK: {sdist.name}')
        return 0
    if args.stage_sdist:
        path = stage_sdist(args.stage_sdist)
        print(path)
        return 0
    if args.verify_source:
        sdist, manifest = (path.resolve() for path in args.verify_source)
        verify_source_artifact(sdist, manifest)
        inspect_sdist(sdist, expected_libtiff=libtiff_source_members())
        print(f'source artifact verification OK: {sdist.name}')
        return 0
    if args.write_wheel_receipt:
        wheel_dir, identifiers, source_manifest = (
            path.resolve() for path in args.write_wheel_receipt
        )
        receipt = write_runner_receipt(
            wheel_dir,
            identifiers,
            source_manifest,
        )
        print(receipt)
        return 0
    if args.assemble_release:
        if args.expected_runners is None:
            parser.error('--assemble-release requires --expected-runners JSON')
        expected_text = args.expected_runners.read_text(encoding='utf-8')
        try:
            expected_value = json.loads(expected_text)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            parser.error(f'cannot read --expected-runners JSON: {error}')
        expected_runners = (
            expected_value
            if isinstance(expected_value, list)
            else expected_value.get('runners')
            if isinstance(expected_value, dict)
            else None
        )
        if not isinstance(expected_runners, list) or not all(
            isinstance(runner, str) for runner in expected_runners
        ):
            parser.error(
                '--expected-runners JSON must contain a `runners` string array'
            )
        output_dir, source_dir, runners_dir = (
            path.resolve() for path in args.assemble_release
        )
        manifest = assemble_release(
            output_dir,
            source_dir,
            runners_dir,
            expected_runners,
        )
        print(manifest)
        return 0
    if args.verify_release:
        directory = args.verify_release.resolve()
        manifest = directory / RELEASE_MANIFEST_NAME
        validate_release_manifest(manifest, directory)
        print(f'release artifact verification OK: {directory}')
        return 0

    with tempfile.TemporaryDirectory(prefix='gometry-wheel-build-') as tmp:
        wheel = build_wheel(Path(tmp))
        inspect_wheel(wheel)
        smoke_install(wheel)
        print(f'wheel smoke OK: {wheel.name}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
