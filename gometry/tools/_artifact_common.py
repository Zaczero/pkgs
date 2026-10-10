"""Shared artifact identity, path and process primitives."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import shlex
import shutil
import subprocess
import sys
import sysconfig
import tempfile
import tomllib
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
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
RUNNER_RECEIPT_NAME = 'gometry-wheel-receipt.json'
RELEASE_MANIFEST_NAME = 'gometry-release-manifest.json'
RELEASE_BUILD_INPUTS = (
    'Cargo.toml',
    'Cargo.lock',
    '.cargo/config.toml',
    'rust-toolchain.toml',
    'gometry/Cargo.toml',
    'gometry/pyproject.toml',
    'gometry/rust-toolchain.toml',
    'gometry/native/proj-sys/PROJSRC/proj-9.6.2.tar.gz',
)
ABSOLUTE_BUILD_PATH = re.compile(
    r'(?i)(?:/home/[^/\s]+/|/Users/[^/\s]+/|/nix/store/|/tmp/|'
    r'/private/tmp/|/var/folders/|[a-z]:\\(?:Users|Temp)\\)'
)
SOURCE_TOOL_MEMBERS = frozenset({
    'tools/_artifact_common.py',
    'tools/artifact_content.py',
    'tools/quality_runtime.py',
    'tools/release_artifact.py',
    'tools/runtime_artifact.py',
    'tools/source_artifact.py',
    'tools/wheel_smoke.py',
})
PREPARED_OUTPUT_FILES = (
    'README.md',
    'LICENSE-APACHE.md',
    'LICENSE-MIT.md',
    'LICENSE-THIRD-PARTY.md',
    'python/gometry/_lib.pyi',
)
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
    """Run a command with live output so bounded preparation can track progress."""
    subprocess.run(cmd, cwd=cwd, env=env, check=True)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _copy_sha256(source: Path, destination: Path) -> str:
    digest = hashlib.sha256()
    with source.open('rb') as input_stream, destination.open('wb') as output_stream:
        for chunk in iter(lambda: input_stream.read(1024 * 1024), b''):
            digest.update(chunk)
            output_stream.write(chunk)
    return digest.hexdigest()


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _maturin_command(root: Path = ROOT) -> list[str]:
    candidates = [Path(sys.executable).with_name('maturin')]
    if executable := shutil.which('maturin'):
        candidates.append(Path(executable))
    for executable in candidates:
        if not executable.is_file():
            continue
        result = subprocess.run(
            [str(executable), '--version'],
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
        )
        if (
            result.returncode == 0
            and result.stdout.strip() == f'maturin {MATURIN_VERSION}'
        ):
            return [str(executable)]
    raise AssertionError(
        f'maturin {MATURIN_VERSION} must already be available on the selected '
        'interpreter or PATH; install it through the project environment setup'
    )


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


def _tool_output(
    command: list[str],
    *,
    env: Mapping[str, str] | None = None,
    cwd: Path = ROOT,
) -> str:
    result = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode or not result.stdout.strip():
        raise AssertionError(
            f'could not identify tool output for {command!r}: {result.stderr.strip()}'
        )
    return result.stdout.strip()


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

    Rust targets x86_64 at x86-64-v2 on every operating system. Other native
    compilers use the equivalent ``-march`` spelling where meaningful; MSVC
    has no exact x86-64-v2 mode, so its C/C++ compiler default is retained.
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
        rustflags = ['-C', 'target-cpu=x86-64-v2']
        if not target.endswith('-msvc'):
            cflags = ['-march=x86-64-v2']
            cxxflags = ['-march=x86-64-v2']
            target_policy = 'x86-64-v2'
        else:
            target_policy = 'x86-64-v2-msvc-c-default'
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


def _target_policy_identity(target: str) -> dict[str, Any]:
    policy = release_target_policy(target)
    if target.endswith('-apple-darwin'):
        family = 'darwin'
    elif '-pc-windows-' in target or '-unknown-windows-' in target:
        family = 'windows-msvc'
    elif '-unknown-linux-musl' in target:
        family = 'linux-musl'
    elif '-unknown-linux-gnu' in target:
        family = 'linux-gnu'
    else:
        raise AssertionError(f'unsupported release target platform: {target!r}')
    return {
        **policy,
        'raw_target_arch': target.split('-', 1)[0],
        'platform_family': family,
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
    cargo_target = os.environ.get('CARGO_BUILD_TARGET')
    if cargo_target is not None and cargo_target != policy['target']:
        raise AssertionError(
            f'CARGO_BUILD_TARGET disagrees with release policy: '
            f'{cargo_target!r} != {policy["target"]!r}'
        )
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
    rustflags = os.environ.get('RUSTFLAGS')
    if rustflags is not None and rustflags != expected_flags['rustflags']:
        raise AssertionError(
            f'RUSTFLAGS disagrees with release policy: '
            f'{rustflags!r} != {expected_flags["rustflags"]!r}'
        )
    encoded_rustflags = os.environ.get('CARGO_ENCODED_RUSTFLAGS')
    if encoded_rustflags is not None:
        expected_encoded = '\x1f'.join(policy['rustflags'])
        if encoded_rustflags != expected_encoded:
            raise AssertionError(
                'CARGO_ENCODED_RUSTFLAGS disagrees with release policy: '
                f'{encoded_rustflags!r} != {expected_encoded!r}'
            )
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
        variable = f'{prefix}_{target_suffix}'
        value = os.environ.get(variable)
        expected = expected_flags[key]
        if value is None:
            if not present:
                continue
            raise AssertionError(
                f'{variable} is missing; release policy requires {expected!r}'
            )
        if value != expected:
            raise AssertionError(
                f'{variable} disagrees with release policy: {value!r} != {expected!r}'
            )
    return policy


def _rust_target_triple(
    root: Path = ROOT,
    *,
    environment: Mapping[str, str] | None = None,
) -> str:
    environment = os.environ if environment is None else environment
    configured = environment.get('CARGO_BUILD_TARGET', '').strip()
    if configured and (
        '-' not in configured or '/' in configured or '\\' in configured
    ):
        raise AssertionError(f'invalid CARGO_BUILD_TARGET: {configured!r}')
    if configured:
        return configured
    config_files: list[Path] = []
    for directory in (root.resolve(), *root.resolve().parents):
        config_files.extend(
            path
            for path in (
                directory / '.cargo' / 'config.toml',
                directory / '.cargo' / 'config',
            )
            if path.is_file()
        )
    cargo_home = Path(environment.get('CARGO_HOME', Path.home() / '.cargo'))
    config_files.extend(
        path
        for path in (cargo_home / 'config.toml', cargo_home / 'config')
        if path.is_file() and path not in config_files
    )
    for config_path in config_files:
        try:
            config = tomllib.loads(config_path.read_text(encoding='utf-8'))
        except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
            raise AssertionError(
                f'cannot read Cargo configuration {config_path}: {error}'
            ) from error
        build = config.get('build')
        selected = build.get('target') if isinstance(build, dict) else None
        if selected is None:
            continue
        if not isinstance(selected, str) or (
            '-' not in selected or '/' in selected or '\\' in selected
        ):
            raise AssertionError(
                f'Cargo build.target must select one Rust target triple: {selected!r}'
            )
        return selected
    result = subprocess.run(
        ['rustc', '-vV'],
        cwd=root,
        env=environment,
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
    # Replace inherited values so the selected release policy is explicit and
    # arm64 cannot inherit an x86 setting from the host.
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
    env['CARGO_BUILD_TARGET'] = target
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
            env['RUSTFLAGS'] = value
        else:
            # The native build owners apply this value to CMake when the
            # unsuffixed variable is absent. The target-specific spelling is
            # consumed by cc-based dependencies such as zlib and SQLite.
            env.pop(prefix, None)
            target_variable = f'{prefix}_{target.replace("-", "_")}'
            env[target_variable] = value
    env[f'NIX_DONT_SET_RPATH_{target.replace("-", "_")}'] = '1'
    # Keep staged-sdist builds on the checkout's configured Cargo cache.
    # Cargo resolves workspace, configuration, and relative environment paths.
    metadata = json.loads(
        subprocess.check_output(
            [
                'cargo',
                'metadata',
                '--no-deps',
                '--locked',
                '--format-version=1',
                '--manifest-path',
                str(ROOT / 'Cargo.toml'),
            ],
            cwd=ROOT,
            env=env,
            text=True,
        )
    )
    env['CARGO_TARGET_DIR'] = metadata['target_directory']
    return env


def _source_build_env() -> dict[str, str]:
    env = release_build_env()
    # The sdist command does not build Rust code. Keeping the PEP 517 options
    # off this bootstrap step makes the distinction explicit; the final wheel
    # build receives the locked distribution profile from release_build_env.
    env.pop('MATURIN_PEP517_ARGS', None)
    return env


def _prepared_output_hashes(root: Path = ROOT) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for relative in PREPARED_OUTPUT_FILES:
        path = root / relative
        if not path.is_file():
            raise AssertionError(f'prepared source output is missing: {relative}')
        hashes[relative] = sha256_file(path)
    return hashes


def _prepared_input_identity(hashes: Mapping[str, str]) -> str:
    value = ''.join(f'{name}\0{digest}\n' for name, digest in sorted(hashes.items()))
    return _sha256_bytes(value.encode('ascii'))


def _prepared_input_record(hashes: Mapping[str, str]) -> dict[str, Any]:
    files = dict(sorted(hashes.items()))
    return {
        'identity_sha256': _prepared_input_identity(files),
        'files': files,
    }


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
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


def _runtime_interpreter() -> dict[str, Any]:
    suffix = sysconfig.get_config_var('EXT_SUFFIX')
    if not isinstance(suffix, str) or not suffix:
        raise AssertionError('selected interpreter has no extension suffix')
    return {
        'pid': os.getpid(),
        'executable': sys.executable,
        'resolved_executable': str(Path(sys.executable).resolve()),
        'prefix': sys.prefix,
        'version': sys.version,
        'implementation': platform.python_implementation(),
        'cache_tag': sys.implementation.cache_tag,
        'ext_suffix': suffix,
        'gil_disabled': bool(sysconfig.get_config_var('Py_GIL_DISABLED')),
    }


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f'.{path.name}.', suffix='.tmp', dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, 'w', encoding='utf-8') as stream:
            stream.write(json.dumps(value, indent=2, sort_keys=True) + '\n')
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
