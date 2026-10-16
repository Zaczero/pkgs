"""Per-wheel CIBW evidence, release receipts and benchmark binding."""

from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import sys
import sysconfig
import tarfile
import tempfile
import textwrap
import venv
from collections.abc import Mapping
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import TYPE_CHECKING, Any

from _artifact_common import (
    HEX_DIGEST,
    IDENTIFIER,
    MATURIN_VERSION,
    MONOREPO,
    RELEASE_BUILD_INPUTS,
    RELEASE_ENV_VARS,
    RELEASE_MANIFEST_NAME,
    RELEASE_MATURIN_ARGS,
    RELEASE_PROFILE,
    ROOT,
    RUNNER_RECEIPT_NAME,
    RUNNER_TARGETS,
    _atomic_json,
    _copy_sha256,
    _effective_release_policy,
    _flag_text,
    _load_json,
    _package_toolchain_channel,
    _package_version,
    _runtime_interpreter,
    _sha256_bytes,
    _target_policy_identity,
    _tool_output,
    _write_json,
    release_build_env,
    release_target_policy,
    run,
    sha256_file,
)
from artifact_content import (
    X86_64_V2_TARGET_FEATURES,
    X86_64_V3_PLUS_TARGET_FEATURES,
    build_identifier_policy,
    inspect_wheel,
    libtiff_source_members,
    source_input_digests,
    validate_wheel_native_target,
    wheel_architecture,
    wheel_matches_identifier,
    wheel_python_members,
    wheel_target_compatible,
)
from quality_runtime import (
    quality_receipt_path,
    read_quality_receipt,
    validate_quality_producer_inputs,
)
from runtime_artifact import (
    SMOKE_EXPECTED_ARCHITECTURE_ENV,
    SMOKE_EXPECTED_HOST_ARCH_ENV,
    SMOKE_EXPECTED_NATIVE_SOURCE_INPUTS_ENV,
    SMOKE_EXPECTED_SOURCE_INPUTS_ENV,
    SMOKE_EXPECTED_TARGET_ENV,
    SMOKE_EXPECTED_VERSION_ENV,
    SMOKE_GRID_ENV,
    verify_runtime,
)
from source_artifact import (
    SOURCE_MANIFEST_NAME,
    SOURCE_MANIFEST_SCHEMA,
    extract_sdist,
    inspect_sdist,
    sdist_prepared_license_hashes,
    sdist_python_member_hashes,
    sdist_toolchain_channel,
    source_identity,
    stage_sdist,
    validate_source_identity,
    validate_source_manifest,
    verify_sdist_dependency_sources,
    verify_source_artifact,
)

if TYPE_CHECKING:
    from collections.abc import Sequence
CIBUILDWHEEL_VERSION = '4.1.0'
RUNNER_RECEIPT_SCHEMA = 4
RELEASE_MANIFEST_SCHEMA = 4
CIBW_TEST_SOURCE_FILES = (
    'gometry/tools/wheel_smoke.py',
    'gometry/tools/_artifact_common.py',
    'gometry/tools/artifact_content.py',
    'gometry/tools/source_artifact.py',
    'gometry/tools/runtime_artifact.py',
    'gometry/tools/quality_runtime.py',
    'gometry/tools/release_artifact.py',
    'gometry/tests/tooling/test_wheel_smoke.py',
)
CIBW_TEST_SOURCES = tuple(
    f'test-sources/{relative}' for relative in CIBW_TEST_SOURCE_FILES
)


def _release_build_input_hashes(project_root: Path) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for relative in RELEASE_BUILD_INPUTS:
        path = project_root / relative
        if not path.is_file():
            raise AssertionError(f'release build input is missing: {relative}')
        hashes[relative] = sha256_file(path)
    return hashes


def build_wheel_from_sdist(sdist: Path, out_dir: Path, *, env: dict[str, str]) -> Path:
    sdist_version = inspect_sdist(
        sdist,
        expected_libtiff=libtiff_source_members(),
        expected_toolchain_channel=sdist_toolchain_channel(sdist),
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
        capture_output=False,
    )
    wheels = sorted(out_dir.glob('gometry-*.whl'))
    if len(wheels) != 1:
        raise AssertionError(f'expected one gometry wheel, found {wheels!r}')
    wheel_version = inspect_wheel(wheels[0])['version']
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
    wheels: Sequence[Path],
    identifiers: Sequence[str],
    *,
    inspections: Mapping[Path, Mapping[str, Any]] | None = None,
) -> dict[str, Path]:
    if len(wheels) != len(identifiers):
        raise AssertionError(
            f'wheel count does not match cibuildwheel plan: '
            f'{len(wheels)} != {len(identifiers)}'
        )
    matches: dict[str, list[Path]] = {
        identifier: [
            wheel
            for wheel in wheels
            if wheel_matches_identifier(
                wheel,
                identifier,
                None if inspections is None else inspections[wheel]['tags'],
            )
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


def _validate_build_identity(value: Any, *, field: str = 'build') -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TypeError(f'{field} identity is not an object')
    expected_fields = {
        'target',
        'raw_target_arch',
        'architecture',
        'target_policy',
        'rustflags',
        'cflags',
        'cxxflags',
        'command',
        'profile',
        'interpreter',
        'compiler',
        'cargo',
        'maturin',
        'dependencies',
        'settings',
    }
    if set(value) != expected_fields:
        raise AssertionError(f'{field} identity fields are not exact')
    target = value.get('target')
    if not isinstance(target, str):
        raise TypeError(f'{field}.target is malformed')
    expected = release_target_policy(target)
    if (
        value.get('raw_target_arch') != target.split('-', 1)[0]
        or value.get('architecture') != expected['architecture']
        or value.get('target_policy') != expected['target_policy']
        or value.get('rustflags') != expected['rustflags']
        or value.get('cflags') != expected['cflags']
        or value.get('cxxflags') != expected['cxxflags']
    ):
        raise AssertionError(f'{field} release flags are not target policy')
    if value.get('profile') != RELEASE_PROFILE:
        raise AssertionError(f'{field} profile is not {RELEASE_PROFILE}')

    command = value.get('command')
    if not isinstance(command, dict) or set(command) != {
        'dispatched',
        'config_sha256',
        'host_log_sha256',
        'host_exit_code',
        'capture_exit_code',
    }:
        raise AssertionError(f'{field}.command fields are not exact')
    for digest_field in ('config_sha256', 'host_log_sha256'):
        digest = command.get(digest_field)
        if not isinstance(digest, str) or HEX_DIGEST.fullmatch(digest) is None:
            raise AssertionError(f'{field}.command.{digest_field} is malformed')
    for status_field in ('host_exit_code', 'capture_exit_code'):
        status = command.get(status_field)
        if not isinstance(status, int) or isinstance(status, bool) or status != 0:
            raise AssertionError(f'{field}.command.{status_field} is not successful')
    dispatched = command.get('dispatched')
    if not isinstance(dispatched, dict) or set(dispatched) != {
        'cibuildwheel_argv',
        'frontend',
        'config_settings',
    }:
        raise AssertionError(f'{field}.command.dispatched fields are not exact')
    argv = dispatched.get('cibuildwheel_argv')
    if (
        not isinstance(argv, list)
        or not argv
        or not all(isinstance(argument, str) and argument for argument in argv)
    ):
        raise AssertionError(f'{field}.command dispatch argv is malformed')
    if not any(
        f'cibuildwheel=={CIBUILDWHEEL_VERSION}' in argument for argument in argv
    ):
        raise AssertionError(f'{field}.command does not pin CIBW 4.1.0')
    if dispatched.get('frontend') != 'build[uv]' or dispatched.get(
        'config_settings'
    ) != {'MATURIN_PEP517_ARGS': RELEASE_MATURIN_ARGS}:
        raise AssertionError(f'{field}.command frontend/profile dispatch is invalid')

    interpreter = value.get('interpreter')
    if not isinstance(interpreter, dict) or set(interpreter) != {
        'executable',
        'prefix',
        'version',
        'implementation',
        'cache_tag',
        'ext_suffix',
        'gil_disabled',
    }:
        raise AssertionError(f'{field}.interpreter fields are not exact')
    for name in (
        'executable',
        'prefix',
        'version',
        'implementation',
        'cache_tag',
        'ext_suffix',
    ):
        if not isinstance(interpreter.get(name), str) or not interpreter[name]:
            raise AssertionError(f'{field}.interpreter.{name} is malformed')
    if not isinstance(interpreter.get('gil_disabled'), bool):
        raise TypeError(f'{field}.interpreter.gil_disabled is malformed')

    compiler = value.get('compiler')
    if (
        not isinstance(compiler, dict)
        or set(compiler) != {'observed_at', 'rustc_vv'}
        or compiler.get('observed_at') != 'before-build'
        or not isinstance(compiler.get('rustc_vv'), str)
        or 'rustc ' not in compiler['rustc_vv']
        or 'commit-hash:' not in compiler['rustc_vv']
    ):
        raise AssertionError(f'{field}.compiler is not a full before-build snapshot')
    cargo = value.get('cargo')
    if not isinstance(cargo, str) or not cargo.startswith('cargo '):
        raise AssertionError(f'{field}.cargo version is malformed')
    dependencies = value.get('dependencies')
    if not isinstance(dependencies, dict):
        raise TypeError(f'{field}.dependencies is malformed')

    maturin = value.get('maturin')
    maturin_fields = {
        'unrepaired_wheel_sha256',
        'wheel_member',
        'wheel_member_sha256',
        'generator',
        'repaired_wheel_member_sha256',
        'repaired_generator',
    }
    if not isinstance(maturin, dict) or set(maturin) != maturin_fields:
        raise AssertionError(f'{field}.maturin fields are not exact')
    for name in (
        'unrepaired_wheel_sha256',
        'wheel_member_sha256',
        'repaired_wheel_member_sha256',
    ):
        digest = maturin.get(name)
        if not isinstance(digest, str) or HEX_DIGEST.fullmatch(digest) is None:
            raise AssertionError(f'{field}.maturin.{name} is malformed')
    if not isinstance(maturin.get('wheel_member'), str) or not maturin['wheel_member']:
        raise AssertionError(f'{field}.maturin WHEEL member is malformed')
    if (
        maturin.get('generator') != f'maturin ({MATURIN_VERSION})'
        or maturin.get('repaired_generator') != f'maturin ({MATURIN_VERSION})'
    ):
        raise AssertionError(f'{field}.maturin Generator is not the pinned backend')

    settings = value.get('settings')
    if not isinstance(settings, dict):
        raise TypeError(f'{field}.settings is malformed')
    if settings.get('config_sha256') != command['config_sha256']:
        raise AssertionError(f'{field}.settings config hash differs from dispatch')
    for name in ('source_manifest_sha256', 'source_sdist_sha256'):
        digest = settings.get(name)
        if not isinstance(digest, str) or HEX_DIGEST.fullmatch(digest) is None:
            raise AssertionError(f'{field}.settings.{name} is malformed')
    environment = settings.get('environment')
    if not isinstance(environment, dict):
        raise TypeError(f'{field}.settings environment is missing')
    for name in RELEASE_ENV_VARS:
        if not isinstance(environment.get(name), str):
            raise TypeError(f'{field}.settings is missing {name}')
    if environment['GOMETRY_RELEASE_TARGET'] != target:
        raise AssertionError(f'{field}.settings target differs from build target')
    expected_environment = {
        'GOMETRY_RELEASE_TARGET_POLICY': expected['target_policy'],
        'GOMETRY_RELEASE_RUSTFLAGS': _flag_text(expected['rustflags']),
        'GOMETRY_RELEASE_CFLAGS': _flag_text(expected['cflags']),
        'GOMETRY_RELEASE_CXXFLAGS': _flag_text(expected['cxxflags']),
        'RUSTFLAGS': _flag_text(expected['rustflags']),
        'CARGO_ENCODED_RUSTFLAGS': '\x1f'.join(expected['rustflags']),
        f'CFLAGS_{target.replace("-", "_")}': _flag_text(expected['cflags']),
        f'CXXFLAGS_{target.replace("-", "_")}': _flag_text(expected['cxxflags']),
    }
    if any(
        environment.get(name) != value for name, value in expected_environment.items()
    ):
        raise AssertionError(f'{field}.settings flags differ from target policy')
    if environment['MATURIN_PEP517_ARGS'] != RELEASE_MATURIN_ARGS:
        raise AssertionError(f'{field}.settings Maturin profile dispatch is invalid')
    if environment.get('CARGO_BUILD_TARGET') != target:
        raise AssertionError(f'{field}.settings CARGO_BUILD_TARGET is invalid')
    if _target_policy_identity(target)['platform_family'] == 'darwin' and (
        environment.get('MACOSX_DEPLOYMENT_TARGET') != '11.0'
    ):
        raise AssertionError(f'{field}.settings macOS deployment floor is invalid')
    if not isinstance(settings.get('native'), dict):
        raise TypeError(f'{field}.settings has no native artifact facts')
    if not isinstance(settings.get('unrepaired_native'), dict):
        raise TypeError(f'{field}.settings has no unrepaired native facts')
    wheel_tags = settings.get('wheel_tags')
    if (
        not isinstance(wheel_tags, list)
        or not wheel_tags
        or not all(isinstance(tag, str) for tag in wheel_tags)
    ):
        raise AssertionError(f'{field}.settings wheel tags are malformed')
    build_inputs = settings.get('build_inputs')
    if (
        not isinstance(build_inputs, dict)
        or set(build_inputs) != set(RELEASE_BUILD_INPUTS)
        or not all(
            isinstance(digest, str) and HEX_DIGEST.fullmatch(digest) is not None
            for digest in build_inputs.values()
        )
    ):
        raise AssertionError(f'{field}.settings build-input identity is malformed')
    policy = _target_policy_identity(target)
    validate_wheel_native_target(
        {'native': settings['native'], 'tags': wheel_tags},
        policy,
        label=f'{field}.settings.native',
    )
    validate_wheel_native_target(
        {'native': settings['unrepaired_native']},
        policy,
        label=f'{field}.settings.unrepaired_native',
        validate_tags=False,
    )
    test_sources = settings.get('test_sources')
    if not isinstance(test_sources, dict) or set(test_sources) != set(
        CIBW_TEST_SOURCE_FILES
    ):
        raise AssertionError(f'{field}.settings has no CIBW test-source identity')
    if settings.get('test_source_root') != 'test-sources':
        raise AssertionError(f'{field}.settings test-source root is not normalized')
    for name, digest in test_sources.items():
        if (
            not isinstance(name, str)
            or not isinstance(digest, str)
            or HEX_DIGEST.fullmatch(digest) is None
        ):
            raise AssertionError(f'{field}.settings test-source identity is malformed')
    return {
        'target': expected['target'],
        'raw_target_arch': value['raw_target_arch'],
        'architecture': expected['architecture'],
        'target_policy': expected['target_policy'],
        'rustflags': list(expected['rustflags']),
        'cflags': list(expected['cflags']),
        'cxxflags': list(expected['cxxflags']),
        'command': command,
        'profile': RELEASE_PROFILE,
        'interpreter': interpreter,
        'compiler': compiler,
        'cargo': cargo,
        'maturin': maturin,
        'dependencies': dependencies,
        'settings': settings,
    }


def _validate_smoke_identity(
    value: Any,
    *,
    field: str,
    source: Mapping[str, Any],
    build: Mapping[str, Any],
    extension_member: str,
    extension_sha256: str,
) -> dict[str, Any]:
    expected_fields = {
        'status',
        'exit_code',
        'interpreter',
        'module',
        'source_inputs_sha256',
        'native_source_inputs_sha256',
        'distribution',
        'platform',
        'log_sha256',
    }
    if not isinstance(value, dict) or set(value) != expected_fields:
        raise AssertionError(f'{field} fields are not exact')
    if value.get('status') != 'passed' or value.get('exit_code') != 0:
        raise AssertionError(f'{field} did not pass')
    for key in ('source_inputs_sha256', 'native_source_inputs_sha256'):
        if value.get(key) != source.get(key):
            raise AssertionError(f'{field}.{key} differs from staged source')
    log_digest = value.get('log_sha256')
    if not isinstance(log_digest, str) or HEX_DIGEST.fullmatch(log_digest) is None:
        raise AssertionError(f'{field}.log_sha256 is malformed')

    interpreter = value.get('interpreter')
    expected_interpreter_fields = {
        'pid',
        'executable',
        'prefix',
        'version',
        'implementation',
        'cache_tag',
        'ext_suffix',
        'gil_disabled',
    }
    if (
        not isinstance(interpreter, dict)
        or set(interpreter) != expected_interpreter_fields
    ):
        raise AssertionError(f'{field}.interpreter fields are not exact')
    pid = interpreter.get('pid')
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        raise AssertionError(f'{field}.interpreter.pid is malformed')
    for key in (
        'executable',
        'prefix',
        'version',
        'implementation',
        'cache_tag',
        'ext_suffix',
    ):
        if not isinstance(interpreter.get(key), str) or not interpreter[key]:
            raise AssertionError(f'{field}.interpreter.{key} is malformed')
    if not isinstance(interpreter.get('gil_disabled'), bool):
        raise TypeError(f'{field}.interpreter.gil_disabled is malformed')
    producer = build['interpreter']
    for key in ('cache_tag', 'ext_suffix', 'gil_disabled'):
        if interpreter[key] != producer[key]:
            raise AssertionError(f'{field} runtime ABI differs from producer')

    module = value.get('module')
    if not isinstance(module, dict) or set(module) != {'facade', 'extension'}:
        raise AssertionError(f'{field}.module fields are not exact')
    facade = module.get('facade')
    extension = module.get('extension')
    if not isinstance(facade, dict) or set(facade) != {'member', 'sha256'}:
        raise AssertionError(f'{field}.module.facade is malformed')
    if facade.get('member') != 'gometry/__init__.py':
        raise AssertionError(f'{field}.module.facade member is not canonical')
    if (
        not isinstance(facade.get('sha256'), str)
        or HEX_DIGEST.fullmatch(facade['sha256']) is None
    ):
        raise AssertionError(f'{field}.module.facade hash is malformed')
    if (
        not isinstance(extension, dict)
        or set(extension) != {'member', 'sha256'}
        or extension.get('member') != extension_member
        or extension.get('sha256') != extension_sha256
    ):
        raise AssertionError(f'{field}.module.extension differs from repaired wheel')

    distribution = value.get('distribution')
    if not isinstance(distribution, dict) or set(distribution) != {
        'name',
        'version',
        'metadata_sha256',
    }:
        raise AssertionError(f'{field}.distribution is malformed')
    if (
        distribution.get('name') != 'gometry'
        or not isinstance(distribution.get('version'), str)
        or not distribution['version']
        or not isinstance(distribution.get('metadata_sha256'), str)
        or HEX_DIGEST.fullmatch(distribution['metadata_sha256']) is None
    ):
        raise AssertionError(f'{field}.distribution identity is invalid')

    platform_info = value.get('platform')
    expected_platform_fields = {
        'machine',
        'architecture',
        'target',
        'target_arch',
        'platform',
        'libc',
        'loader',
        'gil_disabled',
        'wheel_tags',
        'native_image',
    }
    if (
        not isinstance(platform_info, dict)
        or set(platform_info) != expected_platform_fields
        or platform_info.get('target') != build['target']
        or platform_info.get('target_arch') != build['raw_target_arch']
        or platform_info.get('architecture') != build['architecture']
        or platform_info.get('wheel_tags') != build['settings']['wheel_tags']
        or platform_info.get('gil_disabled') != interpreter['gil_disabled']
        or not isinstance(platform_info.get('machine'), str)
        or not isinstance(platform_info.get('platform'), str)
        or not isinstance(platform_info.get('libc'), (list, tuple))
        or not isinstance(platform_info.get('loader'), dict)
        or not isinstance(platform_info.get('wheel_tags'), list)
        or not isinstance(platform_info.get('native_image'), dict)
    ):
        raise AssertionError(f'{field}.platform identity is invalid')
    loader = platform_info['loader']
    if set(loader) != {'runtime_libc', 'loader_paths', 'libc_paths'}:
        raise AssertionError(f'{field}.platform loader identity is malformed')
    family = _target_policy_identity(build['target'])['platform_family']
    expected_libc = (
        family.removeprefix('linux-')
        if family.startswith('linux-')
        else 'not-applicable'
    )
    if (
        loader.get('runtime_libc') != expected_libc
        or not isinstance(loader.get('loader_paths'), list)
        or not isinstance(loader.get('libc_paths'), list)
        or not all(isinstance(path, str) for path in loader['loader_paths'])
        or not all(isinstance(path, str) for path in loader['libc_paths'])
        or (
            expected_libc in {'gnu', 'musl'}
            and (not loader['loader_paths'] or not loader['libc_paths'])
        )
        or (
            expected_libc == 'not-applicable'
            and (loader['loader_paths'] or loader['libc_paths'])
        )
    ):
        raise AssertionError(
            f'{field}.platform loader/libc differs from the build target'
        )
    for name, prefix in (
        ('loader_paths', 'loader'),
        ('libc_paths', 'libc'),
    ):
        if any(
            len(parts := PurePosixPath(path).parts) != 2
            or parts[0] != prefix
            or not parts[1]
            for path in loader[name]
        ):
            raise AssertionError(
                f'{field}.platform {name} are not portable logical paths'
            )
    validate_wheel_native_target(
        {'native': platform_info['native_image']},
        _target_policy_identity(build['target']),
        label=f'{field}.platform.native_image',
        tags=platform_info['wheel_tags'],
    )
    if distribution.get('version') != source.get('version'):
        raise AssertionError(f'{field} distribution version differs from source')
    return value


def _validate_runner_target(runner: str, build: Mapping[str, Any]) -> None:
    expected = RUNNER_TARGETS.get(runner)
    if expected is None:
        raise AssertionError(f'unknown gometry release runner: {runner!r}')
    target = build.get('target')
    raw_arch = build.get('raw_target_arch')
    expected_arch = expected.split('-', 1)[0]
    if not isinstance(target, str) or not isinstance(raw_arch, str):
        raise TypeError(f'runner {runner!r} has no raw target identity')
    if raw_arch != target.split('-', 1)[0] or raw_arch != expected_arch:
        raise AssertionError(
            f'runner {runner!r} has an unexpected raw target architecture: '
            f'{raw_arch!r} != {expected_arch!r}'
        )
    if expected.endswith('-unknown-linux-gnu'):
        if not target.endswith(('-unknown-linux-gnu', '-unknown-linux-musl')):
            raise AssertionError(
                f'Linux runner {runner!r} has an unexpected target: {target!r}'
            )
    elif target != expected:
        raise AssertionError(
            f'runner {runner!r} has an unexpected release target: '
            f'{target!r} != {expected!r}'
        )


def _hook_source_manifest(
    evidence_dir: Path,
    settings: Mapping[str, Any],
    manifest_path: Path | None = None,
) -> dict[str, Any]:
    expected_path = evidence_dir / 'source' / SOURCE_MANIFEST_NAME
    manifest_path = expected_path if manifest_path is None else manifest_path
    if manifest_path.resolve() != expected_path.resolve():
        raise AssertionError('CIBW source manifest is not the preseeded mounted copy')
    expected_hash = settings.get('source_manifest_sha256')
    if (
        not isinstance(expected_hash, str)
        or sha256_file(expected_path) != expected_hash
    ):
        raise AssertionError('CIBW source manifest changed after dispatch')
    manifest = _load_json(expected_path)
    if (
        manifest.get('schema') != SOURCE_MANIFEST_SCHEMA
        or manifest.get('kind') != 'gometry-source-manifest'
        or manifest.get('package') != 'gometry'
    ):
        raise AssertionError('CIBW mounted source manifest has an invalid schema')
    validate_source_identity(source_identity(manifest), field='CIBW source')
    sdist = manifest.get('sdist')
    if (
        not isinstance(sdist, dict)
        or sdist.get('sha256') != settings.get('source_sdist_sha256')
        or not isinstance(sdist.get('filename'), str)
        or not (expected_path.parent / sdist['filename']).is_file()
    ):
        raise AssertionError('CIBW mounted sdist differs from the source dispatch')
    return manifest


def record_build(
    identifier: str,
    source_manifest_path: Path,
    evidence_dir: Path,
    project_root: Path,
) -> None:
    observed_identifier = os.environ.get('CIBUILDWHEEL_BUILD_IDENTIFIER')
    if observed_identifier != identifier:
        raise AssertionError(
            'before-build identifier differs from CIBW environment: '
            f'{identifier!r} != {observed_identifier!r}'
        )
    item_path, item = _pending_wheel_item(evidence_dir, identifier)
    policy = build_identifier_policy(identifier)
    _effective_release_policy(policy['target'])
    if os.environ.get('CARGO_BUILD_TARGET') != policy['target']:
        raise AssertionError('before-build CARGO_BUILD_TARGET differs from identifier')
    if os.environ.get('MATURIN_PEP517_ARGS') != RELEASE_MATURIN_ARGS:
        raise AssertionError(
            'before-build Maturin arguments are not locked release-dist'
        )

    build = item.get('build')
    if not isinstance(build, dict):
        raise TypeError('before-build has no preseeded wheel identity')
    expected_environment = build.get('settings', {}).get('environment')
    if not isinstance(expected_environment, dict):
        raise TypeError('before-build has no dispatched environment record')
    source_manifest = _hook_source_manifest(
        evidence_dir,
        build['settings'],
        source_manifest_path,
    )
    project_root = project_root.resolve()
    if not project_root.is_dir():
        raise AssertionError(f'CIBW package directory is missing: {project_root}')
    actual_environment: dict[str, str] = {}
    for name, expected in expected_environment.items():
        if name == 'PATH':
            actual_environment[name] = 'cargo-bin-prepended'
            continue
        actual = os.environ.get(name)
        if actual is None or actual != expected:
            raise AssertionError(
                f'before-build environment differs for {name}: '
                f'{actual!r} != {expected!r}'
            )
        actual_environment[name] = actual

    interpreter = _runtime_interpreter()
    compiler = _tool_output(['rustc', '-vV'], cwd=project_root)
    cargo = _tool_output(['cargo', '--version'], cwd=project_root)
    build['interpreter'] = interpreter
    build['compiler'] = {'observed_at': 'before-build', 'rustc_vv': compiler}
    build['cargo'] = cargo
    build['dependencies'] = source_manifest['cargo']
    settings = build['settings']
    settings['environment'] = actual_environment
    expected_build_inputs = settings.get('build_inputs')
    if not isinstance(expected_build_inputs, dict):
        raise TypeError('before-build has no preseeded source build inputs')
    if _release_build_input_hashes(project_root) != expected_build_inputs:
        raise AssertionError('before-build source inputs differ from dispatch')
    settings['unrepaired_native'] = None
    settings['native'] = None
    build['settings'] = settings
    item['build'] = build
    _atomic_json(item_path, item)
    print(
        'gometry before-build facts: '
        + json.dumps(
            {
                'identifier': identifier,
                'interpreter': interpreter,
                'compiler': compiler,
                'cargo': cargo,
            },
            sort_keys=True,
            separators=(',', ':'),
        ),
        file=sys.stderr,
        flush=True,
    )


def record_repaired_wheel(
    identifier: str,
    unrepaired_wheel: Path,
    wheel_dir: Path,
    evidence_dir: Path,
) -> None:
    observed_identifier = os.environ.get('CIBUILDWHEEL_BUILD_IDENTIFIER')
    if observed_identifier != identifier:
        raise AssertionError(
            'repair identifier differs from CIBW environment: '
            f'{identifier!r} != {observed_identifier!r}'
        )
    item_path, item = _pending_wheel_item(evidence_dir, identifier)
    build = item.get('build')
    if (
        not isinstance(build, dict)
        or not isinstance(build.get('compiler'), dict)
        or build.get('compiler', {}).get('observed_at') != 'before-build'
    ):
        raise AssertionError('repair has no completed before-build observation')
    source_manifest = _hook_source_manifest(evidence_dir, build['settings'])
    expected_python_members = source_manifest['python_members']
    unrepaired_wheel = unrepaired_wheel.resolve()
    wheel_dir = wheel_dir.resolve()
    repaired_wheels = sorted(wheel_dir.glob('gometry-*.whl'))
    if len(repaired_wheels) != 1:
        raise AssertionError(
            f'repair destination must contain one wheel, found {repaired_wheels!r}'
        )
    repaired_wheel = repaired_wheels[0]
    raw = inspect_wheel(
        unrepaired_wheel,
        release=True,
        expected_python_members=expected_python_members,
        expected_prepared_inputs=source_manifest['prepared_inputs']['files'],
    )
    repaired = inspect_wheel(
        repaired_wheel,
        release=True,
        expected_python_members=expected_python_members,
        expected_prepared_inputs=source_manifest['prepared_inputs']['files'],
    )
    if raw['generator'] != f'maturin ({MATURIN_VERSION})':
        raise AssertionError('unrepaired wheel has the wrong Maturin Generator')
    if repaired['generator'] != f'maturin ({MATURIN_VERSION})':
        raise AssertionError('repaired wheel has the wrong Maturin Generator')
    if not wheel_matches_identifier(
        repaired_wheel,
        identifier,
        repaired['tags'],
    ):
        raise AssertionError('repaired wheel does not match its build identifier')
    if not wheel_target_compatible(
        repaired_wheel,
        build['target'],
        repaired['tags'],
    ):
        raise AssertionError('repaired wheel target differs from its release policy')
    policy = build_identifier_policy(identifier)
    validate_wheel_native_target(
        raw,
        policy,
        label=str(unrepaired_wheel),
        validate_tags=False,
    )
    validate_wheel_native_target(repaired, policy, label=str(repaired_wheel))
    raw_wheel_members = raw['members']
    repaired_wheel_members = repaired['members']
    raw_wheel_member = f'{raw["dist_info"]}/WHEEL'
    repaired_wheel_member = f'{repaired["dist_info"]}/WHEEL'
    if raw_wheel_member not in raw_wheel_members:
        raise AssertionError('unrepaired wheel has no WHEEL member')
    if repaired_wheel_member not in repaired_wheel_members:
        raise AssertionError('repaired wheel has no WHEEL member')
    if len(repaired['extension_members']) != 1:
        raise AssertionError('repaired wheel has no unique native extension')
    extension_member, extension_payload = next(
        iter(repaired['extension_members'].items())
    )
    build['maturin'] = {
        'unrepaired_wheel_sha256': sha256_file(unrepaired_wheel),
        'wheel_member': raw_wheel_member,
        'wheel_member_sha256': raw_wheel_members[raw_wheel_member]['sha256'],
        'generator': raw['generator'],
        'repaired_wheel_member_sha256': repaired_wheel_members[repaired_wheel_member][
            'sha256'
        ],
        'repaired_generator': repaired['generator'],
    }
    build['settings']['unrepaired_native'] = raw['native']
    build['settings']['native'] = repaired['native']
    build['settings']['wheel_tags'] = repaired['tags']
    item.update({
        'filename': repaired_wheel.name,
        'sha256': sha256_file(repaired_wheel),
        'extension_member': extension_member,
        'extension_sha256': _sha256_bytes(extension_payload),
        'build': build,
    })
    _atomic_json(item_path, item)
    print(
        'gometry repaired-wheel facts: '
        + json.dumps(
            {
                'identifier': identifier,
                'unrepaired_sha256': build['maturin']['unrepaired_wheel_sha256'],
                'generator': raw['generator'],
                'repaired_sha256': item['sha256'],
                'native': repaired['native'],
            },
            sort_keys=True,
            separators=(',', ':'),
        ),
        file=sys.stderr,
        flush=True,
    )


def _smoke_grid(
    destination: Path, *, expected_source_manifest: Path | None = None
) -> Path:
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
        if not archive_path.is_file() and expected_source_manifest is not None:
            sdist = _source_sdist_from_manifest(expected_source_manifest)
            root = (
                f'gometry-{sdist.name.removeprefix("gometry-").removesuffix(".tar.gz")}'
            )
            sdist_member = f'{root}/gometry/native/proj-sys/PROJSRC/proj-9.6.2.tar.gz'
            with tarfile.open(sdist) as source:
                stream = source.extractfile(sdist_member)
                if stream is None:
                    raise AssertionError(f'sdist is missing {sdist_member}')
                archive_path = destination / 'proj-9.6.2.tar.gz'
                with archive_path.open('wb') as output:
                    shutil.copyfileobj(stream, output)
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


def _extract_smoke_grid_from_sdist(sdist: Path, destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    root = f'gometry-{sdist.name.removeprefix("gometry-").removesuffix(".tar.gz")}'
    proj_member = f'{root}/gometry/native/proj-sys/PROJSRC/proj-9.6.2.tar.gz'
    proj_archive = destination.with_name('proj-9.6.2.tar.gz')
    fixture_member = 'proj-9.6.2/data/tests/test_vgrid_pixelispoint.tif'
    try:
        with tarfile.open(sdist) as source:
            stream = source.extractfile(proj_member)
            if stream is None:
                raise AssertionError(f'sdist is missing {proj_member}')
            with proj_archive.open('wb') as output:
                shutil.copyfileobj(stream, output)
        with tarfile.open(proj_archive) as archive:
            fixture = archive.extractfile(fixture_member)
            if fixture is None:
                raise AssertionError(f'PROJ source archive is missing {fixture_member}')
            with destination.open('wb') as output:
                shutil.copyfileobj(fixture, output)
    except (OSError, tarfile.TarError) as error:
        raise AssertionError(
            f'cannot extract staged PROJ smoke grid: {error}'
        ) from error
    finally:
        proj_archive.unlink(missing_ok=True)
    return destination


def _smoke_environment(
    *,
    expected_source_manifest: Path | None = None,
    identifier: str | None = None,
    source_manifest: Mapping[str, Any] | None = None,
) -> dict[str, str]:
    if source_manifest is not None:
        expected_source_inputs = source_manifest['source_inputs_sha256']
        expected_native_inputs = source_manifest['native_source_inputs_sha256']
        expected_version = source_manifest['sdist']['version']
    elif expected_source_manifest is not None:
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
    if identifier is None:
        selected = _effective_release_policy()
        policy = _target_policy_identity(selected['target'])
    else:
        policy = build_identifier_policy(identifier)
        _effective_release_policy(policy['target'])
    values = {
        SMOKE_EXPECTED_SOURCE_INPUTS_ENV: expected_source_inputs,
        SMOKE_EXPECTED_NATIVE_SOURCE_INPUTS_ENV: expected_native_inputs,
        SMOKE_EXPECTED_VERSION_ENV: expected_version,
        SMOKE_EXPECTED_TARGET_ENV: policy['target'],
        SMOKE_EXPECTED_ARCHITECTURE_ENV: policy['raw_target_arch'],
        SMOKE_EXPECTED_HOST_ARCH_ENV: policy['architecture'],
        'GOMETRY_SMOKE_CPU_FEATURES': json.dumps(
            {
                'v2': sorted(X86_64_V2_TARGET_FEATURES),
                'v3_plus': sorted(X86_64_V3_PLUS_TARGET_FEATURES),
            },
            separators=(',', ':'),
        ),
    }
    env = os.environ.copy()
    env.update(values)
    return env


def _pending_wheel_item(
    evidence_dir: Path, identifier: str
) -> tuple[Path, dict[str, Any]]:
    item_path = evidence_dir / identifier / 'wheel-item.json'
    try:
        item = json.loads(item_path.read_text(encoding='utf-8'))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AssertionError(f'cannot read pending CIBW wheel item: {error}') from error
    if not isinstance(item, dict) or item.get('identifier') != identifier:
        raise AssertionError(
            f'pending CIBW wheel item has the wrong identifier: {identifier}'
        )
    return item_path, item


def _portable_interpreter(interpreter: Mapping[str, Any]) -> dict[str, Any]:
    cache_tag = interpreter.get('cache_tag')
    if not isinstance(cache_tag, str) or not cache_tag:
        raise AssertionError('runtime interpreter has no cache tag')
    return {
        'pid': interpreter['pid'],
        'executable': f'python/{cache_tag}',
        'prefix': f'python-prefix/{cache_tag}',
        'version': interpreter['version'],
        'implementation': interpreter['implementation'],
        'cache_tag': cache_tag,
        'ext_suffix': interpreter['ext_suffix'],
        'gil_disabled': interpreter['gil_disabled'],
    }


def installed_wheel_runtime_receipt(
    *,
    identifier: str | None,
    profile: str,
    source: Mapping[str, Any],
    build: Mapping[str, Any],
    wheel: Mapping[str, Any],
    prepared_inputs: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Project an admitted installed-wheel record for runtime verification."""
    required_wheel_fields = (
        'filename',
        'sha256',
        'extension_member',
        'extension_sha256',
    )
    if not all(isinstance(wheel.get(name), str) for name in required_wheel_fields):
        raise AssertionError('installed wheel has an incomplete runtime identity')
    if not isinstance(build, Mapping) or not isinstance(source, Mapping):
        raise TypeError('installed wheel source or build identity is malformed')
    projected = {
        'identifier': identifier,
        'profile': profile,
        'source': dict(source),
        'build': dict(build),
        'wheel': {name: wheel[name] for name in required_wheel_fields},
        'python_members': source.get('python_members'),
    }
    if prepared_inputs is not None:
        projected['prepared_inputs'] = dict(prepared_inputs)
    return projected


def smoke_installed(
    *,
    expected_source_manifest: Path | None = None,
    wheel: Path | None = None,
    identifier: str | None = None,
    evidence_dir: Path | None = None,
) -> dict[str, Any] | None:
    source_manifest_path = (
        expected_source_manifest.resolve()
        if expected_source_manifest is not None
        else None
    )
    item_path: Path | None = None
    runtime_record: dict[str, Any] | None = None
    if evidence_dir is not None:
        if identifier is None or wheel is None or source_manifest_path is None:
            raise AssertionError(
                'recorded installed smoke requires identifier, wheel and source manifest'
            )
        observed_identifier = os.environ.get('CIBUILDWHEEL_BUILD_IDENTIFIER')
        if observed_identifier != identifier:
            raise AssertionError(
                'installed smoke identifier differs from CIBW environment: '
                f'{identifier!r} != {observed_identifier!r}'
            )
        item_path, item = _pending_wheel_item(evidence_dir, identifier)
        if not all(
            isinstance(item.get(name), str)
            for name in (
                'filename',
                'sha256',
                'extension_member',
                'extension_sha256',
            )
        ):
            raise AssertionError(
                'installed smoke has no completed repaired-wheel record'
            )
        build_record = item.get('build')
        if not isinstance(build_record, dict):
            raise AssertionError('installed smoke has no build identity')
        source_manifest = _hook_source_manifest(
            evidence_dir,
            build_record.get('settings', {}),
            source_manifest_path,
        )
        if item['filename'] != wheel.name or item['sha256'] != sha256_file(wheel):
            raise AssertionError(
                'installed smoke wheel differs from repaired-wheel hook'
            )
        runtime_receipt = installed_wheel_runtime_receipt(
            identifier=identifier,
            profile=RELEASE_PROFILE,
            source=source_identity(source_manifest),
            build=item['build'],
            wheel=item,
            prepared_inputs=source_manifest['prepared_inputs']['files'],
        )
        facade, extension, runtime_record = verify_runtime(
            ROOT,
            expected_wheel=wheel.resolve(),
            build_receipt=runtime_receipt,
            runtime_selection={
                'executable': sys.executable,
                'prefix': sys.prefix,
                'cache_tag': sys.implementation.cache_tag,
                'ext_suffix': sysconfig.get_config_var('EXT_SUFFIX'),
                'gil_disabled': bool(sysconfig.get_config_var('Py_GIL_DISABLED')),
            },
        )
        del facade, extension
        print(
            'gometry runtime facts: '
            + json.dumps(runtime_record, sort_keys=True, separators=(',', ':')),
            file=sys.stderr,
            flush=True,
        )

    with tempfile.TemporaryDirectory(prefix='gometry-installed-smoke-') as tmp:
        grid = _smoke_grid(
            Path(tmp) / 'grid',
            expected_source_manifest=source_manifest_path,
        )
        env = _smoke_environment(
            expected_source_manifest=source_manifest_path,
            identifier=identifier,
            source_manifest=(source_manifest if evidence_dir is not None else None),
        )
        env[SMOKE_GRID_ENV] = str(grid)
        names = {
            SMOKE_GRID_ENV,
            SMOKE_EXPECTED_SOURCE_INPUTS_ENV,
            SMOKE_EXPECTED_NATIVE_SOURCE_INPUTS_ENV,
            SMOKE_EXPECTED_VERSION_ENV,
            SMOKE_EXPECTED_TARGET_ENV,
            SMOKE_EXPECTED_ARCHITECTURE_ENV,
            SMOKE_EXPECTED_HOST_ARCH_ENV,
            'GOMETRY_SMOKE_CPU_FEATURES',
        }
        previous = {name: os.environ.get(name) for name in names}
        try:
            os.environ.update({name: env[name] for name in names})
            # This smoke must execute inside the already-admitted interpreter.
            exec(  # noqa: S102
                compile(textwrap.dedent(SMOKE), '<gometry-wheel-smoke>', 'exec'),
                {
                    '__name__': '__main__',
                },
            )
        finally:
            for name, value in previous.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value

    if item_path is not None and runtime_record is not None:
        item_path, item = _pending_wheel_item(evidence_dir, identifier or '')
        interpreter = runtime_record['interpreter']
        item['smoke'] = {
            'status': 'passed',
            'exit_code': 0,
            'interpreter': _portable_interpreter(interpreter),
            'module': runtime_record['module'],
            'source_inputs_sha256': runtime_record['source_inputs_sha256'],
            'native_source_inputs_sha256': runtime_record[
                'native_source_inputs_sha256'
            ],
            'distribution': runtime_record['distribution'],
            'platform': runtime_record['platform'],
            'log_sha256': None,
        }
        _atomic_json(item_path, item)
    return runtime_record


def write_runner_receipt(
    wheel_dir: Path,
    identifiers_path: Path,
    source_manifest_path: Path,
    *,
    evidence_dir: Path,
    build_log: Path,
    build_exit_code: int,
    capture_exit_code: int,
    receipt_path: Path | None = None,
    runner: str | None = None,
) -> Path:
    wheels = sorted(wheel_dir.glob('gometry-*.whl'))
    identifiers = _read_identifiers(identifiers_path)
    if (
        not isinstance(build_exit_code, int)
        or isinstance(build_exit_code, bool)
        or not isinstance(capture_exit_code, int)
        or isinstance(capture_exit_code, bool)
    ):
        raise TypeError('CIBW and log-capture exit codes must be integers')
    evidence_dir = evidence_dir.resolve()
    build_log = build_log.resolve()
    log_digest = sha256_file(build_log) if build_log.is_file() else None
    source_manifest_path = source_manifest_path.resolve()
    source_manifest = validate_source_manifest(
        _source_sdist_from_manifest(source_manifest_path),
        source_manifest_path,
    )
    build_items: dict[str, dict[str, Any]] = {}
    for identifier in identifiers:
        item_path, item = _pending_wheel_item(evidence_dir, identifier)
        build = item.get('build')
        if not isinstance(build, dict):
            raise TypeError(f'pending wheel item has no build: {identifier}')
        command = build.get('command')
        if not isinstance(command, dict):
            raise TypeError(f'pending wheel item has no dispatch: {identifier}')
        command.update({
            'host_log_sha256': log_digest,
            'host_exit_code': build_exit_code,
            'capture_exit_code': capture_exit_code,
        })
        item['build'] = build
        if build_exit_code != 0 or capture_exit_code != 0 or log_digest is None:
            _atomic_json(item_path, item)
            continue
        smoke = item.get('smoke')
        if not isinstance(smoke, dict) or smoke.get('status') != 'passed':
            _atomic_json(item_path, item)
            raise AssertionError(
                f'CIBW did not complete installed smoke for {identifier}'
            )
        smoke['log_sha256'] = log_digest
        item['smoke'] = smoke
        _finalize_test_source_identity(build)
        item['build'] = build
        _portableize_wheel_item(item, evidence_dir, wheel_dir)
        _atomic_json(item_path, item)
        build_items[identifier] = item
    if build_exit_code != 0 or capture_exit_code != 0:
        raise AssertionError(
            'CIBW runner did not complete successfully: '
            f'build={build_exit_code}, capture={capture_exit_code}'
        )
    if log_digest is None:
        raise AssertionError(f'CIBW host build log is missing: {build_log}')
    if not wheels:
        raise AssertionError(f'no gometry wheels found in {wheel_dir}')

    inspections = {
        wheel: inspect_wheel(
            wheel,
            release=True,
            expected_python_members=source_manifest['python_members'],
            expected_prepared_inputs=source_manifest['prepared_inputs']['files'],
        )
        for wheel in wheels
    }
    matches = _match_wheels_to_identifiers(
        wheels,
        identifiers,
        inspections=inspections,
    )
    records = []
    for identifier in identifiers:
        wheel = matches[identifier]
        inspection = inspections[wheel]
        item = build_items.get(identifier)
        if item is None:
            raise AssertionError(f'no completed hook evidence for {identifier}')
        build = _validate_build_identity(item['build'], field=f'build {identifier}')
        _validate_runner_target(
            runner or os.environ.get('GOMETRY_BUILD_RUNNER', 'local'),
            build,
        )
        if item.get('filename') != wheel.name or item.get('sha256') != sha256_file(
            wheel
        ):
            raise AssertionError(
                f'final CIBW wheel differs from repaired-wheel hook: {identifier}'
            )
        if item.get('extension_member') not in inspection['extension_members']:
            raise AssertionError(f'final wheel extension member changed: {identifier}')
        if item.get('extension_sha256') != _sha256_bytes(
            inspection['extension_members'][item['extension_member']]
        ):
            raise AssertionError(f'final wheel extension bytes changed: {identifier}')
        if not wheel_target_compatible(wheel, build['target'], inspection['tags']):
            raise AssertionError(f'final wheel target mismatch: {identifier}')
        validate_wheel_native_target(
            inspection,
            _target_policy_identity(build['target']),
            label=wheel.name,
        )
        if build['dependencies'] != source_manifest['cargo']:
            raise AssertionError(
                f'wheel dependency closure differs from source: {identifier}'
            )
        smoke = _validate_smoke_identity(
            item['smoke'],
            field=f'smoke {identifier}',
            source=source_identity(source_manifest),
            build=build,
            extension_member=item['extension_member'],
            extension_sha256=item['extension_sha256'],
        )
        records.append({
            'identifier': identifier,
            'filename': wheel.name,
            'sha256': sha256_file(wheel),
            'extension_member': item['extension_member'],
            'extension_sha256': item['extension_sha256'],
            'build': build,
            'smoke': smoke,
        })
    if receipt_path is None:
        receipt_path = wheel_dir / RUNNER_RECEIPT_NAME
    if receipt_path.parent != wheel_dir:
        raise AssertionError('wheel receipt must be a sibling of the wheel archives')
    value = {
        'schema': RUNNER_RECEIPT_SCHEMA,
        'kind': 'gometry-wheel-receipt',
        'profile': RELEASE_PROFILE,
        'runner': runner or os.environ.get('GOMETRY_BUILD_RUNNER', 'local'),
        'source': source_identity(source_manifest),
        'planned_identifiers': identifiers,
        'wheels': records,
    }
    _write_json(receipt_path, value)
    validate_runner_receipt(
        receipt_path,
        wheel_dir,
        source_manifest_path,
        inspections=inspections,
    )
    return receipt_path


def _portableize_wheel_item(
    item: dict[str, Any], evidence_dir: Path, wheel_dir: Path
) -> None:
    build = item.get('build')
    if not isinstance(build, dict):
        raise TypeError('cannot normalize wheel item without build identity')
    interpreter = build.get('interpreter')
    if not isinstance(interpreter, dict):
        raise TypeError('cannot normalize wheel item without producer interpreter')
    producer = {
        'executable': f'python/{interpreter["cache_tag"]}',
        'prefix': f'python-prefix/{interpreter["cache_tag"]}',
        'version': interpreter['version'],
        'implementation': interpreter['implementation'],
        'cache_tag': interpreter['cache_tag'],
        'ext_suffix': interpreter['ext_suffix'],
        'gil_disabled': interpreter['gil_disabled'],
    }
    build['interpreter'] = producer
    command = build['command']
    dispatch = command['dispatched']
    argv = []
    runner_temp = evidence_dir.resolve().parent.parent
    wheel_dir = wheel_dir.resolve()
    for argument in dispatch['cibuildwheel_argv']:
        value = argument
        if argument == str(wheel_dir):
            value = '<runner-workspace>/dist'
        elif str(runner_temp) in argument:
            value = argument.replace(str(runner_temp), '<runner-temp>')
        elif Path(argument).is_absolute() or PureWindowsPath(argument).is_absolute():
            raise AssertionError(
                f'cannot normalize CIBW dispatch path without an owner: {argument!r}'
            )
        argv.append(value)
    dispatch['cibuildwheel_argv'] = argv
    environment = build['settings']['environment']
    wrapper = environment.get('RUSTC_WRAPPER', '')
    if wrapper and (
        Path(wrapper).is_absolute() or PureWindowsPath(wrapper).is_absolute()
    ):
        environment['RUSTC_WRAPPER'] = 'sccache'
    item['build'] = build
    smoke = item.get('smoke')
    platform_info = smoke.get('platform') if isinstance(smoke, dict) else None
    loader = platform_info.get('loader') if isinstance(platform_info, dict) else None
    if not isinstance(loader, dict):
        raise TypeError('cannot normalize wheel item without runtime loader facts')
    for name, logical_root in (
        ('loader_paths', 'loader'),
        ('libc_paths', 'libc'),
    ):
        paths = loader.get(name)
        if not isinstance(paths, list) or not all(
            isinstance(path, str) and path for path in paths
        ):
            raise AssertionError(f'cannot normalize runtime {name}')
        loader[name] = sorted({
            f'{logical_root}/{PurePosixPath(path).name}' for path in paths
        })
    item['smoke'] = smoke


def _finalize_test_source_identity(build: dict[str, Any]) -> None:
    settings = build.get('settings')
    if not isinstance(settings, dict):
        raise TypeError('wheel item settings are malformed')
    source_root = settings.get('test_source_root')
    test_sources = settings.get('test_sources')
    if not isinstance(source_root, str) or not isinstance(test_sources, dict):
        raise TypeError('wheel item has no local CIBW test-source handoff')
    root = Path(source_root)
    for relative, expected in test_sources.items():
        path = root / relative
        if not path.is_file() or sha256_file(path) != expected:
            raise AssertionError(
                f'CIBW test source changed before finalization: {relative}'
            )
    settings['test_source_root'] = 'test-sources'
    build['settings'] = settings


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
    inspections: Mapping[Path, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    receipt = _load_json(receipt_path)
    if (
        set(receipt)
        != {
            'schema',
            'kind',
            'profile',
            'runner',
            'source',
            'planned_identifiers',
            'wheels',
        }
        or receipt.get('schema') != RUNNER_RECEIPT_SCHEMA
        or receipt.get('kind') != 'gometry-wheel-receipt'
    ):
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
    expected_source = source_identity(source_manifest)
    if source != expected_source:
        raise AssertionError(f'wheel receipt source identity mismatch: {receipt_path}')
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
    source_manifest_file = wheel_dir / SOURCE_MANIFEST_NAME
    has_source_manifest = source_manifest_file.is_file()
    source_filename = source_manifest.get('sdist', {}).get('filename')
    source_sdist_file = (
        wheel_dir / source_filename if isinstance(source_filename, str) else None
    )
    has_source_sdist = source_sdist_file is not None and source_sdist_file.is_file()
    if has_source_manifest != has_source_sdist:
        raise AssertionError(
            'runner directory contains only half of its source artifact'
        )
    if has_source_manifest and source_sdist_file is not None:
        local_source = validate_source_manifest(
            source_sdist_file,
            source_manifest_file,
        )
        if source_identity(local_source) != expected_source:
            raise AssertionError(
                'runner-directory source identity differs from receipt'
            )
    source_files = (
        {SOURCE_MANIFEST_NAME, source_filename}
        if has_source_manifest and isinstance(source_filename, str)
        else set()
    )
    expected_files = {path.name for path in files} | {receipt_path.name} | source_files
    if actual_files != expected_files:
        raise AssertionError(
            f'wheel directory contains files outside its receipt: '
            f'{sorted(actual_files - expected_files)}'
        )
    archive_inspections = (
        dict(inspections)
        if inspections is not None
        else {
            wheel: inspect_wheel(
                wheel,
                release=True,
                expected_python_members=source_manifest['python_members'],
                expected_prepared_inputs=source_manifest['prepared_inputs']['files'],
            )
            for wheel in files
        }
    )
    if set(archive_inspections) != set(files):
        raise AssertionError(
            'runner receipt inspection set differs from wheel directory'
        )
    matches = _match_wheels_to_identifiers(
        files,
        planned,
        inspections=archive_inspections,
    )
    listed: dict[str, dict[str, Any]] = {}
    for item in wheels:
        expected_item_fields = {
            'identifier',
            'filename',
            'sha256',
            'extension_member',
            'extension_sha256',
            'build',
            'smoke',
        }
        if not isinstance(item, dict) or set(item) != expected_item_fields:
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
        inspection = archive_inspections[wheel]
        build = _validate_build_identity(item['build'], field=f'build {identifier}')
        _validate_runner_target(runner, build)
        if build['dependencies'] != source['cargo']:
            raise AssertionError(
                f'wheel build dependencies differ from source: {identifier}'
            )
        if item['filename'] != wheel.name:
            raise AssertionError(f'wheel receipt filename mismatch for {identifier}')
        if item['sha256'] != sha256_file(wheel):
            raise AssertionError(f'wheel SHA-256 mismatch for {wheel.name}')
        if wheel_architecture(wheel, inspection['tags']) != build['architecture']:
            raise AssertionError(
                f'wheel receipt architecture mismatch for {wheel.name}'
            )
        if not wheel_target_compatible(wheel, build['target'], inspection['tags']):
            raise AssertionError(f'wheel receipt target mismatch for {wheel.name}')
        validate_wheel_native_target(
            inspection,
            _target_policy_identity(build['target']),
            label=wheel.name,
        )
        if not wheel_matches_identifier(wheel, identifier, inspection['tags']):
            raise AssertionError(f'wheel identifier mismatch for {wheel.name}')
        if len(inspection['extension_members']) != 1:
            raise AssertionError(f'wheel has no unique extension: {wheel.name}')
        extension_member, extension_payload = next(
            iter(inspection['extension_members'].items())
        )
        extension_digest = _sha256_bytes(extension_payload)
        if (
            item['extension_member'] != extension_member
            or item['extension_sha256'] != extension_digest
        ):
            raise AssertionError(f'wheel extension identity mismatch: {wheel.name}')
        wheel_record = build['maturin']
        wheel_member = f'{inspection["dist_info"]}/WHEEL'
        if (
            wheel_record['repaired_wheel_member_sha256']
            != inspection['members'][wheel_member]['sha256']
            or wheel_record['repaired_generator'] != inspection['generator']
        ):
            raise AssertionError(f'wheel Generator identity mismatch: {wheel.name}')
        smoke = _validate_smoke_identity(
            item['smoke'],
            field=f'smoke {identifier}',
            source=source,
            build=build,
            extension_member=extension_member,
            extension_sha256=extension_digest,
        )
        if smoke['log_sha256'] != build['command']['host_log_sha256']:
            raise AssertionError(f'wheel smoke log is not runner-bound: {identifier}')
    return receipt


def _read_identifiers_from_values(values: Sequence[str]) -> list[str]:
    result = list(values)
    if not result or len(result) != len(set(result)):
        raise AssertionError('cibuildwheel identifier plan is empty or duplicated')
    for identifier in result:
        if IDENTIFIER.fullmatch(identifier) is None:
            raise AssertionError(f'invalid cibuildwheel identifier: {identifier!r}')
    return result


def _cibw_policy_environment(
    policy: Mapping[str, Any], *, runner: str, host_environment: Mapping[str, str]
) -> dict[str, str]:
    target = str(policy['target'])
    rustflags = _flag_text(policy['rustflags'])
    cflags = _flag_text(policy['cflags'])
    cxxflags = _flag_text(policy['cxxflags'])
    toolchain = host_environment.get('RUSTUP_TOOLCHAIN') or _package_toolchain_channel()
    values = {
        'CARGO_BUILD_TARGET': target,
        'RUSTFLAGS': rustflags,
        'CARGO_ENCODED_RUSTFLAGS': '\x1f'.join(policy['rustflags']),
        f'CFLAGS_{target.replace("-", "_")}': cflags,
        f'CXXFLAGS_{target.replace("-", "_")}': cxxflags,
        'CPPFLAGS': '',
        'TARGET_CFLAGS': '',
        'TARGET_CXXFLAGS': '',
        'HOST_CFLAGS': '',
        'HOST_CXXFLAGS': '',
        'CMAKE_ARGS': '',
        'MATURIN_PEP517_ARGS': RELEASE_MATURIN_ARGS,
        'RUSTUP_TOOLCHAIN': toolchain,
        'SOURCE_DATE_EPOCH': host_environment.get('SOURCE_DATE_EPOCH', ''),
        'GOMETRY_RELEASE_TARGET': target,
        'GOMETRY_RELEASE_TARGET_POLICY': str(policy['target_policy']),
        'GOMETRY_RELEASE_RUSTFLAGS': rustflags,
        'GOMETRY_RELEASE_CFLAGS': cflags,
        'GOMETRY_RELEASE_CXXFLAGS': cxxflags,
    }
    if not values['SOURCE_DATE_EPOCH'].isdigit():
        raise AssertionError('SOURCE_DATE_EPOCH is missing or malformed')
    if str(policy['platform_family']) == 'darwin':
        values['MACOSX_DEPLOYMENT_TARGET'] = '11.0'
    if str(policy['platform_family']).startswith('linux-'):
        values.update({
            'RUSTC_WRAPPER': '/opt/sccache/sccache',
            'SCCACHE_GHA_ENABLED': host_environment.get('SCCACHE_GHA_ENABLED', 'on'),
            'SCCACHE_GHA_RW_MODE': host_environment.get(
                'SCCACHE_GHA_RW_MODE', 'READ_ONLY'
            ),
        })
        values['PATH'] = '$HOME/.cargo/bin:$PATH'
    elif host_environment.get('SCCACHE_PATH'):
        values['RUSTC_WRAPPER'] = host_environment['SCCACHE_PATH']
    _validate_runner_target(runner, policy)
    return values


def _cibw_environment_text(values: Mapping[str, str]) -> str:
    assignments = []
    for name, value in sorted(values.items()):
        if name == 'PATH' and value == '$HOME/.cargo/bin:$PATH':
            assignments.append('PATH="$HOME/.cargo/bin:$PATH"')
        else:
            if '\n' in value or '\r' in value:
                raise AssertionError(
                    f'CIBW environment value contains a newline: {name}'
                )
            assignments.append(f'{name}={shlex.quote(value)}')
    return ' '.join(assignments)


def _toml_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def _repair_command(
    policy: Mapping[str, Any],
    *,
    identifier: str,
    evidence_dir: str,
) -> str:
    family = str(policy['platform_family'])
    if family.startswith('linux-'):
        repair = 'auditwheel repair -w "{dest_dir}" "{wheel}"'
    elif family == 'darwin':
        repair = (
            'delocate-wheel --require-archs {delocate_archs} '
            '-w "{dest_dir}" -v "{wheel}"'
        )
    elif family == 'windows-msvc':
        repair = 'delvewheel repair -w "{dest_dir}" -v "{wheel}"'
    else:
        raise AssertionError(f'unsupported cibuildwheel platform family: {family}')
    hook = (
        'python test-sources/gometry/tools/wheel_smoke.py --record-repaired-wheel '
        f'--identifier {shlex.quote(identifier)} --unrepaired-wheel "{{wheel}}" '
        f'--wheel-dir "{{dest_dir}}" --evidence-dir "{evidence_dir}"'
    )
    return f'{repair} && {hook}'


def prepare_cibw_project(
    sdist: Path,
    source_manifest_path: Path,
    project_root: Path,
) -> Path:
    """Extract the staged sdist and sibling test sources for one CIBW run."""
    sdist = sdist.resolve()
    source_manifest_path = source_manifest_path.resolve()
    project_root = project_root.resolve()
    verify_source_artifact(sdist, source_manifest_path)
    inspect_sdist(
        sdist,
        expected_libtiff=libtiff_source_members(),
        expected_toolchain_channel=sdist_toolchain_channel(sdist),
    )
    if project_root.exists() and any(project_root.iterdir()):
        raise AssertionError(f'CIBW project directory is not empty: {project_root}')
    source_root = project_root / 'source'
    source_root.mkdir(parents=True, exist_ok=True)
    with tarfile.open(sdist) as archive:
        extract_sdist(archive, source_root)
    roots = [entry for entry in source_root.iterdir() if entry.is_dir()]
    if len(roots) != 1 or roots[0].name != sdist.name.removesuffix('.tar.gz'):
        raise AssertionError(
            f'staged source sdist has no unique CIBW project root: {roots}'
        )
    package_root = roots[0]
    test_root = package_root / 'test-sources'
    for relative in CIBW_TEST_SOURCE_FILES:
        source = MONOREPO / relative
        if not source.is_file():
            raise AssertionError(f'CIBW test source is missing: {source}')
        destination = test_root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
    return package_root


def prepare_cibw_evidence(
    identifiers_path: Path,
    source_manifest_path: Path,
    evidence_dir: Path,
    config_path: Path,
    *,
    runner: str,
    project_root: Path,
    package_dir: Path,
    cibw_argv: Sequence[str],
) -> str:
    """Create exact-ID CIBW overrides and seed each transition receipt."""
    identifiers = _read_identifiers(identifiers_path)
    evidence_dir = evidence_dir.resolve()
    config_path = config_path.resolve()
    project_root = project_root.resolve()
    package_dir = package_dir.resolve()
    if evidence_dir.exists() and any(evidence_dir.iterdir()):
        raise AssertionError(f'CIBW evidence directory is not empty: {evidence_dir}')
    evidence_dir.mkdir(parents=True, exist_ok=True)
    try:
        package_dir.relative_to(project_root)
    except ValueError as error:
        raise AssertionError(
            'CIBW package directory must be under its project root'
        ) from error
    for relative in CIBW_TEST_SOURCES:
        if not (package_dir / relative).is_file():
            raise AssertionError(f'CIBW test source is not staged: {relative}')
    source_manifest_path = source_manifest_path.resolve()
    source_sdist = _source_sdist_from_manifest(source_manifest_path)
    source_manifest = _load_json(source_manifest_path)
    if (
        source_manifest.get('schema') != SOURCE_MANIFEST_SCHEMA
        or source_manifest.get('kind') != 'gometry-source-manifest'
        or source_manifest.get('package') != 'gometry'
    ):
        raise AssertionError('CIBW source manifest has an invalid schema')
    validate_source_identity(source_identity(source_manifest), field='CIBW source')
    source_input_dir = evidence_dir / 'source'
    source_input_dir.mkdir()
    staged_sdist = source_input_dir / source_sdist.name
    staged_manifest = source_input_dir / SOURCE_MANIFEST_NAME
    copied_sdist_sha256 = _copy_sha256(source_sdist, staged_sdist)
    if copied_sdist_sha256 != source_manifest['sdist']['sha256']:
        raise AssertionError('copied CIBW source sdist differs from its manifest')
    shutil.copyfile(source_manifest_path, staged_manifest)
    if sha256_file(staged_manifest) != sha256_file(source_manifest_path):
        raise AssertionError('copied CIBW source manifest changed')
    staged_grid = _extract_smoke_grid_from_sdist(
        staged_sdist,
        evidence_dir / 'proj-grid.tif',
    )
    if not staged_grid.is_file():
        raise AssertionError('CIBW source sdist produced no PROJ smoke grid')

    linux_container = runner.startswith(('ubuntu-', 'manylinux', 'musllinux'))
    hook_evidence = '/gometry-evidence' if linux_container else evidence_dir.as_posix()
    hook_source = f'{hook_evidence}/source/{SOURCE_MANIFEST_NAME}'
    host_environment = os.environ.copy()
    selected = []
    overrides = []
    test_sources = CIBW_TEST_SOURCES
    test_source_hashes: dict[str, str] = {}
    for relative in CIBW_TEST_SOURCE_FILES:
        path = MONOREPO / relative
        if not path.is_file():
            raise AssertionError(f'CIBW test source is missing: {path}')
        test_source_hashes[relative] = sha256_file(path)
        staged_test_source = package_dir / 'test-sources' / relative
        if sha256_file(staged_test_source) != test_source_hashes[relative]:
            raise AssertionError(f'CIBW test-source staging changed bytes: {relative}')
    for identifier in identifiers:
        policy = build_identifier_policy(identifier)
        _validate_runner_target(runner, policy)
        environment = _cibw_policy_environment(
            policy,
            runner=runner,
            host_environment=host_environment,
        )
        test_environment = {
            **environment,
            SMOKE_GRID_ENV: f'{hook_evidence}/proj-grid.tif',
        }
        before_build = (
            'python test-sources/gometry/tools/wheel_smoke.py --record-build '
            f'--identifier {shlex.quote(identifier)} '
            '--project-root "{package}" '
            f'--source-manifest "{hook_source}" '
            f'--evidence-dir "{hook_evidence}"'
        )
        test_command = (
            'python test-sources/gometry/tools/wheel_smoke.py --installed '
            f'--identifier {shlex.quote(identifier)} --wheel "{{wheel}}" '
            f'--source-manifest "{hook_source}" '
            f'--evidence-dir "{hook_evidence}" '
            '&& python -m pytest '
            'test-sources/gometry/tests/tooling/test_wheel_smoke.py'
        )
        if (
            linux_container
            and test_environment.get('SCCACHE_GHA_RW_MODE') == 'READ_WRITE'
        ):
            test_command += ' && /opt/sccache/sccache --show-stats'
        overrides.extend((
            '',
            '[[tool.cibuildwheel.overrides]]',
            f'select = {_toml_string(identifier)}',
            f'environment = {_toml_string(_cibw_environment_text(environment))}',
            f'test-environment = {_toml_string(_cibw_environment_text(test_environment))}',
            f'before-build = {_toml_string(before_build)}',
            f'repair-wheel-command = {_toml_string(_repair_command(policy, identifier=identifier, evidence_dir=hook_evidence))}',
            f'test-command = {_toml_string(test_command)}',
        ))
        selected.append({
            'identifier': identifier,
            'policy': policy,
            'environment': environment,
        })

    config = '\n'.join((
        '[tool.cibuildwheel]',
        'build-frontend = "build[uv]"',
        'test-sources = [',
        *[f'  {_toml_string(item)},' for item in test_sources],
        ']',
        'test-requires = ["pytest>=9.1"]',
        'environment-pass = [',
        *[
            f'  {_toml_string(item)},'
            for item in (
                'ACTIONS_RESULTS_URL',
                'ACTIONS_RUNTIME_TOKEN',
                'MATURIN_PEP517_ARGS',
                'RUSTUP_TOOLCHAIN',
                'SCCACHE_GHA_ENABLED',
                'SCCACHE_GHA_RW_MODE',
                'SOURCE_DATE_EPOCH',
            )
        ],
        ']',
        *overrides,
        '',
    ))
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(config, encoding='utf-8')
    config_digest = sha256_file(config_path)
    dispatch = {
        'cibuildwheel_argv': list(cibw_argv),
        'frontend': 'build[uv]',
        'config_settings': {'MATURIN_PEP517_ARGS': RELEASE_MATURIN_ARGS},
    }
    for row in selected:
        identifier = row['identifier']
        policy = row['policy']
        build = {
            'target': policy['target'],
            'raw_target_arch': policy['raw_target_arch'],
            'architecture': policy['architecture'],
            'target_policy': policy['target_policy'],
            'rustflags': policy['rustflags'],
            'cflags': policy['cflags'],
            'cxxflags': policy['cxxflags'],
            'command': {
                'dispatched': dispatch,
                'config_sha256': config_digest,
                'host_log_sha256': None,
                'host_exit_code': None,
                'capture_exit_code': None,
            },
            'profile': RELEASE_PROFILE,
            'interpreter': None,
            'compiler': None,
            'cargo': None,
            'maturin': None,
            'dependencies': source_manifest['cargo'],
            'settings': {
                'environment': row['environment'],
                'config_sha256': config_digest,
                'test_sources': test_source_hashes,
                'test_source_root': str(package_dir / 'test-sources'),
                'source_manifest_sha256': sha256_file(staged_manifest),
                'source_sdist_sha256': sha256_file(staged_sdist),
                'build_inputs': _release_build_input_hashes(package_dir),
            },
        }
        _atomic_json(
            evidence_dir / identifier / 'wheel-item.json',
            {'identifier': identifier, 'build': build},
        )
    return config_digest


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
    prepared_licenses = sdist_prepared_license_hashes(sdist)
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
    inspections_by_runner: dict[str, dict[Path, dict[str, Any]]] = {}
    for receipt_path in receipts:
        runner_wheels = sorted(receipt_path.parent.glob('gometry-*.whl'))
        runner_inspections = {
            wheel: inspect_wheel(
                wheel,
                release=True,
                expected_python_members=source_manifest['python_members'],
                expected_prepared_inputs=prepared_licenses,
            )
            for wheel in runner_wheels
        }
        receipt = validate_runner_receipt(
            receipt_path,
            receipt_path.parent,
            source_manifest_path,
            inspections=runner_inspections,
        )
        runner = receipt['runner']
        if receipt_path.parent.name != f'gometry-wheels-{runner}':
            raise AssertionError(
                f'runner receipt directory does not identify {runner}: {receipt_path}'
            )
        if runner in by_runner:
            raise AssertionError(f'duplicate gometry runner receipt: {runner}')
        by_runner[runner] = (receipt_path, receipt)
        inspections_by_runner[runner] = runner_inspections
    actual_runners = set(by_runner)
    if actual_runners != set(expected):
        raise AssertionError(
            f'gometry runner set mismatch: missing={sorted(set(expected) - actual_runners)}, '
            f'extra={sorted(actual_runners - set(expected))}'
        )

    wheels: list[dict[str, Any]] = []
    release_inspections: dict[Path, dict[str, Any]] = {}
    copied_names = {sdist.name}
    shutil.copyfile(sdist, output_dir / sdist.name)
    for runner in expected:
        receipt_path, receipt = by_runner[runner]
        runner_inspections = inspections_by_runner[runner]
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
            release_inspections[output_dir / filename] = runner_inspections[
                source_wheel
            ]
            wheels.append({
                'identifier': item['identifier'],
                'filename': filename,
                'sha256': item['sha256'],
                'runner': runner,
                'extension_member': item['extension_member'],
                'extension_sha256': item['extension_sha256'],
                'build': item['build'],
                'smoke': item['smoke'],
            })
    manifest = {
        'schema': RELEASE_MANIFEST_SCHEMA,
        'kind': 'gometry-release-manifest',
        'package': 'gometry',
        'profile': RELEASE_PROFILE,
        'source': source_identity(source_manifest),
        'expected_runners': expected,
        'wheels': wheels,
    }
    manifest_path = output_dir / RELEASE_MANIFEST_NAME
    _write_json(manifest_path, manifest)
    validate_release_manifest(
        manifest_path,
        output_dir,
        expected_runners=expected,
        inspections=release_inspections,
    )
    return manifest_path


def validate_release_manifest(
    manifest_path: Path,
    distribution_dir: Path,
    *,
    expected_runners: Sequence[str] | None = None,
    inspections: Mapping[Path, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    if (
        manifest_path.parent != distribution_dir
        or manifest_path.name != RELEASE_MANIFEST_NAME
    ):
        raise AssertionError('release manifest must be the sibling canonical manifest')
    manifest = _load_json(manifest_path)
    if (
        manifest.get('schema') != RELEASE_MANIFEST_SCHEMA
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
    source = validate_source_identity(
        manifest.get('source'),
        field='release source',
    )
    source_filename = source['filename']
    source_digest = source['sha256']
    source_sdist = distribution_dir / source_filename
    if not source_sdist.is_file():
        raise AssertionError(f'release sdist is missing: {source_sdist}')
    prepared_licenses = sdist_prepared_license_hashes(source_sdist)
    wheels = manifest.get('wheels')
    if not isinstance(wheels, list) or not wheels:
        raise AssertionError('release manifest has no wheels')
    filenames = {source_filename}
    seen_identifiers: set[str] = set()
    seen_runners: set[str] = set()
    for item in wheels:
        if not isinstance(item, dict) or set(item) != {
            'identifier',
            'filename',
            'sha256',
            'runner',
            'extension_member',
            'extension_sha256',
            'build',
            'smoke',
        }:
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
        build = _validate_build_identity(
            item.get('build'),
            field=f'release wheel {filename} build',
        )
        _validate_runner_target(runner, build)
        if build['dependencies'] != source['cargo']:
            raise AssertionError(
                f'release wheel dependency identity mismatch: {filename}'
            )
        filenames.add(filename)
        seen_identifiers.add(identifier)
        seen_runners.add(runner)
        path = distribution_dir / filename
        if not path.is_file():
            raise AssertionError(f'release wheel is missing: {path}')
        if sha256_file(path) != digest:
            raise AssertionError(f'release wheel SHA-256 mismatch: {filename}')
        inspection = (
            inspections[path]
            if inspections is not None and path in inspections
            else inspect_wheel(
                path,
                release=True,
                expected_python_members=source['python_members'],
                expected_prepared_inputs=prepared_licenses,
            )
        )
        if not wheel_matches_identifier(path, identifier, inspection['tags']):
            raise AssertionError(
                f'release wheel does not match its build identifier: {filename} / {identifier}'
            )
        if wheel_architecture(path, inspection['tags']) != build['architecture']:
            raise AssertionError(f'release wheel architecture mismatch: {filename}')
        if not wheel_target_compatible(path, build['target'], inspection['tags']):
            raise AssertionError(f'release wheel target mismatch: {filename}')
        validate_wheel_native_target(
            inspection,
            _target_policy_identity(build['target']),
            label=filename,
        )
        if not wheel_python_members(inspection['members']) == source['python_members']:
            raise AssertionError(
                f'release wheel Python members differ from source: {filename}'
            )
        extensions = inspection['extension_members']
        if len(extensions) != 1:
            raise AssertionError(f'release wheel has no unique extension: {filename}')
        extension_name, extension_payload = next(iter(extensions.items()))
        extension_digest = _sha256_bytes(extension_payload)
        if (
            item['extension_member'] != extension_name
            or item['extension_sha256'] != extension_digest
        ):
            raise AssertionError(
                f'release wheel extension identity mismatch: {filename}'
            )
        maturin = build['maturin']
        wheel_member = f'{inspection["dist_info"]}/WHEEL'
        if (
            maturin['repaired_wheel_member_sha256']
            != inspection['members'][wheel_member]['sha256']
            or maturin['repaired_generator'] != inspection['generator']
        ):
            raise AssertionError(
                f'release wheel Generator identity mismatch: {filename}'
            )
        smoke = _validate_smoke_identity(
            item['smoke'],
            field=f'release wheel {filename} smoke',
            source=source,
            build=build,
            extension_member=extension_name,
            extension_sha256=extension_digest,
        )
        if smoke['log_sha256'] != build['command']['host_log_sha256']:
            raise AssertionError(
                f'release wheel smoke log is not runner-bound: {filename}'
            )
    if seen_runners != set(runners):
        raise AssertionError(
            f'release manifest does not cover every runner: '
            f'missing={sorted(set(runners) - seen_runners)}'
        )
    if sha256_file(source_sdist) != source_digest:
        raise AssertionError(f'release sdist SHA-256 mismatch: {source_filename}')
    sdist_version = inspect_sdist(
        source_sdist,
        expected_libtiff=libtiff_source_members(),
    )
    if sdist_version != source['version']:
        raise AssertionError(
            'release manifest sdist version does not match its archive'
        )
    evidence = verify_sdist_dependency_sources(source_sdist, require_normalized=True)
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
    if sdist_python_member_hashes(source_sdist) != source['python_members']:
        raise AssertionError('release source Python members differ from manifest')
    actual_files = {path.name for path in distribution_dir.iterdir()}
    expected_files = filenames | {manifest_path.name}
    if actual_files != expected_files:
        raise AssertionError(
            f'release distribution directory mismatch: '
            f'missing={sorted(expected_files - actual_files)}, '
            f'extra={sorted(actual_files - expected_files)}'
        )
    return manifest


def _quality_benchmark_root(receipt_path: Path) -> Path | None:
    if len(receipt_path.parts) < 4 or receipt_path.parts[-3:] != (
        'target',
        'quality-runtime',
        'receipt.json',
    ):
        return None
    return receipt_path.parents[2]


def _quality_benchmark_subject(
    root: Path,
    quality_receipt: Mapping[str, Any],
    receipt_digest: str,
) -> dict[str, Any]:
    wheel = quality_receipt['wheel']
    source = quality_receipt['source']
    wheel_item = {
        'filename': wheel['filename'],
        'sha256': wheel['sha256'],
        'extension_member': wheel['extension_member'],
        'extension_sha256': wheel['extension_sha256'],
    }
    return {
        'kind': 'quality',
        'path': str(quality_receipt_path(root).resolve()),
        'receipt_sha256': receipt_digest,
        'source': {
            'commit': None,
            'version': source['version'],
            'source_inputs_sha256': source['source_inputs_sha256'],
            'native_source_inputs_sha256': source['native_source_inputs_sha256'],
            'python_members': dict(source['python_members']),
        },
        'profile': quality_receipt['profile'],
        'build': quality_receipt['build'],
        'wheel': wheel_item,
        'wheels': [wheel_item],
        'package_root': quality_receipt['package_root'],
        'python_members': dict(quality_receipt['python_members']),
        'prepared_inputs': quality_receipt['prepared_inputs']['files'],
        'runtime_receipt': dict(quality_receipt),
    }


def validate_benchmark_receipt(receipt_path: Path) -> dict[str, Any]:
    """Validate an existing quality, release, or runner subject for benchmarking."""
    receipt_path = receipt_path.resolve()
    if receipt_path.is_dir():
        if (receipt_path / RELEASE_MANIFEST_NAME).is_file():
            receipt_path = receipt_path / RELEASE_MANIFEST_NAME
        elif (receipt_path / RUNNER_RECEIPT_NAME).is_file():
            receipt_path = receipt_path / RUNNER_RECEIPT_NAME
        elif (
            receipt_path.name == 'quality-runtime'
            and receipt_path.parent.name == 'target'
            and (receipt_path / 'receipt.json').is_file()
        ):
            receipt_path = receipt_path / 'receipt.json'
        else:
            raise AssertionError(
                f'benchmark receipt directory has no canonical receipt: {receipt_path}'
            )
    quality_root = _quality_benchmark_root(receipt_path)
    if quality_root is not None:
        root = quality_root
        canonical_path = quality_receipt_path(root).resolve()
        if receipt_path != canonical_path:
            raise AssertionError(
                'quality benchmark receipt must be at its canonical path'
            )
        quality_receipt = read_quality_receipt(root)
        validate_quality_producer_inputs(root, quality_receipt)
        return _quality_benchmark_subject(
            root,
            quality_receipt,
            sha256_file(canonical_path),
        )
    if receipt_path.name == RELEASE_MANIFEST_NAME:
        receipt = validate_release_manifest(receipt_path, receipt_path.parent)
        return {
            'kind': 'release',
            'path': str(receipt_path),
            'receipt_sha256': sha256_file(receipt_path),
            'source': receipt['source'],
            'wheels': receipt['wheels'],
        }
    if receipt_path.name == RUNNER_RECEIPT_NAME:
        source_manifest_path = receipt_path.parent / SOURCE_MANIFEST_NAME
        receipt = validate_runner_receipt(
            receipt_path,
            receipt_path.parent,
            source_manifest_path,
        )
        return {
            'kind': 'runner',
            'path': str(receipt_path),
            'receipt_sha256': sha256_file(receipt_path),
            'source': receipt['source'],
            'wheels': receipt['wheels'],
        }
    raise AssertionError(
        f'benchmark receipt must be {RELEASE_MANIFEST_NAME} or {RUNNER_RECEIPT_NAME}'
    )


def read_benchmark_receipt(
    receipt_path: Path, expected_receipt_sha256: str
) -> dict[str, Any]:
    """Read the parent-validated receipt projection without rescanning its wheels."""
    receipt_path = receipt_path.resolve()
    if (
        not isinstance(expected_receipt_sha256, str)
        or HEX_DIGEST.fullmatch(expected_receipt_sha256) is None
        or sha256_file(receipt_path) != expected_receipt_sha256
    ):
        raise AssertionError('validated benchmark receipt changed before dispatch')
    quality_root = _quality_benchmark_root(receipt_path)
    if quality_root is not None:
        return _quality_benchmark_subject(
            quality_root,
            read_quality_receipt(quality_root),
            expected_receipt_sha256,
        )
    receipt = _load_json(receipt_path)
    if receipt_path.name == RELEASE_MANIFEST_NAME:
        if (
            receipt.get('schema') != RELEASE_MANIFEST_SCHEMA
            or receipt.get('kind') != 'gometry-release-manifest'
        ):
            raise AssertionError('validated release receipt schema changed')
        source = validate_source_identity(receipt.get('source'), field='release source')
        wheels = receipt.get('wheels')
    elif receipt_path.name == RUNNER_RECEIPT_NAME:
        if (
            receipt.get('schema') != RUNNER_RECEIPT_SCHEMA
            or receipt.get('kind') != 'gometry-wheel-receipt'
        ):
            raise AssertionError('validated runner receipt schema changed')
        source = validate_source_identity(receipt.get('source'), field='runner source')
        wheels = receipt.get('wheels')
    else:
        raise AssertionError('validated benchmark receipt has an unknown file name')
    if not isinstance(wheels, list) or not wheels:
        raise AssertionError('validated benchmark receipt has no wheel items')
    for item in wheels:
        if not isinstance(item, dict):
            raise TypeError('validated benchmark receipt wheel is malformed')
        if (
            not isinstance(item.get('extension_member'), str)
            or not isinstance(item.get('extension_sha256'), str)
            or HEX_DIGEST.fullmatch(item['extension_sha256']) is None
            or not isinstance(item.get('sha256'), str)
            or HEX_DIGEST.fullmatch(item['sha256']) is None
        ):
            raise AssertionError('validated benchmark receipt extension is malformed')
    return {
        'kind': 'release' if receipt_path.name == RELEASE_MANIFEST_NAME else 'runner',
        'path': str(receipt_path),
        'receipt_sha256': expected_receipt_sha256,
        'source': source,
        'wheels': wheels,
    }


def match_benchmark_receipt(
    receipt: Mapping[str, Any],
    extension: Mapping[str, Any],
    distribution: Mapping[str, Any] | None,
    *,
    runtime_binding: Mapping[str, Any],
    verify_archive: bool = True,
) -> dict[str, Any]:
    """Bind admitted facade, distribution, extension, and wheel facts to a subject."""
    source = receipt.get('source')
    if not isinstance(source, Mapping):
        raise TypeError('benchmark receipt has no source identity')
    if 'commit' not in source:
        raise AssertionError('benchmark receipt source identity has no commit field')
    commit = source['commit']
    if commit is not None and (
        not isinstance(commit, str) or re.fullmatch(r'[0-9a-f]{40,64}', commit) is None
    ):
        raise AssertionError('benchmark receipt source commit is malformed')
    extension_path = extension.get('path')
    extension_sha256 = extension.get('sha256')
    size_bytes = extension.get('size_bytes')
    build_info = extension.get('build_info')
    if (
        not isinstance(extension_path, str)
        or not isinstance(extension_sha256, str)
        or HEX_DIGEST.fullmatch(extension_sha256) is None
        or not isinstance(size_bytes, int)
        or isinstance(size_bytes, bool)
        or not isinstance(build_info, dict)
    ):
        raise AssertionError('loaded benchmark extension identity is malformed')
    if not isinstance(distribution, Mapping):
        raise TypeError('loaded gometry distribution identity is missing')
    if str(distribution.get('name', '')).lower() != 'gometry' or distribution.get(
        'version'
    ) != source.get('version'):
        raise AssertionError(
            'benchmark receipt does not identify the loaded distribution'
        )
    if build_info.get('source_inputs_sha256') != source.get(
        'source_inputs_sha256'
    ) or build_info.get('native_source_inputs_sha256') != source.get(
        'native_source_inputs_sha256'
    ):
        raise AssertionError(
            'benchmark receipt source/native identity differs from extension'
        )

    if not isinstance(runtime_binding, Mapping):
        raise TypeError('benchmark runtime binding is missing')
    subject_path = receipt.get('path')
    subject_digest = receipt.get('receipt_sha256')
    if (
        not isinstance(subject_path, str)
        or runtime_binding.get('path') != subject_path
        or runtime_binding.get('sha256') != subject_digest
    ):
        raise AssertionError('benchmark runtime binding names another receipt')
    facade = runtime_binding.get('facade')
    runtime_extension = runtime_binding.get('extension')
    binding_wheel = runtime_binding.get('wheel')
    binding_distribution = runtime_binding.get('distribution')
    python_members = runtime_binding.get('python_members')
    if (
        not isinstance(facade, Mapping)
        or not isinstance(runtime_extension, Mapping)
        or not isinstance(binding_wheel, Mapping)
        or not isinstance(binding_distribution, Mapping)
        or not isinstance(python_members, Mapping)
    ):
        raise TypeError('benchmark runtime binding is malformed')
    if (
        runtime_extension.get('path') != extension_path
        or runtime_extension.get('sha256') != extension_sha256
        or binding_distribution != distribution
    ):
        raise AssertionError('benchmark runtime files differ from loaded identity')
    if (
        not isinstance(facade.get('path'), str)
        or not isinstance(facade.get('sha256'), str)
        or facade.get('sha256') != python_members.get('gometry/__init__.py')
    ):
        raise AssertionError('benchmark facade differs from admitted Python members')
    try:
        Path(str(facade['path'])).resolve().relative_to(
            Path(str(distribution['location'])).resolve()
        )
    except (KeyError, ValueError) as error:
        raise AssertionError(
            'benchmark facade is outside the owning gometry distribution'
        ) from error

    if receipt.get('kind') == 'quality':
        raw_receipt = receipt.get('runtime_receipt')
        if not isinstance(raw_receipt, Mapping):
            raise AssertionError('quality benchmark subject lost its runtime receipt')
        raw_wheel = raw_receipt.get('wheel')
        root = _quality_benchmark_root(Path(subject_path))
        if not isinstance(raw_wheel, Mapping) or root is None:
            raise AssertionError('quality benchmark subject has malformed wheel paths')
        expected_wheel_path = (root.parent / str(raw_wheel.get('path'))).resolve()
        expected_python_members = source.get('python_members')
        expected_profile = raw_receipt.get('build', {}).get('profile')
        wheel_items = receipt.get('wheels')
    else:
        expected_wheel_path = (
            Path(subject_path).resolve().parent / str(binding_wheel.get('filename'))
        ).resolve()
        expected_python_members = source.get('python_members')
        expected_profile = RELEASE_PROFILE
        wheel_items = receipt.get('wheels')
    if runtime_binding.get('profile') != expected_profile:
        raise AssertionError('benchmark runtime profile differs from receipt')
    if not isinstance(wheel_items, list):
        raise TypeError('benchmark receipt has no wheel records')
    matches = [
        item
        for item in wheel_items
        if isinstance(item, Mapping)
        and item.get('filename') == binding_wheel.get('filename')
        and item.get('sha256') == binding_wheel.get('archive', {}).get('sha256')
        and item.get('extension_member') == binding_wheel.get('extension_member')
        and item.get('extension_sha256') == binding_wheel.get('extension_sha256')
    ]
    if len(matches) != 1:
        raise AssertionError(
            'benchmark receipt does not uniquely bind the admitted wheel archive/member'
        )
    item = matches[0]
    wheel_path = binding_wheel.get('path')
    archive_identity = binding_wheel.get('archive')
    if (
        not isinstance(wheel_path, str)
        or Path(wheel_path).resolve() != expected_wheel_path
        or not isinstance(archive_identity, Mapping)
        or archive_identity.get('sha256') != item.get('sha256')
    ):
        raise AssertionError('benchmark wheel archive differs from selected receipt')
    if verify_archive and (
        not expected_wheel_path.is_file()
        or sha256_file(expected_wheel_path) != archive_identity.get('sha256')
    ):
        raise AssertionError(
            'benchmark wheel archive bytes differ from selected receipt'
        )
    expected_member = item.get('extension_member')
    if expected_member != binding_wheel.get('extension_member'):
        raise AssertionError('benchmark wheel member differs from selected receipt')
    profile = expected_profile
    if receipt.get('kind') == 'quality':
        if python_members != raw_receipt.get('python_members'):
            raise AssertionError('quality Python overlay differs from its receipt')
    elif dict(python_members) != dict(expected_python_members):
        raise AssertionError('admitted Python members differ from release wheel')
    if verify_archive:
        wheel_inspection = inspect_wheel(
            expected_wheel_path,
            release=profile != 'dev',
            expected_python_members=(
                expected_python_members if receipt.get('kind') != 'quality' else None
            ),
        )
        extension_members = wheel_inspection['extension_members']
        if (
            set(extension_members) != {expected_member}
            or _sha256_bytes(next(iter(extension_members.values()))) != extension_sha256
        ):
            raise AssertionError('admitted extension bytes differ from wheel member')
    if (
        Path(extension_path).resolve()
        != Path(str(distribution['location'])).resolve() / expected_member
    ):
        raise AssertionError(
            'loaded extension is not owned by the selected distribution'
        )
    return {
        'source_commit': commit,
        'receipt_path': receipt['path'],
        'receipt_sha256': receipt['receipt_sha256'],
        'wheel_filename': item['filename'],
        'wheel_sha256': item['sha256'],
        'extension_member': item['extension_member'],
        'extension_sha256': item['extension_sha256'],
    }


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
    cpu_features = json.loads(os.environ['GOMETRY_SMOKE_CPU_FEATURES'])
    features = set(target_features.split(','))
    required_v2_features = set(cpu_features['v2'])
    forbidden_v3_features = set(cpu_features['v3_plus'])
    assert required_v2_features <= features, target_features
    assert not forbidden_v3_features.intersection(features), target_features

machine = platform.machine().lower()
machine_architecture = (
    'x86_64' if machine in {'x86_64', 'amd64'} else
    'arm64' if machine in {'aarch64', 'arm64'} else machine
)
assert machine_architecture == os.environ['GOMETRY_SMOKE_EXPECTED_HOST_ARCHITECTURE']
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
assert projected.crs == gm.CRS('EPSG:3857')
assert point.buffer(1000.0).area > 0
assert gm.distance(point, other) > 0
info = gm.CRS(4326).info
assert info['authority'] == 'EPSG' and info['code'] == '4326'
assert gm.crs_grid('null')['available'] is True
transformed = gm.crs_transform(4326, 3857, 21.0, 52.0)
assert np.all(np.isfinite(transformed))

wkb = point.to_wkb(include_srid=True)
assert gm.from_wkb(wkb).crs == gm.CRS('EPSG:4326')
assert gm.from_wkt(point.to_wkt(), crs=4326).crs == gm.CRS('EPSG:4326')
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

mask = gm.contains(gm.box(20, 51, 22, 53), x=[21.0, 30.0], y=[52.0, 52.0])
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
