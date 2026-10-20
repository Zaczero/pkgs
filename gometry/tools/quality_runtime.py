"""Local identified DEV runtime preparation and reuse."""

from __future__ import annotations

import errno
import json
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any

from _artifact_common import (
    HEX_DIGEST,
    MATURIN_VERSION,
    PREPARED_OUTPUT_FILES,
    RELEASE_ENV_VARS,
    _atomic_json,
    _maturin_command,
    _package_version,
    _prepared_input_identity,
    _prepared_input_record,
    _prepared_output_hashes,
    _runtime_interpreter,
    _rust_target_triple,
    _sha256_bytes,
    _tool_output,
    run,
    sha256_file,
)
from artifact_content import inspect_wheel, python_member_hashes, source_input_digests
from source_artifact import (
    cargo_metadata,
    prepare_license_outputs,
    required_source_records,
)

if TYPE_CHECKING:
    from collections.abc import Mapping


# This is the narrow recoverable-admission signal, not a terminal error.
class QualityPreparationRequired(Exception):  # ruff: ignore[error-suffix-on-exception-name]
    """A known missing or stale local DEV artifact can be rebuilt once."""


QUALITY_RECEIPT_SCHEMA = 1
QUALITY_RECEIPT_NAME = 'receipt.json'


def _quality_settings_identity(
    files: Mapping[str, str],
    environment: Mapping[str, str | None],
) -> str:
    return _sha256_bytes(
        json.dumps(
            {
                'files': files,
                'environment': environment,
            },
            sort_keys=True,
            separators=(',', ':'),
        ).encode('utf-8')
    )


def quality_receipt_path(root: Path) -> Path:
    return root / 'target' / 'quality-runtime' / QUALITY_RECEIPT_NAME


def _quality_fixed_input_paths(root: Path) -> tuple[tuple[str, Path], ...]:
    workspace = root.parent
    return (
        ('Cargo.toml', workspace / 'Cargo.toml'),
        ('Cargo.lock', workspace / 'Cargo.lock'),
        ('.cargo/config.toml', workspace / '.cargo' / 'config.toml'),
        ('gometry/Cargo.toml', root / 'Cargo.toml'),
        ('gometry/pyproject.toml', root / 'pyproject.toml'),
        ('gometry/rust-toolchain.toml', root / 'rust-toolchain.toml'),
        ('gometry/native/source_roots.txt', root / 'native' / 'source_roots.txt'),
        (
            'vendor/proj/proj-9.9.0.tar.gz',
            root.parent / 'vendor' / 'proj' / 'proj-9.9.0.tar.gz',
        ),
        ('gometry/README.md', root / 'README.md'),
        ('gometry/LICENSE-APACHE.md', root / 'LICENSE-APACHE.md'),
        ('gometry/LICENSE-MIT.md', root / 'LICENSE-MIT.md'),
        ('gometry/LICENSE-THIRD-PARTY.md', root / 'LICENSE-THIRD-PARTY.md'),
    )


def read_quality_receipt(root: Path) -> dict[str, Any]:
    """Read a receipt that is well-formed and was built by the pinned Maturin."""
    return _load_quality_receipt(root, allow_stale=False)


def _load_quality_receipt(root: Path, *, allow_stale: bool) -> dict[str, Any]:
    """Validate a receipt's shape and optionally require current build inputs.

    Preparing a runtime reads the previous receipt to decide whether it can be
    reused. Older Maturin versions and input inventories require a rebuild,
    while inconsistent receipt identities remain errors.
    """
    path = quality_receipt_path(root)
    try:
        receipt = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AssertionError(
            f'quality runtime receipt is unavailable: {path}: {error}'
        ) from error
    if not isinstance(receipt, dict):
        raise TypeError(f'quality runtime receipt is not an object: {path}')
    if receipt.get('kind') != 'gometry-quality-runtime':
        raise AssertionError(f'quality runtime receipt kind is malformed: {path}')
    schema = receipt.get('schema')
    if type(schema) is not int or schema != QUALITY_RECEIPT_SCHEMA:
        raise AssertionError(f'quality runtime receipt schema is unsupported: {path}')
    if set(receipt) != {
        'schema',
        'kind',
        'package_root',
        'profile',
        'source',
        'prepared_inputs',
        'build',
        'wheel',
        'python_members',
    }:
        raise AssertionError(f'quality runtime receipt fields are malformed: {path}')
    package_root = receipt.get('package_root')
    if (
        not isinstance(package_root, str)
        or not package_root
        or not Path(package_root).is_absolute()
    ):
        raise AssertionError(f'quality runtime package root is malformed: {path}')
    source = receipt['source']
    source_fields = {
        'source_inputs_sha256',
        'native_source_inputs_sha256',
        'version',
        'python_members',
        'cargo',
    }
    if not isinstance(source, dict) or set(source) != source_fields:
        raise AssertionError(f'quality runtime source identity is malformed: {path}')
    for name in ('source_inputs_sha256', 'native_source_inputs_sha256'):
        digest = source.get(name)
        if not isinstance(digest, str) or HEX_DIGEST.fullmatch(digest) is None:
            raise AssertionError(f'quality runtime {name} is malformed: {path}')
    if not isinstance(source.get('version'), str) or not source['version']:
        raise AssertionError(f'quality runtime source version is malformed: {path}')
    if (
        not isinstance(source.get('python_members'), dict)
        or not source['python_members']
        or (source['python_members'] != receipt['python_members'])
    ):
        raise AssertionError(
            f'quality runtime Python-member identity is malformed: {path}'
        )
    if not all(
        isinstance(name, str)
        and name.startswith('gometry/')
        and isinstance(digest, str)
        and HEX_DIGEST.fullmatch(digest) is not None
        for name, digest in receipt['python_members'].items()
    ):
        raise AssertionError(
            f'quality runtime Python-member hashes are malformed: {path}'
        )
    cargo = source.get('cargo')
    if not isinstance(cargo, dict) or set(cargo) != {'lock_sha256', 'required_sources'}:
        raise AssertionError(f'quality runtime Cargo identity is malformed: {path}')
    if (
        not isinstance(cargo.get('lock_sha256'), str)
        or HEX_DIGEST.fullmatch(cargo['lock_sha256']) is None
        or not isinstance(cargo.get('required_sources'), list)
        or not cargo['required_sources']
        or not all(isinstance(record, dict) for record in cargo['required_sources'])
    ):
        raise AssertionError(f'quality runtime Cargo identity is malformed: {path}')
    build = receipt['build']
    if not isinstance(build, dict) or set(build) != {
        'profile',
        'target',
        'raw_target_arch',
        'interpreter',
        'compiler',
        'cargo',
        'maturin_version',
        'settings',
        'command',
    }:
        raise AssertionError(f'quality runtime build identity is malformed: {path}')
    if build.get('profile') != 'dev' or receipt.get('profile') != 'dev':
        raise AssertionError(f'quality runtime profile is not dev: {path}')
    target = build.get('target')
    raw_target_arch = build.get('raw_target_arch')
    if (
        not isinstance(target, str)
        or not target
        or not isinstance(raw_target_arch, str)
        or raw_target_arch != target.split('-', 1)[0]
    ):
        raise AssertionError(f'quality runtime target identity is malformed: {path}')
    interpreter = build.get('interpreter')
    if (
        not isinstance(interpreter, dict)
        or set(interpreter)
        != {
            'pid',
            'executable',
            'resolved_executable',
            'prefix',
            'version',
            'implementation',
            'cache_tag',
            'ext_suffix',
            'gil_disabled',
        }
        or not all(
            isinstance(interpreter.get(name), str)
            for name in (
                'executable',
                'resolved_executable',
                'prefix',
                'version',
                'implementation',
                'cache_tag',
                'ext_suffix',
            )
        )
        or not isinstance(interpreter.get('pid'), int)
        or isinstance(interpreter.get('pid'), bool)
        or not isinstance(interpreter.get('gil_disabled'), bool)
    ):
        raise AssertionError(
            f'quality runtime interpreter identity is malformed: {path}'
        )
    if any(
        not interpreter[name]
        for name in (
            'executable',
            'resolved_executable',
            'prefix',
            'version',
            'implementation',
            'cache_tag',
            'ext_suffix',
        )
    ):
        raise AssertionError(
            f'quality runtime interpreter identity is incomplete: {path}'
        )
    compiler = build.get('compiler')
    if (
        not isinstance(compiler, dict)
        or set(compiler) != {'observed_at', 'rustc_vv'}
        or compiler.get('observed_at') != 'before-build'
        or not isinstance(compiler.get('rustc_vv'), str)
        or not compiler['rustc_vv']
    ):
        raise AssertionError(f'quality runtime compiler identity is malformed: {path}')
    build_cargo = build.get('cargo')
    if (
        not isinstance(build_cargo, dict)
        or set(build_cargo) != {'version', 'dependencies'}
        or not isinstance(build_cargo.get('version'), str)
        or not build_cargo['version']
        or build_cargo.get('dependencies') != cargo
    ):
        raise AssertionError(
            f'quality runtime build Cargo identity is malformed: {path}'
        )
    settings = build.get('settings')
    if (
        not isinstance(settings, dict)
        or set(settings) != {'files', 'environment', 'identity_sha256'}
        or not isinstance(settings.get('files'), dict)
        or not settings['files']
        or not isinstance(settings.get('environment'), dict)
        or not settings['environment']
        or not all(
            isinstance(name, str)
            and isinstance(digest, str)
            and HEX_DIGEST.fullmatch(digest) is not None
            for name, digest in settings['files'].items()
        )
        or not all(
            isinstance(name, str) and (value is None or isinstance(value, str))
            for name, value in settings['environment'].items()
        )
        or settings.get('identity_sha256')
        != _quality_settings_identity(settings['files'], settings['environment'])
    ):
        raise AssertionError(f'quality runtime settings identity is malformed: {path}')
    if not allow_stale and not {
        logical for logical, _ in _quality_fixed_input_paths(root)
    }.issubset(settings['files']):
        raise AssertionError(f'quality runtime settings omit fixed inputs: {path}')
    required_environment = {
        'CARGO_BUILD_TARGET',
        'NIX_DONT_SET_RPATH',
        'PYO3_PYTHON',
        f'NIX_DONT_SET_RPATH_{target.replace("-", "_")}',
    }
    if (
        not required_environment.issubset(settings['environment'])
        or settings['environment']['CARGO_BUILD_TARGET'] != target
        or settings['environment']['NIX_DONT_SET_RPATH'] != '1'
        or settings['environment'][f'NIX_DONT_SET_RPATH_{target.replace("-", "_")}']
        != '1'
        or settings['environment']['PYO3_PYTHON'] != interpreter['resolved_executable']
    ):
        raise AssertionError(
            f'quality runtime settings environment is incomplete: {path}'
        )
    command = build.get('command')
    if (
        not isinstance(command, list)
        or not command
        or not all(isinstance(argument, str) and argument for argument in command)
    ):
        raise AssertionError(f'quality runtime build command is malformed: {path}')
    maturin_version = build.get('maturin_version')
    if (
        not isinstance(maturin_version, str)
        or not maturin_version
        or (not allow_stale and maturin_version != MATURIN_VERSION)
    ):
        raise AssertionError(f'quality runtime Maturin version is malformed: {path}')
    wheel = receipt['wheel']
    if not isinstance(wheel, dict) or set(wheel) != {
        'path',
        'filename',
        'sha256',
        'generator',
        'extension_member',
        'extension_sha256',
    }:
        raise AssertionError(f'quality runtime wheel identity is malformed: {path}')
    if (
        not isinstance(wheel.get('path'), str)
        or not wheel['path']
        or Path(wheel['path']).is_absolute()
        or '..' in Path(wheel['path']).parts
        or not isinstance(wheel.get('filename'), str)
        or Path(wheel['filename']).name != wheel['filename']
        or not wheel['filename'].endswith('.whl')
        or wheel.get('generator') != f'maturin ({maturin_version})'
        or not isinstance(wheel.get('extension_member'), str)
        or not wheel['extension_member'].startswith('gometry/')
        or '..' in Path(wheel['extension_member']).parts
    ):
        raise AssertionError(f'quality runtime wheel path is malformed: {path}')
    for name in ('sha256', 'extension_sha256'):
        digest = wheel.get(name)
        if not isinstance(digest, str) or HEX_DIGEST.fullmatch(digest) is None:
            raise AssertionError(f'quality runtime wheel {name} is malformed: {path}')
    wheel_path_parts = Path(wheel['path']).parts
    if (
        len(wheel_path_parts) < 3
        or wheel_path_parts[-2] != wheel['sha256']
        or wheel_path_parts[-1] != wheel['filename']
    ):
        raise AssertionError(f'quality runtime wheel path is inconsistent: {path}')
    prepared = receipt['prepared_inputs']
    if (
        not isinstance(prepared, dict)
        or set(prepared) != {'identity_sha256', 'files'}
        or not isinstance(prepared.get('identity_sha256'), str)
        or HEX_DIGEST.fullmatch(prepared['identity_sha256']) is None
        or not isinstance(prepared.get('files'), dict)
        or not prepared['files']
        or set(prepared['files']) != set(PREPARED_OUTPUT_FILES)
        or not all(
            isinstance(name, str)
            and isinstance(digest, str)
            and HEX_DIGEST.fullmatch(digest) is not None
            for name, digest in prepared['files'].items()
        )
        or _prepared_input_identity(prepared['files']) != prepared['identity_sha256']
    ):
        raise AssertionError(
            f'quality runtime prepared-input identity is malformed: {path}'
        )
    if prepared['files'].get('python/gometry/_lib.pyi') != receipt[
        'python_members'
    ].get('gometry/_lib.pyi'):
        raise AssertionError(
            f'quality runtime prepared stub identity disagrees: {path}'
        )
    return receipt


def quality_settings(root: Path, environment: Mapping[str, str]) -> dict[str, Any]:
    files: dict[str, str] = {}
    for logical, path in _quality_fixed_input_paths(root):
        if not path.is_file():
            raise AssertionError(f'quality build input is missing: {path}')
        files[logical] = sha256_file(path)
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
    for path in config_files:
        logical = f'cargo-config/{path.resolve().as_posix().lstrip("/")}'
        files[logical] = sha256_file(path)
    for name in (
        'PYO3_CONFIG_FILE',
        'CMAKE_TOOLCHAIN_FILE',
        *sorted(
            name for name in environment if name.startswith('CMAKE_TOOLCHAIN_FILE_')
        ),
    ):
        selected = environment.get(name)
        if not selected:
            continue
        path = Path(selected)
        if not path.is_absolute():
            path = root / path
        if not path.is_file():
            raise AssertionError(f'quality build configuration file is missing: {path}')
        logical = f'build-config/{name}/{path.resolve().as_posix().lstrip("/")}'
        files[logical] = sha256_file(path)
    variables = {
        'MATURIN_PEP517_ARGS',
        'CARGO_BUILD_TARGET',
        'CARGO_BUILD_RUSTC',
        'CARGO_BUILD_RUSTC_WRAPPER',
        'CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER',
        'RUSTFLAGS',
        'CARGO_ENCODED_RUSTFLAGS',
        'RUSTC',
        'RUSTC_WRAPPER',
        'RUSTC_WORKSPACE_WRAPPER',
        'RUSTUP_TOOLCHAIN',
        'CARGO_HOME',
        'SOURCE_DATE_EPOCH',
        'PYO3_PYTHON',
        'PYO3_CONFIG_FILE',
        'PYO3_CROSS',
        'PYO3_CROSS_LIB_DIR',
        'PYO3_CROSS_PYTHON_VERSION',
        'PYO3_CROSS_PYTHON_IMPLEMENTATION',
        'CFLAGS',
        'CXXFLAGS',
        'CPPFLAGS',
        'TARGET_CFLAGS',
        'TARGET_CXXFLAGS',
        'TARGET_CC',
        'TARGET_CXX',
        'TARGET_AR',
        'HOST_CFLAGS',
        'HOST_CXXFLAGS',
        'HOST_CC',
        'HOST_CXX',
        'HOST_AR',
        'CC',
        'CXX',
        'AR',
        'CROSS_COMPILE',
        'CMAKE_ARGS',
        'CMAKE_TOOLCHAIN_FILE',
        'CMAKE_C_FLAGS',
        'CMAKE_CXX_FLAGS',
        'CMAKE_C_FLAGS_RELEASE',
        'CMAKE_CXX_FLAGS_RELEASE',
        'NIX_DONT_SET_RPATH',
        'NIX_LDFLAGS',
        'MACOSX_DEPLOYMENT_TARGET',
    }
    variables.update(
        name
        for name in environment
        if name.startswith((
            'CFLAGS_',
            'CXXFLAGS_',
            'CC_',
            'CXX_',
            'AR_',
            'CMAKE_TOOLCHAIN_FILE_',
            'NIX_DONT_SET_RPATH_',
            'CARGO_PROFILE_',
        ))
    )
    variables.update(
        name
        for name in environment
        if name.startswith('CARGO_TARGET_')
        and name.rsplit('_', 1)[-1] in {'LINKER', 'RUSTFLAGS'}
    )
    ordered_variables = sorted(variables)
    environment_values = {name: environment.get(name) for name in ordered_variables}
    return {
        'files': files,
        'environment': environment_values,
        'identity_sha256': _quality_settings_identity(files, environment_values),
    }


def validate_quality_producer_inputs(
    root: Path,
    receipt: Mapping[str, Any],
    *,
    check_python_members: bool = True,
    check_prepared_inputs: bool = True,
) -> None:
    """Check the live source/build contract represented by a quality receipt."""
    build = receipt.get('build')
    source = receipt.get('source')
    python_members = receipt.get('python_members')
    if (
        not isinstance(build, dict)
        or not isinstance(source, dict)
        or not isinstance(python_members, dict)
    ):
        raise TypeError('quality receipt has no producer input identity')
    build_cargo = build.get('cargo')
    dependencies = (
        build_cargo.get('dependencies') if isinstance(build_cargo, dict) else None
    )
    if not isinstance(dependencies, dict) or source.get('cargo') != dependencies:
        raise AssertionError(
            'quality receipt source and build dependency identities disagree'
        )
    prepared = receipt.get('prepared_inputs')
    prepared_files = prepared.get('files') if isinstance(prepared, dict) else None
    if not isinstance(prepared_files, dict) or prepared_files.get(
        'python/gometry/_lib.pyi'
    ) != python_members.get('gometry/_lib.pyi'):
        raise AssertionError('quality receipt prepared-stub identity is malformed')

    actual_digests = source_input_digests(root)
    if actual_digests['source_inputs_sha256'] != source.get(
        'source_inputs_sha256'
    ) or actual_digests['native_source_inputs_sha256'] != source.get(
        'native_source_inputs_sha256'
    ):
        raise QualityPreparationRequired(
            'quality runtime source/native inputs are stale'
        )
    if _package_version(root) != source.get('version'):
        raise QualityPreparationRequired('quality runtime source version is stale')
    if check_python_members and (
        python_member_hashes(root / 'python' / 'gometry') != python_members
    ):
        raise QualityPreparationRequired('quality runtime Python members are stale')
    if check_prepared_inputs and _prepared_output_hashes(root) != prepared_files:
        raise QualityPreparationRequired('quality runtime prepared inputs are stale')

    settings_environment = os.environ.copy()
    for name in RELEASE_ENV_VARS:
        settings_environment.pop(name, None)
    settings_environment.pop('MATURIN_PEP517_ARGS', None)
    target = build.get('target')
    if not isinstance(target, str):
        raise TypeError('quality runtime has no producer target')
    settings_environment['CARGO_BUILD_TARGET'] = target
    settings_environment['NIX_DONT_SET_RPATH'] = '1'
    settings_environment[f'NIX_DONT_SET_RPATH_{target.replace("-", "_")}'] = '1'
    interpreter = build.get('interpreter')
    build_settings = build.get('settings')
    build_environment = (
        build_settings.get('environment') if isinstance(build_settings, dict) else None
    )
    producer_python = (
        interpreter.get('resolved_executable')
        if isinstance(interpreter, dict)
        else None
    )
    if (
        not isinstance(build_environment, dict)
        or not isinstance(producer_python, str)
        or build_environment.get('PYO3_PYTHON') != producer_python
    ):
        raise AssertionError('quality runtime has inconsistent producer Python facts')
    settings_environment['PYO3_PYTHON'] = producer_python
    if quality_settings(root, settings_environment) != build_settings:
        raise QualityPreparationRequired('quality runtime build settings are stale')

    # The receipt records Cargo's producer-time resolution; admission binds it
    # to the current manifests, lockfile and build settings without invoking Cargo.
    if dependencies.get('lock_sha256') != sha256_file(root.parent / 'Cargo.lock'):
        raise QualityPreparationRequired('quality runtime Cargo.lock input is stale')


def _materialize_quality_package(
    wheel: Path,
    *,
    scratch: Path,
    root: Path,
    python_members: Mapping[str, str],
    wheel_sha256: str,
    inspection: Mapping[str, Any],
) -> Path:
    if (
        not isinstance(wheel_sha256, str)
        or HEX_DIGEST.fullmatch(wheel_sha256) is None
        or sha256_file(wheel) != wheel_sha256
    ):
        raise AssertionError('quality wheel differs from its package identity')
    if len(inspection['extension_members']) != 1:
        raise AssertionError('quality wheel has no unique native extension')
    identity = _sha256_bytes(
        (wheel_sha256 + _prepared_input_identity(python_members)).encode('ascii')
    )
    package_root = scratch / 'packages' / identity
    if not package_root.is_dir():
        package_root.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(
            prefix=f'.{identity[:12]}-', dir=package_root.parent
        ) as temporary:
            temporary_root = Path(temporary)
            with zipfile.ZipFile(wheel) as archive:
                archive.extractall(temporary_root)
            source_package = root / 'python' / 'gometry'
            destination_package = temporary_root / 'gometry'
            expected_names = set(python_members)
            for old_member in (
                set(python_member_hashes(destination_package)) - expected_names
            ):
                relative = PurePosixPath(old_member).relative_to('gometry')
                destination_package.joinpath(*relative.parts).unlink()
            for logical in python_members:
                relative = PurePosixPath(logical).relative_to('gometry')
                source = source_package.joinpath(*relative.parts)
                destination = destination_package.joinpath(*relative.parts)
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, destination)
            _verify_materialized_quality_package(
                temporary_root,
                inspection,
                python_members,
            )
            try:
                os.replace(temporary_root, package_root)
            except OSError as error:
                if (
                    error.errno not in {errno.EEXIST, errno.ENOTEMPTY, errno.EISDIR}
                    or not package_root.is_dir()
                ):
                    raise
    _verify_materialized_quality_package(package_root, inspection, python_members)
    return package_root


def _verify_materialized_quality_package(
    package_root: Path,
    inspection: Mapping[str, Any],
    python_members: Mapping[str, str],
) -> None:
    if python_member_hashes(package_root / 'gometry') != dict(python_members):
        raise AssertionError('prepared quality Python members differ from source')
    archive_python_members = inspection['python_members']
    expected_files = (set(inspection['members']) - set(archive_python_members)) | set(
        python_members
    )
    actual_files = {
        path.relative_to(package_root).as_posix()
        for path in package_root.rglob('*')
        if path.is_file()
        and '__pycache__' not in path.relative_to(package_root).parts
        and path.suffix not in {'.pyc', '.pyo'}
    }
    if actual_files != expected_files:
        raise AssertionError(
            'prepared quality distribution members differ from its wheel: '
            f'missing={sorted(expected_files - actual_files)}, '
            f'extra={sorted(actual_files - expected_files)}'
        )
    for member, record in inspection['members'].items():
        if member in archive_python_members:
            continue
        path = package_root.joinpath(*PurePosixPath(member).parts)
        if sha256_file(path) != record['sha256']:
            raise AssertionError(
                f'prepared quality distribution member differs from its wheel: {member}'
            )


def _admit_quality_candidate(root: Path, receipt: Mapping[str, Any]) -> None:
    """Verify a staged package in a fresh interpreter before publishing its receipt."""
    tools = root / 'tools'
    code = f"""
import json
import sys
from pathlib import Path

sys.path.insert(0, {str(tools)!r})
from _artifact_common import _runtime_interpreter
from runtime_artifact import verify_runtime

receipt = json.load(sys.stdin)
actual = _runtime_interpreter()
selection = {{
    name: actual[name]
    for name in (
        "executable",
        "prefix",
        "implementation",
        "cache_tag",
        "ext_suffix",
        "gil_disabled",
    )
}}
wheel = receipt["wheel"]
verify_runtime(
    Path({str(root)!r}),
    expected_wheel=Path({str(root.parent)!r}) / wheel["path"],
    build_receipt=receipt,
    runtime_selection=selection,
)
"""
    result = subprocess.run(
        [sys.executable, '-c', code],
        cwd=root,
        input=json.dumps(receipt, separators=(',', ':')),
        capture_output=True,
        text=True,
        check=False,
    )
    if result.stdout:
        sys.stderr.write(result.stdout)
    if result.stderr:
        sys.stderr.write(result.stderr)
    if result.returncode:
        raise AssertionError(
            f'quality runtime candidate failed its actual-process admission '
            f'(exit {result.returncode})'
        )


def prepare_quality_runtime(root: Path, *, scratch: Path) -> dict[str, Any]:
    """Build or reuse an identified DEV wheel and prepare its runtime package."""
    root = root.resolve()
    scratch = scratch.resolve()
    scratch.mkdir(parents=True, exist_ok=True)
    environment = os.environ.copy()
    present_release_values = [name for name in RELEASE_ENV_VARS if name in environment]
    if present_release_values:
        raise AssertionError(
            'quality DEV preparation cannot inherit release identity fields: '
            + ', '.join(sorted(present_release_values))
        )
    inherited_maturin_args = environment.get('MATURIN_PEP517_ARGS')
    if inherited_maturin_args and inherited_maturin_args not in {
        '--profile dev',
        '--locked --profile dev',
    }:
        raise AssertionError(
            'quality DEV preparation cannot inherit release Maturin arguments'
        )
    for name in RELEASE_ENV_VARS:
        environment.pop(name, None)
    environment.pop('MATURIN_PEP517_ARGS', None)
    environment.pop('GOMETRY_RELEASE_PROFILE', None)
    environment.pop('GOMETRY_RELEASE_SOURCE_COMMIT', None)
    environment['PYO3_PYTHON'] = str(Path(sys.executable).resolve())
    lock_path = root.parent / 'Cargo.lock'
    lock_before = sha256_file(lock_path)
    prepare_license_outputs(root)
    if sha256_file(lock_path) != lock_before:
        raise AssertionError('license generation changed Cargo.lock')
    source_digests = source_input_digests(root)
    python_members = python_member_hashes(root / 'python' / 'gometry')
    environment['CARGO_TARGET_DIR'] = str(scratch / 'cargo-target')
    target = _rust_target_triple(root, environment=environment)
    environment['CARGO_BUILD_TARGET'] = target
    environment['NIX_DONT_SET_RPATH'] = '1'
    environment[f'NIX_DONT_SET_RPATH_{target.replace("-", "_")}'] = '1'
    metadata = cargo_metadata(root.parent, locked=True)
    dependencies = {
        'lock_sha256': lock_before,
        'required_sources': required_source_records(metadata, root=root.parent),
    }
    maturin_command = _maturin_command(root)
    maturin_version = MATURIN_VERSION
    compiler = _tool_output(['rustc', '-vV'], env=environment, cwd=root)
    cargo = _tool_output(['cargo', '--version'], env=environment, cwd=root)
    settings = quality_settings(root, environment)
    interpreter = _runtime_interpreter()
    if interpreter['executable'] != sys.executable:
        raise AssertionError('quality build selected a different Python executable')

    receipt_path = quality_receipt_path(root)
    current_members = python_members
    if receipt_path.is_file():
        previous = _load_quality_receipt(root, allow_stale=True)
        previous_build = previous['build']
        previous_source = previous['source']
        previous_wheel = previous['wheel']
        previous_cargo = previous_build['cargo']
        previous_interpreter = previous_build['interpreter']
        wheel_path = root.parent / previous_wheel['path']
        if wheel_path.exists():
            if not wheel_path.is_file():
                raise AssertionError('quality receipt wheel path is not a file')
            if sha256_file(wheel_path) != previous_wheel['sha256']:
                raise AssertionError('quality receipt wheel archive digest is corrupt')
        prepared_inputs = _prepared_input_record(_prepared_output_hashes(root))
        can_reuse = (
            previous_build['settings'].get('identity_sha256')
            == settings['identity_sha256']
            and previous_build['compiler']['rustc_vv'] == compiler
            and previous_cargo['version'] == cargo
            and previous_cargo['dependencies'] == dependencies
            and previous_build['maturin_version'] == maturin_version
            and previous_source['source_inputs_sha256']
            == source_digests['source_inputs_sha256']
            and previous_source['native_source_inputs_sha256']
            == source_digests['native_source_inputs_sha256']
            and previous_interpreter['cache_tag'] == interpreter['cache_tag']
            and previous_interpreter['ext_suffix'] == interpreter['ext_suffix']
            and previous_interpreter['gil_disabled'] == interpreter['gil_disabled']
            and wheel_path.is_file()
        )
        if can_reuse:
            if wheel_path.name != previous_wheel['filename']:
                raise AssertionError('quality receipt wheel filename is inconsistent')
            wheel_inspection = inspect_wheel(
                wheel_path,
                release=False,
                expected_prepared_inputs=prepared_inputs['files'],
            )
            if wheel_inspection['generator'] != f'maturin ({MATURIN_VERSION})':
                raise AssertionError(
                    'reused quality wheel has the wrong Maturin Generator'
                )
            extension_member = previous_wheel['extension_member']
            extension_payload = wheel_inspection['extension_members'].get(
                extension_member
            )
            if (
                len(wheel_inspection['extension_members']) != 1
                or extension_payload is None
                or _sha256_bytes(extension_payload)
                != previous_wheel['extension_sha256']
            ):
                raise AssertionError(
                    'reused quality wheel extension identity is corrupt'
                )
            package_root = _materialize_quality_package(
                wheel_path,
                scratch=scratch,
                root=root,
                python_members=current_members,
                wheel_sha256=previous_wheel['sha256'],
                inspection=wheel_inspection,
            )
            refreshed = {
                **previous,
                'package_root': str(package_root),
                'python_members': current_members,
                'prepared_inputs': prepared_inputs,
                'source': {
                    **previous_source,
                    **source_digests,
                    'version': _package_version(root),
                    'python_members': current_members,
                },
            }
            _admit_quality_candidate(root, refreshed)
            validate_quality_producer_inputs(root, refreshed)
            _atomic_json(receipt_path, refreshed)
            return refreshed
    else:
        prepared_inputs = _prepared_input_record(_prepared_output_hashes(root))
    build_env = environment.copy()
    command = [
        *maturin_command,
        'build',
        '--locked',
        '--profile',
        'dev',
        '--interpreter',
        sys.executable,
    ]
    with tempfile.TemporaryDirectory(
        prefix='quality-build-', dir=scratch
    ) as temporary_build:
        wheel_dir = Path(temporary_build)
        run(
            [*command, '--out', str(wheel_dir)],
            cwd=root,
            env=build_env,
            capture_output=False,
        )
        wheels = sorted(wheel_dir.glob('gometry-*.whl'))
        if len(wheels) != 1:
            raise AssertionError(f'expected one quality DEV wheel, found {wheels!r}')
        wheel = wheels[0]
        inspection = inspect_wheel(
            wheel,
            release=False,
            expected_python_members=python_members,
            expected_prepared_inputs=prepared_inputs['files'],
        )
        if inspection['generator'] != f'maturin ({MATURIN_VERSION})':
            raise AssertionError('quality DEV wheel has the wrong Maturin Generator')
        if len(inspection['extension_members']) != 1:
            raise AssertionError('quality DEV wheel has no unique native extension')
        extension_member, extension_payload = next(
            iter(inspection['extension_members'].items())
        )
        wheel_digest = sha256_file(wheel)
        archive_dir = scratch / 'artifacts' / wheel_digest
        archive_path = archive_dir / wheel.name
        archive_dir.mkdir(parents=True, exist_ok=True)
        if archive_path.exists():
            if sha256_file(archive_path) != wheel_digest:
                raise AssertionError('content-addressed quality wheel was replaced')
        else:
            temporary_archive = archive_path.with_name(
                f'.{archive_path.name}.{os.getpid()}.tmp'
            )
            shutil.copyfile(wheel, temporary_archive)
            os.replace(temporary_archive, archive_path)
    package_root = _materialize_quality_package(
        archive_path,
        scratch=scratch,
        root=root,
        python_members=python_members,
        wheel_sha256=wheel_digest,
        inspection=inspection,
    )
    receipt = {
        'schema': QUALITY_RECEIPT_SCHEMA,
        'kind': 'gometry-quality-runtime',
        'package_root': str(package_root),
        'profile': 'dev',
        'source': {
            **source_digests,
            'version': _package_version(root),
            'python_members': current_members,
            'cargo': dependencies,
        },
        'prepared_inputs': prepared_inputs,
        'build': {
            'profile': 'dev',
            'target': target,
            'raw_target_arch': target.split('-', 1)[0],
            'interpreter': interpreter,
            'compiler': {'observed_at': 'before-build', 'rustc_vv': compiler},
            'cargo': {'version': cargo, 'dependencies': dependencies},
            'maturin_version': MATURIN_VERSION,
            'settings': settings,
            'command': command,
        },
        'wheel': {
            'path': archive_path.relative_to(root.parent).as_posix(),
            'filename': wheel.name,
            'sha256': wheel_digest,
            'generator': inspection['generator'],
            'extension_member': extension_member,
            'extension_sha256': _sha256_bytes(extension_payload),
        },
        'python_members': current_members,
    }
    _admit_quality_candidate(root, receipt)
    validate_quality_producer_inputs(root, receipt)
    _atomic_json(receipt_path, receipt)
    return receipt


def refresh_quality_runtime(root: Path) -> dict[str, Any]:
    """Publish generated stub bytes into the prepared runtime used by children."""
    root = root.resolve()
    receipt = read_quality_receipt(root)
    validate_quality_producer_inputs(
        root,
        receipt,
        check_python_members=False,
        check_prepared_inputs=False,
    )
    python_members = python_member_hashes(root / 'python' / 'gometry')
    prepared_inputs = _prepared_input_record(_prepared_output_hashes(root))
    wheel_path = root.parent / receipt['wheel']['path']
    inspection = inspect_wheel(
        wheel_path,
        release=False,
        expected_prepared_inputs=receipt['prepared_inputs']['files'],
    )
    updated_root = _materialize_quality_package(
        wheel_path,
        scratch=quality_receipt_path(root).parent,
        root=root,
        python_members=python_members,
        wheel_sha256=receipt['wheel']['sha256'],
        inspection=inspection,
    )
    updated = {
        **receipt,
        'package_root': str(updated_root),
        'python_members': python_members,
        'source': {
            **receipt['source'],
            'python_members': python_members,
        },
        'prepared_inputs': prepared_inputs,
    }
    _admit_quality_candidate(root, updated)
    validate_quality_producer_inputs(root, updated)
    _atomic_json(quality_receipt_path(root), updated)
    return updated
