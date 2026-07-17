"""Shared benchmark identity and provenance layer.

Owns the variant-artifact probe, environment-equivalence evidence, file and
tool identities, git metadata, and the recorded runtime environment used by
compare.py and matrix.py to freeze and re-verify a comparison identity.
"""

from __future__ import annotations

import hashlib
import json
import os
import shlex
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal, NotRequired, TypedDict

try:
    from bench.system import BenchmarkError
except ModuleNotFoundError:  # Direct ``python bench/<tool>.py`` execution.
    from system import (  # type: ignore[import-not-found, no-redef]
        BenchmarkError,
    )

if TYPE_CHECKING:
    from collections.abc import Sequence

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PYTHON_HASH_SEED = '0'
DEPENDENCY_MODULES = {
    'anyio': 'anyio',
    'h11': 'h11',
    'h2': 'h2',
    'hpack': 'hpack',
    'starlette': 'starlette',
    'uvloop': 'uvloop',
    'websockets': 'websockets',
    'wsproto': 'wsproto',
}


VariantEnvironmentMode = Literal['equivalent', 'confounded-opt-out']


@dataclass(frozen=True, slots=True)
class NamedCommand:
    name: str
    argv: tuple[str, ...]


class FileIdentity(TypedDict):
    path: str
    sha256: str | None


class DependencyIdentity(TypedDict):
    distribution: str
    version: str | None
    path: str | None
    sha256: str | None


class VariantArtifacts(TypedDict):
    executable: str | None
    executable_sha256: str | None
    extension: str | None
    extension_sha256: str | None
    python_executable: str | None
    python_executable_sha256: str | None
    python_version: str | None
    python_implementation: str | None
    python_cache_tag: str | None
    python_abi_flags: str | None
    python_gil_enabled: bool | None
    python_package: str | None
    python_package_sha256: str | None
    dependencies: dict[str, DependencyIdentity]
    command_inputs: dict[str, FileIdentity]
    probe_error: NotRequired[str]


class VariantEnvironmentDifference(TypedDict):
    field: str
    control: str | bool | None
    candidate: str | bool | None


class VariantEnvironmentEvidence(TypedDict):
    mode: VariantEnvironmentMode
    differences: list[VariantEnvironmentDifference]


class ToolIdentity(TypedDict):
    executable: str | None
    executable_sha256: str | None
    version: str | None


def subprocess_environment() -> dict[str, str]:
    """Return the fixed environment shared by server variants and load tools."""
    environment = os.environ.copy()
    environment['PYTHONHASHSEED'] = DEFAULT_PYTHON_HASH_SEED
    environment.pop('NO_COLOR', None)
    return environment


def result_environment() -> dict[str, str]:
    exact = {
        'ASAN_OPTIONS',
        'GOMAXPROCS',
        'LD_LIBRARY_PATH',
        'MALLOC_CONF',
        'PATH',
        'PYTHONHASHSEED',
        'PYTHONPATH',
        'SSL_CERT_DIR',
        'SSL_CERT_FILE',
        'UBSAN_OPTIONS',
    }
    prefixes = ('H2CORN_', 'MALLOC_', 'PYTHON', 'RUST_', 'TOKIO_', 'UVLOOP_')
    return {
        name: value
        for name, value in sorted(subprocess_environment().items())
        if name in exact or name.startswith(prefixes)
    }


def command_version(command: Sequence[str]) -> str | None:
    try:
        result = subprocess.run(
            command,
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=5.0,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    output = (result.stdout or result.stderr).strip()
    return output.splitlines()[0] if output else None


def file_sha256(path: Path) -> str | None:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return None


def tree_sha256(path: Path) -> str | None:
    """Hash stable package contents while excluding interpreter caches.

    A missing path hashes to ``None`` (absent), never to an empty digest.
    """
    try:
        if not path.exists():
            return None
        if path.is_file():
            return file_sha256(path)
        digest = hashlib.sha256()
        files = sorted(
            item
            for item in path.rglob('*')
            if item.is_file()
            and '__pycache__' not in item.parts
            and item.suffix not in {'.pyc', '.pyo'}
        )
        for item in files:
            digest.update(item.relative_to(path).as_posix().encode())
            digest.update(b'\0')
            digest.update(item.read_bytes())
            digest.update(b'\0')
        return digest.hexdigest()
    except OSError:
        return None


def file_identity(path: Path) -> FileIdentity:
    resolved = path.resolve()
    return {'path': str(resolved), 'sha256': file_sha256(resolved)}


def tool_identity(executable_name: str, version_args: Sequence[str]) -> ToolIdentity:
    executable = shutil.which(executable_name)
    if executable is None and Path(executable_name).is_file():
        executable = executable_name
    resolved = Path(executable).resolve() if executable is not None else None
    return {
        'executable': str(resolved) if resolved is not None else None,
        'executable_sha256': file_sha256(resolved) if resolved is not None else None,
        'version': command_version([
            str(resolved) if resolved is not None else executable_name,
            *version_args,
        ]),
    }


def resolved_executable(command: NamedCommand) -> Path | None:
    executable = shutil.which(command.argv[0])
    if executable is None:
        path = Path(command.argv[0])
        if not path.is_file():
            return None
        executable = str(path)
    try:
        return Path(executable).resolve(strict=True)
    except OSError:
        return None


def invoked_executable(command: NamedCommand) -> Path | None:
    """Return the executable invocation path without resolving venv symlinks."""
    executable = shutil.which(command.argv[0])
    if executable is None:
        path = Path(command.argv[0])
        if not path.is_file():
            return None
        executable = str(path)
    return Path(executable).absolute()


def command_python(command: NamedCommand, executable: Path) -> Path | None:
    invoked = invoked_executable(command)
    if invoked is not None and invoked.name.startswith('python'):
        return invoked
    try:
        first_line = (
            executable.open('rb').readline(512).decode(errors='replace').strip()
        )
    except OSError:
        return None
    if not first_line.startswith('#!'):
        return None
    try:
        shebang = shlex.split(first_line[2:])
        interpreter = shebang[0]
    except (ValueError, IndexError):
        return None
    if Path(interpreter).name == 'env' and len(shebang) >= 2:
        resolved = shutil.which(shebang[1])
        return Path(resolved).absolute() if resolved is not None else None
    path = Path(interpreter)
    return (
        path.absolute() if path.name.startswith('python') and path.is_file() else None
    )


def _command_input_files(command: NamedCommand) -> dict[str, FileIdentity]:
    inputs: dict[str, FileIdentity] = {}
    for argument in command.argv[1:]:
        candidate = Path(argument)
        if candidate.is_file():
            identity = file_identity(candidate)
            inputs[identity['path']] = identity
    return inputs


def variant_artifacts(command: NamedCommand) -> VariantArtifacts:
    executable = resolved_executable(command)
    record: VariantArtifacts = {
        'executable': str(executable) if executable is not None else None,
        'executable_sha256': file_sha256(executable)
        if executable is not None
        else None,
        'extension': None,
        'extension_sha256': None,
        'python_executable': None,
        'python_executable_sha256': None,
        'python_version': None,
        'python_implementation': None,
        'python_cache_tag': None,
        'python_abi_flags': None,
        'python_gil_enabled': None,
        'python_package': None,
        'python_package_sha256': None,
        'dependencies': {},
        'command_inputs': _command_input_files(command),
    }
    if executable is None or (python := command_python(command, executable)) is None:
        return record
    record['python_executable'] = str(python)
    record['python_executable_sha256'] = file_sha256(python)
    probe_script = f"""
import importlib.metadata as metadata
import importlib.util
import json
import sys

import h2corn
import h2corn._lib

dependencies = {{}}
for distribution, module in {DEPENDENCY_MODULES!r}.items():
    try:
        spec = importlib.util.find_spec(module)
        if spec is None:
            path = None
        elif spec.submodule_search_locations:
            path = next(iter(spec.submodule_search_locations), None)
        else:
            path = spec.origin
        dependencies[distribution] = {{
            'version': metadata.version(distribution),
            'path': path,
        }}
    except (ImportError, metadata.PackageNotFoundError):
        dependencies[distribution] = {{'version': None, 'path': None}}

print(json.dumps({{
    'python': sys.version,
    'implementation': sys.implementation.name,
    'cache_tag': sys.implementation.cache_tag,
    'abi_flags': getattr(sys, 'abiflags', ''),
    'gil_enabled': sys._is_gil_enabled() if hasattr(sys, '_is_gil_enabled') else None,
    'package': h2corn.__path__[0],
    'extension': h2corn._lib.__file__,
    'dependencies': dependencies,
}}))
"""
    probe = subprocess.run(
        [str(python), '-c', probe_script],
        cwd=ROOT,
        env=subprocess_environment(),
        capture_output=True,
        text=True,
        timeout=10.0,
        check=False,
    )
    if probe.returncode != 0:
        record['probe_error'] = (probe.stderr or probe.stdout)[-2_000:]
        return record
    try:
        details = json.loads(probe.stdout)
    except json.JSONDecodeError:
        record['probe_error'] = f'unexpected probe output: {probe.stdout[-2_000:]!r}'
        return record
    package = Path(details['package']).resolve()
    extension = Path(details['extension']).resolve()
    dependencies: dict[str, DependencyIdentity] = {}
    for distribution, dependency in details['dependencies'].items():
        dependency_path = dependency['path']
        path = Path(dependency_path).resolve() if dependency_path is not None else None
        dependencies[distribution] = {
            'distribution': distribution,
            'version': dependency['version'],
            'path': str(path) if path is not None else None,
            'sha256': tree_sha256(path) if path is not None else None,
        }
    record.update({
        'python_package': str(package),
        'python_package_sha256': tree_sha256(package),
        'extension': str(extension),
        'extension_sha256': file_sha256(extension),
        'python_version': details['python'],
        'python_implementation': details['implementation'],
        'python_cache_tag': details['cache_tag'],
        'python_abi_flags': details['abi_flags'],
        'python_gil_enabled': details['gil_enabled'],
        'dependencies': dependencies,
    })
    return record


def _variant_environment_differences(
    control: VariantArtifacts, candidate: VariantArtifacts
) -> list[VariantEnvironmentDifference]:
    """Compare only the runtime and shared dependencies a code A/B must hold fixed."""
    differences: list[VariantEnvironmentDifference] = []

    def compare(
        field: str, control_value: str | bool | None, candidate_value: str | bool | None
    ) -> None:
        if control_value != candidate_value:
            differences.append({
                'field': field,
                'control': control_value,
                'candidate': candidate_value,
            })

    control_python = control['python_executable'] is not None
    candidate_python = candidate['python_executable'] is not None
    compare('python_runtime_present', control_python, candidate_python)
    if not control_python and not candidate_python:
        return differences

    control_error = control.get('probe_error')
    candidate_error = candidate.get('probe_error')
    if control_error is not None or candidate_error is not None:
        differences.append({
            'field': 'python_probe_error',
            'control': control_error,
            'candidate': candidate_error,
        })

    for field, control_value, candidate_value in (
        (
            'python_executable_sha256',
            control['python_executable_sha256'],
            candidate['python_executable_sha256'],
        ),
        ('python_version', control['python_version'], candidate['python_version']),
        (
            'python_implementation',
            control['python_implementation'],
            candidate['python_implementation'],
        ),
        (
            'python_cache_tag',
            control['python_cache_tag'],
            candidate['python_cache_tag'],
        ),
        (
            'python_abi_flags',
            control['python_abi_flags'],
            candidate['python_abi_flags'],
        ),
        (
            'python_gil_enabled',
            control['python_gil_enabled'],
            candidate['python_gil_enabled'],
        ),
    ):
        compare(field, control_value, candidate_value)

    dependency_names = sorted(
        set(DEPENDENCY_MODULES)
        | control['dependencies'].keys()
        | candidate['dependencies'].keys()
    )
    for distribution in dependency_names:
        control_dependency = control['dependencies'].get(distribution)
        candidate_dependency = candidate['dependencies'].get(distribution)
        compare(
            f'dependencies.{distribution}.present',
            control_dependency is not None,
            candidate_dependency is not None,
        )
        if control_dependency is None or candidate_dependency is None:
            continue
        compare(
            f'dependencies.{distribution}.version',
            control_dependency['version'],
            candidate_dependency['version'],
        )
        compare(
            f'dependencies.{distribution}.sha256',
            control_dependency['sha256'],
            candidate_dependency['sha256'],
        )
    return differences


def variant_environment_evidence(
    control: VariantArtifacts,
    candidate: VariantArtifacts,
    *,
    allow_drift: bool,
) -> VariantEnvironmentEvidence:
    differences = _variant_environment_differences(control, candidate)
    if differences and not allow_drift:
        fields = ', '.join(difference['field'] for difference in differences[:8])
        suffix = '' if len(differences) <= 8 else ', ...'
        raise BenchmarkError(
            'control and candidate do not share an equivalent Python runtime and '
            f'dependency environment ({fields}{suffix}); rebuild equivalent '
            'environments or pass --allow-variant-environment-drift to retain '
            'explicitly confounded diagnostic output'
        )
    return {
        'mode': 'confounded-opt-out' if differences else 'equivalent',
        'differences': differences,
    }


def git_metadata() -> dict[str, object]:
    def git(*command: str, binary: bool = False) -> bytes | str | None:
        try:
            result = subprocess.run(
                ['git', *command],
                cwd=ROOT,
                capture_output=True,
                text=not binary,
                timeout=10.0,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            return None
        return result.stdout if result.returncode == 0 else None

    head = git('rev-parse', 'HEAD')
    status = git('status', '--short', '--', '.')
    diff = git('diff', '--binary', 'HEAD', '--', '.', binary=True)
    untracked = git(
        'ls-files', '--others', '--exclude-standard', '-z', '--', '.', binary=True
    )
    untracked_hashes: dict[str, str | None] = {}
    if isinstance(untracked, bytes):
        for raw_path in sorted(filter(None, untracked.split(b'\0'))):
            path = raw_path.decode(errors='surrogateescape')
            untracked_hashes[path] = file_sha256(ROOT / path)
    return {
        'head': head.strip() if isinstance(head, str) else None,
        'status_short': status.splitlines() if isinstance(status, str) else None,
        'tracked_diff_sha256': hashlib.sha256(diff).hexdigest()
        if isinstance(diff, bytes)
        else None,
        'untracked_sha256': untracked_hashes,
    }


def _fingerprint_file(path: Path) -> tuple[str, int, int, int] | None:
    try:
        stat = path.stat()
    except OSError:
        return None
    return (str(path), stat.st_size, stat.st_mtime_ns, stat.st_ino)


def path_fingerprint(path: Path) -> tuple[tuple[str, int, int, int] | None, ...]:
    """Cheap stat identity of a file or package tree.

    Covers the same file set as :func:`tree_sha256`: every regular file
    outside ``__pycache__`` and bytecode caches. A changed size, mtime, or
    inode anywhere in the set changes the fingerprint.
    """
    if path.is_file():
        return (_fingerprint_file(path),)
    try:
        files = sorted(
            item
            for item in path.rglob('*')
            if item.is_file()
            and '__pycache__' not in item.parts
            and item.suffix not in {'.pyc', '.pyo'}
        )
    except OSError:
        return ()
    return tuple(_fingerprint_file(item) for item in files)
