"""Admit and identify the Python process that produces a benchmark result."""

from __future__ import annotations

import hashlib
import importlib.metadata
import importlib.util
import json
import os
import platform
import sys
import sysconfig
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_GOMETRY_ROOT = Path(__file__).resolve().parents[2]
_TOOLS = _GOMETRY_ROOT / 'tools'
_DRIVERS = _GOMETRY_ROOT / 'benches' / 'drivers'
for _path in (_TOOLS, _DRIVERS):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from _artifact_common import sha256_file
from bench_doctor import (
    PACKAGES,
    _loaded_native_libraries,
    _package_identity,
    _run,
    _source_inputs_digest,
    _uv_lock_identity,
    benchmark_source_inputs_identity,
)
import quality_runtime
import release_artifact
import runtime_artifact
from benchmark_identity import (
    IDENTITY_SCHEMA_VERSION,
    identity_for_arm,
    validate_producer_identity,
)

_DEPENDENCIES = (
    'gometry',
    'numpy',
    'shapely',
    'pyproj',
    'h3',
    'geopandas',
    'pyarrow',
    'mercantile',
    'pygeohash',
)


def _file_identity(path: Path) -> dict[str, Any]:
    resolved = path.resolve()
    return {
        'path': str(resolved),
        'sha256': sha256_file(resolved),
        'size_bytes': resolved.stat().st_size,
    }


def _distribution_identity(distribution: Any) -> dict[str, Any]:
    dist_info = Path(distribution._path)
    metadata = dist_info / 'METADATA'
    record = dist_info / 'RECORD'
    return {
        'name': 'gometry',
        'version': distribution.version,
        'location': str(Path(distribution.locate_file('')).resolve()),
        'metadata': _file_identity(metadata),
        'record': _file_identity(record) if record.is_file() else None,
        'record_available': record.is_file(),
    }


def default_subject_path(root: Path = _GOMETRY_ROOT) -> Path:
    return quality_runtime.quality_receipt_path(root).resolve()


def load_subject(
    receipt_path: Path | None = None,
    *,
    expected_receipt_sha256: str | None = None,
) -> dict[str, Any]:
    """Read one pinned quality, runner, or release subject."""
    selected_path = (
        default_subject_path() if receipt_path is None else receipt_path.resolve()
    )
    if expected_receipt_sha256 is None:
        subject = release_artifact.validate_benchmark_receipt(selected_path)
        actual_digest = subject['receipt_sha256']
        selected_path = Path(subject['path'])
    else:
        actual_digest = expected_receipt_sha256
    subject = release_artifact.read_benchmark_receipt(
        selected_path,
        actual_digest,
    )
    return subject


def package_root_for_subject(
    subject: dict[str, Any],
    explicit: Path | str | None = None,
) -> Path:
    """Resolve the exact parent directory that the child must prepend to PYTHONPATH."""
    if subject.get('kind') == 'quality':
        root = Path(subject['package_root']).resolve()
        if explicit is not None and Path(explicit).resolve() != root:
            raise AssertionError('explicit package root differs from quality receipt')
        return root
    if explicit is not None:
        return Path(explicit).resolve()
    spec = importlib.util.find_spec('gometry')
    origin = spec.origin if spec is not None else None
    if not isinstance(origin, str):
        raise TypeError(
            'release benchmark subjects require an explicit installed package root'
        )
    return Path(origin).resolve().parent.parent


def environment_for_subject(
    subject: dict[str, Any],
    package_root: Path,
    *,
    invocation_id: str,
    control: bool,
    base: dict[str, str] | None = None,
) -> dict[str, str]:
    """Build a child environment with its admitted package root explicitly selected."""
    environment = dict(os.environ if base is None else base)
    old_path = environment.get('PYTHONPATH')
    environment['PYTHONPATH'] = os.pathsep.join(
        item
        for item in (
            str(package_root.resolve()),
            old_path,
        )
        if item
    )
    environment['GOMETRY_BENCH_SUBJECT_PATH'] = str(Path(subject['path']).resolve())
    environment['GOMETRY_BENCH_SUBJECT_SHA256'] = subject['receipt_sha256']
    environment['GOMETRY_BENCH_PACKAGE_ROOT'] = str(package_root.resolve())
    environment['GOMETRY_BENCH_INVOCATION_ID'] = invocation_id
    if control:
        environment['GOMETRY_BENCH_CONTROL'] = '1'
    else:
        environment.pop('GOMETRY_BENCH_CONTROL', None)
    return environment


def _release_wheel(
    subject: dict[str, Any],
    package_root: Path,
) -> tuple[dict[str, Any], Path, dict[str, Any]]:
    try:
        distribution = importlib.metadata.distribution('gometry')
    except importlib.metadata.PackageNotFoundError as error:
        raise AssertionError(
            f'admitted gometry distribution is unavailable: {error}'
        ) from error
    installed_root = Path(distribution.locate_file('')).resolve()
    if installed_root != package_root:
        raise AssertionError(
            'installed gometry distribution is outside the selected package root: '
            f'{installed_root} != {package_root}'
        )
    suffix = sysconfig.get_config_var('EXT_SUFFIX')
    source = subject.get('source')
    wheels = subject.get('wheels')
    if not isinstance(suffix, str) or not isinstance(source, dict):
        raise TypeError('benchmark subject has malformed runtime selection facts')
    if not isinstance(wheels, list):
        raise TypeError('benchmark subject has no wheel records')
    receipt_path = Path(subject['path'])
    candidates: list[tuple[dict[str, Any], Path]] = []
    for item in wheels:
        if not isinstance(item, dict):
            continue
        member = item.get('extension_member')
        filename = item.get('filename')
        if (
            not isinstance(member, str)
            or not Path(member).name.endswith(suffix)
            or not isinstance(filename, str)
        ):
            continue
        extension_path = Path(distribution.locate_file(member)).resolve()
        wheel_path = receipt_path.parent / filename
        try:
            if (
                extension_path.is_file()
                and sha256_file(extension_path) == item.get('extension_sha256')
                and wheel_path.is_file()
                and sha256_file(wheel_path) == item.get('sha256')
            ):
                candidates.append((item, wheel_path.resolve()))
        except OSError:
            continue
    if len(candidates) != 1:
        raise AssertionError(
            'benchmark subject does not uniquely identify the installed wheel/member'
        )
    item, wheel_path = candidates[0]
    runtime_receipt = release_artifact.installed_wheel_runtime_receipt(
        identifier=item.get('identifier'),
        profile=str(subject.get('profile') or item.get('build', {}).get('profile')),
        source=source,
        build=item['build'],
        wheel=item,
        prepared_inputs=subject.get('prepared_inputs'),
    )
    return runtime_receipt, wheel_path, item


@dataclass(slots=True)
class BenchmarkRuntime:
    subject: dict[str, Any]
    admission: Any
    runtime: tuple[Any, Any, dict[str, Any]]
    receipt_path: Path
    receipt_sha256: str
    wheel_path: Path
    wheel_item: dict[str, Any]
    package_root: Path
    identity: dict[str, Any] | None = None

    def collect_identity(self) -> dict[str, Any]:
        if self.identity is not None:
            raise AssertionError('benchmark process identity was already collected')
        _, extension_module, _ = self.runtime
        extension_path = self.admission.extension_path
        extension = {
            **_file_identity(extension_path),
            'build_info': getattr(extension_module, '__build_info__', None),
        }
        build_info = extension['build_info']
        source = self.subject['source']
        expected_source = source.get('source_inputs_sha256')
        override = os.environ.get('GOMETRY_BENCH_SOURCE_INPUTS_SHA256')
        if override is not None and override != expected_source:
            raise AssertionError(
                'GOMETRY_BENCH_SOURCE_INPUTS_SHA256 differs from selected subject'
            )
        source_digest = benchmark_source_inputs_identity(
            build_info,
            _source_inputs_digest(),
            expected_source,
        )
        facade_identity = _file_identity(self.admission.facade_path)
        distribution = _distribution_identity(self.admission.distribution)
        packages = {
            name: identity
            for name in _DEPENDENCIES
            if (identity := _package_identity(name)) is not None
        }
        packages['gometry'] = {
            'available': True,
            'version': distribution['version'],
            'display_name': PACKAGES['gometry'],
            'module': facade_identity,
            'distribution': distribution,
            'native_libraries': [],
        }
        binding = {
            'kind': self.subject['kind'],
            'path': str(self.receipt_path),
            'sha256': self.receipt_sha256,
            'profile': self.admission.receipt['build']['profile'],
            'package_root': str(self.package_root),
            'facade': facade_identity,
            'extension': extension,
            'wheel': {
                'path': str(self.wheel_path),
                'filename': self.wheel_item['filename'],
                'archive': _file_identity(self.wheel_path),
                'extension_member': self.wheel_item['extension_member'],
                'extension_sha256': self.wheel_item['extension_sha256'],
            },
            'distribution': distribution,
            'python_members': dict(self.admission.python_members),
        }
        if binding['wheel']['archive']['sha256'] != self.wheel_item['sha256']:
            raise AssertionError('selected benchmark wheel changed after admission')
        if (
            self.subject['kind'] == 'quality'
            and self.admission.package_root != self.package_root
        ):
            raise AssertionError('quality package root differs from runtime admission')

        native_libraries = _loaded_native_libraries()
        native_libraries = [
            extension if item.get('path') == extension['path'] else item
            for item in native_libraries
        ]
        if not any(item.get('path') == extension['path'] for item in native_libraries):
            native_libraries.append(extension)
        harness_commit = _run(['git', 'rev-parse', 'HEAD'])
        if not harness_commit:
            raise AssertionError('cannot identify benchmark harness commit')
        python_build = {
            'implementation': platform.python_implementation(),
            'version': platform.python_version(),
            'build': platform.python_build(),
            'debug': bool(sysconfig.get_config_var('Py_DEBUG')),
        }
        gil_probe = getattr(sys, '_is_gil_enabled', None)
        source_commit = source.get('commit')
        common = {
            'identity_schema_version': IDENTITY_SCHEMA_VERSION,
            'harness_commit': harness_commit,
            'source_inputs_sha256': source_digest,
            'extension': extension,
            'uv_lock': _uv_lock_identity(),
            'packages': packages,
            'loaded_native_libraries': native_libraries,
            'python': {
                'executable': sys.executable,
                'resolved_executable': str(Path(sys.executable).resolve()),
                'prefix': sys.prefix,
                'version': sys.version,
                'implementation': platform.python_implementation(),
                'cache_tag': sys.implementation.cache_tag,
                'ext_suffix': sysconfig.get_config_var('EXT_SUFFIX'),
                'gil_disabled': bool(sysconfig.get_config_var('Py_GIL_DISABLED')),
                'gil_enabled': bool(gil_probe()) if callable(gil_probe) else True,
                'build': python_build,
            },
            'runtime_binding': binding,
            'source_commit': source_commit,
            'source_receipt': (
                {
                    'path': binding['path'],
                    'sha256': binding['sha256'],
                }
                if source_commit is not None
                else None
            ),
            'source_wheel': (
                {
                    'filename': binding['wheel']['filename'],
                    'sha256': binding['wheel']['archive']['sha256'],
                    'extension_member': binding['wheel']['extension_member'],
                    'extension_sha256': binding['wheel']['extension_sha256'],
                }
                if source_commit is not None
                else None
            ),
        }
        validate_identity_for_subject(
            common,
            self.subject,
            side='A',
            row='benchmark-runtime',
            verify_archive=False,
        )
        self.identity = common
        return common

    def complete(self) -> dict[str, Any]:
        if self.identity is None:
            raise AssertionError(
                'benchmark process completed before identity collection'
            )
        if sha256_file(self.receipt_path) != self.receipt_sha256:
            raise AssertionError('benchmark subject receipt changed during measurement')
        if (
            sha256_file(self.wheel_path)
            != self.identity['runtime_binding']['wheel']['archive']['sha256']
        ):
            raise AssertionError('benchmark subject wheel changed during measurement')
        runtime_artifact.verify_runtime_completion(self.admission, self.runtime)
        return self.identity


def admit_runtime(
    receipt_path: Path | None = None,
    *,
    expected_receipt_sha256: str | None = None,
    package_root: Path | str | None = None,
) -> BenchmarkRuntime:
    subject = load_subject(
        receipt_path,
        expected_receipt_sha256=expected_receipt_sha256,
    )
    selected_package_root = package_root_for_subject(subject, package_root)
    if subject['kind'] == 'quality':
        runtime_receipt = subject['runtime_receipt']
        root = Path(subject['path']).parents[2]
        wheel = runtime_receipt['wheel']
        wheel_path = (root.parent / wheel['path']).resolve()
        wheel_item = {
            'filename': wheel['filename'],
            'sha256': wheel['sha256'],
            'extension_member': wheel['extension_member'],
            'extension_sha256': wheel['extension_sha256'],
        }
        admission = runtime_artifact.inspect_runtime_inputs(
            root,
            expected_wheel=wheel_path,
            build_receipt=runtime_receipt,
        )
    else:
        runtime_receipt, wheel_path, wheel_item = _release_wheel(
            subject,
            selected_package_root,
        )
        admission = runtime_artifact.inspect_runtime_inputs(
            _GOMETRY_ROOT,
            expected_wheel=wheel_path,
            build_receipt=runtime_receipt,
        )
    actual_package_root = admission.package_dir.parent.resolve()
    if actual_package_root != selected_package_root:
        raise AssertionError(
            'admitted Gometry package root differs from the selected path: '
            f'{actual_package_root} != {selected_package_root}'
        )
    runtime_artifact.check_runtime_resolution(admission)
    runtime = runtime_artifact.verify_admitted_runtime(admission)
    return BenchmarkRuntime(
        subject=subject,
        admission=admission,
        runtime=runtime,
        receipt_path=Path(subject['path']),
        receipt_sha256=subject['receipt_sha256'],
        wheel_path=wheel_path,
        wheel_item=wheel_item,
        package_root=selected_package_root,
    )


def validate_identity_for_subject(
    identity: dict[str, Any],
    subject: dict[str, Any],
    *,
    side: str,
    row: str,
    verify_archive: bool = True,
) -> dict[str, Any]:
    """Bind an actual-process identity to the selected receipt before timing."""
    binding = identity.get('runtime_binding')
    if not isinstance(binding, dict):
        raise TypeError('case process did not report runtime admission')
    if (
        binding.get('kind') != subject.get('kind')
        or binding.get('path') != subject.get('path')
        or binding.get('sha256') != subject.get('receipt_sha256')
    ):
        raise AssertionError(
            'case process runtime binding differs from selected subject'
        )
    if binding.get('python_members') != subject.get('python_members') and (
        binding.get('python_members') != subject.get('source', {}).get('python_members')
    ):
        raise AssertionError('case process Python members differ from selected subject')
    packages = identity.get('packages')
    gometry = packages.get('gometry') if isinstance(packages, dict) else None
    if not isinstance(gometry, dict):
        raise TypeError('case process identity has no gometry package')
    attributed = identity_for_arm(
        identity,
        side=side,
        row=row,
        runtime='gometry',
        module=gometry['module'],
        distribution=gometry['distribution'],
        native_libraries=[
            {
                key: identity['extension'][key]
                for key in ('path', 'sha256', 'size_bytes')
            }
        ],
        native_library_status='identified',
        source_commit=identity.get('source_commit'),
        source_receipt=identity.get('source_receipt'),
        source_wheel=identity.get('source_wheel'),
    )
    validate_producer_identity(attributed)
    receipt_facts = release_artifact.match_benchmark_receipt(
        subject,
        identity['extension'],
        gometry['distribution'],
        runtime_binding=binding,
        verify_archive=verify_archive,
    )
    if identity.get('source_commit') != receipt_facts['source_commit']:
        raise AssertionError('case process source commit differs from selected subject')
    if receipt_facts['source_commit'] is None:
        if (
            identity.get('source_receipt') is not None
            or identity.get('source_wheel') is not None
        ):
            raise AssertionError('unattributed producer contains receipt fields')
    elif identity.get('source_receipt') != {
        'path': receipt_facts['receipt_path'],
        'sha256': receipt_facts['receipt_sha256'],
    } or identity.get('source_wheel') != {
        'filename': receipt_facts['wheel_filename'],
        'sha256': receipt_facts['wheel_sha256'],
        'extension_member': receipt_facts['extension_member'],
        'extension_sha256': receipt_facts['extension_sha256'],
    }:
        raise AssertionError('case process source attribution differs from subject')
    return attributed


def identity_sha256(identity: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(',', ':')).encode('utf-8')
    ).hexdigest()
