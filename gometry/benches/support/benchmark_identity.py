"""Validation helpers for benchmark producer identity records.

The run manifest owns the comparison matrix, while each arm owns the identity
of the implementation that produced its observation.  Common process facts
are deliberately kept at the top level; implementation facts are nested so a
gometry artifact cannot be mistaken for a comparator artifact.
"""

from __future__ import annotations

import copy
import re
from pathlib import Path
from typing import Any

IDENTITY_SCHEMA_VERSION = 3
_SHA256 = re.compile(r'^[0-9a-f]{64}$')


def _fail(message: str) -> ValueError:
    return ValueError(f'invalid benchmark producer identity: {message}')


def _sha256(value: Any, *, label: str, allow_none: bool = False) -> str | None:
    if value is None and allow_none:
        return None
    if not isinstance(value, str) or not _SHA256.fullmatch(value):
        raise _fail(f'{label} must be a lowercase SHA-256 digest')
    return value


def _file_identity(value: Any, *, label: str, allow_none: bool = False) -> None:
    if value is None and allow_none:
        return
    if not isinstance(value, dict):
        raise _fail(f'{label} must be an object')
    path = value.get('path')
    if allow_none and path is None:
        if value.get('sha256') is not None or value.get('size_bytes') is not None:
            raise _fail(f'{label} unavailable identity must have null digest and size')
        return
    if not isinstance(path, str) or not path:
        raise _fail(f'{label}.path must be a non-empty string')
    _sha256(value.get('sha256'), label=f'{label}.sha256')
    size = value.get('size_bytes')
    if not isinstance(size, int) or isinstance(size, bool) or size < 0:
        raise _fail(f'{label}.size_bytes must be a non-negative integer')


def _distribution_identity(value: Any, *, label: str) -> None:
    if not isinstance(value, dict):
        raise _fail(f'{label} must be an object')
    for field in ('name', 'version', 'location'):
        if not isinstance(value.get(field), str) or not value[field]:
            raise _fail(f'{label}.{field} must be a non-empty string')
    _file_identity(value.get('metadata'), label=f'{label}.metadata')
    record_available = value.get('record_available')
    if not isinstance(record_available, bool):
        raise _fail(f'{label}.record_available must be boolean')
    _file_identity(value.get('record'), label=f'{label}.record', allow_none=True)
    if record_available != (value.get('record') is not None):
        raise _fail(f'{label}.record_available disagrees with record identity')


def _package_identity(value: Any, *, label: str) -> None:
    if not isinstance(value, dict):
        raise _fail(f'{label} must be an object')
    if value.get('available') is not True:
        raise _fail(f'{label} is not available')
    _file_identity(value.get('module'), label=f'{label}.module')
    _distribution_identity(value.get('distribution'), label=f'{label}.distribution')
    native = value.get('native_libraries')
    if not isinstance(native, list):
        raise _fail(f'{label}.native_libraries must be a list')
    for index, item in enumerate(native):
        _file_identity(item, label=f'{label}.native_libraries[{index}]')


def _runtime_binding_identity(value: Any) -> None:
    if not isinstance(value, dict):
        raise _fail('runtime_binding must be an object')
    for field in ('kind', 'path', 'profile', 'package_root'):
        if not isinstance(value.get(field), str) or not value[field]:
            raise _fail(f'runtime_binding.{field} must be a non-empty string')
    if value['kind'] not in {'quality', 'release', 'runner'}:
        raise _fail('runtime_binding.kind is unsupported')
    if value['profile'] not in {'dev', 'release'}:
        raise _fail('runtime_binding.profile is unsupported')
    _sha256(value.get('sha256'), label='runtime_binding.sha256')
    _file_identity(value.get('facade'), label='runtime_binding.facade')
    extension = value.get('extension')
    _file_identity(extension, label='runtime_binding.extension')
    wheel = value.get('wheel')
    if not isinstance(wheel, dict):
        raise _fail('runtime_binding.wheel must be an object')
    for field in ('filename', 'extension_member'):
        if not isinstance(wheel.get(field), str) or not wheel[field]:
            raise _fail(f'runtime_binding.wheel.{field} must be a non-empty string')
    _file_identity(wheel.get('archive'), label='runtime_binding.wheel.archive')
    _sha256(
        wheel.get('extension_sha256'),
        label='runtime_binding.wheel.extension_sha256',
    )
    if wheel['extension_sha256'] != extension['sha256']:
        raise _fail('runtime binding wheel extension differs from loaded extension')
    if Path(wheel['extension_member']).name != Path(extension['path']).name:
        raise _fail('runtime binding wheel member differs from loaded extension path')
    if not isinstance(value.get('python_members'), dict) or not value['python_members']:
        raise _fail('runtime_binding.python_members must be a non-empty object')
    for member, digest in value['python_members'].items():
        if (
            not isinstance(member, str)
            or not member.startswith('gometry/')
            or not isinstance(digest, str)
            or _SHA256.fullmatch(digest) is None
        ):
            raise _fail('runtime_binding.python_members contains an invalid member')
    if value['python_members'].get('gometry/__init__.py') != value['facade']['sha256']:
        raise _fail('runtime binding facade differs from its Python member digest')
    _distribution_identity(
        value.get('distribution'),
        label='runtime_binding.distribution',
    )


def validate_producer_identity(value: Any) -> dict[str, Any]:
    """Validate one complete identity record and return it unchanged."""
    if not isinstance(value, dict):
        raise _fail('top level must be an object')
    if value.get('identity_schema_version') != IDENTITY_SCHEMA_VERSION:
        raise _fail('unsupported identity schema version')
    harness_commit = value.get('harness_commit')
    if not isinstance(harness_commit, str) or not harness_commit:
        raise _fail('harness_commit must be a non-empty string')
    _sha256(value.get('source_inputs_sha256'), label='source_inputs_sha256')
    extension = value.get('extension')
    _file_identity(extension, label='extension')
    build_info = extension.get('build_info') if isinstance(extension, dict) else None
    if not isinstance(build_info, dict):
        raise _fail('extension.build_info must be an object')
    if build_info.get('source_inputs_sha256') != value['source_inputs_sha256']:
        raise _fail('extension build facts do not match source identity')
    lock = value.get('uv_lock')
    _file_identity(lock, label='uv_lock', allow_none=True)
    python = value.get('python')
    if not isinstance(python, dict):
        raise _fail('python must be an object')
    for field in (
        'executable',
        'resolved_executable',
        'prefix',
        'version',
        'implementation',
        'cache_tag',
        'ext_suffix',
    ):
        if not isinstance(python.get(field), str) or not python[field]:
            raise _fail(f'python.{field} must be a non-empty string')
    for field in ('gil_disabled', 'gil_enabled'):
        if not isinstance(python.get(field), bool):
            raise _fail(f'python.{field} must be boolean')
    build = python.get('build')
    if not isinstance(build, dict):
        raise _fail('python.build must be an object')
    if not isinstance(build.get('implementation'), str) or not build['implementation']:
        raise _fail('python.build.implementation must be a non-empty string')
    if not isinstance(build.get('version'), str) or not build['version']:
        raise _fail('python.build.version must be a non-empty string')
    packages = value.get('packages')
    if not isinstance(packages, dict) or not packages:
        raise _fail('packages must be a non-empty object')
    for name, package in packages.items():
        if not isinstance(name, str) or not name:
            raise _fail('package names must be non-empty strings')
        _package_identity(package, label=f'packages[{name!r}]')
    libraries = value.get('loaded_native_libraries')
    if not isinstance(libraries, list):
        raise _fail('loaded_native_libraries must be a list')
    for index, item in enumerate(libraries):
        _file_identity(item, label=f'loaded_native_libraries[{index}]')
    runtime_binding = value.get('runtime_binding')
    _runtime_binding_identity(runtime_binding)
    gometry_package = packages.get('gometry')
    if not isinstance(gometry_package, dict):
        raise _fail('packages must identify the admitted gometry package')
    if gometry_package.get('module') != runtime_binding['facade']:
        raise _fail('gometry package module differs from admitted facade')
    if gometry_package.get('distribution') != runtime_binding['distribution']:
        raise _fail('gometry distribution differs from runtime binding')
    if extension != runtime_binding['extension']:
        raise _fail('loaded extension differs from runtime binding')
    implementation = value.get('implementation')
    if not isinstance(implementation, dict):
        raise _fail('implementation attribution is missing')
    if implementation.get('runtime') not in {'gometry', 'comparator'}:
        raise _fail('implementation.runtime must be gometry or comparator')
    for field in ('source_commit', 'source_receipt', 'source_wheel'):
        if field not in implementation:
            raise _fail(f'implementation.{field} is missing')
    for field in ('row', 'module_name', 'native_library_status'):
        if not isinstance(implementation.get(field), str) or not implementation[field]:
            raise _fail(f'implementation.{field} must be a non-empty string')
    source_commit = implementation.get('source_commit')
    if source_commit is not None and (
        not isinstance(source_commit, str)
        or re.fullmatch(r'[0-9a-f]{40,64}', source_commit) is None
    ):
        raise _fail('implementation.source_commit must be a source commit or null')
    module = implementation.get('module')
    _file_identity(module, label='implementation.module', allow_none=True)
    distribution = implementation.get('distribution')
    if distribution is not None:
        _distribution_identity(distribution, label='implementation.distribution')
    native = implementation.get('native_libraries')
    if not isinstance(native, list):
        raise _fail('implementation.native_libraries must be a list')
    for index, item in enumerate(native):
        _file_identity(item, label=f'implementation.native_libraries[{index}]')
    source_receipt = implementation.get('source_receipt')
    source_wheel = implementation.get('source_wheel')
    if source_commit is None:
        if source_receipt is not None or source_wheel is not None:
            raise _fail('source receipt/wheel facts require a producer source commit')
    else:
        if (
            not isinstance(source_receipt, dict)
            or set(source_receipt) != {'path', 'sha256'}
            or not isinstance(source_receipt.get('path'), str)
            or not source_receipt['path']
        ):
            raise _fail('implementation.source_receipt is malformed')
        _sha256(
            source_receipt.get('sha256'),
            label='implementation.source_receipt.sha256',
        )
        if (
            not isinstance(source_wheel, dict)
            or set(source_wheel)
            != {
                'filename',
                'sha256',
                'extension_member',
                'extension_sha256',
            }
            or not isinstance(source_wheel.get('filename'), str)
            or not source_wheel['filename']
            or not isinstance(source_wheel.get('extension_member'), str)
            or not source_wheel['extension_member']
        ):
            raise _fail('implementation.source_wheel is malformed')
        _sha256(source_wheel.get('sha256'), label='implementation.source_wheel.sha256')
        _sha256(
            source_wheel.get('extension_sha256'),
            label='implementation.source_wheel.extension_sha256',
        )
        if implementation['runtime'] != 'gometry':
            raise _fail('source receipt attribution is only valid for gometry')
        if source_receipt != {
            'path': runtime_binding['path'],
            'sha256': runtime_binding['sha256'],
        }:
            raise _fail('source receipt differs from runtime binding')
        if source_wheel != {
            'filename': runtime_binding['wheel']['filename'],
            'sha256': runtime_binding['wheel']['archive']['sha256'],
            'extension_member': runtime_binding['wheel']['extension_member'],
            'extension_sha256': runtime_binding['wheel']['extension_sha256'],
        }:
            raise _fail('source wheel differs from runtime binding')
        if not any(
            item.get('sha256') == source_wheel['extension_sha256'] for item in native
        ):
            raise _fail('source receipt extension differs from arm native identity')
        if not any(
            isinstance(item.get('path'), str)
            and Path(item['path']).name
            == source_wheel['extension_member'].rsplit('/', 1)[-1]
            and item.get('sha256') == source_wheel['extension_sha256']
            for item in native
        ):
            raise _fail('source receipt member differs from loaded extension path')
    arm = value.get('arm')
    if not isinstance(arm, dict) or arm.get('side') not in {'A', 'B'}:
        raise _fail('arm attribution must name side A or B')
    if not isinstance(arm.get('row'), str) or not arm['row']:
        raise _fail('arm.row must be a non-empty string')
    if implementation['row'] != arm['row']:
        raise _fail('implementation and arm rows disagree')
    return value


def common_identity(value: dict[str, Any]) -> dict[str, Any]:
    """Project the process/build identity without side-specific attribution."""
    return {
        key: item
        for key, item in value.items()
        if key
        not in {
            'implementation',
            'arm',
        }
    }


def identity_for_arm(
    common: dict[str, Any],
    *,
    side: str,
    row: str,
    runtime: str,
    module: dict[str, Any] | None,
    distribution: dict[str, Any] | None,
    native_libraries: list[dict[str, Any]],
    native_library_status: str,
    source_commit: str | None = None,
    source_receipt: dict[str, Any] | None = None,
    source_wheel: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Attach explicit implementation and arm attribution to common facts."""
    value = copy.deepcopy(common)
    for field in ('source_commit', 'source_receipt', 'source_wheel'):
        value.pop(field, None)
    value['implementation'] = {
        'runtime': runtime,
        'row': row,
        'module_name': row.split('.', 1)[0]
        if runtime == 'comparator'
        else 'gometry._lib',
        'module': module,
        'distribution': distribution,
        'native_libraries': native_libraries,
        'native_library_status': native_library_status,
        'source_commit': source_commit,
        'source_receipt': source_receipt,
        'source_wheel': source_wheel,
    }
    value['arm'] = {'side': side, 'row': row}
    return validate_producer_identity(value)
