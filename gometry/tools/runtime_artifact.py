"""Actual-process Gometry runtime admission and native test evidence."""

from __future__ import annotations

import importlib.metadata
import importlib.util
import json
import platform
import sys
import sysconfig
from dataclasses import asdict, dataclass, fields
from pathlib import Path, PurePosixPath
from types import ModuleType
from typing import TYPE_CHECKING, Any

from _artifact_common import (
    HEX_DIGEST,
    MATURIN_VERSION,
    SOURCE_INPUTS_FORMAT,
    _runtime_interpreter,
    _sha256_bytes,
    _target_architecture,
    _target_policy_identity,
    sha256_file,
)
from artifact_content import (
    build_identifier_policy,
    inspect_wheel,
    native_image_facts,
    python_member_hashes,
    validate_wheel_native_target,
    wheel_matches_identifier,
    wheel_target_compatible,
)
from quality_runtime import (
    QualityPreparationRequired,
    quality_receipt_path,
    read_quality_receipt,
    validate_quality_producer_inputs,
)

if TYPE_CHECKING:
    from collections.abc import Mapping
SMOKE_GRID_ENV = 'GOMETRY_SMOKE_GRID'
SMOKE_EXPECTED_SOURCE_INPUTS_ENV = 'GOMETRY_SMOKE_EXPECTED_SOURCE_INPUTS_SHA256'
SMOKE_EXPECTED_NATIVE_SOURCE_INPUTS_ENV = (
    'GOMETRY_SMOKE_EXPECTED_NATIVE_SOURCE_INPUTS_SHA256'
)
SMOKE_EXPECTED_VERSION_ENV = 'GOMETRY_SMOKE_EXPECTED_VERSION'
SMOKE_EXPECTED_TARGET_ENV = 'GOMETRY_SMOKE_EXPECTED_TARGET'
SMOKE_EXPECTED_ARCHITECTURE_ENV = 'GOMETRY_SMOKE_EXPECTED_ARCHITECTURE'
SMOKE_EXPECTED_HOST_ARCH_ENV = 'GOMETRY_SMOKE_EXPECTED_HOST_ARCHITECTURE'


def _runtime_loader_identity() -> dict[str, Any]:
    if not sys.platform.startswith('linux'):
        return {
            'runtime_libc': 'not-applicable',
            'loader_paths': [],
            'libc_paths': [],
        }
    maps_path = Path('/proc/self/maps')
    try:
        mapped_paths = {
            line.rsplit(None, 1)[-1].removesuffix(' (deleted)')
            for line in maps_path.read_text(encoding='utf-8').splitlines()
            if line.rsplit(None, 1)[-1].startswith('/')
        }
    except OSError as error:
        raise AssertionError(
            f'cannot inspect the active Linux loader maps: {error}'
        ) from error
    names = {path: PurePosixPath(path).name for path in mapped_paths}
    gnu_libc = sorted(path for path, name in names.items() if name == 'libc.so.6')
    gnu_loaders = sorted(
        path for path, name in names.items() if name.startswith('ld-linux')
    )
    musl_loaders = sorted(
        path for path, name in names.items() if name.startswith('ld-musl-')
    )
    if (gnu_libc and musl_loaders) or (musl_loaders and gnu_loaders):
        raise AssertionError('active Linux process maps more than one libc family')
    if gnu_libc:
        if not gnu_loaders:
            raise AssertionError('active GNU libc has no mapped dynamic loader')
        return {
            'runtime_libc': 'gnu',
            'loader_paths': gnu_loaders,
            'libc_paths': gnu_libc,
        }
    if musl_loaders:
        return {
            'runtime_libc': 'musl',
            'loader_paths': musl_loaders,
            'libc_paths': musl_loaders,
        }
    raise AssertionError('active Linux process has no identifiable libc loader')


def _runtime_selection_matches(
    actual: Mapping[str, Any], selection: Mapping[str, Any] | None
) -> None:
    if selection is None:
        return
    allowed = {
        'executable',
        'prefix',
        'implementation',
        'cache_tag',
        'ext_suffix',
        'gil_disabled',
    }
    if set(selection) - allowed:
        raise AssertionError('runtime selection contains unknown identity fields')
    for name, expected in selection.items():
        if actual.get(name) != expected:
            raise AssertionError(
                f'selected runtime {name} does not match this process: '
                f'{expected!r} != {actual.get(name)!r}'
            )


@dataclass(frozen=True)
class WheelAdmission:
    path: Path
    sha256: str
    member_sha256: dict[str, str]
    python_members: frozenset[str]
    record: str
    signatures: frozenset[str]
    tags: tuple[str, ...]
    native: dict[str, Any]


@dataclass(frozen=True)
class RuntimeAdmission:
    root: Path
    receipt: dict[str, Any]
    package_root: Path | None
    package_dir: Path
    facade_path: Path
    extension_path: Path
    extension_member: str
    extension_sha256: str
    python_members: dict[str, str]
    interpreter: dict[str, Any]
    target_policy: dict[str, Any]
    distribution: Any
    distribution_version: str
    distribution_metadata_sha256: str
    distribution_files: frozenset[str] | None
    wheel: WheelAdmission | None
    runtime_selection: Mapping[str, Any] | None
    shared_inputs: bool = False


def admission_record(admission: RuntimeAdmission) -> dict[str, Any]:
    """Serialize inert admission evidence for a single benchmark run."""
    record = {
        field.name: getattr(admission, field.name)
        for field in fields(admission)
        if field.name not in {'distribution', 'wheel', 'shared_inputs'}
    }
    record['wheel'] = None if admission.wheel is None else asdict(admission.wheel)
    record['interpreter'] = {
        key: value for key, value in admission.interpreter.items() if key != 'pid'
    }
    # A JSON round trip also normalizes paths, sets and tuples for comparison at
    # run completion without retaining live distribution objects.
    return json.loads(
        json.dumps(
            record,
            default=lambda value: (
                str(value) if isinstance(value, Path) else sorted(value)
            ),
        )
    )


def admitted_runtime_inputs(record: Mapping[str, Any]) -> RuntimeAdmission:
    """Bind digest-admitted run evidence to this worker's distribution."""
    values = dict(record)
    for name in (
        'root',
        'package_dir',
        'facade_path',
        'extension_path',
        'package_root',
    ):
        if values[name] is not None:
            values[name] = Path(values[name])
    interpreter = _runtime_interpreter()
    if {key: value for key, value in interpreter.items() if key != 'pid'} != values[
        'interpreter'
    ]:
        raise AssertionError('worker interpreter differs from run admission')
    values['interpreter'] = interpreter
    package_root = values['package_root'] or values['package_dir'].parent
    distributions = [
        item
        for item in importlib.metadata.distributions(path=[str(package_root)])
        if str(item.metadata.get('Name', '')).lower() == 'gometry'
    ]
    if len(distributions) != 1:
        raise AssertionError('admitted package has no unique gometry distribution')
    values['distribution'] = distributions[0]
    inventory = values['distribution_files']
    values['distribution_files'] = None if inventory is None else frozenset(inventory)
    wheel = values['wheel']
    if wheel is not None:
        wheel = dict(wheel)
        wheel['path'] = Path(wheel['path'])
        for name in ('python_members', 'signatures'):
            wheel[name] = frozenset(wheel[name])
        wheel['tags'] = tuple(wheel['tags'])
        values['wheel'] = WheelAdmission(**wheel)
    return RuntimeAdmission(**values, shared_inputs=True)


def _resident_gometry_modules() -> dict[str, ModuleType]:
    modules: dict[str, ModuleType] = {}
    for name, value in tuple(sys.modules.items()):
        if name != 'gometry' and not name.startswith('gometry.'):
            continue
        if not isinstance(value, ModuleType):
            raise TypeError(f'sys.modules contains a non-module {name!r}')
        modules[name] = value
    return modules


def _validate_resident_module_files(
    modules: Mapping[str, ModuleType],
    *,
    facade_path: Path,
    extension_path: Path,
    extension_sha256: str,
    package_dir: Path,
    python_members: Mapping[str, str],
    extension_payload: bytes | None = None,
) -> None:
    if not modules:
        return
    if 'gometry' not in modules or 'gometry._lib' not in modules:
        raise AssertionError('a partial Gometry import is already resident')
    for name, module in modules.items():
        module_file = getattr(module, '__file__', None)
        if not isinstance(module_file, str) or not module_file:
            raise AssertionError(f'resident Gometry module has no file: {name}')
        path = Path(module_file).resolve()
        if name == 'gometry':
            if path != facade_path or sha256_file(path) != python_members.get(
                'gometry/__init__.py'
            ):
                raise AssertionError('a different Gometry facade is already imported')
        elif name == 'gometry._lib':
            payload = (
                path.read_bytes() if extension_payload is None else extension_payload
            )
            if path != extension_path or _sha256_bytes(payload) != extension_sha256:
                raise AssertionError(
                    'a different Gometry native extension is already imported'
                )
        else:
            try:
                relative = path.relative_to(package_dir).as_posix()
            except ValueError as error:
                raise AssertionError(
                    f'resident Gometry module is outside the selected package: {name}'
                ) from error
            logical = f'gometry/{relative}'
            if (
                python_members.get(logical) is None
                or sha256_file(path) != python_members[logical]
            ):
                raise AssertionError(
                    f'resident Gometry module differs from selected package: {name}'
                )


def _preparation_mismatch(kind: Any, message: str) -> None:
    if kind == 'gometry-quality-runtime':
        raise QualityPreparationRequired(message)
    raise AssertionError(message)


def inspect_runtime_inputs(
    root: Path,
    *,
    expected_wheel: Path | None = None,
    build_receipt: Mapping[str, Any] | None = None,
    runtime_selection: Mapping[str, Any] | None = None,
) -> RuntimeAdmission:
    """Validate inert source, wheel, distribution and runtime-selection inputs."""
    root = root.resolve()
    if build_receipt is None:
        if not quality_receipt_path(root).is_file():
            raise QualityPreparationRequired('quality runtime receipt is missing')
        receipt = read_quality_receipt(root)
    else:
        receipt = dict(build_receipt)
    build = receipt.get('build')
    source = receipt.get('source')
    wheel_record = receipt.get('wheel')
    if not all(isinstance(value, dict) for value in (build, source, wheel_record)):
        raise AssertionError('runtime receipt lacks build, source or wheel identity')
    kind = receipt.get('kind')
    quality_receipt = kind == 'gometry-quality-runtime'
    sanitizer_receipt = kind == 'gometry-sanitizer-runtime'
    benchmark_wheel_receipt = kind == 'gometry-benchmark-wheel'
    if sanitizer_receipt:
        from sanitizers import validate_sanitizer_producer_inputs

        validate_sanitizer_producer_inputs(root, receipt)
    python_members = receipt.get('python_members', source.get('python_members'))
    if (
        not isinstance(python_members, dict)
        or not python_members
        or not all(
            isinstance(name, str)
            and name.startswith('gometry/')
            and isinstance(digest, str)
            and HEX_DIGEST.fullmatch(digest) is not None
            for name, digest in python_members.items()
        )
    ):
        raise AssertionError('runtime receipt has no valid Python-member identity')
    source_inputs = source.get('source_inputs_sha256')
    native_inputs = source.get('native_source_inputs_sha256')
    source_version = source.get('version')
    if (
        not isinstance(source_inputs, str)
        or HEX_DIGEST.fullmatch(source_inputs) is None
        or not isinstance(native_inputs, str)
        or HEX_DIGEST.fullmatch(native_inputs) is None
        or not isinstance(source_version, str)
        or not source_version
    ):
        raise AssertionError('runtime receipt has invalid source/native identities')

    if quality_receipt:
        validate_quality_producer_inputs(root, receipt)

    expected_target = build.get('target')
    if not isinstance(expected_target, str):
        raise TypeError('runtime receipt has no producer target')
    expected_raw_arch = build.get('raw_target_arch') or expected_target.split('-', 1)[0]
    profile = build.get('profile')
    if not isinstance(profile, str):
        raise TypeError('runtime receipt has no producer profile')
    interpreter = _runtime_interpreter()
    _runtime_selection_matches(interpreter, runtime_selection)
    producer_interpreter = build.get('interpreter')
    if not isinstance(producer_interpreter, dict):
        raise TypeError('runtime receipt has no producer interpreter')
    abi_matches = (
        interpreter['cache_tag'] == producer_interpreter.get('cache_tag')
        and interpreter['ext_suffix'] == producer_interpreter.get('ext_suffix')
        and interpreter['gil_disabled'] == producer_interpreter.get('gil_disabled')
    )
    if not abi_matches:
        _preparation_mismatch(kind, 'runtime ABI differs from the producer ABI')

    package_root_value = receipt.get('package_root')
    package_root = (
        Path(package_root_value).resolve()
        if isinstance(package_root_value, str)
        else None
    )
    if package_root is not None and not package_root.is_dir():
        _preparation_mismatch(
            kind, f'prepared runtime package root is missing: {package_root}'
        )

    if expected_wheel is None and (quality_receipt or sanitizer_receipt):
        wheel_path_value = wheel_record.get('path')
        if not isinstance(wheel_path_value, str):
            raise AssertionError('quality runtime receipt has no wheel path')
        expected_wheel = root.parent / wheel_path_value
    wheel_inspection: dict[str, Any] | None = None
    target_policy = _target_policy_identity(expected_target)
    if expected_wheel is not None:
        expected_wheel = expected_wheel.resolve()
        wheel_digest = wheel_record.get('sha256')
        if (
            not isinstance(wheel_digest, str)
            or HEX_DIGEST.fullmatch(wheel_digest) is None
        ):
            raise AssertionError('runtime receipt has a malformed wheel digest')
        if not expected_wheel.is_file() or sha256_file(expected_wheel) != wheel_digest:
            _preparation_mismatch(
                kind, 'installed smoke wheel differs from its build receipt'
            )
        wheel_inspection = inspect_wheel(
            expected_wheel,
            release=profile != 'dev',
            expected_python_members=None if quality_receipt else python_members,
            expected_prepared_inputs=(
                receipt['prepared_inputs']['files']
                if quality_receipt
                else receipt.get('prepared_inputs')
            ),
        )
        if wheel_inspection['generator'] != f'maturin ({MATURIN_VERSION})':
            raise AssertionError('installed wheel has the wrong Maturin Generator')
        identifier = receipt.get('identifier')
        if identifier is not None:
            if not isinstance(identifier, str) or not wheel_matches_identifier(
                expected_wheel,
                identifier,
                wheel_inspection['tags'],
            ):
                raise AssertionError(
                    'installed wheel does not match its CIBW identifier'
                )
            target_policy = build_identifier_policy(identifier)
        if sanitizer_receipt:
            from sanitizers import validate_sanitizer_wheel_tags

            validate_sanitizer_wheel_tags(wheel_inspection['tags'], interpreter)
        elif not wheel_target_compatible(
            expected_wheel, expected_target, wheel_inspection['tags']
        ):
            raise AssertionError('installed wheel tag is incompatible with its target')
        validate_wheel_native_target(
            wheel_inspection,
            target_policy,
            label=str(expected_wheel),
            validate_tags=not (
                quality_receipt or sanitizer_receipt or benchmark_wheel_receipt
            ),
        )
        extension_records = wheel_inspection['extension_members']
        if len(extension_records) != 1:
            raise AssertionError('installed wheel has no unique gometry extension')
        extension_member, extension_payload = next(iter(extension_records.items()))
        extension_digest = _sha256_bytes(extension_payload)
        if extension_member != wheel_record.get(
            'extension_member'
        ) or extension_digest != wheel_record.get('extension_sha256'):
            raise AssertionError('installed wheel extension differs from its receipt')
    else:
        extension_member = wheel_record.get('extension_member')
        extension_digest = wheel_record.get('extension_sha256')
        if (
            not isinstance(extension_member, str)
            or not isinstance(extension_digest, str)
            or HEX_DIGEST.fullmatch(extension_digest) is None
        ):
            raise AssertionError('runtime receipt has a malformed extension identity')

    try:
        if package_root is None:
            distribution = importlib.metadata.distribution('gometry')
        else:
            matches = [
                candidate
                for candidate in importlib.metadata.distributions(
                    path=[str(package_root)]
                )
                if str(candidate.metadata.get('Name', '')).lower() == 'gometry'
            ]
            if len(matches) != 1:
                _preparation_mismatch(
                    kind,
                    'prepared runtime does not contain one gometry distribution',
                )
            distribution = matches[0]
    except importlib.metadata.PackageNotFoundError as error:
        _preparation_mismatch(kind, f'gometry distribution is unavailable: {error}')
    metadata = distribution.metadata
    distribution_name = metadata.get('Name')
    distribution_version = metadata.get('Version')
    if (
        not isinstance(distribution_name, str)
        or distribution_name.lower() != 'gometry'
        or not isinstance(distribution_version, str)
        or distribution_version != source_version
    ):
        _preparation_mismatch(kind, 'gometry distribution differs from source identity')
    distribution_metadata = distribution.read_text('METADATA')
    if not isinstance(distribution_metadata, str):
        raise TypeError('gometry distribution METADATA is unreadable')
    distribution_metadata_sha256 = _sha256_bytes(distribution_metadata.encode('utf-8'))
    facade_path = Path(distribution.locate_file('gometry/__init__.py')).resolve()
    if not facade_path.is_file():
        _preparation_mismatch(kind, 'gometry distribution has no installed facade file')
    package_dir = facade_path.parent
    if (
        package_root is not None
        and facade_path != (package_root / 'gometry' / '__init__.py').resolve()
    ):
        _preparation_mismatch(kind, 'prepared runtime facade has the wrong path')
    actual_python_members = python_member_hashes(package_dir)
    if actual_python_members != python_members:
        _preparation_mismatch(
            kind, 'installed Python package differs from its member identity'
        )
    extension_path = Path(distribution.locate_file(extension_member)).resolve()
    if not extension_path.is_file():
        _preparation_mismatch(kind, 'gometry native extension is not installed')
    try:
        extension_relative = extension_path.relative_to(package_dir).as_posix()
    except ValueError as error:
        raise AssertionError(
            'gometry native extension is outside the package'
        ) from error
    if f'gometry/{extension_relative}' != extension_member:
        _preparation_mismatch(
            kind, 'installed native extension has the wrong member path'
        )
    extension_payload = extension_path.read_bytes()
    actual_extension_sha256 = _sha256_bytes(extension_payload)
    if actual_extension_sha256 != extension_digest:
        _preparation_mismatch(
            kind, 'installed native extension differs from its receipt'
        )
    if sanitizer_receipt:
        from sanitizers import verify_asan_image

        verify_asan_image(extension_path)
    distribution_files = distribution.files
    normalized_distribution_files = (
        None
        if distribution_files is None
        else frozenset(
            PurePosixPath(str(member)).as_posix() for member in distribution_files
        )
    )
    if wheel_inspection is not None:
        if normalized_distribution_files is None:
            raise AssertionError('gometry distribution has no installed file inventory')
        installed_names = set(normalized_distribution_files)
        dist_info = wheel_inspection['dist_info']
        metadata_member = f'{dist_info}/METADATA'
        wheel_member = f'{dist_info}/WHEEL'
        metadata_owners = {
            name
            for name in installed_names
            if PurePosixPath(name).name == 'METADATA'
            and len(PurePosixPath(name).parts) == 2
            and PurePosixPath(name).parts[0].endswith('.dist-info')
        }
        if metadata_owners != {metadata_member} or wheel_member not in installed_names:
            _preparation_mismatch(
                kind, 'gometry dist-info does not own the selected wheel'
            )
        if (
            _sha256_bytes(wheel_inspection['payloads'][metadata_member])
            != distribution_metadata_sha256
            or distribution.locate_file(metadata_member).resolve().parent
            != distribution.locate_file(wheel_member).resolve().parent
        ):
            _preparation_mismatch(
                kind, 'gometry dist-info differs from the selected wheel'
            )
        immutable_quality_tree = quality_receipt and package_root is not None
        signatures = set(wheel_inspection['signatures'])
        for member_name, record in wheel_inspection['members'].items():
            if not immutable_quality_tree and (
                member_name in signatures or member_name == wheel_inspection['record']
            ):
                continue
            if quality_receipt and member_name in wheel_inspection['python_members']:
                continue
            installed_path = Path(distribution.locate_file(member_name))
            if not installed_path.is_file():
                _preparation_mismatch(
                    kind,
                    f'wheel distribution member is not installed: {member_name}',
                )
            installed_digest = (
                actual_python_members[member_name]
                if member_name in actual_python_members
                else actual_extension_sha256
                if member_name == extension_member
                else sha256_file(installed_path)
            )
            if installed_digest != record['sha256']:
                _preparation_mismatch(
                    kind,
                    f'installed distribution member differs from wheel: {member_name}',
                )

    actual_native = native_image_facts(extension_payload, extension_path)
    validate_wheel_native_target(
        {'native': actual_native},
        target_policy,
        label=str(extension_path),
        validate_tags=False,
    )
    if wheel_inspection is not None and (
        actual_native != wheel_inspection['native']['extension']
    ):
        raise AssertionError('loaded native image facts differ from the repaired wheel')
    libc = platform.libc_ver()
    loader_identity = _runtime_loader_identity()
    platform_family = target_policy['platform_family']
    if platform_family.startswith('linux-'):
        expected_runtime_libc = platform_family.removeprefix('linux-')
        if loader_identity['runtime_libc'] != expected_runtime_libc:
            raise AssertionError('runtime loader libc differs from build target')
        if libc[0] and (expected_runtime_libc == 'musl') != ('musl' in libc[0].lower()):
            raise AssertionError('runtime libc differs from build target')
    elif loader_identity['runtime_libc'] != 'not-applicable':
        raise AssertionError('non-Linux runtime unexpectedly reports a Linux libc')
    machine = platform.machine().lower()
    machine_arch = (
        'x86_64'
        if machine in {'x86_64', 'amd64'}
        else 'arm64'
        if machine in {'aarch64', 'arm64'}
        else machine
    )
    build_arch = _target_architecture(expected_raw_arch)
    if machine_arch != build_arch:
        raise AssertionError('runtime machine architecture differs from build target')

    resident_modules = _resident_gometry_modules()
    _validate_resident_module_files(
        resident_modules,
        facade_path=facade_path,
        extension_path=extension_path,
        extension_sha256=actual_extension_sha256,
        package_dir=package_dir,
        python_members=actual_python_members,
    )

    wheel_admission = (
        None
        if wheel_inspection is None or expected_wheel is None
        else WheelAdmission(
            path=expected_wheel,
            sha256=wheel_record['sha256'],
            member_sha256={
                name: record['sha256']
                for name, record in wheel_inspection['members'].items()
            },
            python_members=frozenset(wheel_inspection['python_members']),
            record=wheel_inspection['record'],
            signatures=frozenset(wheel_inspection['signatures']),
            tags=tuple(wheel_inspection['tags']),
            native=wheel_inspection['native'],
        )
    )
    return RuntimeAdmission(
        root=root,
        receipt=receipt,
        package_root=package_root,
        package_dir=package_dir,
        facade_path=facade_path,
        extension_path=extension_path,
        extension_member=extension_member,
        extension_sha256=actual_extension_sha256,
        python_members=actual_python_members,
        interpreter=interpreter,
        target_policy=target_policy,
        distribution=distribution,
        distribution_version=distribution_version,
        distribution_metadata_sha256=distribution_metadata_sha256,
        distribution_files=normalized_distribution_files,
        wheel=wheel_admission,
        runtime_selection=runtime_selection,
    )


def verify_runtime(
    root: Path,
    *,
    expected_wheel: Path | None = None,
    build_receipt: Mapping[str, Any] | None = None,
    runtime_selection: Mapping[str, Any] | None = None,
) -> tuple[ModuleType, ModuleType, dict[str, Any]]:
    admission = inspect_runtime_inputs(
        root,
        expected_wheel=expected_wheel,
        build_receipt=build_receipt,
        runtime_selection=runtime_selection,
    )
    return verify_admitted_runtime(admission)


def check_import_resolution(expected_facade: Path) -> None:
    """Require the interpreter's current import path to select this facade."""
    if _resident_gometry_modules():
        raise AssertionError('Gometry was imported before benchmark runtime admission')
    try:
        spec = importlib.util.find_spec('gometry')
    except (ImportError, ModuleNotFoundError, ValueError) as error:
        raise AssertionError(
            f'cannot resolve admitted Gometry facade: {error}'
        ) from error
    origin = spec.origin if spec is not None else None
    expected_facade = expected_facade.resolve()
    if not isinstance(origin, str) or Path(origin).resolve() != expected_facade:
        raise AssertionError(
            'Python import path does not select the admitted Gometry facade: '
            f'{origin!r} != {str(expected_facade)!r}'
        )


def check_runtime_resolution(admission: RuntimeAdmission) -> None:
    """Require the interpreter's current import path to select the admitted facade."""
    check_import_resolution(admission.facade_path)


def verify_runtime_completion(
    admission: RuntimeAdmission,
    runtime: tuple[ModuleType, ModuleType, dict[str, Any]],
) -> None:
    """Recheck the admitted process and package after its benchmark observation."""
    facade, extension, _ = runtime
    if admission.receipt.get('kind') == 'gometry-sanitizer-runtime':
        from sanitizers import validate_sanitizer_producer_inputs, verify_asan_runtime

        validate_sanitizer_producer_inputs(admission.root, admission.receipt)
        verify_asan_runtime(admission.receipt)
    modules = _resident_gometry_modules()
    if (
        modules.get('gometry') is not facade
        or modules.get('gometry._lib') is not extension
    ):
        raise AssertionError('Gometry modules changed after benchmark admission')
    if _runtime_interpreter() != admission.interpreter:
        raise AssertionError('Python interpreter changed after benchmark admission')
    _validate_resident_module_files(
        modules,
        facade_path=admission.facade_path,
        extension_path=admission.extension_path,
        extension_sha256=admission.extension_sha256,
        package_dir=admission.package_dir,
        python_members=admission.python_members,
    )
    if not admission.shared_inputs and (
        python_member_hashes(admission.package_dir) != admission.python_members
    ):
        raise AssertionError(
            'loaded Gometry Python files changed after benchmark admission'
        )

    distribution = admission.distribution
    metadata = distribution.read_text('METADATA')
    if (
        not isinstance(metadata, str)
        or _sha256_bytes(metadata.encode('utf-8'))
        != admission.distribution_metadata_sha256
        or Path(distribution.locate_file('gometry/__init__.py')).resolve()
        != admission.facade_path
        or Path(distribution.locate_file(admission.extension_member)).resolve()
        != admission.extension_path
    ):
        raise AssertionError(
            'Gometry distribution ownership changed after benchmark admission'
        )
    if not admission.shared_inputs and admission.distribution_files is not None:
        current_files = distribution.files
        if (
            current_files is None
            or frozenset(
                PurePosixPath(str(member)).as_posix() for member in current_files
            )
            != admission.distribution_files
        ):
            raise AssertionError(
                'Gometry distribution inventory changed after benchmark admission'
            )

    wheel = admission.wheel
    if wheel is not None and not admission.shared_inputs:
        if sha256_file(wheel.path) != wheel.sha256:
            raise AssertionError('runtime wheel changed after benchmark admission')
        immutable_quality_tree = (
            admission.receipt.get('kind') == 'gometry-quality-runtime'
            and admission.package_root is not None
        )
        for member, expected_digest in wheel.member_sha256.items():
            if (
                (
                    not immutable_quality_tree
                    and (member == wheel.record or member in wheel.signatures)
                )
                or member in admission.python_members
                or (
                    admission.receipt.get('kind') == 'gometry-quality-runtime'
                    and member in wheel.python_members
                )
                or member == admission.extension_member
            ):
                continue
            installed_path = Path(distribution.locate_file(member))
            if (
                not installed_path.is_file()
                or sha256_file(installed_path) != expected_digest
            ):
                raise AssertionError(
                    f'installed wheel member changed after benchmark admission: {member}'
                )

    if (
        not admission.shared_inputs
        and admission.receipt.get('kind') == 'gometry-quality-runtime'
    ):
        if read_quality_receipt(admission.root) != admission.receipt:
            raise AssertionError('quality runtime receipt changed after admission')
        try:
            validate_quality_producer_inputs(admission.root, admission.receipt)
        except QualityPreparationRequired as error:
            raise AssertionError(
                f'quality runtime inputs changed after benchmark admission: {error}'
            ) from error


def verify_admitted_runtime(
    admission: RuntimeAdmission,
) -> tuple[ModuleType, ModuleType, dict[str, Any]]:
    """Import and verify the runtime selected by completed inert admission."""
    root = admission.root
    receipt = admission.receipt
    build = receipt['build']
    source = receipt['source']
    python_members = admission.python_members
    package_root = admission.package_root
    package_dir = admission.package_dir
    facade_path = admission.facade_path
    extension_path = admission.extension_path
    actual_extension_sha256 = admission.extension_sha256
    source_inputs = source['source_inputs_sha256']
    native_inputs = source['native_source_inputs_sha256']
    expected_target = build['target']
    expected_raw_arch = build.get('raw_target_arch') or expected_target.split('-', 1)[0]
    profile = build['profile']
    target_policy = admission.target_policy
    distribution = admission.distribution
    version = admission.distribution_version

    resident_modules = _resident_gometry_modules()
    if resident_modules:
        _validate_resident_module_files(
            resident_modules,
            facade_path=facade_path,
            extension_path=extension_path,
            extension_sha256=actual_extension_sha256,
            package_dir=package_dir,
            python_members=python_members,
        )
    elif package_root is not None:
        package_text = str(package_root)
        if package_text in sys.path:
            sys.path.remove(package_text)
        sys.path.insert(0, package_text)

    facade = importlib.import_module('gometry')
    extension = importlib.import_module('gometry._lib')
    if receipt.get('kind') == 'gometry-sanitizer-runtime':
        from sanitizers import verify_asan_runtime

        verify_asan_runtime(receipt)
    extension_payload = extension_path.read_bytes()
    _validate_resident_module_files(
        _resident_gometry_modules(),
        facade_path=facade_path,
        extension_path=extension_path,
        extension_sha256=actual_extension_sha256,
        package_dir=package_dir,
        python_members=python_members,
        extension_payload=extension_payload,
    )
    if (
        not admission.shared_inputs
        and python_member_hashes(package_dir) != python_members
    ):
        raise AssertionError(
            'loaded Gometry Python files changed after runtime admission'
        )
    if not admission.shared_inputs and receipt.get('kind') == 'gometry-quality-runtime':
        try:
            validate_quality_producer_inputs(root, receipt)
        except QualityPreparationRequired as error:
            raise AssertionError(
                f'quality runtime inputs changed after admission: {error}'
            ) from error
    build_info = getattr(extension, '__build_info__', None)
    expected_info_fields = {
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
    if not isinstance(build_info, dict) or set(build_info) != expected_info_fields:
        raise AssertionError(
            'native build-info schema is missing or has unknown fields'
        )
    profile = build.get('profile')
    if profile == 'dev':
        expected_profile = {
            'profile_family': 'debug',
            'opt_level': '0',
            'debug_assertions': True,
        }
    else:
        expected_profile = {
            'profile_family': 'release',
            'opt_level': '3',
            'debug_assertions': False,
        }
    expected_info = {
        'schema_version': 1,
        'source_inputs_format': SOURCE_INPUTS_FORMAT,
        'source_inputs_sha256': source_inputs,
        'native_source_inputs_format': SOURCE_INPUTS_FORMAT,
        'native_source_inputs_sha256': native_inputs,
        'target': expected_target,
        'target_arch': expected_raw_arch,
        **expected_profile,
    }
    for name, expected in expected_info.items():
        if build_info.get(name) != expected:
            raise AssertionError(
                f'native build-info {name} mismatch: '
                f'{build_info.get(name)!r} != {expected!r}'
            )
    if not isinstance(build_info.get('target_features'), str):
        raise TypeError('native build-info target features are malformed')

    interpreter = admission.interpreter

    wheel = admission.wheel
    metadata = distribution.metadata
    name = metadata.get('Name')
    version = metadata.get('Version')
    if (
        not isinstance(name, str)
        or name.lower() != 'gometry'
        or version != admission.distribution_version
        or version != getattr(facade, '__version__', None)
    ):
        raise AssertionError('gometry facade and distribution versions disagree')
    distribution_metadata = distribution.read_text('METADATA')
    if not isinstance(distribution_metadata, str):
        raise TypeError('gometry distribution METADATA is unreadable')
    distribution_metadata_sha256 = _sha256_bytes(distribution_metadata.encode('utf-8'))
    if distribution_metadata_sha256 != admission.distribution_metadata_sha256:
        raise AssertionError('gometry distribution metadata changed after admission')
    if (
        distribution.locate_file('gometry/__init__.py').resolve()
        != admission.facade_path
        or distribution.locate_file(admission.extension_member).resolve()
        != admission.extension_path
    ):
        raise AssertionError('gometry distribution ownership changed after admission')

    if not admission.shared_inputs and admission.distribution_files is not None:
        current_distribution_files = distribution.files
        if (
            current_distribution_files is None
            or frozenset(
                PurePosixPath(str(member)).as_posix()
                for member in current_distribution_files
            )
            != admission.distribution_files
        ):
            raise AssertionError(
                'gometry distribution file inventory changed after admission'
            )

    if wheel is not None and not admission.shared_inputs:
        if sha256_file(wheel.path) != wheel.sha256:
            raise AssertionError('runtime wheel changed after inert admission')
        immutable_quality_tree = (
            receipt.get('kind') == 'gometry-quality-runtime'
            and package_root is not None
        )
        for member, expected_digest in wheel.member_sha256.items():
            if (
                (
                    not immutable_quality_tree
                    and (member == wheel.record or member in wheel.signatures)
                )
                or member in python_members
                or (
                    receipt.get('kind') == 'gometry-quality-runtime'
                    and member in wheel.python_members
                )
                or member == admission.extension_member
            ):
                continue
            installed_path = Path(distribution.locate_file(member))
            if (
                not installed_path.is_file()
                or sha256_file(installed_path) != expected_digest
            ):
                raise AssertionError(
                    f'installed wheel member changed after admission: {member}'
                )

    target_policy = admission.target_policy
    actual_native = (
        wheel.native['extension']
        if admission.shared_inputs and wheel is not None
        else native_image_facts(extension_payload, extension_path)
    )
    if wheel is None:
        validate_wheel_native_target(
            {'native': actual_native},
            target_policy,
            label=str(extension_path),
            validate_tags=False,
        )
    elif actual_native != wheel.native['extension']:
        raise AssertionError('loaded native image facts differ from the repaired wheel')
    libc = platform.libc_ver()
    expected_libc = target_policy['platform_family']
    loader_identity = _runtime_loader_identity()
    if expected_libc.startswith('linux-'):
        expected_runtime_libc = expected_libc.removeprefix('linux-')
        if loader_identity['runtime_libc'] != expected_runtime_libc:
            raise AssertionError(
                f'runtime loader libc differs from build target: '
                f'{loader_identity!r} != {expected_runtime_libc}'
            )
        if libc[0] and (expected_runtime_libc == 'musl') != ('musl' in libc[0].lower()):
            raise AssertionError(
                f'runtime libc differs from native target: {libc!r} != {expected_libc}'
            )
    elif loader_identity['runtime_libc'] != 'not-applicable':
        raise AssertionError('non-Linux runtime unexpectedly reports a Linux libc')
    machine = platform.machine().lower()
    machine_arch = (
        'x86_64'
        if machine in {'x86_64', 'amd64'}
        else 'arm64'
        if machine in {'aarch64', 'arm64'}
        else machine
    )
    runtime_record = {
        'interpreter': interpreter,
        'module': {
            'facade': {
                'member': 'gometry/__init__.py',
                'sha256': sha256_file(facade_path),
            },
            'extension': {
                'member': admission.extension_member,
                'sha256': sha256_file(extension_path),
            },
        },
        'source_inputs_sha256': source_inputs,
        'native_source_inputs_sha256': native_inputs,
        'distribution': {
            'name': name,
            'version': version,
            'metadata_sha256': distribution_metadata_sha256,
        },
        'platform': {
            'machine': machine,
            'architecture': machine_arch,
            'target': build_info['target'],
            'target_arch': build_info['target_arch'],
            'platform': sysconfig.get_platform(),
            'libc': libc,
            'loader': loader_identity,
            'gil_disabled': interpreter['gil_disabled'],
            'wheel_tags': [] if wheel is None else list(wheel.tags),
            'native_image': (
                {'extension': actual_native, 'libraries': {}}
                if wheel is None
                else wheel.native
            ),
        },
    }
    if (
        sysconfig.get_config_var('Py_GIL_DISABLED')
        and hasattr(sys, '_is_gil_enabled')
        and sys._is_gil_enabled()
    ):
        raise AssertionError('gometry re-enabled the GIL in a free-threaded runtime')
    if receipt.get('kind') == 'gometry-sanitizer-runtime':
        print(
            'gometry ASan runtime admission: '
            + json.dumps(
                {
                    'extension': str(extension_path),
                    'sha256': actual_extension_sha256,
                    'source_inputs_sha256': source_inputs,
                    'pid': interpreter['pid'],
                },
                sort_keys=True,
            ),
            file=sys.stderr,
            flush=True,
        )
    return facade, extension, runtime_record
