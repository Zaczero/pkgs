"""Release-wheel inspection, functional smoke checks, and benchmark binding."""

from __future__ import annotations

import importlib.metadata
import json
import os
import re
import shutil
import sys
import tarfile
import tempfile
import textwrap
import venv
from collections.abc import Mapping
from importlib.machinery import EXTENSION_SUFFIXES
from pathlib import Path, PurePosixPath
from typing import Any

from _artifact_common import (
    HEX_DIGEST,
    RELEASE_PROFILE,
    ROOT,
    _effective_release_policy,
    _load_json,
    _package_version,
    _sha256_bytes,
    _target_policy_identity,
    release_build_env,
    run,
    sha256_file,
)
from artifact_content import (
    X86_64_V2_TARGET_FEATURES,
    X86_64_V3_PLUS_TARGET_FEATURES,
    inspect_wheel,
    libtiff_source_members,
    source_input_digests,
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
)
from source_artifact import (
    _verify_extracted_sdist,
    _verify_locked_sdist,
    extract_sdist,
    inspect_sdist,
    sdist_toolchain_channel,
    stage_sdist,
    validate_source_manifest,
)


def build_wheel_from_sdist(sdist: Path, out_dir: Path, *, env: dict[str, str]) -> Path:
    sdist_version = inspect_sdist(
        sdist,
        expected_libtiff=libtiff_source_members(),
        expected_toolchain_channel=sdist_toolchain_channel(sdist),
    )
    with tempfile.TemporaryDirectory(prefix='gometry-wheel-source-') as temporary:
        extracted = Path(temporary)
        with tarfile.open(sdist) as archive:
            extract_sdist(archive, extracted)
        roots = [path for path in extracted.iterdir() if path.is_dir()]
        if len(roots) != 1:
            raise AssertionError('sdist has no unique source root')
        source = roots[0]
        # Cargo prunes monorepo-only lock entries before a standalone locked build.
        evidence = _verify_extracted_sdist(source)
        _verify_locked_sdist(source, expected_lock=evidence['normalized_lock_bytes'])
        run(
            [
                'uv',
                'build',
                '--wheel',
                '--python',
                sys.executable,
                '--out-dir',
                str(out_dir),
                str(source),
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
    sdist = stage_sdist(out_dir)
    return build_wheel_from_sdist(sdist, out_dir, env=release_build_env())


def _smoke_grid(
    destination: Path, *, expected_source_manifest: Path | None = None
) -> Path:
    configured = os.environ.get(SMOKE_GRID_ENV)
    if configured:
        path = Path(configured)
        if not path.is_file():
            raise AssertionError(f'{SMOKE_GRID_ENV} does not name a file: {path}')
        return path
    archive_path = ROOT / 'native' / 'proj-sys' / 'PROJSRC' / 'proj-9.9.0.tar.gz'
    member_name = 'proj-9.9.0/data/tests/test_vgrid_pixelispoint.tif'
    destination.mkdir(parents=True, exist_ok=True)
    path = destination / Path(member_name).name
    try:
        if not archive_path.is_file() and expected_source_manifest is not None:
            sdist = _source_sdist_from_manifest(expected_source_manifest)
            root = (
                f'gometry-{sdist.name.removeprefix("gometry-").removesuffix(".tar.gz")}'
            )
            sdist_member = f'{root}/gometry/native/proj-sys/PROJSRC/proj-9.9.0.tar.gz'
            with tarfile.open(sdist) as source:
                stream = source.extractfile(sdist_member)
                if stream is None:
                    raise AssertionError(f'sdist is missing {sdist_member}')
                archive_path = destination / 'proj-9.9.0.tar.gz'
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


def _smoke_extension_member(
    extension_members: Mapping[str, bytes],
) -> str:
    if len(extension_members) != 1:
        raise AssertionError(
            f'wheel must contain one Gometry extension: {sorted(extension_members)}'
        )
    member = next(iter(extension_members))
    path = PurePosixPath(member)
    if path.parent != PurePosixPath('gometry') or not any(
        path.name == f'_lib{suffix}' for suffix in EXTENSION_SUFFIXES
    ):
        raise AssertionError(
            f'wheel extension member is not importable by this interpreter: {member}'
        )
    return member


def _check_smoke_wheel_tags(wheel: Path, tags: list[str]) -> None:
    from packaging.tags import parse_tag, sys_tags
    from packaging.utils import parse_wheel_filename

    try:
        filename_tags = parse_wheel_filename(wheel.name)[3]
        wheel_tags = set().union(*(parse_tag(tag) for tag in tags))
    except ValueError as error:
        raise AssertionError(
            f'wheel has invalid compatibility tags: {wheel}'
        ) from error
    if wheel_tags != filename_tags:
        raise AssertionError('wheel filename and WHEEL compatibility tags differ')
    if not wheel_tags.intersection(sys_tags()):
        raise AssertionError(
            f'wheel compatibility tags do not match this interpreter: {tags}'
        )


def _installed_smoke_extension_member() -> str:
    distribution = importlib.metadata.distribution('gometry')
    if distribution.files is None:
        raise AssertionError('installed gometry distribution lacks a file inventory')
    extensions = {
        PurePosixPath(str(member)).as_posix(): b''
        for member in distribution.files
        if PurePosixPath(str(member)).parent == PurePosixPath('gometry')
        and PurePosixPath(str(member)).name.startswith('_lib')
        and PurePosixPath(str(member)).suffix.lower() in {'.so', '.pyd', '.dylib'}
    }
    return _smoke_extension_member(extensions)


def _smoke_environment(
    *,
    extension_member: str,
    extension_sha256: str,
    expected_source_manifest: Path | None = None,
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
    selected = _effective_release_policy()
    policy = _target_policy_identity(selected['target'])
    values = {
        SMOKE_EXPECTED_SOURCE_INPUTS_ENV: expected_source_inputs,
        SMOKE_EXPECTED_NATIVE_SOURCE_INPUTS_ENV: expected_native_inputs,
        SMOKE_EXPECTED_VERSION_ENV: expected_version,
        SMOKE_EXPECTED_TARGET_ENV: policy['target'],
        SMOKE_EXPECTED_ARCHITECTURE_ENV: policy['raw_target_arch'],
        SMOKE_EXPECTED_HOST_ARCH_ENV: policy['architecture'],
        'GOMETRY_SMOKE_EXPECTED_EXTENSION_MEMBER': extension_member,
        'GOMETRY_SMOKE_EXPECTED_EXTENSION_SHA256': extension_sha256,
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


def smoke_installed(*, expected_source_manifest: Path | None = None) -> None:
    with tempfile.TemporaryDirectory(prefix='gometry-installed-smoke-') as tmp:
        grid = _smoke_grid(
            Path(tmp) / 'grid',
            expected_source_manifest=expected_source_manifest,
        )
        member = _installed_smoke_extension_member()
        extension = Path(importlib.metadata.distribution('gometry').locate_file(member))
        env = _smoke_environment(
            extension_member=member,
            extension_sha256=sha256_file(extension),
            expected_source_manifest=expected_source_manifest,
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
            'GOMETRY_SMOKE_EXPECTED_EXTENSION_MEMBER',
            'GOMETRY_SMOKE_EXPECTED_EXTENSION_SHA256',
            'GOMETRY_SMOKE_CPU_FEATURES',
        }
        previous = {name: os.environ.get(name) for name in names}
        try:
            os.environ.update({name: env[name] for name in names})
            exec(  # ruff: ignore[exec-builtin]
                compile(textwrap.dedent(SMOKE), '<gometry-wheel-smoke>', 'exec'),
                {'__name__': '__main__'},
            )
        finally:
            for name, value in previous.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value


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
    """Validate a prepared quality runtime for benchmarking."""
    receipt_path = receipt_path.resolve()
    if receipt_path.is_dir():
        receipt_path /= 'receipt.json'
    root = _quality_benchmark_root(receipt_path)
    if root is None:
        raise AssertionError('select a quality runtime receipt or a wheel')
    quality_receipt = read_quality_receipt(root)
    validate_quality_producer_inputs(root, quality_receipt)
    return _quality_benchmark_subject(root, quality_receipt, sha256_file(receipt_path))


def read_benchmark_receipt(
    receipt_path: Path, expected_receipt_sha256: str
) -> dict[str, Any]:
    """Read a parent-validated quality subject without rescanning its wheel."""
    receipt_path = receipt_path.resolve()
    if (
        not isinstance(expected_receipt_sha256, str)
        or HEX_DIGEST.fullmatch(expected_receipt_sha256) is None
        or sha256_file(receipt_path) != expected_receipt_sha256
    ):
        raise AssertionError('validated benchmark receipt changed before dispatch')
    root = _quality_benchmark_root(receipt_path)
    if root is None:
        raise AssertionError('select a quality runtime receipt or a wheel')
    return _quality_benchmark_subject(
        root, read_quality_receipt(root), expected_receipt_sha256
    )


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
import hashlib
import importlib.metadata
import importlib.machinery
import json
import os
import platform
from pathlib import Path, PurePosixPath
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
expected_extension_member = os.environ['GOMETRY_SMOKE_EXPECTED_EXTENSION_MEMBER']
member_path = PurePosixPath(expected_extension_member)
assert member_path.parent == PurePosixPath('gometry')
assert any(
    member_path.name == f'_lib{suffix}'
    for suffix in importlib.machinery.EXTENSION_SUFFIXES
), expected_extension_member
distribution = importlib.metadata.distribution('gometry')
distribution_files = distribution.files
assert distribution_files is not None
members = {PurePosixPath(str(member)).as_posix() for member in distribution_files}
assert expected_extension_member in members, expected_extension_member
extension_path = Path(lib.__file__).resolve()
assert extension_path == Path(distribution.locate_file(expected_extension_member)).resolve()
assert hashlib.sha256(extension_path.read_bytes()).hexdigest() == os.environ[
    'GOMETRY_SMOKE_EXPECTED_EXTENSION_SHA256'
]

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
    'schema_version': 1,
    'source_inputs_format': 'sha256-path-nul-content-nul-v1',
    'source_inputs_sha256': os.environ['GOMETRY_SMOKE_EXPECTED_SOURCE_INPUTS_SHA256'],
    'native_source_inputs_format': 'sha256-path-nul-content-nul-v1',
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
    inspection = inspect_wheel(wheel)
    _check_smoke_wheel_tags(wheel, inspection['tags'])
    extension_member = _smoke_extension_member(inspection['extension_members'])
    extension_sha256 = _sha256_bytes(inspection['extension_members'][extension_member])
    with tempfile.TemporaryDirectory(prefix='gometry-wheel-smoke-') as tmp:
        temporary = Path(tmp)
        venv_dir = temporary / 'venv'
        venv.EnvBuilder(with_pip=True).create(venv_dir)
        python = venv_dir / ('Scripts/python.exe' if os.name == 'nt' else 'bin/python')
        run([str(python), '-m', 'pip', 'install', str(wheel)])
        neutral = temporary / 'neutral'
        neutral.mkdir()
        grid = _smoke_grid(temporary / 'grid')
        env = _smoke_environment(
            extension_member=extension_member,
            extension_sha256=extension_sha256,
            expected_source_manifest=expected_source_manifest,
        )
        env.pop('PYTHONPATH', None)
        env.pop('PYTHONHOME', None)
        env[SMOKE_GRID_ENV] = str(grid)
        run(
            [str(python), '-W', 'error', '-c', textwrap.dedent(SMOKE)],
            cwd=neutral,
            env=env,
        )
