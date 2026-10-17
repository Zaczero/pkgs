"""Private artifact records for native runtime-admission tests."""

from __future__ import annotations

import importlib.metadata
import json
import os
import zipfile
from pathlib import Path

import _artifact_common as artifact_common
import artifact_content
import pytest
import quality_runtime
import runtime_artifact
from gometry import _lib

GOMETRY_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope='session')
def quality_artifact_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    workspace = tmp_path_factory.mktemp('quality-artifact')
    root = workspace / 'gometry'
    root.mkdir()
    for name in ('build.rs', 'src', 'native'):
        (root / name).symlink_to(
            GOMETRY_ROOT / name, target_is_directory=name != 'build.rs'
        )
    for logical, source in quality_runtime._quality_fixed_input_paths(GOMETRY_ROOT):
        destination = workspace / logical
        if destination.exists():
            continue
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.symlink_to(source.resolve())
    distribution = importlib.metadata.distribution('gometry')
    package_root = Path(distribution.locate_file('')).resolve()
    (root / 'python').mkdir()
    (root / 'python' / 'gometry').symlink_to(
        package_root / 'gometry', target_is_directory=True
    )
    wheel_metadata = distribution.read_text('WHEEL')
    assert wheel_metadata is not None
    tag = next(
        line.removeprefix('Tag: ')
        for line in wheel_metadata.splitlines()
        if line.startswith('Tag: ')
    )
    archive_directory = workspace / 'artifacts'
    archive_directory.mkdir()
    wheel = archive_directory / f'gometry-{distribution.version}-{tag}.whl'
    assert distribution.files is not None
    with zipfile.ZipFile(wheel, 'w', compression=zipfile.ZIP_STORED) as archive:
        for member in distribution.files:
            archive.write(distribution.locate_file(member), str(member))
    wheel_digest = artifact_common.sha256_file(wheel)
    content_directory = archive_directory / wheel_digest
    content_directory.mkdir()
    wheel = wheel.rename(content_directory / wheel.name)
    inspection = artifact_content.inspect_wheel(wheel, release=False)
    extension_member, extension = inspection['extension_members'].popitem()
    python_members = artifact_content.python_member_hashes(package_root / 'gometry')
    prepared = artifact_common._prepared_input_record(
        artifact_common._prepared_output_hashes(root)
    )
    interpreter = runtime_artifact._runtime_interpreter()
    info = _lib.__build_info__
    if callable(info):
        info = info()
    target = info['target']
    environment = os.environ.copy()
    for name in artifact_common.RELEASE_ENV_VARS:
        environment.pop(name, None)
    environment.pop('MATURIN_PEP517_ARGS', None)
    environment.update({
        'CARGO_BUILD_TARGET': target,
        'NIX_DONT_SET_RPATH': '1',
        f'NIX_DONT_SET_RPATH_{target.replace("-", "_")}': '1',
        'PYO3_PYTHON': interpreter['resolved_executable'],
    })
    dependencies = {
        'lock_sha256': artifact_common.sha256_file(workspace / 'Cargo.lock'),
        'required_sources': [{'name': 'artifact-fixture'}],
    }
    receipt = {
        'schema': 1,
        'kind': 'gometry-quality-runtime',
        'package_root': str(package_root),
        'profile': 'dev',
        'source': {
            **artifact_content.source_input_digests(root),
            'version': distribution.version,
            'python_members': python_members,
            'cargo': dependencies,
        },
        'prepared_inputs': prepared,
        'build': {
            'profile': 'dev',
            'target': target,
            'raw_target_arch': target.split('-', 1)[0],
            'interpreter': interpreter,
            'compiler': {'observed_at': 'before-build', 'rustc_vv': 'artifact fixture'},
            'cargo': {'version': 'artifact fixture', 'dependencies': dependencies},
            'maturin_version': artifact_common.MATURIN_VERSION,
            'settings': quality_runtime.quality_settings(root, environment),
            'command': ['artifact-fixture'],
        },
        'wheel': {
            'path': wheel.relative_to(workspace).as_posix(),
            'filename': wheel.name,
            'sha256': wheel_digest,
            'generator': inspection['generator'],
            'extension_member': extension_member,
            'extension_sha256': artifact_common._sha256_bytes(extension),
        },
        'python_members': python_members,
    }
    path = quality_runtime.quality_receipt_path(root)
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(receipt), encoding='utf-8')
    quality_runtime.read_quality_receipt(root)
    quality_runtime.validate_quality_producer_inputs(root, receipt)
    return root
