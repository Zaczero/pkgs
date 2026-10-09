"""Artifact identity and wheel RECORD boundary tests."""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import json
import os
import platform
import shutil
import struct
import subprocess
import sys
import sysconfig
import tarfile
import zipfile
from pathlib import Path
from typing import Any

import pytest

GOMETRY_ROOT = Path(__file__).resolve().parents[2]
TOOLS = GOMETRY_ROOT / 'tools'
BENCHMARK_SUPPORT = GOMETRY_ROOT / 'benches' / 'support'
for _path in (TOOLS, BENCHMARK_SUPPORT):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

import _artifact_common as artifact_common  # noqa: E402
import artifact_content  # noqa: E402
import benchmark_runtime  # noqa: E402
import quality_runtime  # noqa: E402
import release_artifact  # noqa: E402
import runtime_artifact  # noqa: E402
import source_artifact  # noqa: E402
import wheel_smoke as wheel_cli  # noqa: E402


def _record_digest(payload: bytes) -> str:
    encoded = base64.urlsafe_b64encode(hashlib.sha256(payload).digest())
    return 'sha256=' + encoded.rstrip(b'=').decode('ascii')


def _record_bytes(
    members: dict[str, bytes],
    *,
    rows: list[tuple[str, str, str]] | None = None,
) -> bytes:
    if rows is None:
        rows = [
            (name, _record_digest(payload), str(len(payload)))
            for name, payload in sorted(members.items())
            if not name.endswith(('/RECORD.jws', '/RECORD.p7s'))
            and not name.endswith('/RECORD')
        ]
        record_name = next(
            name for name in members if name.endswith('.dist-info/RECORD')
        )
        rows.append((record_name, '', ''))
    stream = io.StringIO(newline='')
    writer = csv.writer(stream, lineterminator='\n')
    writer.writerows(rows)
    return stream.getvalue().encode('utf-8')


def _wheel(
    tmp_path: Path,
    *,
    content: dict[str, bytes] | None = None,
    rows: list[tuple[str, str, str]] | None = None,
    signatures: bool = False,
    name: str = 'fixture.whl',
) -> Path:
    members = {
        'gometry/__init__.py': b'from ._api import value\n',
        'gometry/_api.py': b'value = 17\n',
        'gometry/py.typed': b'',
        'fixture-1.dist-info/METADATA': b'Name: gometry\nVersion: 1.0\n',
        'fixture-1.dist-info/WHEEL': (
            b'Wheel-Version: 1.0\n'
            b'Generator: maturin (1.14.1)\n'
            b'Tag: cp314-cp314-manylinux_2_17_x86_64\n'
        ),
    }
    if content is not None:
        members.update(content)
    record_name = 'fixture-1.dist-info/RECORD'
    members[record_name] = b''
    members[record_name] = _record_bytes(members, rows=rows)
    if signatures:
        members['fixture-1.dist-info/RECORD.jws'] = b'signature'
    path = tmp_path / name
    with zipfile.ZipFile(path, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        for member, payload in members.items():
            archive.writestr(member, payload)
    return path


def _quality_wheel(
    directory: Path,
    *,
    version: str,
    python_members: dict[str, bytes],
    licenses: dict[str, bytes],
    extension_payload: bytes,
) -> Path:
    dist_info = f'gometry-{version}.dist-info'
    extension = f'gometry/_lib{sysconfig.get_config_var("EXT_SUFFIX")}'
    members = {
        **python_members,
        extension: extension_payload,
        f'{dist_info}/METADATA': (f'Name: gometry\nVersion: {version}\n'.encode()),
        f'{dist_info}/WHEEL': (
            b'Wheel-Version: 1.0\n'
            b'Generator: maturin (1.14.1)\n'
            b'Tag: cp314-cp314-manylinux_2_17_x86_64\n'
        ),
        **{
            f'{dist_info}/licenses/{name}': payload
            for name, payload in licenses.items()
        },
    }
    record_name = f'{dist_info}/RECORD'
    members[record_name] = b''
    members[record_name] = _record_bytes(members)
    path = directory / f'gometry-{version}-cp314-cp314-manylinux_2_17_x86_64.whl'
    with zipfile.ZipFile(path, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        for name, payload in members.items():
            archive.writestr(name, payload)
    return path


def _inspection(path: Path) -> dict[str, Any]:
    with zipfile.ZipFile(path) as archive:
        return artifact_content.inspect_wheel_members(archive)


def _source_root(tmp_path: Path) -> Path:
    root = tmp_path / 'source-root'
    (root / 'src').mkdir(parents=True)
    (root / 'native').mkdir()
    (root / 'build.rs').write_text('fn main() {}\n', encoding='utf-8')
    (root / 'src' / 'lib.rs').write_text('pub fn value() {}\n', encoding='utf-8')
    (root / 'native' / 'build.c').write_text(
        'int value(void) { return 1; }\n', encoding='utf-8'
    )
    return root


def _producer_git_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Path, str]:
    repo = tmp_path / 'producer-repo'
    root = repo / 'gometry'
    package = root / 'python' / 'gometry'
    package.mkdir(parents=True)
    files = {
        '.gitignore': (
            'gometry/python/gometry/ignored.py\n'
            'gometry/python/gometry/*.so\n'
            'gometry/python/gometry/__pycache__/\n'
        ),
        '.gitmodules': (
            '[submodule "libtiff"]\n'
            '\tpath = gometry/native/libtiff-sys/libtiff\n'
            '\turl = https://example.invalid/libtiff.git\n'
        ),
        'Cargo.toml': '[workspace]\nmembers = ["gometry"]\n',
        'Cargo.lock': '# fixture lock\n',
        '.cargo/config.toml': '[build]\n',
        'rust-toolchain.toml': '[toolchain]\nchannel = "nightly"\n',
        'gometry/Cargo.toml': '[package]\nname = "gometry"\n',
        'gometry/pyproject.toml': '[project]\nname = "gometry"\n',
        'gometry/rust-toolchain.toml': '[toolchain]\nchannel = "nightly"\n',
        'gometry/native/proj-sys/PROJSRC/proj-9.6.2.tar.gz': 'proj fixture',
        'gometry/build.rs': 'fn main() {}\n',
        'gometry/src/lib.rs': 'pub fn value() {}\n',
        'gometry/native/fixture/Cargo.toml': '[package]\nname = "fixture"\n',
        'gometry/native/fixture/src/lib.rs': 'pub fn value() {}\n',
        'gometry/native-licenses.toml': '[[licenses]]\n',
        'gometry/native/notices/nlohmann-json-license.md': 'notice\n',
        'gometry/native/libtiff-sys/notices/libtiff-license.md': 'license\n',
        'gometry/native/libtiff-sys/notices/libtiff-lzw-license.md': 'license\n',
        'gometry/README.md': 'source readme\n',
        '.github/scripts/render_pypi_readme.py': 'print("readme")\n',
        '.github/scripts/gen_licenses.py': 'print("licenses")\n',
        '.github/scripts/about.toml': '[about]\n',
        '.github/scripts/about.hbs': '{{about}}\n',
        'gometry/python/gometry/__init__.py': 'value = 1\n',
        'gometry/python/gometry/_lib.pyi': 'def value() -> int: ...\n',
        'gometry/python/gometry/api.py': 'def value() -> int: return 1\n',
        'gometry/tools/_artifact_common.py': 'common\n',
        'gometry/tools/artifact_content.py': 'content\n',
        'gometry/tools/quality_runtime.py': 'quality\n',
        'gometry/tools/release_artifact.py': 'release\n',
        'gometry/tools/runtime_artifact.py': 'runtime\n',
        'gometry/tools/source_artifact.py': 'source\n',
        'gometry/tools/wheel_smoke.py': 'cli\n',
    }
    for relative, contents in files.items():
        path = repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents, encoding='utf-8')

    libtiff = root / 'native' / 'libtiff-sys' / 'libtiff'
    libtiff.mkdir(parents=True)
    subprocess.run(['git', 'init', '-q'], cwd=repo, check=True)
    subprocess.run(
        ['git', 'config', 'user.name', 'Artifact Fixture'],
        cwd=repo,
        check=True,
    )
    subprocess.run(
        ['git', 'config', 'user.email', 'artifact@example.invalid'],
        cwd=repo,
        check=True,
    )
    subprocess.run(['git', 'add', '--all'], cwd=repo, check=True)

    subprocess.run(['git', 'init', '-q'], cwd=libtiff, check=True)
    subprocess.run(
        ['git', 'config', 'user.name', 'Artifact Fixture'],
        cwd=libtiff,
        check=True,
    )
    subprocess.run(
        ['git', 'config', 'user.email', 'artifact@example.invalid'],
        cwd=libtiff,
        check=True,
    )
    (libtiff / 'VERSION').write_text('4.7.2\n', encoding='utf-8')
    subprocess.run(['git', 'add', 'VERSION'], cwd=libtiff, check=True)
    subprocess.run(
        ['git', 'commit', '-m', 'fixture libtiff'],
        cwd=libtiff,
        check=True,
        capture_output=True,
        text=True,
    )
    libtiff_commit = subprocess.run(
        ['git', 'rev-parse', 'HEAD'],
        cwd=libtiff,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    subprocess.run(
        [
            'git',
            'update-index',
            '--add',
            '--cacheinfo',
            '160000',
            libtiff_commit,
            'gometry/native/libtiff-sys/libtiff',
        ],
        cwd=repo,
        check=True,
    )
    subprocess.run(
        ['git', 'commit', '-m', 'fixture producer'],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )
    commit = subprocess.run(
        ['git', 'rev-parse', 'HEAD'],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    monkeypatch.setattr(source_artifact, 'LIBTIFF_COMMIT', libtiff_commit)
    return repo, root, commit


@pytest.mark.parametrize(
    ('dirty_readme', 'git_available', 'expected_bound'),
    [(False, True, True), (True, True, False), (False, False, False)],
    ids=(
        'rendered-readme-keeps-captured-commit',
        'dirty-original-readme-stays-unbound',
        'unavailable-head-stays-unbound',
    ),
)
def test_stage_sdist_keeps_initial_readme_commit_eligibility(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    dirty_readme: bool,
    git_available: bool,
    expected_bound: bool,
) -> None:
    _, root, captured_head = _producer_git_fixture(tmp_path, monkeypatch)
    readme = root / 'README.md'
    if dirty_readme:
        readme.write_text('dirty original README\n', encoding='utf-8')
    original_readme = readme.read_bytes()
    monkeypatch.setattr(source_artifact, 'ROOT', root)
    monkeypatch.setattr(
        source_artifact,
        '_capture_git_head',
        lambda: captured_head if git_available else None,
    )
    monkeypatch.setattr(source_artifact, 'verify_libtiff_checkout', lambda: None)
    prepared_hashes = {'README.md': 'a' * 64}

    def prepare_source_outputs() -> dict[str, str]:
        readme.write_text('rendered PyPI README\n', encoding='utf-8')
        return prepared_hashes

    monkeypatch.setattr(
        source_artifact,
        '_prepare_source_outputs',
        prepare_source_outputs,
    )
    monkeypatch.setattr(
        source_artifact,
        '_prepared_output_hashes',
        lambda: prepared_hashes,
    )
    source_digests = {
        'source_inputs_sha256': 'b' * 64,
        'native_source_inputs_sha256': 'c' * 64,
    }
    monkeypatch.setattr(
        source_artifact,
        'source_input_digests',
        lambda: source_digests,
    )
    evidence = {
        **source_digests,
        'preimage_lock_sha256': 'd' * 64,
        'normalized_lock_sha256': 'e' * 64,
        'preimage_lock_bytes': b'preimage',
        'normalized_lock_bytes': b'normalized',
        'required_sources': [],
    }
    monkeypatch.setattr(
        source_artifact,
        '_verify_extracted_sdist',
        lambda *_args, **_kwargs: evidence,
    )
    monkeypatch.setattr(
        source_artifact,
        '_verify_locked_sdist',
        lambda *_args, **_kwargs: {
            **source_digests,
            'normalized_lock_sha256': 'e' * 64,
        },
    )
    monkeypatch.setattr(source_artifact, '_compare_restage_trees', lambda *_args: None)
    monkeypatch.setattr(
        source_artifact, 'inspect_sdist', lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(source_artifact, 'libtiff_source_members', set)
    monkeypatch.setattr(
        source_artifact,
        'validate_source_manifest',
        lambda *_args, **_kwargs: {},
    )

    def run_maturin_sdist(out_dir: Path, *, cwd: Path = root) -> None:
        package_root = 'gometry-1.0.0'
        out_dir.mkdir(parents=True, exist_ok=True)
        archive_path = out_dir / f'{package_root}.tar.gz'
        with tarfile.open(archive_path, 'w:gz') as archive:
            for relative in sorted(source_artifact.SOURCE_TOOL_MEMBERS):
                archive.add(
                    root / relative,
                    arcname=f'{package_root}/{relative}',
                    recursive=False,
                )
            archive.add(
                root / 'python' / 'gometry',
                arcname=f'{package_root}/gometry/python/gometry',
            )

    monkeypatch.setattr(source_artifact, '_run_maturin_sdist', run_maturin_sdist)
    retain = source_artifact._retained_producer_commit

    def retain_from_captured_root(
        producer_commit: str | None,
        initial_snapshot: dict[str, str],
        current_snapshot: dict[str, str],
        *,
        exclude_readme: bool = False,
    ) -> str | None:
        return retain(
            producer_commit,
            initial_snapshot,
            current_snapshot,
            root,
            exclude_readme=exclude_readme,
        )

    monkeypatch.setattr(
        source_artifact,
        '_retained_producer_commit',
        retain_from_captured_root,
    )
    published_commits: list[str | None] = []

    def source_manifest(*, commit: str | None, **_kwargs: Any) -> dict[str, Any]:
        published_commits.append(commit)
        return {'commit': commit}

    monkeypatch.setattr(source_artifact, '_source_manifest', source_manifest)

    staged = source_artifact.stage_sdist(tmp_path / 'staged')
    observed_commit = captured_head if expected_bound else None
    assert published_commits == [observed_commit]
    assert json.loads(
        (staged.parent / source_artifact.SOURCE_MANIFEST_NAME).read_text(
            encoding='utf-8'
        )
    ) == {'commit': observed_commit}
    assert readme.read_bytes() == original_readme


def test_stage_sdist_propagates_missing_source_input_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, root, _ = _producer_git_fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(source_artifact, 'ROOT', root)
    monkeypatch.setattr(source_artifact, '_capture_git_head', lambda: None)
    monkeypatch.setattr(
        source_artifact,
        'verify_libtiff_checkout',
        lambda: pytest.fail('missing source input must stop before staging'),
    )
    source_dir = root / 'src'
    moved_source_dir = root / 'src-temporarily-missing'
    source_dir.rename(moved_source_dir)
    output_dir = tmp_path / 'missing-source-stage'
    try:
        with pytest.raises(AssertionError, match='cannot read build input directory'):
            source_artifact.stage_sdist(output_dir)
        assert not output_dir.exists()
    finally:
        moved_source_dir.rename(source_dir)


def test_producer_commit_binding_uses_captured_finite_input_projection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, root, commit = _producer_git_fixture(tmp_path, monkeypatch)
    producer_paths = source_artifact._producer_input_paths(root)
    assert 'gometry/python/gometry/api.py' in producer_paths
    assert 'gometry/python/api.py' not in producer_paths
    initial = source_artifact._producer_input_snapshot(root)
    assert (
        source_artifact._retained_producer_commit(
            commit,
            initial,
            initial,
            root,
        )
        == commit
    )

    for relative in (
        'gometry/src/lib.rs',
        'gometry/native/fixture/Cargo.toml',
        'Cargo.lock',
        '.cargo/config.toml',
        'gometry/python/gometry/api.py',
        'gometry/python/gometry/_lib.pyi',
        '.github/scripts/gen_licenses.py',
        '.github/scripts/about.hbs',
        'gometry/native-licenses.toml',
        'gometry/native/notices/nlohmann-json-license.md',
        'gometry/tools/source_artifact.py',
    ):
        path = repo / relative
        original = path.read_bytes()
        path.write_bytes(original + b'\nchanged\n')
        assert not source_artifact._producer_commit_is_bound(commit, root)
        path.write_bytes(original)
        assert source_artifact._producer_commit_is_bound(commit, root)

    readme = root / 'README.md'
    original_readme = readme.read_bytes()
    readme.write_bytes(original_readme + b'rendered\n')
    assert not source_artifact._producer_commit_is_bound(commit, root)
    rendered = source_artifact._producer_input_snapshot(root)
    assert (
        source_artifact._retained_producer_commit(
            commit,
            initial,
            rendered,
            root,
            exclude_readme=True,
        )
        == commit
    )
    assert (
        source_artifact._retained_producer_commit(
            commit,
            initial,
            rendered,
            root,
        )
        is None
    )


def test_producer_commit_binding_handles_staging_untracked_and_ignored_inputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, root, commit = _producer_git_fixture(tmp_path, monkeypatch)
    changed = root / 'src' / 'lib.rs'
    original = changed.read_bytes()
    changed.write_bytes(original + b'// staged change\n')
    subprocess.run(['git', 'add', 'gometry/src/lib.rs'], cwd=repo, check=True)
    changed.write_bytes(original)
    assert not source_artifact._producer_commit_is_bound(commit, root)
    subprocess.run(
        ['git', 'reset', '--quiet', 'HEAD', '--', 'gometry/src/lib.rs'],
        cwd=repo,
        check=True,
    )
    changed.write_bytes(original)

    for relative in (
        'gometry/python/gometry/new_module.py',
        'gometry/python/gometry/ignored.py',
    ):
        path = repo / relative
        path.write_text('value = 2\n', encoding='utf-8')
        assert not source_artifact._producer_commit_is_bound(commit, root)
        path.unlink()


def test_producer_commit_binding_ignores_non_inputs_and_keeps_captured_head(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, root, captured_head = _producer_git_fixture(tmp_path, monkeypatch)
    initial = source_artifact._producer_input_snapshot(root)
    package = root / 'python' / 'gometry'
    (package / '__pycache__').mkdir()
    (package / '__pycache__' / 'cached.pyc').write_bytes(b'bytecode')
    (package / 'local.so').write_bytes(b'local extension')
    (root / 'tests').mkdir()
    (root / 'tests' / 'test_wip.py').write_text('pass\n', encoding='utf-8')
    (repo / 'worker-harness.py').write_text('pass\n', encoding='utf-8')

    assert source_artifact._producer_commit_is_bound(captured_head, root)
    current = source_artifact._producer_input_snapshot(root)
    assert current == initial
    subprocess.run(
        ['git', 'add', 'gometry/tests/test_wip.py', 'worker-harness.py'],
        cwd=repo,
        check=True,
    )
    subprocess.run(
        ['git', 'commit', '-m', 'unrelated fixture work'],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )
    assert (
        source_artifact._retained_producer_commit(
            captured_head,
            initial,
            current,
            root,
        )
        == captured_head
    )


def test_producer_commit_binding_drops_late_changes_and_unavailable_heads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, root, captured_head = _producer_git_fixture(tmp_path, monkeypatch)
    initial = source_artifact._producer_input_snapshot(root)
    (root / 'src' / 'lib.rs').write_text('pub fn late_change() {}\n', encoding='utf-8')
    current = source_artifact._producer_input_snapshot(root)
    assert (
        source_artifact._retained_producer_commit(
            captured_head,
            initial,
            current,
            root,
        )
        is None
    )
    assert (
        source_artifact._retained_producer_commit(
            None,
            initial,
            initial,
            root,
        )
        is None
    )


def test_source_identity_accepts_null_commit_but_requires_current_schema() -> None:
    identity: dict[str, Any] = {
        'commit': None,
        'filename': 'gometry-1.0.0.tar.gz',
        'sha256': 'a' * 64,
        'version': '1.0.0',
        'source_inputs_format': artifact_common.SOURCE_INPUTS_FORMAT,
        'source_inputs_sha256': 'b' * 64,
        'python_members': {'gometry/__init__.py': 'c' * 64},
        'native_source_inputs_format': artifact_common.SOURCE_INPUTS_FORMAT,
        'native_source_inputs_sha256': 'd' * 64,
        'cargo': {
            'lock_preimage_sha256': 'e' * 64,
            'lock_normalized_sha256': 'f' * 64,
            'required_sources': [{'name': 'fixture'}],
        },
        'toolchain': {
            'channel': 'nightly',
            'maturin': artifact_common.MATURIN_VERSION,
            'cargo': 'cargo 1.0',
            'rustc': 'rustc -vV',
            'maturin_pep517_args': artifact_common.RELEASE_MATURIN_ARGS,
        },
    }

    assert source_artifact.validate_source_identity(identity) is identity
    assert source_artifact.SOURCE_MANIFEST_SCHEMA == 4
    assert release_artifact.RUNNER_RECEIPT_SCHEMA == 4
    assert release_artifact.RELEASE_MANIFEST_SCHEMA == 4
    for invalid_commit in ('', False, 7, 'not-a-commit'):
        with pytest.raises(AssertionError, match='malformed source commit'):
            source_artifact.validate_source_identity({
                **identity,
                'commit': invalid_commit,
            })
    missing_commit = dict(identity)
    del missing_commit['commit']
    with pytest.raises(AssertionError, match='fields are not exact'):
        source_artifact.validate_source_identity(missing_commit)


def test_source_manifest_rejects_schema_three(tmp_path: Path) -> None:
    source_sdist = tmp_path / 'gometry-1.0.0.tar.gz'
    source_sdist.write_bytes(b'previous receipt')
    manifest_path = tmp_path / source_artifact.SOURCE_MANIFEST_NAME
    manifest_path.write_text(
        json.dumps({
            'schema': 3,
            'kind': 'gometry-source-manifest',
        }),
        encoding='utf-8',
    )

    with pytest.raises(AssertionError, match='unsupported gometry source manifest'):
        source_artifact.validate_source_manifest(source_sdist, manifest_path)


def test_record_inspection_hashes_members_and_retains_only_needed_payloads(
    tmp_path: Path,
) -> None:
    wheel = _wheel(tmp_path)

    result = _inspection(wheel)

    assert result['dist_info'] == 'fixture-1.dist-info'
    assert result['members']['gometry/_api.py'] == {
        'size': 11,
        'sha256': hashlib.sha256(b'value = 17\n').hexdigest(),
    }
    assert result['payloads']['gometry/_api.py'] == b'value = 17\n'
    assert 'fixture-1.dist-info/RECORD' not in result['payloads']


def test_absolute_build_paths_are_allowed_only_in_dev_wheels(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    python_members = {
        'gometry/_lib.pyi': b'def value() -> int: ...\n',
        'gometry/py.typed': b'',
    }
    licenses = {
        'LICENSE-APACHE.md': b'apache\n',
        'LICENSE-MIT.md': b'mit\n',
        'LICENSE-THIRD-PARTY.md': b'third-party\n',
    }
    wheel = _quality_wheel(
        tmp_path,
        version='1.0.0',
        python_members=python_members,
        licenses=licenses,
        extension_payload=(
            b'\x7fELF\x00debug\x00/home/user/checkout/gometry/src/lib.rs\x00'
        ),
    )
    monkeypatch.setattr(
        artifact_content,
        '_verify_native_debug_sections',
        lambda *_args: None,
    )
    monkeypatch.setattr(
        artifact_content,
        'native_image_facts',
        lambda *_args: {},
    )
    expected_licenses = {
        name: hashlib.sha256(payload).hexdigest() for name, payload in licenses.items()
    }

    dev_inspection = artifact_content.inspect_wheel(
        wheel,
        release=False,
        expected_prepared_inputs=expected_licenses,
    )

    assert dev_inspection['extension_members'][
        f'gometry/_lib{sysconfig.get_config_var("EXT_SUFFIX")}'
    ].endswith(b'/home/user/checkout/gometry/src/lib.rs\x00')
    with pytest.raises(
        AssertionError, match='wheel contents leak absolute build paths'
    ):
        artifact_content.inspect_wheel(
            wheel,
            release=True,
            expected_prepared_inputs=expected_licenses,
        )


def test_runtime_loader_facts_match_the_active_platform() -> None:
    facts = runtime_artifact._runtime_loader_identity()

    if sys.platform.startswith('linux'):
        assert facts['runtime_libc'] in {'gnu', 'musl'}
        assert facts['loader_paths']
        assert facts['libc_paths']
        assert all(Path(path).is_file() for path in facts['loader_paths'])
        assert all(Path(path).is_file() for path in facts['libc_paths'])
    else:
        assert facts == {
            'runtime_libc': 'not-applicable',
            'loader_paths': [],
            'libc_paths': [],
        }


def test_runtime_preimport_resolution_rejects_an_installed_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    selected = tmp_path / 'admitted' / 'gometry' / '__init__.py'
    fallback = tmp_path / 'installed' / 'gometry' / '__init__.py'
    admission = type('Admission', (), {'facade_path': selected.resolve()})()
    monkeypatch.setattr(runtime_artifact, '_resident_gometry_modules', dict)
    monkeypatch.setattr(
        runtime_artifact.importlib.util,
        'find_spec',
        lambda _name: type('Spec', (), {'origin': str(fallback)})(),
    )

    with pytest.raises(AssertionError, match='does not select the admitted'):
        runtime_artifact.check_runtime_resolution(admission)


def test_runtime_completion_rechecks_admitted_package_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    admission = runtime_artifact.inspect_runtime_inputs(GOMETRY_ROOT)
    runtime = runtime_artifact.verify_admitted_runtime(admission)
    runtime_artifact.verify_runtime_completion(admission, runtime)

    monkeypatch.setattr(runtime_artifact, 'python_member_hashes', lambda _path: {})
    with pytest.raises(AssertionError, match='Python files changed'):
        runtime_artifact.verify_runtime_completion(admission, runtime)


@pytest.mark.parametrize(
    ('member', 'message'),
    [
        ('gometry/__init__.py', 'Python package differs'),
        ('extension', 'native extension differs'),
        ('metadata-owner', 'one gometry distribution'),
    ],
    ids=('facade-only-change', 'wrong-worktree-extension', 'metadata-owner-mismatch'),
)
def test_runtime_admission_rejects_package_or_extension_from_another_tree(
    tmp_path: Path,
    member: str,
    message: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(runtime_artifact, '_resident_gometry_modules', dict)
    quality_receipt = quality_runtime.read_quality_receipt(GOMETRY_ROOT)
    original_root = Path(quality_receipt['package_root'])
    alternate_root = tmp_path / 'alternate-site-packages'

    def link_or_copy(source: str, destination: str) -> str:
        try:
            os.link(source, destination)
        except OSError:
            shutil.copy2(source, destination)
        return destination

    shutil.copytree(original_root, alternate_root, copy_function=link_or_copy)
    if member == 'metadata-owner':
        shutil.copytree(
            next(original_root.glob('gometry*.dist-info')),
            alternate_root / 'second-gometry.dist-info',
            copy_function=link_or_copy,
        )
    elif member == 'extension':
        member = quality_receipt['wheel']['extension_member']
    if member != 'metadata-owner':
        changed_file = alternate_root / member
        payload = (
            bytearray(changed_file.read_bytes()) if not member.endswith('.py') else None
        )
        changed_file.unlink()
        if member.endswith('.py'):
            changed_file.write_bytes(b'from __future__ import annotations\n')
        else:
            assert payload is not None
            payload[-1] ^= 1
            changed_file.write_bytes(payload)

    quality_receipt['package_root'] = str(alternate_root)
    wheel_path = (GOMETRY_ROOT.parent / quality_receipt['wheel']['path']).resolve()
    with pytest.raises(runtime_artifact.QualityPreparationRequired, match=message):
        runtime_artifact.inspect_runtime_inputs(
            GOMETRY_ROOT,
            expected_wheel=wheel_path,
            build_receipt=quality_receipt,
        )


def test_installed_wheel_runtime_projection_preserves_the_existing_shape() -> None:
    source = {
        'commit': None,
        'version': '1.2.3',
        'python_members': {'gometry/__init__.py': 'a' * 64},
    }
    build = {'profile': 'release', 'target': 'x86_64-unknown-linux-gnu'}
    wheel = {
        'filename': 'gometry-1.2.3-cp314-cp314-manylinux_2_34_x86_64.whl',
        'sha256': 'b' * 64,
        'extension_member': 'gometry/_lib.cpython-314-x86_64-linux-gnu.so',
        'extension_sha256': 'c' * 64,
        'unrelated': 'ignored',
    }

    projected = release_artifact.installed_wheel_runtime_receipt(
        identifier='cp314-manylinux_x86_64',
        profile='release',
        source=source,
        build=build,
        wheel=wheel,
        prepared_inputs={'README.md': 'd' * 64},
    )

    assert projected == {
        'identifier': 'cp314-manylinux_x86_64',
        'profile': 'release',
        'source': source,
        'build': build,
        'wheel': {
            'filename': wheel['filename'],
            'sha256': wheel['sha256'],
            'extension_member': wheel['extension_member'],
            'extension_sha256': wheel['extension_sha256'],
        },
        'python_members': source['python_members'],
        'prepared_inputs': {'README.md': 'd' * 64},
    }


def test_record_accepts_only_matching_unrecorded_signature_members(
    tmp_path: Path,
) -> None:
    wheel = _wheel(tmp_path, signatures=True)

    result = _inspection(wheel)

    assert result['signatures'] == ['fixture-1.dist-info/RECORD.jws']
    assert result['members']['fixture-1.dist-info/RECORD.jws']['size'] == 9


@pytest.mark.parametrize(
    ('mutation', 'message'),
    [
        ('content', 'digest mismatch'),
        ('size', 'size mismatch'),
    ],
    ids=('stale-hash', 'wrong-size'),
)
def test_record_rejects_stale_content_or_size(
    tmp_path: Path,
    mutation: str,
    message: str,
) -> None:
    baseline = _wheel(tmp_path)
    with zipfile.ZipFile(baseline) as archive:
        members = {name: archive.read(name) for name in archive.namelist()}
    if mutation == 'content':
        members['gometry/_api.py'] = b'value = 18\n'
    else:
        record_name = 'fixture-1.dist-info/RECORD'
        rows = list(csv.reader(io.StringIO(members[record_name].decode())))
        rows = [
            (row[0], row[1], '10') if row[0] == 'gometry/_api.py' else tuple(row)
            for row in rows
        ]
        members[record_name] = _record_bytes(members, rows=rows)
    wheel = tmp_path / 'mutated-record.whl'
    with zipfile.ZipFile(wheel, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        for name, payload in members.items():
            archive.writestr(name, payload)

    with pytest.raises(AssertionError, match=message):
        _inspection(wheel)


@pytest.mark.parametrize(
    ('row_change', 'message'),
    [
        ('missing', 'membership mismatch'),
        ('duplicate', 'duplicate path'),
        ('extra', 'membership mismatch'),
        ('blank', 'malformed'),
    ],
    ids=('missing-row', 'duplicate-row', 'extra-row', 'blank-hash'),
)
def test_record_rejects_membership_and_blank_hash_mutations(
    tmp_path: Path, row_change: str, message: str
) -> None:
    baseline = _wheel(tmp_path)
    with zipfile.ZipFile(baseline) as archive:
        members = {name: archive.read(name) for name in archive.namelist()}
    record_name = 'fixture-1.dist-info/RECORD'
    rows = list(csv.reader(io.StringIO(members[record_name].decode())))
    if row_change == 'missing':
        rows = [row for row in rows if row[0] != 'gometry/_api.py']
    elif row_change == 'duplicate':
        rows.append(next(row for row in rows if row[0] == 'gometry/_api.py'))
    elif row_change == 'extra':
        rows.append(('gometry/not-in-archive.py', _record_digest(b'x'), '1'))
    else:
        rows = [
            [row[0], '', row[2]] if row[0] == 'gometry/_api.py' else row for row in rows
        ]
    changed_rows = [tuple(row) for row in rows]
    members[record_name] = _record_bytes(members, rows=changed_rows)
    wheel = tmp_path / f'{row_change}.whl'
    with zipfile.ZipFile(wheel, 'w') as archive:
        for name, payload in members.items():
            archive.writestr(name, payload)

    with pytest.raises(AssertionError, match=message):
        _inspection(wheel)


def test_record_rejects_duplicate_archive_paths_and_noncanonical_names(
    tmp_path: Path,
) -> None:
    wheel = _wheel(tmp_path)
    duplicate = tmp_path / 'duplicate.whl'
    with zipfile.ZipFile(wheel) as source, zipfile.ZipFile(duplicate, 'w') as output:
        for name in source.namelist():
            output.writestr(name, source.read(name))
        with pytest.warns(UserWarning, match='Duplicate name'):
            output.writestr('gometry/_api.py', b'value = 17\n')

    with pytest.raises(AssertionError, match='duplicate members'):
        _inspection(duplicate)

    unsafe = tmp_path / 'unsafe.whl'
    with zipfile.ZipFile(unsafe, 'w') as archive:
        archive.writestr('../outside.py', b'x')

    with pytest.raises(AssertionError, match='non-canonical archive path'):
        _inspection(unsafe)

    windows_absolute = tmp_path / 'windows-absolute.whl'
    with zipfile.ZipFile(windows_absolute, 'w') as archive:
        archive.writestr('C:/outside.py', b'x')

    with pytest.raises(AssertionError, match='non-canonical archive path'):
        _inspection(windows_absolute)


def test_record_rejects_sha1_even_when_the_row_digest_matches(tmp_path: Path) -> None:
    baseline = _wheel(tmp_path)
    with zipfile.ZipFile(baseline) as source:
        members = {name: source.read(name) for name in source.namelist()}
    record_name = 'fixture-1.dist-info/RECORD'
    # SHA-1 is deliberately used to verify that wheel admission rejects it.
    weak_digest = (
        base64
        .urlsafe_b64encode(
            hashlib.sha1(members['gometry/_api.py']).digest()  # noqa: S324
        )
        .rstrip(b'=')
        .decode('ascii')
    )
    rows = list(csv.reader(io.StringIO(members[record_name].decode('utf-8'))))
    rows = [
        (row[0], f'sha1={weak_digest}', row[2])
        if row[0] == 'gometry/_api.py'
        else tuple(row)
        for row in rows
    ]
    members[record_name] = _record_bytes(members, rows=rows)
    weak_record = tmp_path / 'weak-record.whl'
    with zipfile.ZipFile(weak_record, 'w') as archive:
        for name, payload in members.items():
            archive.writestr(name, payload)

    with pytest.raises(AssertionError, match='weak or unsupported hash'):
        _inspection(weak_record)


def test_record_rejects_an_archive_member_without_a_record_row(tmp_path: Path) -> None:
    baseline = _wheel(tmp_path)
    with zipfile.ZipFile(baseline) as source:
        members = {name: source.read(name) for name in source.namelist()}
    members['gometry/injected.py'] = b'injected = True\n'
    changed = tmp_path / 'unrecorded-member.whl'
    with zipfile.ZipFile(changed, 'w') as archive:
        for name, payload in members.items():
            archive.writestr(name, payload)

    with pytest.raises(AssertionError, match='membership mismatch'):
        _inspection(changed)


def test_python_member_identity_catches_record_correct_adapter_and_extra_module(
    tmp_path: Path,
) -> None:
    package = tmp_path / 'python' / 'gometry'
    package.mkdir(parents=True)
    (package / '__init__.py').write_bytes(b'value = 17\n')
    (package / '_api.py').write_bytes(b'value = 17\n')
    (package / '_geoparquet.py').write_bytes(b'CURRENT = True\n')
    (package / 'py.typed').write_bytes(b'')
    expected = artifact_content.python_member_hashes(package)

    wheel = _wheel(
        tmp_path,
        content={'gometry/_geoparquet.py': b'OLD = True\n'},
        name='record-correct-stale-adapter.whl',
    )
    actual = artifact_content.wheel_python_members(_inspection(wheel)['members'])
    with pytest.raises(AssertionError, match='changed'):
        artifact_content._verify_python_member_identity(
            actual,
            expected,
            label=wheel.name,
        )

    wheel = _wheel(
        tmp_path,
        content={'gometry/extra.py': b'EXTRA = True\n'},
        name='record-correct-extra-module.whl',
    )
    actual = artifact_content.wheel_python_members(_inspection(wheel)['members'])
    with pytest.raises(AssertionError, match='extra'):
        artifact_content._verify_python_member_identity(
            actual,
            expected,
            label=wheel.name,
        )


def test_python_member_identity_requires_py_typed(tmp_path: Path) -> None:
    package = tmp_path / 'python' / 'gometry'
    package.mkdir(parents=True)
    (package / '__init__.py').write_bytes(b'value = 17\n')
    (package / '_api.py').write_bytes(b'value = 17\n')
    (package / 'py.typed').write_bytes(b'')
    expected = artifact_content.python_member_hashes(package)

    wheel = _wheel(tmp_path)
    with zipfile.ZipFile(wheel) as source:
        members = {
            name: source.read(name)
            for name in source.namelist()
            if name != 'gometry/py.typed'
        }
    record_name = 'fixture-1.dist-info/RECORD'
    members[record_name] = b''
    members[record_name] = _record_bytes(members)
    without_marker = tmp_path / 'without-py-typed.whl'
    with zipfile.ZipFile(without_marker, 'w') as archive:
        for name, payload in members.items():
            archive.writestr(name, payload)
    actual = artifact_content.wheel_python_members(
        _inspection(without_marker)['members']
    )

    with pytest.raises(AssertionError, match=r'gometry/py\.typed'):
        artifact_content._verify_python_member_identity(
            actual,
            expected,
            label=without_marker.name,
        )


def test_cibw_policy_keeps_raw_aarch64_and_separates_gnu_from_musl() -> None:
    gnu = artifact_content.build_identifier_policy('cp314-manylinux_aarch64')
    musl = artifact_content.build_identifier_policy('cp314-musllinux_aarch64')

    assert gnu['raw_target_arch'] == 'aarch64'
    assert gnu['architecture'] == 'arm64'
    assert gnu['target'] == 'aarch64-unknown-linux-gnu'
    assert musl['target'] == 'aarch64-unknown-linux-musl'
    assert musl['platform_family'] == 'linux-musl'
    release_artifact._validate_runner_target('ubuntu-24.04-arm', musl)

    with pytest.raises(AssertionError, match='raw target architecture'):
        release_artifact._validate_runner_target(
            'ubuntu-24.04-arm',
            {**gnu, 'target': 'x86_64-unknown-linux-musl'},
        )
    with pytest.raises(AssertionError, match='unsupported gometry wheel Python tag'):
        artifact_content.build_identifier_policy('cp315-manylinux_x86_64')
    with pytest.raises(AssertionError, match='free-threaded Python tag is unsupported'):
        artifact_content.build_identifier_policy('cp313t-manylinux_x86_64')
    with pytest.raises(AssertionError, match='unsupported gometry wheel platform'):
        artifact_content.build_identifier_policy('cp314-manylinux2014_x86_64')


def test_native_libc_versions_must_fit_the_advertised_manylinux_floor() -> None:
    policy = artifact_content.build_identifier_policy('cp314-manylinux_x86_64')
    native = {
        'extension': {
            'format': 'ELF',
            'architecture': 'x86_64',
            'libc': 'gnu',
            'needed': ['libc.so.6'],
            'glibc_versions': ['GLIBC_2.17'],
        },
        'libraries': {},
    }
    artifact_content.validate_wheel_native_target(
        {'native': native},
        policy,
        label='manylinux wheel',
        tags=['cp314-cp314-manylinux_2_17_x86_64'],
    )
    artifact_content.validate_wheel_native_target(
        {'native': native},
        policy,
        label='legacy manylinux wheel',
        tags=['cp314-cp314-manylinux2014_x86_64'],
    )

    native['extension']['glibc_versions'] = ['GLIBC_2.28']
    with pytest.raises(AssertionError, match='understates its GLIBC requirement'):
        artifact_content.validate_wheel_native_target(
            {'native': native},
            policy,
            label='manylinux wheel',
            tags=['cp314-cp314-manylinux_2_17_x86_64'],
        )


def _gometry_elf_payload() -> tuple[Path, bytes]:
    if not sys.platform.startswith('linux'):
        pytest.skip('ELF dynamic-version inspection is Linux-specific')
    import gometry._lib as native_extension

    path = Path(native_extension.__file__).resolve()
    payload = path.read_bytes()
    if not payload.startswith(b'\x7fELF'):
        pytest.skip('the loaded Gometry extension is not an ELF image')
    return path, payload


def _corrupt_elf_version_need_pointer(payload: bytes) -> bytes:
    endian = {'\x01': '<', '\x02': '>'}.get(chr(payload[5]))
    if payload[4] != 2 or endian is None:
        raise AssertionError('test ELF fixture must use a supported 64-bit encoding')
    phoff = struct.unpack_from(f'{endian}Q', payload, 32)[0]
    phentsize = struct.unpack_from(f'{endian}H', payload, 54)[0]
    phnum = struct.unpack_from(f'{endian}H', payload, 56)[0]
    dynamic_ranges: list[tuple[int, int]] = []
    for index in range(phnum):
        header = struct.unpack_from(
            f'{endian}IIQQQQQQ',
            payload,
            phoff + index * phentsize,
        )
        if header[0] == 2:
            dynamic_ranges.append((header[2], header[5]))
    if len(dynamic_ranges) != 1:
        raise AssertionError('test ELF fixture must have one dynamic table')
    dynamic_offset, dynamic_size = dynamic_ranges[0]
    dynamic_end = dynamic_offset + dynamic_size
    dynamic_entry_size = struct.calcsize(f'{endian}qQ')
    for cursor in range(dynamic_offset, dynamic_end, dynamic_entry_size):
        tag, _value = struct.unpack_from(f'{endian}qQ', payload, cursor)
        if tag == 0:
            break
        if tag == 0x6FFFFFFE:
            corrupted = bytearray(payload)
            struct.pack_into(f'{endian}Q', corrupted, cursor + 8, 0xFFFFFFFFFFFFF000)
            return bytes(corrupted)
    raise AssertionError('test GNU ELF fixture has no DT_VERNEED entry')


def test_elf_glibc_versions_ignore_inert_strings_outside_version_need_table() -> None:
    path, payload = _gometry_elf_payload()
    actual = artifact_content.native_image_facts(payload, path)

    if actual.get('libc') != 'gnu':
        pytest.skip('GNU GLIBC version-floor inspection applies to GNU ELF images')
    assert actual['glibc_versions']
    with_inert_string = artifact_content.native_image_facts(
        payload + b'\0GLIBC_99.0\0',
        path,
    )

    assert with_inert_string == actual


def test_elf_rejects_an_out_of_bounds_dynamic_version_need_table() -> None:
    path, payload = _gometry_elf_payload()
    actual = artifact_content.native_image_facts(payload, path)
    if actual.get('libc') != 'gnu':
        pytest.skip('GNU GLIBC version-floor inspection applies to GNU ELF images')
    assert actual['glibc_versions']
    corrupted = _corrupt_elf_version_need_pointer(payload)

    with pytest.raises(AssertionError, match='version-need record'):
        artifact_content.native_image_facts(corrupted, path)


def _valid_build_identity() -> dict[str, Any]:
    target = 'x86_64-unknown-linux-gnu'
    policy = artifact_common.release_target_policy(target)
    rustflags = artifact_common._flag_text(policy['rustflags'])
    cflags = artifact_common._flag_text(policy['cflags'])
    cxxflags = artifact_common._flag_text(policy['cxxflags'])
    digest = 'a' * 64
    native = {
        'extension': {
            'format': 'ELF',
            'architecture': 'x86_64',
            'libc': 'gnu',
            'needed': ['libc.so.6'],
            'glibc_versions': ['GLIBC_2.17'],
        },
        'libraries': {},
    }
    environment = {
        'GOMETRY_RELEASE_TARGET': target,
        'GOMETRY_RELEASE_TARGET_POLICY': policy['target_policy'],
        'GOMETRY_RELEASE_RUSTFLAGS': rustflags,
        'GOMETRY_RELEASE_CFLAGS': cflags,
        'GOMETRY_RELEASE_CXXFLAGS': cxxflags,
        'CARGO_BUILD_TARGET': target,
        'RUSTFLAGS': rustflags,
        'CARGO_ENCODED_RUSTFLAGS': '\x1f'.join(policy['rustflags']),
        f'CFLAGS_{target.replace("-", "_")}': cflags,
        f'CXXFLAGS_{target.replace("-", "_")}': cxxflags,
        'MATURIN_PEP517_ARGS': artifact_common.RELEASE_MATURIN_ARGS,
    }
    return {
        **policy,
        'raw_target_arch': 'x86_64',
        'command': {
            'dispatched': {
                'cibuildwheel_argv': [
                    'uv',
                    'tool',
                    'run',
                    '--from',
                    'cibuildwheel==4.1.0',
                    'cibuildwheel',
                ],
                'frontend': 'build[uv]',
                'config_settings': {
                    'MATURIN_PEP517_ARGS': artifact_common.RELEASE_MATURIN_ARGS,
                },
            },
            'config_sha256': digest,
            'host_log_sha256': 'b' * 64,
            'host_exit_code': 0,
            'capture_exit_code': 0,
        },
        'profile': artifact_common.RELEASE_PROFILE,
        'interpreter': {
            'executable': 'python/cp314',
            'prefix': 'python-prefix/cp314',
            'version': '3.14.7',
            'implementation': 'CPython',
            'cache_tag': 'cpython-314',
            'ext_suffix': '.cpython-314-x86_64-linux-gnu.so',
            'gil_disabled': False,
        },
        'compiler': {
            'observed_at': 'before-build',
            'rustc_vv': 'rustc 1.90.0-nightly\ncommit-hash: abcdef\n',
        },
        'cargo': 'cargo 1.90.0-nightly',
        'maturin': {
            'unrepaired_wheel_sha256': digest,
            'wheel_member': 'gometry-1.dist-info/WHEEL',
            'wheel_member_sha256': digest,
            'generator': f'maturin ({artifact_common.MATURIN_VERSION})',
            'repaired_wheel_member_sha256': digest,
            'repaired_generator': f'maturin ({artifact_common.MATURIN_VERSION})',
        },
        'dependencies': {'lock_normalized_sha256': digest},
        'settings': {
            'config_sha256': digest,
            'source_manifest_sha256': digest,
            'source_sdist_sha256': digest,
            'environment': environment,
            'test_sources': dict.fromkeys(
                release_artifact.CIBW_TEST_SOURCE_FILES, digest
            ),
            'test_source_root': 'test-sources',
            'native': native,
            'unrepaired_native': native,
            'wheel_tags': [
                'cp314-cp314-manylinux_2_17_x86_64',
            ],
            'build_inputs': dict.fromkeys(artifact_common.RELEASE_BUILD_INPUTS, digest),
        },
    }


def test_smoke_identity_binds_runtime_loader_to_target_libc() -> None:
    build = _valid_build_identity()
    digest = 'a' * 64
    extension_member = 'gometry/_lib.cpython-314-x86_64-linux-gnu.so'
    smoke = {
        'status': 'passed',
        'exit_code': 0,
        'interpreter': {
            'pid': 123,
            'executable': 'python/cpython-314',
            'prefix': 'python-prefix/cpython-314',
            'version': '3.14.7',
            'implementation': 'CPython',
            'cache_tag': 'cpython-314',
            'ext_suffix': '.cpython-314-x86_64-linux-gnu.so',
            'gil_disabled': False,
        },
        'module': {
            'facade': {'member': 'gometry/__init__.py', 'sha256': digest},
            'extension': {'member': extension_member, 'sha256': digest},
        },
        'source_inputs_sha256': digest,
        'native_source_inputs_sha256': digest,
        'distribution': {
            'name': 'gometry',
            'version': '1.0.0',
            'metadata_sha256': digest,
        },
        'platform': {
            'machine': 'x86_64',
            'architecture': 'x86_64',
            'target': build['target'],
            'target_arch': build['raw_target_arch'],
            'platform': 'linux-x86_64',
            'libc': ['glibc', '2.17'],
            'loader': {
                'runtime_libc': 'gnu',
                'loader_paths': ['loader/ld-linux-x86-64.so.2'],
                'libc_paths': ['libc/libc.so.6'],
            },
            'gil_disabled': False,
            'wheel_tags': build['settings']['wheel_tags'],
            'native_image': build['settings']['native'],
        },
        'log_sha256': digest,
    }
    source = {
        'version': '1.0.0',
        'source_inputs_sha256': digest,
        'native_source_inputs_sha256': digest,
    }

    release_artifact._validate_smoke_identity(
        smoke,
        field='smoke',
        source=source,
        build=build,
        extension_member=extension_member,
        extension_sha256=digest,
    )

    smoke['platform']['loader']['runtime_libc'] = 'musl'
    with pytest.raises(AssertionError, match='loader/libc differs'):
        release_artifact._validate_smoke_identity(
            smoke,
            field='smoke',
            source=source,
            build=build,
            extension_member=extension_member,
            extension_sha256=digest,
        )


def test_runner_build_identity_rejects_false_profile_and_partial_guards() -> None:
    build = _valid_build_identity()
    assert release_artifact._validate_build_identity(build)['profile'] == (
        artifact_common.RELEASE_PROFILE
    )

    wrong_profile = json.loads(json.dumps(build))
    wrong_profile['profile'] = 'release'
    with pytest.raises(AssertionError, match='profile'):
        release_artifact._validate_build_identity(wrong_profile)

    missing_guard = json.loads(json.dumps(build))
    del missing_guard['settings']['environment']['GOMETRY_RELEASE_CXXFLAGS']
    with pytest.raises(TypeError, match='GOMETRY_RELEASE_CXXFLAGS'):
        release_artifact._validate_build_identity(missing_guard)


def _embedded_python_identity() -> dict[str, Any]:
    gil_state = getattr(sys, '_is_gil_enabled', None)
    return {
        'version': sys.version,
        'implementation': platform.python_implementation(),
        'cache_tag': sys.implementation.cache_tag,
        'ext_suffix': sysconfig.get_config_var('EXT_SUFFIX'),
        'gil_disabled': bool(sysconfig.get_config_var('Py_GIL_DISABLED')),
        'gil_enabled': gil_state() if gil_state is not None else None,
    }


def _native_receipt_observation(
    executable: Path,
    source_root: Path,
    *,
    invocation_run_id: str,
    nextest_run_id: str,
    embedded_python: dict[str, Any],
) -> dict[str, Any]:
    executable_bytes = executable.read_bytes()
    digests = artifact_content.source_input_digests(source_root)
    return {
        'schema': 2,
        'kind': 'gometry-native-test-binary',
        'invocation_run_id': invocation_run_id,
        'nextest_run_id': nextest_run_id,
        'test_name': 'records_the_running_nextest_binary_when_requested',
        'selected_python': sys.executable,
        'embedded_python': embedded_python,
        'pid': os.getpid(),
        'executable': str(executable),
        'size_bytes': len(executable_bytes),
        'sha256': hashlib.sha256(executable_bytes).hexdigest(),
        'source_inputs_format': artifact_common.SOURCE_INPUTS_FORMAT,
        'source_inputs_sha256': digests['source_inputs_sha256'],
        'native_source_inputs_format': artifact_common.SOURCE_INPUTS_FORMAT,
        'native_source_inputs_sha256': digests['native_source_inputs_sha256'],
        'profile_family': 'debug',
        'opt_level': '0',
        'debug_assertions': True,
        'target': 'x86_64-unknown-linux-gnu',
        'target_arch': 'x86_64',
        'target_features': 'sse2',
    }


def test_native_receipt_binds_running_binary_and_python_to_nextest_junit(
    tmp_path: Path,
) -> None:
    executable = tmp_path / 'gometry-nextest'
    executable.write_bytes(b'\x7fELF' + b'running test executable')
    source_root = _source_root(tmp_path)
    digests = artifact_content.source_input_digests(source_root)
    invocation_run_id = '00000000-0000-4000-8000-000000000001'
    nextest_run_id = '00000000-0000-4000-8000-000000000002'
    embedded_python = _embedded_python_identity()
    observation = _native_receipt_observation(
        executable,
        source_root,
        invocation_run_id=invocation_run_id,
        nextest_run_id=nextest_run_id,
        embedded_python=embedded_python,
    )
    binary_json = tmp_path / 'binary.json'
    binary_json.write_text(json.dumps(observation), encoding='utf-8')
    junit = tmp_path / 'junit.xml'
    junit.write_text(
        f'<testsuites name="nextest-run" tests="1" failures="0" errors="0" '
        f'uuid="{nextest_run_id}"><testsuite name="gometry" tests="1" '
        'disabled="0" errors="0" failures="0"><testcase classname="gometry" '
        'name="native_test_identity::records_the_running_nextest_binary_when_requested"/>'
        '</testsuite></testsuites>',
        encoding='utf-8',
    )

    receipt_path = runtime_artifact.write_native_test_receipt(
        binary_json,
        junit,
        tmp_path / 'receipt.json',
        python=Path(sys.executable),
        runner='ubuntu-latest',
        profile='debug',
        run_id=invocation_run_id,
        root=source_root,
    )
    receipt = json.loads(receipt_path.read_text(encoding='utf-8'))
    assert receipt['binary']['sha256'] == observation['sha256']
    assert receipt['source_inputs_sha256'] == digests['source_inputs_sha256']
    assert receipt['run_id'] == invocation_run_id
    assert receipt['nextest_run_id'] == nextest_run_id
    assert receipt['python']['embedded'] == embedded_python
    assert receipt['junit']['test_cases'][0]['name'] == (
        f'native_test_identity::{observation["test_name"]}'
    )


def test_native_receipt_rejects_a_junit_without_the_executed_elf_witness(
    tmp_path: Path,
) -> None:
    executable = tmp_path / 'gometry-nextest'
    executable.write_bytes(b'\x7fELF' + b'running test executable')
    source_root = _source_root(tmp_path)
    invocation_run_id = '00000000-0000-4000-8000-000000000003'
    nextest_run_id = '00000000-0000-4000-8000-000000000004'
    observation = _native_receipt_observation(
        executable,
        source_root,
        invocation_run_id=invocation_run_id,
        nextest_run_id=nextest_run_id,
        embedded_python=_embedded_python_identity(),
    )
    binary_json = tmp_path / 'binary.json'
    binary_json.write_text(json.dumps(observation), encoding='utf-8')
    junit = tmp_path / 'junit.xml'
    junit.write_text(
        f'<testsuites name="nextest-run" tests="1" failures="0" errors="0" '
        f'uuid="{nextest_run_id}"><testsuite name="gometry" tests="1" '
        'disabled="0" errors="0" failures="0">'
        '<testcase classname="gometry" name="unrelated"/>'
        '</testsuite></testsuites>',
        encoding='utf-8',
    )

    with pytest.raises(AssertionError, match='running-ELF witness'):
        runtime_artifact.write_native_test_receipt(
            binary_json,
            junit,
            tmp_path / 'receipt.json',
            python=Path(sys.executable),
            runner='ubuntu-latest',
            profile='debug',
            run_id=invocation_run_id,
            root=source_root,
        )


def test_native_receipt_rejects_a_different_nextest_junit_run_uuid(
    tmp_path: Path,
) -> None:
    executable = tmp_path / 'gometry-nextest'
    executable.write_bytes(b'\x7fELF' + b'running test executable')
    source_root = _source_root(tmp_path)
    invocation_run_id = '00000000-0000-4000-8000-000000000005'
    nextest_run_id = '00000000-0000-4000-8000-000000000006'
    observation = _native_receipt_observation(
        executable,
        source_root,
        invocation_run_id=invocation_run_id,
        nextest_run_id=nextest_run_id,
        embedded_python=_embedded_python_identity(),
    )
    binary_json = tmp_path / 'binary.json'
    binary_json.write_text(json.dumps(observation), encoding='utf-8')
    junit = tmp_path / 'junit.xml'
    junit.write_text(
        '<testsuites name="nextest-run" tests="1" failures="0" errors="0" '
        'uuid="00000000-0000-4000-8000-000000000007">'
        '<testsuite name="gometry" tests="1" disabled="0" errors="0" failures="0">'
        '<testcase classname="gometry" '
        'name="native_test_identity::records_the_running_nextest_binary_when_requested"/>'
        '</testsuite></testsuites>',
        encoding='utf-8',
    )

    with pytest.raises(AssertionError, match='run UUID differs'):
        runtime_artifact.write_native_test_receipt(
            binary_json,
            junit,
            tmp_path / 'receipt.json',
            python=Path(sys.executable),
            runner='ubuntu-latest',
            profile='debug',
            run_id=invocation_run_id,
            root=source_root,
        )


def test_native_receipt_rejects_embedded_python_abi_mismatch(
    tmp_path: Path,
) -> None:
    executable = tmp_path / 'gometry-nextest'
    executable.write_bytes(b'\x7fELF' + b'running test executable')
    source_root = _source_root(tmp_path)
    invocation_run_id = '00000000-0000-4000-8000-000000000008'
    nextest_run_id = '00000000-0000-4000-8000-000000000009'
    embedded_python = _embedded_python_identity()
    embedded_python['cache_tag'] = 'incompatible-cache-tag'
    observation = _native_receipt_observation(
        executable,
        source_root,
        invocation_run_id=invocation_run_id,
        nextest_run_id=nextest_run_id,
        embedded_python=embedded_python,
    )
    binary_json = tmp_path / 'binary.json'
    binary_json.write_text(json.dumps(observation), encoding='utf-8')
    junit = tmp_path / 'junit.xml'
    junit.write_text(
        f'<testsuites name="nextest-run" tests="1" failures="0" errors="0" '
        f'uuid="{nextest_run_id}"><testsuite name="gometry" tests="1" '
        'disabled="0" errors="0" failures="0"><testcase classname="gometry" '
        'name="native_test_identity::records_the_running_nextest_binary_when_requested"/>'
        '</testsuite></testsuites>',
        encoding='utf-8',
    )

    with pytest.raises(AssertionError, match='embedded Python ABI differs'):
        runtime_artifact.write_native_test_receipt(
            binary_json,
            junit,
            tmp_path / 'receipt.json',
            python=Path(sys.executable),
            runner='ubuntu-latest',
            profile='debug',
            run_id=invocation_run_id,
            root=source_root,
        )


def test_null_commit_still_binds_actual_python_and_wheel_bytes(
    tmp_path: Path,
) -> None:
    receipt_path = quality_runtime.quality_receipt_path(GOMETRY_ROOT)
    subject = benchmark_runtime.load_subject(receipt_path)
    receipt = subject['runtime_receipt']
    root = receipt_path.parents[2]
    wheel_path = (root.parent / receipt['wheel']['path']).resolve()
    admission = runtime_artifact.inspect_runtime_inputs(
        root,
        expected_wheel=wheel_path,
        build_receipt=receipt,
    )
    runtime = runtime_artifact.verify_admitted_runtime(admission)
    session = benchmark_runtime.BenchmarkRuntime(
        subject=subject,
        admission=admission,
        runtime=runtime,
        receipt_path=receipt_path.resolve(),
        receipt_sha256=subject['receipt_sha256'],
        wheel_path=wheel_path,
        wheel_item=subject['wheel'],
        package_root=Path(subject['package_root']).resolve(),
    )
    identity = session.collect_identity()
    binding = identity['runtime_binding']
    package = identity['packages']['gometry']

    matched = release_artifact.match_benchmark_receipt(
        subject,
        identity['extension'],
        package['distribution'],
        runtime_binding=binding,
    )

    assert matched['source_commit'] is None
    assert identity['source_commit'] is None
    assert identity['source_receipt'] is None
    assert identity['source_wheel'] is None

    wrong_facade = json.loads(json.dumps(binding))
    wrong_facade['facade']['sha256'] = '0' * 64
    with pytest.raises(AssertionError, match='facade differs'):
        release_artifact.match_benchmark_receipt(
            subject,
            identity['extension'],
            package['distribution'],
            runtime_binding=wrong_facade,
        )

    wrong_extension = json.loads(json.dumps(binding))
    wrong_extension['extension']['sha256'] = '0' * 64
    with pytest.raises(AssertionError, match='runtime files differ'):
        release_artifact.match_benchmark_receipt(
            subject,
            identity['extension'],
            package['distribution'],
            runtime_binding=wrong_extension,
        )

    wrong_member = json.loads(json.dumps(subject))
    wrong_member['wheels'][0]['extension_member'] = 'gometry/_other.so'
    with pytest.raises(AssertionError, match='uniquely bind'):
        release_artifact.match_benchmark_receipt(
            wrong_member,
            identity['extension'],
            package['distribution'],
            runtime_binding=binding,
        )

    wrong_archive = json.loads(json.dumps(binding))
    wrong_archive['wheel']['archive']['sha256'] = '0' * 64
    with pytest.raises(AssertionError, match='uniquely bind'):
        release_artifact.match_benchmark_receipt(
            subject,
            identity['extension'],
            package['distribution'],
            runtime_binding=wrong_archive,
        )

    altered_root = tmp_path / 'gometry'
    altered_receipt_path = altered_root / 'target' / 'quality-runtime' / 'receipt.json'
    altered_receipt_path.parent.mkdir(parents=True)
    altered_wheel_path = tmp_path / binding['wheel']['filename']
    with zipfile.ZipFile(wheel_path) as original:
        members = [
            (member.filename, original.read(member.filename))
            for member in reversed(original.infolist())
        ]
    with zipfile.ZipFile(
        altered_wheel_path, 'w', compression=zipfile.ZIP_STORED
    ) as changed:
        for name, payload in members:
            changed.writestr(name, payload)
    altered_receipt = json.loads(json.dumps(receipt))
    altered_receipt['wheel']['path'] = altered_wheel_path.name
    altered_receipt_path.write_text(
        json.dumps(altered_receipt),
        encoding='utf-8',
    )
    altered_subject = release_artifact._quality_benchmark_subject(
        altered_root,
        altered_receipt,
        hashlib.sha256(altered_receipt_path.read_bytes()).hexdigest(),
    )
    altered_binding = json.loads(json.dumps(binding))
    altered_binding['path'] = str(altered_receipt_path)
    altered_binding['sha256'] = altered_subject['receipt_sha256']
    altered_binding['wheel']['path'] = str(altered_wheel_path)
    with pytest.raises(AssertionError, match='archive bytes differ'):
        release_artifact.match_benchmark_receipt(
            altered_subject,
            identity['extension'],
            package['distribution'],
            runtime_binding=altered_binding,
        )


def test_direct_runtime_preflight_records_the_selected_interpreter(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    gate = runtime_artifact
    runtime = (object(), object(), {'interpreter': {'pid': 123, 'prefix': '/venv'}})
    admission = type('Admission', (), {'root': tmp_path.resolve()})()
    observed: list[tuple[Path, dict[str, Any] | None]] = []
    monkeypatch.setattr(gate, '_ACTIVE_ADMISSION', None)
    monkeypatch.setattr(gate, '_ACTIVE_RUNTIME', None)

    def inspect(
        root: Path,
        *,
        expected_wheel: Path | None = None,
        build_receipt: Any = None,
        runtime_selection: dict[str, Any] | None = None,
    ) -> Any:
        observed.append((root, runtime_selection or {}))
        return admission

    monkeypatch.setattr(gate, 'inspect_runtime_inputs', inspect)
    monkeypatch.setattr(gate, 'verify_admitted_runtime', lambda _admission: runtime)
    monkeypatch.setattr(
        gate.subprocess,
        'run',
        lambda *_args, **_kwargs: pytest.fail('valid runtime must not be rebuilt'),
    )

    result = gate.require_runtime(tmp_path, prepare_if_stale=False)

    assert result == runtime
    assert observed[0][0] == tmp_path.resolve()
    assert '"pid":123' in capsys.readouterr().err


def test_preimport_stale_runtime_prepares_once_then_readmits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = runtime_artifact
    runtime = (object(), object(), {'interpreter': {'pid': 456}})
    admission = type('Admission', (), {'root': tmp_path.resolve()})()
    attempts: list[Path] = []
    commands: list[tuple[list[str], Path]] = []

    def inspect(root: Path, **_kwargs: Any) -> Any:
        attempts.append(root)
        if len(attempts) == 1:
            raise gate.QualityPreparationRequired('stale native inputs')
        return admission

    def prepare(command: list[str], *, cwd: Path, check: bool) -> Any:
        commands.append((command, cwd))
        assert check is False
        return type('Result', (), {'returncode': 0})()

    monkeypatch.setattr(gate, '_ACTIVE_ADMISSION', None)
    monkeypatch.setattr(gate, '_ACTIVE_RUNTIME', None)
    monkeypatch.setattr(gate, '_resident_gometry_modules', dict)
    monkeypatch.setattr(gate, 'inspect_runtime_inputs', inspect)
    monkeypatch.setattr(gate, 'verify_admitted_runtime', lambda _admission: runtime)
    monkeypatch.setattr(gate.subprocess, 'run', prepare)

    assert gate.require_runtime(tmp_path) == runtime
    assert attempts == [tmp_path.resolve(), tmp_path.resolve()]
    assert commands == [
        (
            [sys.executable, str(tmp_path.resolve() / 'tools' / 'check.py')],
            tmp_path.resolve(),
        )
    ]


def test_readonly_stale_runtime_does_not_prepare(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = runtime_artifact
    monkeypatch.setattr(gate, '_ACTIVE_ADMISSION', None)
    monkeypatch.setattr(gate, '_ACTIVE_RUNTIME', None)
    monkeypatch.setattr(gate, '_resident_gometry_modules', dict)

    def stale(_root: Path) -> Any:
        raise gate.QualityPreparationRequired('missing local receipt')

    monkeypatch.setattr(gate, 'inspect_runtime_inputs', stale)
    monkeypatch.setattr(
        gate.subprocess,
        'run',
        lambda *_args, **_kwargs: pytest.fail(
            'readonly admission must not prepare a runtime'
        ),
    )

    with pytest.raises(SystemExit, match=r'run .*tools/check\.py'):
        gate.require_runtime(tmp_path, prepare_if_stale=False)


def test_runtime_that_remains_stale_after_preparation_is_not_retried(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = runtime_artifact
    attempts: list[Path] = []
    commands: list[list[str]] = []
    monkeypatch.setattr(gate, '_ACTIVE_ADMISSION', None)
    monkeypatch.setattr(gate, '_ACTIVE_RUNTIME', None)
    monkeypatch.setattr(gate, '_resident_gometry_modules', dict)

    def stale(root: Path) -> Any:
        attempts.append(root)
        raise gate.QualityPreparationRequired('still missing local receipt')

    def prepare(command: list[str], *, cwd: Path, check: bool) -> Any:
        commands.append(command)
        assert cwd == tmp_path.resolve()
        assert check is False
        return type('Result', (), {'returncode': 0})()

    monkeypatch.setattr(gate, 'inspect_runtime_inputs', stale)
    monkeypatch.setattr(gate.subprocess, 'run', prepare)

    with pytest.raises(SystemExit, match='remains stale after preparation'):
        gate.require_runtime(tmp_path)

    assert attempts == [tmp_path.resolve(), tmp_path.resolve()]
    assert commands == [
        [sys.executable, str(tmp_path.resolve() / 'tools' / 'check.py')]
    ]


def test_stale_runtime_with_resident_modules_fails_without_preparation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = runtime_artifact
    resident = {'gometry': object()}
    monkeypatch.setattr(gate, '_ACTIVE_ADMISSION', None)
    monkeypatch.setattr(gate, '_ACTIVE_RUNTIME', None)

    def stale(_root: Path) -> Any:
        raise gate.QualityPreparationRequired('stale source inputs')

    monkeypatch.setattr(gate, 'inspect_runtime_inputs', stale)
    monkeypatch.setattr(gate, '_resident_gometry_modules', lambda: resident)
    monkeypatch.setattr(
        gate.subprocess,
        'run',
        lambda *_args, **_kwargs: pytest.fail(
            'a resident runtime must not trigger preparation'
        ),
    )

    with pytest.raises(SystemExit, match='already imported'):
        gate.require_runtime(tmp_path)


def test_runtime_import_failure_is_terminal_without_preparation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = runtime_artifact
    admission = type('Admission', (), {'root': tmp_path.resolve()})()
    monkeypatch.setattr(gate, '_ACTIVE_ADMISSION', None)
    monkeypatch.setattr(gate, '_ACTIVE_RUNTIME', None)
    monkeypatch.setattr(gate, '_resident_gometry_modules', dict)
    monkeypatch.setattr(gate, 'inspect_runtime_inputs', lambda _root: admission)

    def fail_import(_admission: Any) -> Any:
        raise ImportError('native import failed')

    monkeypatch.setattr(
        gate,
        'verify_admitted_runtime',
        fail_import,
    )
    monkeypatch.setattr(
        gate.subprocess,
        'run',
        lambda *_args, **_kwargs: pytest.fail(
            'an import failure must not trigger preparation'
        ),
    )

    with pytest.raises(ImportError, match='native import failed'):
        gate.require_runtime(tmp_path)


def test_corrupt_quality_wheel_is_not_rebuilt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / 'gometry'
    root.mkdir()
    lock_path = tmp_path / 'Cargo.lock'
    lock_path.write_text('version = 4\n', encoding='utf-8')
    archive = tmp_path / 'quality.whl'
    archive.write_bytes(b'corrupted cached wheel')
    receipt_path = tmp_path / 'receipt.json'
    receipt_path.write_text('{}', encoding='utf-8')
    runtime = quality_runtime
    for name in artifact_common.RELEASE_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.delenv('MATURIN_PEP517_ARGS', raising=False)
    interpreter = {
        'executable': sys.executable,
        'cache_tag': 'cpython-314',
        'ext_suffix': '.cpython-314-test.so',
        'gil_disabled': False,
    }
    previous = {
        'build': {
            'cargo': {'version': 'cargo 1', 'dependencies': {}},
            'interpreter': interpreter,
        },
        'source': {
            'source_inputs_sha256': '1' * 64,
            'native_source_inputs_sha256': '2' * 64,
        },
        'wheel': {
            'path': 'quality.whl',
            'sha256': '3' * 64,
        },
    }
    monkeypatch.setattr(runtime, 'quality_receipt_path', lambda _root: receipt_path)
    monkeypatch.setattr(runtime, 'read_quality_receipt', lambda _root: previous)
    monkeypatch.setattr(runtime, 'prepare_license_outputs', lambda _root: None)
    monkeypatch.setattr(
        runtime,
        'source_input_digests',
        lambda _root: {
            'source_inputs_sha256': '1' * 64,
            'native_source_inputs_sha256': '2' * 64,
        },
    )
    monkeypatch.setattr(
        runtime,
        'python_member_hashes',
        lambda _path: {'gometry/__init__.py': '4' * 64},
    )
    monkeypatch.setattr(
        runtime,
        '_rust_target_triple',
        lambda *_args, **_kwargs: 'x86_64-unknown-linux-gnu',
    )
    monkeypatch.setattr(runtime, 'cargo_metadata', lambda *_args, **_kwargs: {})
    monkeypatch.setattr(
        runtime, 'required_source_records', lambda *_args, **_kwargs: []
    )
    monkeypatch.setattr(runtime, '_maturin_command', lambda _root: ['maturin'])
    monkeypatch.setattr(
        runtime,
        '_tool_output',
        lambda command, **_kwargs: 'rustc 1' if command[0] == 'rustc' else 'cargo 1',
    )
    monkeypatch.setattr(runtime, '_runtime_interpreter', lambda: interpreter)
    monkeypatch.setattr(
        runtime,
        'quality_settings',
        lambda _root, _environment: {'identity_sha256': '5' * 64},
    )
    monkeypatch.setattr(
        runtime,
        'run',
        lambda *_args, **_kwargs: pytest.fail(
            'a corrupt cached archive must not start a replacement build'
        ),
    )

    with pytest.raises(AssertionError, match='archive digest is corrupt'):
        runtime.prepare_quality_runtime(
            root,
            scratch=tmp_path / 'quality-runtime',
        )


def test_quality_runtime_receipt_round_trips_and_refreshes_prepared_inputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = tmp_path / 'workspace'
    root = workspace / 'gometry'
    package = root / 'python' / 'gometry'
    package.mkdir(parents=True)
    fixed_inputs = {
        workspace / 'Cargo.toml': '[workspace]\n',
        workspace / 'Cargo.lock': 'version = 4\n',
        workspace / '.cargo' / 'config.toml': '[build]\n',
        root / 'Cargo.toml': ('[package]\nname = "gometry"\nversion = "1.0.0"\n'),
        root / 'pyproject.toml': '[project]\nname = "gometry"\n',
        root / 'rust-toolchain.toml': '[toolchain]\nchannel = "nightly"\n',
        root / 'native' / 'proj-sys' / 'PROJSRC' / 'proj-9.6.2.tar.gz': (
            'fixture archive'
        ),
    }
    for path, contents in fixed_inputs.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents, encoding='utf-8')
    python_payloads = {
        'gometry/__init__.py': b'VALUE = 17\n',
        'gometry/_lib.pyi': b'def value() -> int: ...\n',
        'gometry/py.typed': b'',
    }
    for name, payload in python_payloads.items():
        (package / Path(name).name).write_bytes(payload)

    license_payloads = {
        'LICENSE-APACHE.md': b'apache license\n',
        'LICENSE-MIT.md': b'mit license\n',
        'LICENSE-THIRD-PARTY.md': b'third-party notices\n',
    }

    def prepare_outputs(source_root: Path) -> None:
        (source_root / 'README.md').write_bytes(b'package readme\n')
        for name, payload in license_payloads.items():
            (source_root / name).write_bytes(payload)

    monkeypatch.setenv('CARGO_HOME', str(tmp_path / 'cargo-home'))
    for name in (*artifact_common.RELEASE_ENV_VARS, 'MATURIN_PEP517_ARGS'):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(
        quality_runtime,
        'prepare_license_outputs',
        prepare_outputs,
    )
    monkeypatch.setattr(
        quality_runtime,
        'source_input_digests',
        lambda _root: {
            'source_inputs_sha256': '1' * 64,
            'native_source_inputs_sha256': '2' * 64,
        },
    )
    monkeypatch.setattr(
        quality_runtime,
        '_rust_target_triple',
        lambda *_args, **_kwargs: 'x86_64-unknown-linux-gnu',
    )
    monkeypatch.setattr(
        quality_runtime,
        'cargo_metadata',
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        quality_runtime,
        'required_source_records',
        lambda *_args, **_kwargs: [
            {
                'name': 'fixture',
                'source': 'registry+https://example.invalid/index',
                'checksum': '3' * 64,
            }
        ],
    )
    monkeypatch.setattr(
        quality_runtime,
        '_maturin_command',
        lambda _root: ['maturin'],
    )
    monkeypatch.setattr(
        quality_runtime,
        '_tool_output',
        lambda command, **_kwargs: (
            'rustc fixture' if command[0] == 'rustc' else 'cargo fixture'
        ),
    )
    monkeypatch.setattr(
        quality_runtime, '_admit_quality_candidate', lambda *_args: None
    )
    monkeypatch.setattr(artifact_content, 'native_image_facts', lambda *_args: {})
    builds = 0

    def build_wheel(
        command: list[str],
        *,
        cwd: Path,
        env: dict[str, str],
        capture_output: bool,
    ) -> None:
        nonlocal builds
        assert cwd == root
        assert env['PYO3_PYTHON'] == str(Path(sys.executable).resolve())
        assert not capture_output, 'quality builds must stream their output'
        output = Path(command[command.index('--out') + 1])
        package_members = {
            f'gometry/{path.relative_to(package).as_posix()}': path.read_bytes()
            for path in package.rglob('*')
            if path.is_file()
        }
        _quality_wheel(
            output,
            version='1.0.0',
            python_members=package_members,
            licenses={name: (root / name).read_bytes() for name in license_payloads},
            extension_payload=b'\x7fELF\x00development extension',
        )
        builds += 1

    monkeypatch.setattr(quality_runtime, 'run', build_wheel)

    quality_runtime.prepare_quality_runtime(
        root,
        scratch=root / 'target' / 'quality-runtime',
    )
    parsed = quality_runtime.read_quality_receipt(root)
    quality_runtime.validate_quality_producer_inputs(root, parsed)

    prepared = parsed['prepared_inputs']
    assert set(prepared) == {'identity_sha256', 'files'}
    assert prepared['identity_sha256'] == artifact_common._prepared_input_identity(
        prepared['files']
    )
    assert prepared['files'] == artifact_common._prepared_output_hashes(root)
    assert 'authored_stub' not in parsed
    interpreter = parsed['build']['interpreter']
    assert (
        parsed['build']['settings']['environment']['PYO3_PYTHON']
        == interpreter['resolved_executable']
    )
    alternate_environment = os.environ.copy()
    target = parsed['build']['target']
    alternate_environment.update({
        'CARGO_BUILD_TARGET': target,
        'NIX_DONT_SET_RPATH': '1',
        f'NIX_DONT_SET_RPATH_{target.replace("-", "_")}': '1',
        'PYO3_PYTHON': str(tmp_path / 'other-python'),
    })
    assert (
        quality_runtime.quality_settings(root, alternate_environment)['identity_sha256']
        != parsed['build']['settings']['identity_sha256']
    )

    reused = quality_runtime.prepare_quality_runtime(
        root,
        scratch=root / 'target' / 'quality-runtime',
    )
    assert builds == 1
    assert reused['prepared_inputs'] == parsed['prepared_inputs']
    parsed = quality_runtime.read_quality_receipt(root)
    quality_runtime.validate_quality_producer_inputs(root, parsed)

    (package / '_lib.pyi').write_bytes(b'def value() -> str: ...\n')
    refreshed = quality_runtime.refresh_quality_runtime(root)
    parsed = quality_runtime.read_quality_receipt(root)
    quality_runtime.validate_quality_producer_inputs(root, parsed)
    refreshed_prepared = parsed['prepared_inputs']
    assert refreshed['prepared_inputs'] == refreshed_prepared
    assert refreshed_prepared['files'] == artifact_common._prepared_output_hashes(root)
    assert refreshed_prepared['identity_sha256'] == (
        artifact_common._prepared_input_identity(refreshed_prepared['files'])
    )
    assert builds == 1


def _quality_package_members(
    root: Path,
) -> tuple[Path, dict[str, Any], dict[str, str], dict[str, bytes]]:
    package_root = root / 'package'
    package = package_root / 'gometry'
    dist_info = package_root / 'gometry-1.0.dist-info'
    package.mkdir(parents=True)
    dist_info.mkdir()
    archive_python = {
        'gometry/_lib.pyi': b'archive stub\n',
        'gometry/py.typed': b'',
    }
    staged_python = {
        'gometry/_lib.pyi': b'refreshed stub\n',
        'gometry/py.typed': b'',
    }
    producer_members = {
        **archive_python,
        'gometry/_lib.cpython-314-x86_64-linux-gnu.so': b'native bytes',
        'gometry-1.0.dist-info/METADATA': b'Name: gometry\nVersion: 1.0\n',
        'gometry-1.0.dist-info/WHEEL': b'Wheel-Version: 1.0\n',
        'gometry-1.0.dist-info/RECORD': b'raw RECORD bytes',
        'gometry-1.0.dist-info/RECORD.jws': b'raw signature bytes',
    }
    for name, payload in staged_python.items():
        (package_root / name).write_bytes(payload)
    for name, payload in producer_members.items():
        if name in archive_python:
            continue
        (package_root / name).write_bytes(payload)
    inspection = {
        'python_members': {
            name: hashlib.sha256(payload).hexdigest()
            for name, payload in archive_python.items()
        },
        'signatures': ['gometry-1.0.dist-info/RECORD.jws'],
        'record': 'gometry-1.0.dist-info/RECORD',
        'members': {
            name: {'sha256': hashlib.sha256(payload).hexdigest()}
            for name, payload in producer_members.items()
        },
    }
    staged_hashes = {
        name: hashlib.sha256(payload).hexdigest()
        for name, payload in staged_python.items()
    }
    return package_root, inspection, staged_hashes, producer_members


def test_quality_package_accepts_staged_python_with_unchanged_record_bytes(
    tmp_path: Path,
) -> None:
    package_root, inspection, staged_hashes, _ = _quality_package_members(tmp_path)

    quality_runtime._verify_materialized_quality_package(
        package_root,
        inspection,
        staged_hashes,
    )


@pytest.mark.parametrize(
    'member',
    [
        'gometry-1.0.dist-info/RECORD',
        'gometry-1.0.dist-info/RECORD.jws',
    ],
    ids=('record-bytes', 'signature-bytes'),
)
def test_quality_package_rejects_changed_raw_record_or_signature(
    tmp_path: Path,
    member: str,
) -> None:
    package_root, inspection, staged_hashes, _ = _quality_package_members(tmp_path)
    (package_root / member).write_bytes(b'changed producer-owned bytes')

    with pytest.raises(
        AssertionError,
        match='prepared quality distribution member differs from its wheel',
    ):
        quality_runtime._verify_materialized_quality_package(
            package_root,
            inspection,
            staged_hashes,
        )


@pytest.mark.parametrize(
    ('receipt', 'error'),
    [
        ([], TypeError),
        ({'schema': 1, 'kind': 'other'}, AssertionError),
        ({'schema': 999, 'kind': 'gometry-quality-runtime'}, AssertionError),
        ({'schema': 1, 'kind': 'gometry-quality-runtime'}, AssertionError),
        ({'schema': 2, 'kind': 'gometry-quality-runtime'}, AssertionError),
    ],
    ids=(
        'non-object',
        'wrong-kind',
        'unknown-schema',
        'unsupported-schema-one',
        'malformed-current-schema',
    ),
)
def test_unsupported_or_malformed_quality_receipt_is_not_rebuildable(
    tmp_path: Path,
    receipt: Any,
    error: type[Exception],
) -> None:
    path = quality_runtime.quality_receipt_path(tmp_path)
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(receipt), encoding='utf-8')

    with pytest.raises(error):
        quality_runtime.read_quality_receipt(tmp_path)


def test_wheel_receipt_cli_accepts_its_required_evidence_options(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / artifact_common.RUNNER_RECEIPT_NAME
    observed: dict[str, Any] = {}

    def write_receipt(*args: Any, **kwargs: Any) -> Path:
        observed['args'] = args
        observed['kwargs'] = kwargs
        return output

    monkeypatch.setattr(wheel_cli, 'write_runner_receipt', write_receipt)

    status = wheel_cli.main([
        '--write-wheel-receipt',
        str(tmp_path / 'wheels'),
        str(tmp_path / 'identifiers.txt'),
        str(tmp_path / 'source-manifest.json'),
        '--evidence-dir',
        str(tmp_path / 'evidence'),
        '--build-log',
        str(tmp_path / 'build.log'),
        '--build-exit-code',
        '0',
        '--capture-exit-code',
        '0',
        '--runner',
        'ubuntu-latest',
    ])

    assert status == 0
    assert observed['kwargs']['runner'] == 'ubuntu-latest'
