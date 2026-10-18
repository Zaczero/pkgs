"""Staged source, licensing and Cargo dependency evidence."""

from __future__ import annotations

import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import tomllib
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import TYPE_CHECKING, Any

from _artifact_common import (
    HEX_DIGEST,
    LIBTIFF,
    LIBTIFF_COMMIT,
    LIBTIFF_PIN,
    LIBTIFF_VERSION,
    MATURIN_VERSION,
    REGISTRY_SOURCE,
    RELEASE_BUILD_INPUTS,
    RELEASE_MATURIN_ARGS,
    ROOT,
    SOURCE_INPUTS_FORMAT,
    SOURCE_TOOL_MEMBERS,
    _load_json,
    _maturin_command,
    _package_toolchain_channel,
    _prepared_input_identity,
    _prepared_input_record,
    _prepared_output_hashes,
    _sha256_bytes,
    _source_build_env,
    _tool_output,
    _tool_version,
    _write_json,
    release_build_env,
    run,
    sha256_file,
)
from artifact_content import (
    _source_input_records,
    content_leaks,
    has_hidden_component,
    inspect_libtiff_sdist_members,
    is_hidden_native_path,
    is_python_package_member,
    is_text_member,
    libtiff_source_members,
    python_member_hashes,
    source_input_digests,
    source_root_entries,
    verify_native_notice_texts,
)

if TYPE_CHECKING:
    from collections.abc import Mapping
SOURCE_MANIFEST_NAME = 'gometry-source-manifest.json'
SOURCE_MANIFEST_SCHEMA = 1
_LIBTIFF_GITLINK = 'vendor/libtiff/source'
_PRODUCER_FIXED_INPUTS = frozenset({
    '.gitignore',
    '.gitmodules',
    'gometry/.gitignore',
    'gometry/README.md',
    'gometry/native-licenses.toml',
    '.github/scripts/about.hbs',
    '.github/scripts/about.toml',
    '.github/scripts/gen_licenses.py',
    '.github/scripts/render_pypi_readme.py',
    *RELEASE_BUILD_INPUTS,
    *(f'gometry/{member}' for member in SOURCE_TOOL_MEMBERS),
})
_PRODUCER_SNAPSHOT_INPUTS = _PRODUCER_FIXED_INPUTS


def _producer_pathspecs(root: Path) -> tuple[str, ...]:
    return tuple(
        sorted({
            'gometry/build.rs',
            'gometry/src',
            'gometry/native',
            'gometry/python/gometry',
            _LIBTIFF_GITLINK,
            *(relative for _, relative, _ in source_root_entries(root)),
            *_PRODUCER_FIXED_INPUTS,
        })
    )


def _is_producer_input_path(relative: str, root: Path = ROOT) -> bool:
    if relative in _PRODUCER_FIXED_INPUTS or relative in {
        'gometry/build.rs',
        _LIBTIFF_GITLINK,
    }:
        return True
    if relative.startswith('gometry/src/'):
        return PurePosixPath(relative).name != 'Cargo.toml'
    if relative.startswith('gometry/native/'):
        return not is_hidden_native_path(relative)
    for _, source_root, _ in source_root_entries(root):
        if relative == source_root or relative.startswith(f'{source_root}/'):
            return not has_hidden_component(relative)
    package_prefix = 'gometry/python/gometry/'
    return relative.startswith(package_prefix) and is_python_package_member(
        relative.removeprefix(package_prefix)
    )


def _producer_input_paths(root: Path) -> dict[str, Path]:
    repo = root.parent
    source_files, native_files = _source_input_records(root)
    files = {
        path.relative_to(repo).as_posix(): path
        for _, path in (*source_files, *native_files)
        if not path.relative_to(repo).as_posix().startswith(f'{_LIBTIFF_GITLINK}/')
    }
    manifests = [root / 'native']
    manifests.extend(path for _, _, path in source_root_entries(root))
    for manifest_root in manifests:
        for path in manifest_root.rglob('Cargo.toml'):
            relative = path.relative_to(repo).as_posix()
            if (
                not relative.startswith(f'{_LIBTIFF_GITLINK}/')
                and not has_hidden_component(relative)
                and path.is_file()
            ):
                files[relative] = path
    package_dir = root / 'python' / 'gometry'
    if not package_dir.is_dir():
        raise AssertionError(f'Python package directory is missing: {package_dir}')
    for path in sorted(package_dir.rglob('*')):
        relative_member = path.relative_to(package_dir)
        if path.is_file() and is_python_package_member(relative_member.as_posix()):
            logical = PurePosixPath('gometry/python/gometry') / relative_member
            files[logical.as_posix()] = path
    for relative in _PRODUCER_FIXED_INPUTS:
        path = repo / relative
        if path.is_file():
            files[relative] = path
    return files


def _producer_input_snapshot(root: Path) -> dict[str, str]:
    files = _producer_input_paths(root)
    source_manifests = {
        f'{path}/Cargo.toml' for _, path, _ in source_root_entries(root)
    }
    snapshot = {}
    for relative, path in files.items():
        if (
            relative not in _PRODUCER_SNAPSHOT_INPUTS
            and relative not in source_manifests
        ):
            continue
        metadata = path.lstat()
        link = os.readlink(path) if stat.S_ISLNK(metadata.st_mode) else ''
        value = f'{stat.S_IMODE(metadata.st_mode):o}\0{link}\0{sha256_file(path)}'
        snapshot[relative] = _sha256_bytes(value.encode('utf-8'))
    return snapshot


def _source_tool_hashes(root: Path) -> dict[str, str]:
    return {
        relative: sha256_file(root / relative)
        for relative in sorted(SOURCE_TOOL_MEMBERS)
    }


def _git_paths(
    root: Path,
    arguments: list[str],
) -> set[str] | None:
    try:
        result = subprocess.run(
            ['git', *arguments],
            cwd=root.parent,
            check=False,
            capture_output=True,
        )
    except OSError:
        return None
    if result.returncode:
        return None
    try:
        return {item.decode('utf-8') for item in result.stdout.split(b'\0') if item}
    except UnicodeDecodeError:
        return None


def _captured_libtiff_gitlink_matches(root: Path, commit: str) -> bool:
    try:
        result = subprocess.run(
            ['git', 'ls-tree', '-z', commit, '--', _LIBTIFF_GITLINK],
            cwd=root.parent,
            check=False,
            capture_output=True,
        )
    except OSError:
        return False
    if result.returncode:
        return False
    records = [record for record in result.stdout.split(b'\0') if record]
    if len(records) != 1:
        return False
    metadata, separator, path = records[0].partition(b'\t')
    fields = metadata.split()
    return (
        separator == b'\t'
        and path.decode('utf-8', errors='replace') == _LIBTIFF_GITLINK
        and len(fields) == 3
        and fields[0] == b'160000'
        and fields[1] == b'commit'
        and fields[2].decode('ascii', errors='ignore') == LIBTIFF_COMMIT
    )


def _producer_commit_is_bound(
    commit: str | None,
    root: Path = ROOT,
    *,
    exclude_readme: bool = False,
) -> bool:
    if (
        not isinstance(commit, str)
        or re.fullmatch(r'[0-9a-f]{40,64}', commit) is None
        or not _captured_libtiff_gitlink_matches(root, commit)
    ):
        return False
    captured_paths = _git_paths(
        root,
        [
            'ls-tree',
            '-r',
            '--name-only',
            '-z',
            commit,
            '--',
            *_producer_pathspecs(root),
        ],
    )
    staged_paths = _git_paths(
        root,
        [
            'diff',
            '--cached',
            '--name-only',
            '-z',
            '--no-renames',
            '--no-ext-diff',
            '--ignore-submodules=none',
            commit,
            '--',
            *_producer_pathspecs(root),
        ],
    )
    worktree_paths = _git_paths(
        root,
        [
            'diff',
            '--name-only',
            '-z',
            '--no-renames',
            '--no-ext-diff',
            '--ignore-submodules=none',
            commit,
            '--',
            *_producer_pathspecs(root),
        ],
    )
    if captured_paths is None or staged_paths is None or worktree_paths is None:
        return False
    changed_paths = staged_paths | worktree_paths
    captured_inputs = {
        path for path in captured_paths if _is_producer_input_path(path, root)
    }
    captured_inputs.add(_LIBTIFF_GITLINK)
    try:
        current_files = _producer_input_paths(root)
    except (AssertionError, OSError, ValueError):
        return False
    current_inputs = set(current_files) | {_LIBTIFF_GITLINK}
    if not current_inputs <= captured_inputs:
        return False
    return not any(
        _is_producer_input_path(path, root)
        and not (exclude_readme and path == 'gometry/README.md')
        for path in changed_paths
    )


def _retained_producer_commit(
    producer_commit: str | None,
    initial_snapshot: Mapping[str, str],
    current_snapshot: Mapping[str, str],
    root: Path = ROOT,
    *,
    exclude_readme: bool = False,
) -> str | None:
    if producer_commit is None:
        return None
    initial = {
        path: digest
        for path, digest in initial_snapshot.items()
        if not (exclude_readme and path == 'gometry/README.md')
    }
    current = {
        path: digest
        for path, digest in current_snapshot.items()
        if not (exclude_readme and path == 'gometry/README.md')
    }
    if initial != current or not _producer_commit_is_bound(
        producer_commit,
        root,
        exclude_readme=exclude_readme,
    ):
        return None
    return producer_commit


def _run_maturin_sdist(out_dir: Path, *, cwd: Path = ROOT) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    run(
        [*_maturin_command(), 'sdist', '--out', str(out_dir)],
        cwd=cwd,
        env=_source_build_env(),
    )


def _root_sdist_files(root: Path) -> dict[str, Path]:
    repo = root.parent.resolve()
    files: dict[str, Path] = {}
    for kind, _, source_root in source_root_entries(root):
        if kind != 'root':
            continue
        candidates = (
            [source_root] if source_root.is_file() else sorted(source_root.rglob('*'))
        )
        for path in candidates:
            if not path.is_file():
                continue
            member = path.relative_to(repo).as_posix()
            if has_hidden_component(member):
                continue
            files[member] = path
    return dict(sorted(files.items()))


def _include_root_sdist_files(sdist: Path, root: Path = ROOT) -> None:
    """Add root-owned non-Cargo inputs that Maturin cannot include from a parent path."""
    extras = _root_sdist_files(root)
    if not extras:
        return
    archive_root = (
        f'gometry-{sdist.name.removeprefix("gometry-").removesuffix(".tar.gz")}'
    )
    additions = {
        f'{archive_root}/{relative}': path for relative, path in extras.items()
    }
    source_date_epoch = os.environ.get('SOURCE_DATE_EPOCH')
    if source_date_epoch is not None and not source_date_epoch.isdecimal():
        raise AssertionError(f'invalid SOURCE_DATE_EPOCH: {source_date_epoch!r}')
    archive_mode = stat.S_IMODE(sdist.stat().st_mode)
    with tarfile.open(sdist, 'r:gz') as source:
        members = source.getmembers()
        names = {member.name for member in members}
        duplicates = names & additions.keys()
        if duplicates:
            raise AssertionError(
                f'Maturin already included root-owned sdist inputs: {sorted(duplicates)[:20]}'
            )
        with tempfile.NamedTemporaryFile(
            prefix=f'.{sdist.name}.',
            suffix='.tmp',
            dir=sdist.parent,
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
        try:
            with tarfile.open(
                temporary_path, 'w:gz', format=tarfile.PAX_FORMAT
            ) as output:
                for member in members:
                    stream = source.extractfile(member) if member.isfile() else None
                    if member.isfile() and stream is None:
                        raise AssertionError(
                            f'could not read staged sdist member {member.name}'
                        )
                    output.addfile(member, stream)
                for name, path in additions.items():
                    member = output.gettarinfo(str(path), arcname=name)
                    if source_date_epoch is not None:
                        member.mtime = int(source_date_epoch)
                    member.uid = 0
                    member.gid = 0
                    member.uname = ''
                    member.gname = ''
                    with path.open('rb') as stream:
                        output.addfile(member, stream)
            os.chmod(temporary_path, archive_mode)
            os.replace(temporary_path, sdist)
        except BaseException:
            temporary_path.unlink(missing_ok=True)
            raise


def _prepare_source_outputs(root: Path = ROOT) -> dict[str, str]:
    readme_script = root.parent / '.github' / 'scripts' / 'render_pypi_readme.py'
    license_script = root.parent / '.github' / 'scripts' / 'gen_licenses.py'
    if not readme_script.is_file():
        raise AssertionError(f'PyPI README renderer is missing: {readme_script}')
    if not license_script.is_file():
        raise AssertionError(f'license generator is missing: {license_script}')
    ref = os.environ.get('GITHUB_REF_NAME', 'main')
    run([sys.executable, str(readme_script), 'gometry', ref], cwd=root.parent)
    prepare_license_outputs(root)
    return _prepared_output_hashes(root)


def prepare_license_outputs(root: Path) -> None:
    """Generate checkout-local license files through the existing graph owner."""
    script = root.parent / '.github' / 'scripts' / 'gen_licenses.py'
    if not script.is_file():
        raise AssertionError(f'license generator is missing: {script}')
    run([sys.executable, str(script), 'gometry'], cwd=root.parent)


def _clear_stage_outputs(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for pattern in ('gometry-*.tar.gz', 'gometry-*.whl'):
        for path in out_dir.glob(pattern):
            path.unlink()
    (out_dir / SOURCE_MANIFEST_NAME).unlink(missing_ok=True)


def _single_sdist(directory: Path, *, label: str) -> Path:
    sdists = sorted(directory.glob('gometry-*.tar.gz'))
    if len(sdists) != 1:
        raise AssertionError(f'expected one {label} gometry sdist, found {sdists!r}')
    return sdists[0]


def _source_manifest(
    *,
    commit: str | None,
    sdist: Path,
    prepared_hashes: Mapping[str, str],
    evidence: Mapping[str, Any],
    source_digests: Mapping[str, str],
    python_members: Mapping[str, str],
) -> dict[str, Any]:
    required = evidence['required_sources']
    toolchain_env = release_build_env()
    return {
        'schema': SOURCE_MANIFEST_SCHEMA,
        'kind': 'gometry-source-manifest',
        'package': 'gometry',
        'commit': commit,
        'source_inputs_format': SOURCE_INPUTS_FORMAT,
        'source_inputs_sha256': source_digests['source_inputs_sha256'],
        'python_members': dict(sorted(python_members.items())),
        'native_source_inputs_format': SOURCE_INPUTS_FORMAT,
        'native_source_inputs_sha256': source_digests['native_source_inputs_sha256'],
        'prepared_inputs': _prepared_input_record(prepared_hashes),
        'generated_output_hashes': dict(sorted(prepared_hashes.items())),
        'sdist': {
            'filename': sdist.name,
            'sha256': sha256_file(sdist),
            'version': sdist.name.removeprefix('gometry-').removesuffix('.tar.gz'),
        },
        'cargo': {
            'lock_preimage_sha256': evidence['preimage_lock_sha256'],
            'lock_normalized_sha256': evidence['normalized_lock_sha256'],
            'required_sources': required,
        },
        'toolchain': {
            'channel': _package_toolchain_channel(),
            'maturin': MATURIN_VERSION,
            'cargo': _tool_version(['cargo', '--version'], env=toolchain_env),
            'rustc': _tool_output(['rustc', '-vV'], env=toolchain_env),
            'maturin_pep517_args': RELEASE_MATURIN_ARGS,
        },
    }


def _capture_git_head(root: Path = ROOT) -> str | None:
    try:
        result = subprocess.run(
            ['git', 'rev-parse', '--verify', 'HEAD^{commit}'],
            cwd=root.parent,
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return None
    if result.returncode:
        return None
    commit = result.stdout.strip()
    return commit if re.fullmatch(r'[0-9a-f]{40,64}', commit) else None


def _build_input_snapshot(
    snapshot: Mapping[str, str], root: Path = ROOT
) -> dict[str, str]:
    source_manifests = {
        f'{relative}/Cargo.toml' for _, relative, _ in source_root_entries(root)
    }
    return {
        path: digest
        for path, digest in snapshot.items()
        if path in RELEASE_BUILD_INPUTS or path in source_manifests
    }


def stage_sdist(out_dir: Path) -> Path:
    """Build one native Maturin sdist and bind its source and dependency inputs."""
    out_dir = out_dir.resolve()
    initial_snapshot = _producer_input_snapshot(ROOT)
    producer_commit = _retained_producer_commit(
        _capture_git_head(), initial_snapshot, initial_snapshot
    )
    _clear_stage_outputs(out_dir)
    initial_python_members = python_member_hashes(ROOT / 'python' / 'gometry')
    source_tool_hashes = _source_tool_hashes(ROOT)
    readme = ROOT / 'README.md'
    checkout_readme = readme.read_bytes()
    config = ROOT / '.cargo' / 'config.toml'
    config_directory_existed = config.parent.is_dir()
    previous_config = config.read_bytes() if config.exists() else None
    canonical_config = (ROOT.parent / '.cargo' / 'config.toml').read_bytes()
    destination: Path | None = None
    try:
        verify_libtiff_checkout()
        prepared_hashes = _prepare_source_outputs()
        source_digests = source_input_digests()
        config.parent.mkdir(exist_ok=True)
        config.write_bytes(canonical_config)
        with tempfile.TemporaryDirectory(prefix='gometry-sdist-stage-') as temporary:
            staging = Path(temporary)
            _run_maturin_sdist(staging)
            sdist = _single_sdist(staging, label='staged')
            _include_root_sdist_files(sdist)
            inspect_sdist(sdist, expected_libtiff=libtiff_source_members())
            with tarfile.open(sdist) as archive:
                extracted = staging / 'extracted'
                extract_sdist(archive, extracted)
            roots = [item for item in extracted.iterdir() if item.is_dir()]
            if len(roots) != 1:
                raise AssertionError('staged sdist has no unique source root')
            source_root = roots[0]
            if (
                source_root / '.cargo' / 'config.toml'
            ).read_bytes() != canonical_config:
                raise AssertionError(
                    'sdist Cargo configuration differs from its source'
                )
            if {
                relative: sha256_file(source_root / relative)
                for relative in sorted(SOURCE_TOOL_MEMBERS)
            } != source_tool_hashes:
                raise AssertionError(
                    'sdist source-tool identity differs from staging input'
                )
            evidence = _verify_extracted_sdist(source_root)
            _verify_locked_sdist(
                source_root, expected_lock=evidence['normalized_lock_bytes']
            )
            if {key: evidence[key] for key in source_digests} != source_digests:
                raise AssertionError(
                    'sdist source-input identity differs from checkout'
                )
            python_members = python_member_hashes(source_root / 'python' / 'gometry')
            if python_members != initial_python_members:
                raise AssertionError('sdist Python members differ from staging input')
            if source_input_digests() != source_digests:
                raise AssertionError('checkout source inputs changed while staging')
            if _prepared_output_hashes() != prepared_hashes:
                raise AssertionError('prepared source outputs changed while staging')
            current_snapshot = _producer_input_snapshot(ROOT)
            if _build_input_snapshot(current_snapshot) != _build_input_snapshot(
                initial_snapshot
            ):
                raise AssertionError('release build inputs changed while staging')
            producer_commit = _retained_producer_commit(
                producer_commit, initial_snapshot, current_snapshot, exclude_readme=True
            )
            manifest = _source_manifest(
                commit=producer_commit,
                sdist=sdist,
                prepared_hashes=prepared_hashes,
                evidence=evidence,
                source_digests=source_digests,
                python_members=python_members,
            )
            destination = out_dir / sdist.name
            shutil.copyfile(sdist, destination)
            manifest_path = out_dir / SOURCE_MANIFEST_NAME
            _write_json(manifest_path, manifest)
            validate_source_manifest(destination, manifest_path)
            return destination
    except Exception:
        if destination is not None:
            destination.unlink(missing_ok=True)
        (out_dir / SOURCE_MANIFEST_NAME).unlink(missing_ok=True)
        raise
    finally:
        readme.write_bytes(checkout_readme)
        if previous_config is None:
            config.unlink(missing_ok=True)
        else:
            config.write_bytes(previous_config)
        if not config_directory_existed and config.parent.exists():
            config.parent.rmdir()


def verify_libtiff_checkout() -> None:
    """Reject an uninitialized or altered source before staging the sdist."""
    cmake = LIBTIFF / 'CMakeLists.txt'
    version = LIBTIFF / 'VERSION'
    hint = 'git submodule update --init vendor/libtiff/source'
    if not cmake.is_file() or not version.is_file():
        raise AssertionError(f'libtiff submodule is not initialized; run `{hint}`')
    if version.read_text().strip() != LIBTIFF_VERSION:
        raise AssertionError(f'libtiff VERSION is not {LIBTIFF_VERSION}')
    if 'add_subdirectory(build)' not in cmake.read_text():
        raise AssertionError(
            'libtiff CMakeLists.txt is not the unmodified upstream file'
        )
    pin = LIBTIFF_PIN.read_text()
    required_pin = (
        'url = https://gitlab.com/libtiff/libtiff.git\n'
        f'commit = {LIBTIFF_COMMIT}\n'
        'tag = v4.7.2\n'
        'version = 4.7.2\n'
    )
    if pin != required_pin:
        raise AssertionError(
            'libtiff.pin does not describe the required upstream source'
        )
    result = subprocess.run(
        ['git', '-C', str(LIBTIFF), 'rev-parse', 'HEAD'],
        check=True,
        capture_output=True,
        text=True,
    )
    if result.stdout.strip() != LIBTIFF_COMMIT:
        raise AssertionError(f'libtiff submodule is not pinned to {LIBTIFF_COMMIT}')
    for diff_args in (('diff', '--exit-code'), ('diff', '--cached', '--exit-code')):
        dirty = subprocess.run(
            ['git', '-C', str(LIBTIFF), *diff_args],
            # A non-zero exit is the signal being read, not a failure.
            check=False,
            capture_output=True,
            text=True,
        )
        if dirty.returncode:
            raise AssertionError(
                f'libtiff submodule has tracked changes: {dirty.stdout}'
            )
    status = subprocess.run(
        ['git', '-C', str(LIBTIFF), 'status', '--porcelain', '--untracked-files=all'],
        check=True,
        capture_output=True,
        text=True,
    )
    if status.stdout:
        raise AssertionError(f'libtiff submodule has untracked files: {status.stdout}')
    license_text = (LIBTIFF / 'LICENSE.md').read_bytes()
    first_start = license_text.index(b'Copyright')
    first_end = license_text.index(b'# Lempel-Ziv')
    notices = ROOT.parent / 'vendor' / 'libtiff' / 'notices'
    if (notices / 'libtiff-license.md').read_bytes() != license_text[
        first_start:first_end
    ]:
        raise AssertionError(
            'libtiff license notice extract differs from upstream LICENSE.md'
        )
    if (notices / 'libtiff-lzw-license.md').read_bytes() != license_text[first_end:]:
        raise AssertionError(
            'libtiff LZW notice extract differs from upstream LICENSE.md'
        )


def inspect_sdist(
    path: Path,
    *,
    expected_libtiff: set[str] | None = None,
    expected_toolchain_channel: str | None = None,
) -> str:
    with tarfile.open(path) as archive:
        names = archive.getnames()
        if len(names) != len(set(names)):
            raise AssertionError(f'sdist contains duplicate members: {path}')
        text_members: dict[str, str] = {}
        for member in archive.getmembers():
            if not member.isfile() or not is_text_member(member.name):
                continue
            stream = archive.extractfile(member)
            if stream is None:
                raise AssertionError(f'sdist member could not be read: {member.name}')
            text_members[member.name] = stream.read().decode('utf-8', errors='ignore')
    inspect_libtiff_sdist_members(
        names,
        path,
        expected_libtiff,
    )
    root = f'gometry-{path.name.removeprefix("gometry-").removesuffix(".tar.gz")}'
    required = {
        f'{root}/LICENSE-APACHE.md',
        f'{root}/LICENSE-MIT.md',
        f'{root}/LICENSE-THIRD-PARTY.md',
        f'{root}/Cargo.lock',
        f'{root}/.cargo/config.toml',
        f'{root}/pyproject.toml',
        f'{root}/rust-toolchain.toml',
        f'{root}/gometry/rust-toolchain.toml',
        f'{root}/gometry/native/source_roots.txt',
        f'{root}/tools/wheel_smoke.py',
        f'{root}/tools/_artifact_common.py',
        f'{root}/tools/artifact_content.py',
        f'{root}/tools/source_artifact.py',
        f'{root}/tools/runtime_artifact.py',
        f'{root}/tools/quality_runtime.py',
        f'{root}/tools/release_artifact.py',
        f'{root}/native/cmake-flags/src/lib.rs',
        f'{root}/vendor/h3o/GOMETRY_DIVERGENCE.md',
        f'{root}/vendor/libtiff/libtiff.pin',
        f'{root}/vendor/libtiff/notices/libtiff-license.md',
        f'{root}/vendor/libtiff/notices/libtiff-lzw-license.md',
        f'{root}/vendor/libtiff/source/CMakeLists.txt',
        f'{root}/vendor/libtiff/source/libtiff/tif_lzw.c',
        f'{root}/vendor/nlohmann-json/notices/nlohmann-json-license.md',
        f'{root}/vendor/proj/proj-9.9.0.tar.gz',
        f'{root}/patches/proj/apply.rs',
        f'{root}/patches/proj/9.9.0/aea.cpp',
        f'{root}/patches/proj/9.9.0/merc.cpp',
        f'{root}/patches/proj/9.9.0/gometry_proj_context.cpp',
    }
    if missing := sorted(required - set(names)):
        raise AssertionError(f'sdist missing root release files: {missing}')
    package_info = text_members.get(f'{root}/PKG-INFO', '')
    version_line = next(
        (line for line in package_info.splitlines() if line.startswith('Version: ')),
        '',
    )
    metadata_version = version_line.removeprefix('Version: ')
    filename_version = path.name.removeprefix('gometry-').removesuffix('.tar.gz')
    if not metadata_version or metadata_version != filename_version:
        raise AssertionError(
            'sdist filename/metadata version mismatch: '
            f'{filename_version!r} != {metadata_version!r}'
        )
    pyproject = text_members[f'{root}/pyproject.toml']
    if 'patch.crates-io' in pyproject:
        raise AssertionError('sdist still configures vendored Cargo sources')
    forbidden_configs = (
        'target.x86_64-',
        'profile.release.',
        'unstable.trim-paths',
    )
    if forbidden := [
        line.strip()
        for line in pyproject.splitlines()
        if any(token in line for token in forbidden_configs)
    ]:
        raise AssertionError(
            f'sdist Maturin config duplicates root release policy: {forbidden}'
        )
    channels = []
    for member in (
        f'{root}/rust-toolchain.toml',
        f'{root}/gometry/rust-toolchain.toml',
    ):
        try:
            toolchain = tomllib.loads(text_members[member])
        except tomllib.TOMLDecodeError as error:
            raise AssertionError(
                f'sdist toolchain file is invalid: {member}'
            ) from error
        section = toolchain.get('toolchain')
        channel = section.get('channel') if isinstance(section, dict) else None
        if not isinstance(channel, str) or not channel:
            raise AssertionError(f'sdist toolchain file has no channel: {member}')
        channels.append(channel)
    if channels[0] != channels[1]:
        raise AssertionError(
            f'sdist root/package Rust toolchain channels disagree: {channels!r}'
        )
    if (
        expected_toolchain_channel is not None
        and channels[0] != expected_toolchain_channel
    ):
        raise AssertionError(
            'sdist Rust toolchain channel does not match the expected source: '
            f'{channels[0]!r} != {expected_toolchain_channel!r}'
        )
    forbidden_parts = {'site', 'dev', '__pycache__'}
    suspect = [
        name
        for name in names
        if forbidden_parts.intersection(Path(name).parts)
        or Path(name).suffix in {'.pyc', '.pyo'}
        or any(
            part == 'target' or part.startswith('target-') for part in Path(name).parts
        )
    ]
    if suspect:
        raise AssertionError(
            f'sdist contains old/generated/internal paths: {suspect[:20]}'
        )
    if leaks := content_leaks(text_members):
        raise AssertionError(f'sdist text leaks absolute build paths: {leaks[:20]}')
    verify_native_notice_texts(text_members[f'{root}/LICENSE-THIRD-PARTY.md'], path)
    return metadata_version


def extract_sdist(archive: tarfile.TarFile, destination: Path) -> None:
    if sys.version_info >= (3, 12):
        archive.extractall(destination, filter='data')
        return
    for member in archive.getmembers():
        posix_path = PurePosixPath(member.name)
        windows_path = PureWindowsPath(member.name)
        if (
            posix_path.is_absolute()
            or windows_path.is_absolute()
            or windows_path.drive
            or '..' in posix_path.parts
        ):
            raise AssertionError(f'sdist contains unsafe member path: {member.name!r}')
        if not (member.isdir() or member.isfile()):
            raise AssertionError(f'sdist contains unsupported member: {member.name!r}')
        archive.extract(member, destination)


def cargo_metadata(root: Path, *, locked: bool) -> dict[str, Any]:
    command = [
        'cargo',
        'metadata',
        '--format-version=1',
    ]
    if locked:
        command.append('--locked')
    command.extend(['--manifest-path', str(root / 'gometry' / 'Cargo.toml')])
    env = os.environ.copy()
    env.pop('RUSTC_WRAPPER', None)
    env['CARGO_TERM_COLOR'] = 'never'
    result = subprocess.run(
        command,
        cwd=root,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode:
        raise AssertionError(
            f'cargo metadata {"--locked " if locked else ""}failed '
            f'(exit {result.returncode}): {result.stderr.strip()}'
        )
    try:
        value = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise AssertionError(
            f'cargo metadata returned invalid JSON: {error}'
        ) from error
    if not isinstance(value, dict):
        raise TypeError('cargo metadata returned a non-object')
    return value


def _lock_tuple(package: Mapping[str, Any]) -> tuple[str, str, str | None, str | None]:
    try:
        name = package['name']
        version = package['version']
    except KeyError as error:
        raise AssertionError(
            f'Cargo.lock package is missing {error.args[0]!r}'
        ) from error
    if not isinstance(name, str) or not isinstance(version, str):
        raise TypeError('Cargo.lock package name and version must be strings')
    source = package.get('source')
    checksum = package.get('checksum')
    if source is not None and not isinstance(source, str):
        raise AssertionError(f'Cargo.lock source for {name} must be a string')
    if checksum is not None and not isinstance(checksum, str):
        raise AssertionError(f'Cargo.lock checksum for {name} must be a string')
    return name, version, source, checksum


def _lock_tuples(lock_bytes: bytes) -> set[tuple[str, str, str | None, str | None]]:
    try:
        lock = tomllib.loads(lock_bytes.decode('utf-8'))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise AssertionError(f'Cargo.lock is not valid UTF-8 TOML: {error}') from error
    packages = lock.get('package')
    if not isinstance(packages, list):
        raise TypeError('Cargo.lock has no package array')
    return {_lock_tuple(package) for package in packages}


def _metadata_external_tuples(
    metadata: Mapping[str, Any],
    preimage: set[tuple[str, str, str | None, str | None]],
) -> set[tuple[str, str, str | None, str | None]]:
    packages = metadata.get('packages')
    if not isinstance(packages, list):
        raise TypeError('cargo metadata has no packages array')
    by_identity: dict[
        tuple[str, str, str], set[tuple[str, str, str | None, str | None]]
    ] = {}
    for record in preimage:
        name, version, source, _checksum = record
        if source is not None:
            by_identity.setdefault((name, version, source), set()).add(record)
    resolved: set[tuple[str, str, str | None, str | None]] = set()
    for package in packages:
        if not isinstance(package, dict):
            raise TypeError('cargo metadata package is not an object')
        source = package.get('source')
        if source is None:
            continue
        name = package.get('name')
        version = package.get('version')
        if (
            not isinstance(source, str)
            or not isinstance(name, str)
            or not isinstance(version, str)
        ):
            raise TypeError('cargo metadata package identity is malformed')
        candidates = by_identity.get((name, version, source), set())
        if len(candidates) != 1:
            raise AssertionError(
                'cargo metadata resolved external package absent or ambiguous in '
                f'Cargo.lock preimage: {(name, version, source)!r}'
            )
        resolved.update(candidates)
    return resolved


def required_source_records(
    metadata: Mapping[str, Any], *, root: Path | None = None
) -> list[dict[str, Any]]:
    packages = metadata.get('packages')
    if not isinstance(packages, list):
        raise TypeError('cargo metadata has no packages array')
    expected = (
        ('geographiclib-rs', '0.2.7', REGISTRY_SOURCE, ('accurate',), None),
        # These path packages and their selected features attest the bundled
        # Rust sources; source_roots.txt owns the remaining native inputs.
        ('cmake', '0.1.58', None, (), 'vendor/cmake-rs/cmake'),
        ('h3o', '0.11.0', None, ('std',), 'vendor/h3o'),
        ('numpy', '0.30.0', None, (), 'vendor/rust-numpy'),
        (
            'pyo3',
            '0.30.1',
            None,
            ('macros', 'multiple-pymethods', 'nightly'),
            'vendor/pyo3/pyo3',
        ),
        (
            'pyo3-build-config',
            '0.30.1',
            None,
            (),
            'vendor/pyo3/pyo3-build-config',
        ),
        ('pyo3-ffi', '0.30.1', None, (), 'vendor/pyo3/pyo3-ffi'),
        ('pyo3-macros', '0.30.1', None, (), 'vendor/pyo3/pyo3-macros'),
        (
            'pyo3-macros-backend',
            '0.30.1',
            None,
            (),
            'vendor/pyo3/pyo3-macros-backend',
        ),
        ('proj-sys', '0.27.0', None, ('bundled_proj', 'tiff'), 'vendor/proj-sys'),
        ('gometry-libtiff-sys', '0.1.0', None, (), 'native/libtiff-sys'),
        ('cmake-flags', '0.1.0', None, (), 'native/cmake-flags'),
    )
    records: list[dict[str, Any]] = []
    for name, version, source, features, relative_manifest in expected:
        matches = [
            package
            for package in packages
            if isinstance(package, dict)
            and package.get('name') == name
            and (version is None or package.get('version') == version)
            and package.get('source') == source
        ]
        if len(matches) != 1:
            raise AssertionError(
                f'cargo metadata must resolve exactly one required {name} '
                f'from {source!r}; found {len(matches)}'
            )
        package = matches[0]
        if relative_manifest is not None and root is not None:
            manifest_path = package.get('manifest_path')
            expected_manifest = root / relative_manifest / 'Cargo.toml'
            if (
                not isinstance(manifest_path, str)
                or Path(manifest_path).resolve() != expected_manifest.resolve()
            ):
                raise AssertionError(
                    f'{name} did not resolve from its bundled source path'
                )
        package_id = package.get('id')
        if not isinstance(package_id, str):
            raise TypeError(f'cargo metadata {name} has no package id')
        resolve = metadata.get('resolve')
        if not isinstance(resolve, dict) or not isinstance(resolve.get('nodes'), list):
            raise TypeError('cargo metadata resolve graph is malformed')
        nodes = [
            node
            for node in resolve['nodes']
            if isinstance(node, dict) and node.get('id') == package_id
        ]
        if len(nodes) != 1:
            raise AssertionError(
                f'cargo metadata has no unique feature node for {name}'
            )
        selected = nodes[0].get('features')
        if not isinstance(selected, list) or not all(
            isinstance(feature, str) for feature in selected
        ):
            raise AssertionError(f'cargo metadata feature list for {name} is malformed')
        selected_features = set(selected)
        if name == 'h3o':
            # Cargo metadata unions features from the workspace's implicit
            # h3o path member with the gometry dependency edge. The member's
            # default feature is only the std alias; attest the consumer edge
            # separately and normalize that workspace-only alias.
            h3o_default = package.get('features')
            if not isinstance(h3o_default, dict) or h3o_default.get('default') != [
                'std'
            ]:
                raise AssertionError(
                    'vendored h3o default feature must remain the std alias'
                )
            consumers = [
                candidate
                for candidate in packages
                if isinstance(candidate, dict) and candidate.get('name') == 'gometry'
            ]
            if root is not None:
                expected_consumer = (root / 'gometry' / 'Cargo.toml').resolve()
                consumers = [
                    candidate
                    for candidate in consumers
                    if isinstance(candidate.get('manifest_path'), str)
                    and Path(candidate['manifest_path']).resolve() == expected_consumer
                ]
            if len(consumers) != 1:
                raise AssertionError('cargo metadata has no unique gometry manifest')
            h3o_edges = [
                dependency
                for dependency in consumers[0].get('dependencies', [])
                if isinstance(dependency, dict) and dependency.get('name') == 'h3o'
            ]
            if (
                len(h3o_edges) != 1
                or h3o_edges[0].get('uses_default_features') is not False
                or h3o_edges[0].get('features') != ['std']
            ):
                raise AssertionError(
                    'gometry must select only the vendored h3o std feature'
                )
            if root is not None:
                h3o_path = h3o_edges[0].get('path')
                expected_h3o = (root / 'vendor' / 'h3o').resolve()
                if (
                    not isinstance(h3o_path, str)
                    or Path(h3o_path).resolve() != expected_h3o
                ):
                    raise AssertionError(
                        'gometry must use the bundled h3o package path'
                    )
            selected_features.discard('default')
        expected_features = set(features)
        if selected_features != expected_features:
            raise AssertionError(
                f'{name} feature selection changed: '
                f'{sorted(selected_features)} != {sorted(expected_features)}'
            )
        records.append({
            'name': name,
            'version': package.get('version'),
            'source': source,
            'features': sorted(selected_features),
        })
    return records


def _verify_extracted_sdist(root: Path) -> dict[str, Any]:
    lock_path = root / 'Cargo.lock'
    try:
        preimage_bytes = lock_path.read_bytes()
    except OSError as error:
        raise AssertionError(f'cannot read extracted Cargo.lock: {error}') from error
    preimage = _lock_tuples(preimage_bytes)
    # This is deliberately the first Cargo call. The bytes and records above
    # are the authority against which Cargo's standalone-workspace pruning is
    # checked.
    metadata = cargo_metadata(root, locked=False)
    normalized_bytes = lock_path.read_bytes()
    normalized = _lock_tuples(normalized_bytes)
    external_preimage = {record for record in preimage if record[2] is not None}
    external_normalized = {record for record in normalized if record[2] is not None}
    if unexpected := sorted(external_normalized - external_preimage):
        raise AssertionError(
            'Cargo metadata introduced external packages while normalizing Cargo.lock: '
            f'{unexpected}'
        )
    resolved = _metadata_external_tuples(metadata, preimage)
    if unexpected := sorted(resolved - external_preimage):
        raise AssertionError(
            'cargo metadata resolved external packages absent from Cargo.lock preimage: '
            f'{unexpected}'
        )
    required_sources = required_source_records(metadata, root=root)
    source_digests = source_input_digests(root / 'gometry')
    return {
        'preimage_lock_bytes': preimage_bytes,
        'preimage_lock_sha256': _sha256_bytes(preimage_bytes),
        'normalized_lock_bytes': normalized_bytes,
        'normalized_lock_sha256': _sha256_bytes(normalized_bytes),
        'metadata': metadata,
        'required_sources': required_sources,
        **source_digests,
    }


def _verify_locked_sdist(root: Path, *, expected_lock: bytes) -> dict[str, Any]:
    lock_path = root / 'Cargo.lock'
    before = lock_path.read_bytes()
    if before != expected_lock:
        raise AssertionError(
            'final sdist Cargo.lock differs before locked metadata check'
        )
    metadata = cargo_metadata(root, locked=True)
    after = lock_path.read_bytes()
    if after != before:
        raise AssertionError('cargo metadata --locked mutated Cargo.lock')
    source_digests = source_input_digests(root / 'gometry')
    return {
        'normalized_lock_bytes': after,
        'normalized_lock_sha256': _sha256_bytes(after),
        'metadata': metadata,
        'required_sources': required_source_records(metadata, root=root),
        **source_digests,
    }


def verify_sdist_dependency_sources(path: Path) -> dict[str, Any]:
    """Verify source identities before and after standalone Cargo normalization."""
    with tempfile.TemporaryDirectory(prefix='gometry-sdist-') as tmp:
        extracted = Path(tmp)
        with tarfile.open(path) as archive:
            extract_sdist(archive, extracted)
        roots = [item for item in extracted.iterdir() if item.is_dir()]
        if len(roots) != 1:
            raise AssertionError(f'expected one extracted sdist root, found {roots!r}')
        root = roots[0]
        evidence = _verify_extracted_sdist(root)
        normalized = evidence['normalized_lock_bytes']
        if not isinstance(normalized, bytes):
            raise TypeError('Cargo lock normalization did not return bytes')
        (root / 'Cargo.lock').write_bytes(normalized)
        locked = _verify_locked_sdist(root, expected_lock=normalized)
        return {
            **evidence,
            'locked_metadata': locked['metadata'],
        }


def validate_source_identity(value: Any, *, field: str = 'source') -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TypeError(f'{field} identity is not an object')
    expected_fields = {
        'commit',
        'filename',
        'sha256',
        'version',
        'source_inputs_format',
        'source_inputs_sha256',
        'python_members',
        'native_source_inputs_format',
        'native_source_inputs_sha256',
        'cargo',
        'toolchain',
    }
    if set(value) != expected_fields:
        raise AssertionError(
            f'{field} identity fields are not exact: '
            f'{sorted(set(value) ^ expected_fields)}'
        )
    commit = value.get('commit')
    if commit is not None and (
        not isinstance(commit, str) or re.fullmatch(r'[0-9a-f]{40,64}', commit) is None
    ):
        raise AssertionError(f'{field} identity has a malformed source commit')
    filename = value.get('filename')
    digest = value.get('sha256')
    version = value.get('version')
    if (
        not isinstance(filename, str)
        or Path(filename).name != filename
        or not filename.endswith('.tar.gz')
        or not isinstance(digest, str)
        or HEX_DIGEST.fullmatch(digest) is None
        or not isinstance(version, str)
        or not version
    ):
        raise AssertionError(f'{field} sdist identity is malformed')
    for key in (
        'source_inputs_sha256',
        'native_source_inputs_sha256',
    ):
        value_digest = value.get(key)
        if (
            not isinstance(value_digest, str)
            or HEX_DIGEST.fullmatch(value_digest) is None
        ):
            raise AssertionError(f'{field} has an invalid {key}')
    python_members = value.get('python_members')
    if not isinstance(python_members, dict) or not python_members:
        raise AssertionError(f'{field} Python-member identity is missing')
    for name, digest in python_members.items():
        if (
            not isinstance(name, str)
            or not name.startswith('gometry/')
            or not isinstance(digest, str)
            or HEX_DIGEST.fullmatch(digest) is None
        ):
            raise AssertionError(f'{field} Python-member identity is malformed')
    if value.get('source_inputs_format') != SOURCE_INPUTS_FORMAT:
        raise AssertionError(f'{field} has an unsupported source-input format')
    if value.get('native_source_inputs_format') != SOURCE_INPUTS_FORMAT:
        raise AssertionError(f'{field} has an unsupported native source-input format')
    cargo = value.get('cargo')
    if not isinstance(cargo, dict):
        raise TypeError(f'{field} Cargo identity is missing')
    for key in ('lock_preimage_sha256', 'lock_normalized_sha256'):
        lock_digest = cargo.get(key)
        if (
            not isinstance(lock_digest, str)
            or HEX_DIGEST.fullmatch(lock_digest) is None
        ):
            raise AssertionError(f'{field}.cargo has an invalid {key}')
    required = cargo.get('required_sources')
    if (
        not isinstance(required, list)
        or not required
        or not all(isinstance(record, dict) for record in required)
    ):
        raise AssertionError(f'{field}.cargo required source identity is malformed')
    toolchain = value.get('toolchain')
    if not isinstance(toolchain, dict):
        raise TypeError(f'{field} toolchain identity is missing')
    if set(toolchain) != {
        'channel',
        'maturin',
        'cargo',
        'rustc',
        'maturin_pep517_args',
    }:
        raise AssertionError(f'{field}.toolchain fields are not exact')
    if not isinstance(toolchain.get('channel'), str) or not toolchain['channel']:
        raise AssertionError(f'{field} toolchain channel is malformed')
    if toolchain.get('maturin') != MATURIN_VERSION:
        raise AssertionError(f'{field} maturin identity is not pinned')
    if toolchain.get('maturin_pep517_args') != RELEASE_MATURIN_ARGS:
        raise AssertionError(f'{field} Maturin release profile is not pinned')
    for key in ('cargo', 'rustc'):
        tool = toolchain.get(key)
        if not isinstance(tool, str) or not tool:
            raise AssertionError(f'{field}.toolchain has no {key} identity')
    return value


def source_identity(manifest: Mapping[str, Any]) -> dict[str, Any]:
    sdist = manifest['sdist']
    return {
        'commit': manifest['commit'],
        'filename': sdist['filename'],
        'sha256': sdist['sha256'],
        'version': sdist['version'],
        'source_inputs_format': manifest['source_inputs_format'],
        'source_inputs_sha256': manifest['source_inputs_sha256'],
        'python_members': manifest['python_members'],
        'native_source_inputs_format': manifest['native_source_inputs_format'],
        'native_source_inputs_sha256': manifest['native_source_inputs_sha256'],
        'cargo': manifest['cargo'],
        'toolchain': manifest['toolchain'],
    }


def sdist_toolchain_channel(sdist: Path) -> str:
    root = f'gometry-{sdist.name.removeprefix("gometry-").removesuffix(".tar.gz")}'
    channels = []
    try:
        with tarfile.open(sdist) as archive:
            for relative in ('rust-toolchain.toml', 'gometry/rust-toolchain.toml'):
                stream = archive.extractfile(f'{root}/{relative}')
                if stream is None:
                    raise AssertionError(f'sdist is missing {relative}')
                value = tomllib.loads(stream.read().decode('utf-8'))
                toolchain = value.get('toolchain')
                channel = (
                    toolchain.get('channel') if isinstance(toolchain, dict) else None
                )
                if not isinstance(channel, str) or not channel:
                    raise AssertionError(f'sdist {relative} has no toolchain channel')
                channels.append(channel)
    except (
        OSError,
        UnicodeDecodeError,
        tarfile.TarError,
        tomllib.TOMLDecodeError,
    ) as error:
        raise AssertionError(
            f'cannot inspect sdist toolchain files: {error}'
        ) from error
    if channels[0] != channels[1]:
        raise AssertionError('sdist root/package toolchain channels disagree')
    return channels[0]


def sdist_prepared_license_hashes(sdist: Path) -> dict[str, str]:
    root = f'gometry-{sdist.name.removeprefix("gometry-").removesuffix(".tar.gz")}'
    files = (
        'LICENSE-APACHE.md',
        'LICENSE-MIT.md',
        'LICENSE-THIRD-PARTY.md',
    )
    hashes: dict[str, str] = {}
    try:
        with tarfile.open(sdist) as archive:
            for name in files:
                member = archive.extractfile(f'{root}/{name}')
                if member is None:
                    raise AssertionError(f'sdist is missing prepared license {name}')
                hashes[name] = _sha256_bytes(member.read())
    except (OSError, tarfile.TarError) as error:
        raise AssertionError(f'cannot read sdist license identity: {error}') from error
    return hashes


def validate_source_manifest(sdist: Path, manifest_path: Path) -> dict[str, Any]:
    if (
        manifest_path.parent != sdist.parent
        or manifest_path.name != SOURCE_MANIFEST_NAME
    ):
        raise AssertionError('source manifest must be the sibling canonical manifest')
    manifest = _load_json(manifest_path)
    if (
        manifest.get('schema') != SOURCE_MANIFEST_SCHEMA
        or manifest.get('kind') != 'gometry-source-manifest'
    ):
        raise AssertionError(f'unsupported gometry source manifest: {manifest_path}')
    expected_manifest_fields = {
        'schema',
        'kind',
        'package',
        'commit',
        'source_inputs_format',
        'source_inputs_sha256',
        'python_members',
        'native_source_inputs_format',
        'native_source_inputs_sha256',
        'prepared_inputs',
        'generated_output_hashes',
        'sdist',
        'cargo',
        'toolchain',
    }
    if set(manifest) != expected_manifest_fields:
        raise AssertionError('source manifest fields are not exact')
    if manifest.get('package') != 'gometry':
        raise AssertionError(f'source manifest is not for gometry: {manifest_path}')
    source = manifest.get('sdist')
    if not isinstance(source, dict):
        raise TypeError('source manifest has no sdist record')
    if (
        source.get('filename') != sdist.name
        or not isinstance(source.get('filename'), str)
        or Path(str(source.get('filename'))).name != source.get('filename')
    ):
        raise AssertionError(
            f'source manifest filename mismatch: {source.get("filename")!r} != {sdist.name!r}'
        )
    expected = source.get('sha256')
    if not isinstance(expected, str) or HEX_DIGEST.fullmatch(expected) is None:
        raise AssertionError('source manifest has an invalid sdist SHA-256')
    actual = sha256_file(sdist)
    if actual != expected:
        raise AssertionError(f'sdist SHA-256 mismatch: {actual} != {expected}')
    if source.get('version') != source.get('filename', '').removeprefix(
        'gometry-'
    ).removesuffix('.tar.gz'):
        raise AssertionError(
            'source manifest sdist version does not match its filename'
        )
    validate_source_identity({
        'commit': manifest.get('commit'),
        'filename': source.get('filename'),
        'sha256': source.get('sha256'),
        'version': source.get('version'),
        'source_inputs_format': manifest.get('source_inputs_format'),
        'source_inputs_sha256': manifest.get('source_inputs_sha256'),
        'python_members': manifest.get('python_members'),
        'native_source_inputs_format': manifest.get('native_source_inputs_format'),
        'native_source_inputs_sha256': manifest.get('native_source_inputs_sha256'),
        'cargo': manifest.get('cargo'),
        'toolchain': manifest.get('toolchain'),
    })
    if manifest['toolchain']['channel'] != sdist_toolchain_channel(sdist):
        raise AssertionError(
            'source manifest toolchain channel differs from staged sdist'
        )
    cargo = manifest.get('cargo')
    if not isinstance(cargo, dict):
        raise TypeError('source manifest has no cargo evidence')
    for key in ('lock_preimage_sha256', 'lock_normalized_sha256'):
        value = cargo.get(key)
        if not isinstance(value, str) or HEX_DIGEST.fullmatch(value) is None:
            raise AssertionError(f'source manifest has an invalid {key}')
    prepared = manifest.get('prepared_inputs')
    if not isinstance(prepared, dict):
        raise TypeError('source manifest has no prepared input identity')
    identity = prepared.get('identity_sha256')
    files = prepared.get('files')
    if not isinstance(identity, str) or HEX_DIGEST.fullmatch(identity) is None:
        raise AssertionError('source manifest has an invalid prepared input identity')
    if not isinstance(files, dict) or not files:
        raise AssertionError('source manifest has no prepared input hashes')
    for name, digest in files.items():
        if not isinstance(name, str) or not isinstance(digest, str):
            raise TypeError('source manifest prepared input hashes are malformed')
        if HEX_DIGEST.fullmatch(digest) is None:
            raise AssertionError(f'source manifest has an invalid hash for {name}')
    if _prepared_input_identity(files) != identity:
        raise AssertionError(
            'source manifest prepared input identity does not match files'
        )
    generated = manifest.get('generated_output_hashes')
    if generated != files:
        raise AssertionError(
            'source manifest generated output hashes do not match inputs'
        )
    return manifest


def sdist_python_member_hashes(sdist: Path) -> dict[str, str]:
    with tempfile.TemporaryDirectory(prefix='gometry-source-members-') as tmp:
        extracted = Path(tmp)
        with tarfile.open(sdist) as archive:
            extract_sdist(archive, extracted)
        roots = [entry for entry in extracted.iterdir() if entry.is_dir()]
        if len(roots) != 1:
            raise AssertionError('source sdist has no unique package root')
        return python_member_hashes(roots[0] / 'python' / 'gometry')


def verify_source_artifact(sdist: Path, manifest_path: Path) -> dict[str, Any]:
    manifest = validate_source_manifest(sdist, manifest_path)
    prepared = manifest['prepared_inputs']['files']
    with tarfile.open(sdist) as archive:
        member_names = archive.getnames()
        if len(member_names) != len(set(member_names)):
            raise AssertionError(f'source sdist contains duplicate members: {sdist}')
        roots = sorted({
            Path(member.name).parts[0]
            for member in archive.getmembers()
            if member.name and not Path(member.name).is_absolute()
        })
        if len(roots) != 1:
            raise AssertionError(f'source sdist has no unique root: {sdist}')
        package_root = Path(roots[0]) / 'gometry'
        if f'{package_root.as_posix()}/Cargo.toml' not in member_names:
            raise AssertionError('source sdist has no unique gometry package root')
        for relative, expected in prepared.items():
            member = archive.extractfile(f'{roots[0]}/{relative}')
            if member is None:
                raise AssertionError(
                    f'source sdist is missing prepared output: {relative}'
                )
            actual = _sha256_bytes(member.read())
            if actual != expected:
                raise AssertionError(
                    f'source prepared output hash mismatch: {relative}'
                )
        actual_python_members = sdist_python_member_hashes(sdist)
        if actual_python_members != manifest['python_members']:
            raise AssertionError(
                'source manifest Python members do not match the sdist'
            )
    evidence = verify_sdist_dependency_sources(sdist)
    cargo = manifest['cargo']
    if (
        cargo['lock_preimage_sha256'] != evidence['preimage_lock_sha256']
        or cargo['lock_normalized_sha256'] != evidence['normalized_lock_sha256']
        or cargo['required_sources'] != evidence['required_sources']
    ):
        raise AssertionError('source manifest Cargo evidence does not match the sdist')
    for key in ('source_inputs_sha256', 'native_source_inputs_sha256'):
        if manifest[key] != evidence[key]:
            raise AssertionError(
                f'source manifest {key} does not match the sdist build inputs'
            )
    return manifest
