"""Run evidence and actual-process checks without building a native artifact."""

# ruff: file-ignore[module-import-not-at-top-of-file] - Select checkout tooling before importing it.
from __future__ import annotations

import copy
import importlib.metadata
import json
import os
import sys
from pathlib import Path
from types import ModuleType

import pytest

GOMETRY_ROOT = Path(__file__).resolve().parents[2]
for directory in ('tools', 'benches/support', 'benches/drivers'):
    sys.path.insert(0, str(GOMETRY_ROOT / directory))

import bench_doctor
import benchmark_runtime
import runtime_artifact
from benchmark_evidence import (
    compact_artifact,
    expand_artifact,
    identity_digest,
    read_admission,
)


@pytest.fixture
def process_admission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[runtime_artifact.RuntimeAdmission, tuple[ModuleType, ModuleType, dict]]:
    package = tmp_path / 'gometry'
    package.mkdir()
    facade = package / '__init__.py'
    facade.write_text('value = 17\n')
    extension = package / '_lib.so'
    extension.write_bytes(b'original extension bytes')
    metadata = tmp_path / 'gometry-1.0.0.dist-info'
    metadata.mkdir()
    (metadata / 'METADATA').write_text('Name: gometry\nVersion: 1.0.0\n')
    (metadata / 'RECORD').write_text(
        'gometry/__init__.py,,\ngometry/_lib.so,,\ngometry-1.0.0.dist-info/METADATA,,\n'
    )
    distribution = next(importlib.metadata.distributions(path=[str(tmp_path)]))
    for name in tuple(sys.modules):
        if name == 'gometry' or name.startswith('gometry.'):
            monkeypatch.delitem(sys.modules, name)
    modules = []
    for name, path in (('gometry', facade), ('gometry._lib', extension)):
        module = ModuleType(name)
        module.__file__ = str(path)
        monkeypatch.setitem(sys.modules, name, module)
        modules.append(module)
    admission = runtime_artifact.RuntimeAdmission(
        root=tmp_path,
        receipt={'kind': 'gometry-benchmark-wheel'},
        package_root=tmp_path,
        package_dir=package,
        facade_path=facade,
        extension_path=extension,
        extension_member='gometry/_lib.so',
        extension_sha256=benchmark_runtime.sha256_file(extension),
        python_members={'gometry/__init__.py': benchmark_runtime.sha256_file(facade)},
        interpreter=runtime_artifact._runtime_interpreter(),
        target_policy={},
        distribution=distribution,
        distribution_version='1.0.0',
        distribution_metadata_sha256=benchmark_runtime.sha256_file(
            metadata / 'METADATA'
        ),
        distribution_files=frozenset(str(item) for item in distribution.files or ()),
        wheel=None,
        runtime_selection=None,
        shared_inputs=True,
    )
    return admission, (modules[0], modules[1], {})


def test_admitted_snapshot_binds_this_process_distribution(process_admission) -> None:
    admission, runtime = process_admission
    record = runtime_artifact.admission_record(admission)
    restored = runtime_artifact.admitted_runtime_inputs(record)
    runtime_artifact.verify_runtime_completion(restored, runtime)
    assert runtime_artifact.admission_record(restored) == record
    assert 'pid' not in record['interpreter']


def test_admitted_snapshot_rejects_another_interpreter(process_admission) -> None:
    admission, _ = process_admission
    record = runtime_artifact.admission_record(admission)
    record['interpreter']['prefix'] = '/another/venv'
    with pytest.raises(AssertionError, match='worker interpreter differs'):
        runtime_artifact.admitted_runtime_inputs(record)


@pytest.mark.parametrize('member', ['facade', 'extension'])
def test_process_completion_uses_bytes_not_file_stat(
    process_admission, member: str
) -> None:
    admission, runtime = process_admission
    path = getattr(admission, f'{member}_path')
    before = path.stat()
    payload = path.read_bytes()
    path.write_bytes(bytes(byte ^ 1 for byte in payload))
    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
    assert path.stat().st_size == before.st_size
    assert path.stat().st_mtime_ns == before.st_mtime_ns
    with pytest.raises(AssertionError, match='different Gometry'):
        runtime_artifact.verify_runtime_completion(admission, runtime)


def test_correct_admission_rejects_wrong_actual_extension(
    process_admission, tmp_path: Path
) -> None:
    admission, runtime = process_admission
    other = tmp_path / 'wrong_lib.so'
    other.write_bytes(admission.extension_path.read_bytes())
    runtime[1].__file__ = str(other)
    with pytest.raises(AssertionError, match='different Gometry native extension'):
        runtime_artifact.verify_runtime_completion(admission, runtime)


def test_process_completion_rejects_replaced_module_object(
    process_admission, monkeypatch: pytest.MonkeyPatch
) -> None:
    admission, runtime = process_admission
    replacement = ModuleType('gometry._lib')
    replacement.__file__ = str(admission.extension_path)
    monkeypatch.setitem(sys.modules, 'gometry._lib', replacement)
    with pytest.raises(AssertionError, match='modules changed'):
        runtime_artifact.verify_runtime_completion(admission, runtime)


def test_content_digest_does_not_reuse_preserved_stat(tmp_path: Path) -> None:
    path = tmp_path / 'library.so'
    path.write_bytes(b'first-library')
    before = path.stat()
    original = bench_doctor._content_digest(path)
    path.write_bytes(b'other-library')
    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
    assert bench_doctor._content_digest(path) != original


def test_worker_rejects_changed_distribution_record(process_admission) -> None:
    admission, _ = process_admission
    evidence = {
        'distribution': benchmark_runtime._distribution_identity(admission.distribution)
    }
    record_path = Path(evidence['distribution']['record']['path'])
    before = record_path.stat()
    payload = record_path.read_bytes()
    record_path.write_bytes(payload.replace(b'_lib.so', b'other.x'))
    os.utime(record_path, ns=(before.st_atime_ns, before.st_mtime_ns))
    with pytest.raises(AssertionError, match='distribution ownership differs'):
        benchmark_runtime._verify_distribution(admission, evidence)


@pytest.fixture
def identity_evidence() -> tuple[dict, dict, str]:
    admission_digest = 'a' * 64
    admission = {
        'harness_commit': 'b' * 40,
        'uv_lock': {'sha256': 'c' * 64},
        'runtime': {'python_members': {'gometry/__init__.py': 'd' * 64}},
    }
    identity = {
        'identity_schema_version': 1,
        'harness_commit': admission['harness_commit'],
        'uv_lock': admission['uv_lock'],
        'runtime_binding': {
            'admission_sha256': admission_digest,
            'python_members': admission['runtime']['python_members'],
        },
        'python': {'pid': 123},
    }
    return admission, identity, admission_digest


def test_worker_evidence_round_trip_stores_identity_once(identity_evidence) -> None:
    admission, identity, digest = identity_evidence
    artifact = {
        'before': identity,
        'after': identity,
        'observation': {'identity': identity},
    }
    compact = compact_artifact(artifact, digest)
    assert len(compact['benchmark_evidence']['identities']) == 1
    assert (
        'python_members'
        not in next(iter(compact['benchmark_evidence']['identities'].values()))[
            'runtime_binding'
        ]
    )
    assert expand_artifact(compact, admission, digest) == artifact


@pytest.mark.parametrize('mutation', ['reference', 'identity', 'duplicate', 'unused'])
def test_worker_evidence_rejects_changed_or_duplicate_references(
    identity_evidence, mutation: str
) -> None:
    admission, identity, digest = identity_evidence
    compact = compact_artifact({'identity': identity}, digest)
    table = compact['benchmark_evidence']['identities']
    entry = table[identity_digest(identity)]
    if mutation == 'reference':
        compact['benchmark_evidence']['admission_sha256'] = 'e' * 64
    elif mutation == 'identity':
        entry['python']['pid'] = 456
    elif mutation == 'duplicate':
        entry['runtime_binding']['python_members'] = {}
    else:
        compact['identity'] = {}
    with pytest.raises(ValueError, match=r'evidence|identity'):
        expand_artifact(compact, admission, digest)


def test_admission_digest_rejects_changed_receipt(tmp_path: Path) -> None:
    path = tmp_path / 'admission.json'
    path.write_text(json.dumps({'kind': 'gometry-benchmark-admission'}))
    digest = benchmark_runtime.sha256_file(path)
    assert read_admission(path, digest)['kind'] == 'gometry-benchmark-admission'
    before = path.stat()
    path.write_text(path.read_text().replace('admission', 'admissiom'))
    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
    with pytest.raises(ValueError, match='digest does not match'):
        read_admission(path, digest)


@pytest.mark.parametrize('filename', ['artifact.whl', 'receipt.json'])
def test_run_completion_rejects_changed_shared_artifact(
    tmp_path: Path, filename: str
) -> None:
    artifact = tmp_path / filename
    artifact.write_bytes(b'original artifact evidence')
    original = {
        'path': str(artifact),
        'receipt_sha256': benchmark_runtime.sha256_file(artifact),
    }
    record_path = tmp_path / 'admission.json'
    record_path.write_text(
        json.dumps({'kind': 'gometry-benchmark-admission', 'subject': original})
    )
    subject = {
        'run_admission': {
            'path': str(record_path),
            'sha256': benchmark_runtime.sha256_file(record_path),
        }
    }
    before = artifact.stat()
    artifact.write_bytes(b'modified artifact evidence')
    os.utime(artifact, ns=(before.st_atime_ns, before.st_mtime_ns))
    with pytest.raises(AssertionError, match=r'benchmark.*changed'):
        benchmark_runtime.complete_run_admission(subject)


def test_subject_cannot_acquire_duplicate_run_admissions(tmp_path: Path) -> None:
    with pytest.raises(AssertionError, match='already belongs'):
        benchmark_runtime.prepare_run_admission(
            {'run_admission': {}}, tmp_path, tmp_path / 'another-admission.json'
        )


def test_released_expanded_artifacts_need_no_new_evidence(identity_evidence) -> None:
    _, identity, _ = identity_evidence
    artifact = {'producer_identity': copy.deepcopy(identity)}
    assert expand_artifact(artifact, None, None) is artifact


def test_shared_run_rejects_unreferenced_inline_identity(identity_evidence) -> None:
    admission, identity, digest = identity_evidence
    compact = compact_artifact({'identity': identity}, digest)
    compact['extra_identity'] = identity
    with pytest.raises(ValueError, match='inline benchmark identity'):
        expand_artifact(compact, admission, digest)


def test_shared_run_requires_evidence_reference(identity_evidence) -> None:
    admission, identity, digest = identity_evidence
    with pytest.raises(ValueError, match='omitted its run evidence reference'):
        expand_artifact({'identity': identity}, admission, digest)


@pytest.fixture
def run_evidence(tmp_path: Path, identity_evidence) -> tuple[Path, dict, dict]:
    admission, identity, _ = identity_evidence
    admission['kind'] = 'gometry-benchmark-admission'
    admission_path = tmp_path / 'admission.json'
    admission_path.write_text(json.dumps(admission))
    digest = benchmark_runtime.sha256_file(admission_path)
    identity['runtime_binding']['admission_sha256'] = digest
    raw = tmp_path / 'raw.json'
    raw.write_text(json.dumps(compact_artifact({'identity': identity}, digest)))
    oracle = tmp_path / 'oracle.json'
    oracle.write_text(
        json.dumps(compact_artifact({'artifact_type': 'gometry-public-oracle'}, digest))
    )
    manifest = {
        'artifacts': ['raw.json', 'oracle.json'],
        'oracle_artifact': 'oracle.json',
        'artifact_admission': {
            'path': 'admission.json',
            'sha256': digest,
            'completion_verified': True,
        },
    }
    path = tmp_path / 'run.json'
    path.write_text(json.dumps(manifest))
    return path, manifest, identity


def test_summary_resolves_process_references_from_one_run_record(run_evidence) -> None:
    from summarize_bench import load_run_artifacts

    path, manifest, identity = run_evidence
    artifacts = load_run_artifacts(manifest, path)
    assert artifacts[path.parent / 'raw.json']['identity'] == identity


def test_run_manifest_shares_resource_process_identity(run_evidence) -> None:
    from summarize_bench import _manifest_payload

    path, manifest, identity = run_evidence
    manifest['resources'] = [{'producer_identity': identity}]
    manifest['commands'] = [{'producer_identity': identity}]
    compact = compact_artifact(manifest, manifest['artifact_admission']['sha256'])
    assert len(compact['benchmark_evidence']['identities']) == 1
    path.write_text(json.dumps(compact))
    assert _manifest_payload(path) == manifest


def test_summary_rejects_duplicate_artifact_references(run_evidence) -> None:
    from summarize_bench import load_run_artifacts

    path, manifest, _ = run_evidence
    manifest['artifacts'].append('raw.json')
    with pytest.raises(SystemExit, match='duplicate benchmark artifact'):
        load_run_artifacts(manifest, path)


def test_publication_retains_shared_admission_evidence(
    run_evidence, tmp_path: Path
) -> None:
    import bench
    from summarize_bench import load_run_artifacts

    path, manifest, identity = run_evidence
    stage = bench._stage_run_evidence(
        path, path.read_bytes(), manifest, tmp_path / 'bundles'
    )
    assert (stage / 'admission.json').read_bytes() == (
        path.parent / 'admission.json'
    ).read_bytes()
    artifacts = load_run_artifacts(manifest, stage / 'run.json')
    assert artifacts[stage / 'raw.json']['identity'] == identity


@pytest.mark.parametrize('damage', ['changed', 'incomplete'])
def test_publication_rejects_changed_or_incomplete_run_evidence(
    run_evidence, tmp_path: Path, damage: str
) -> None:
    import bench

    path, manifest, _ = run_evidence
    if damage == 'changed':
        (path.parent / 'admission.json').write_text('{}')
    else:
        manifest['artifact_admission']['completion_verified'] = False
    with pytest.raises(SystemExit, match='admission evidence'):
        bench._stage_run_evidence(
            path, path.read_bytes(), manifest, tmp_path / 'bundles'
        )
