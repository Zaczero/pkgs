"""Run-owned artifact evidence and compact per-process benchmark identities."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


def identity_digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(',', ':')).encode()
    ).hexdigest()


def read_admission(path: Path, digest: str) -> dict[str, Any]:
    payload = path.read_bytes()
    if hashlib.sha256(payload).hexdigest() != digest:
        raise ValueError('benchmark admission evidence digest does not match')
    record = json.loads(payload)
    if (
        not isinstance(record, dict)
        or record.get('kind') != 'gometry-benchmark-admission'
    ):
        raise ValueError('invalid benchmark admission evidence')
    return record


def run_admission(
    manifest: dict[str, Any], manifest_path: Path
) -> tuple[dict[str, Any] | None, str | None]:
    reference = manifest.get('artifact_admission')
    if reference is None:
        return None, None
    if (
        not isinstance(reference, dict)
        or reference.get('completion_verified') is not True
    ):
        raise ValueError('benchmark run artifact admission is incomplete')
    relative = reference.get('path')
    if (
        not isinstance(relative, str)
        or Path(relative).is_absolute()
        or '..' in Path(relative).parts
    ):
        raise ValueError('benchmark admission path must stay inside the run')
    path = (manifest_path.parent / relative).resolve()
    path.relative_to(manifest_path.parent.resolve())
    return read_admission(path, reference['sha256']), reference['sha256']


def compact_artifact(artifact: dict[str, Any], admission_sha256: str) -> dict[str, Any]:
    """Store each process identity once and reference shared run evidence."""
    identities: dict[str, Any] = {}

    def compact(value: Any) -> Any:
        if isinstance(value, list):
            return [compact(item) for item in value]
        if not isinstance(value, dict):
            return value
        if 'identity_schema_version' not in value:
            return {key: compact(item) for key, item in value.items()}
        digest = identity_digest(value)
        binding = value['runtime_binding']
        if binding.get('admission_sha256') != admission_sha256:
            raise ValueError('process identity references a different run admission')
        identities[digest] = {
            key: item
            for key, item in value.items()
            if key not in {'harness_commit', 'uv_lock', 'runtime_binding'}
        } | {
            'runtime_binding': {
                key: item for key, item in binding.items() if key != 'python_members'
            }
        }
        return {'benchmark_identity': digest}

    result = compact(artifact)
    if 'benchmark_evidence' in result:
        raise ValueError('benchmark artifact already contains evidence references')
    result['benchmark_evidence'] = {
        'admission_sha256': admission_sha256,
        'identities': identities,
    }
    return result


def expand_artifact(
    artifact: dict[str, Any],
    admission: dict[str, Any] | None,
    admission_sha256: str | None,
) -> dict[str, Any]:
    """Resolve one worker's identities against its run's admitted evidence."""
    evidence = artifact.get('benchmark_evidence')
    if evidence is None:
        if admission is not None:
            raise ValueError('benchmark artifact omitted its run evidence reference')
        return artifact
    if (
        admission is None
        or not isinstance(evidence, dict)
        or set(evidence) != {'admission_sha256', 'identities'}
        or evidence['admission_sha256'] != admission_sha256
        or not isinstance(evidence['identities'], dict)
    ):
        raise ValueError('benchmark artifact has mismatched run evidence')
    identities = {}
    for digest, compact in evidence['identities'].items():
        if not isinstance(compact, dict):
            raise TypeError('invalid benchmark process identity reference')
        binding = compact.get('runtime_binding')
        if (
            not isinstance(binding, dict)
            or binding.get('admission_sha256') != admission_sha256
            or 'python_members' in binding
            or 'harness_commit' in compact
            or 'uv_lock' in compact
        ):
            raise ValueError('duplicate or mismatched shared benchmark identity')
        identity = compact | {
            'harness_commit': admission['harness_commit'],
            'uv_lock': admission['uv_lock'],
            'runtime_binding': binding
            | {'python_members': admission['runtime']['python_members']},
        }
        if identity_digest(identity) != digest:
            raise ValueError('benchmark process identity digest does not match')
        identities[digest] = identity
    used: set[str] = set()

    def expand(value: Any) -> Any:
        if isinstance(value, list):
            return [expand(item) for item in value]
        if not isinstance(value, dict):
            return value
        if 'benchmark_identity' in value:
            digest = value['benchmark_identity']
            if set(value) != {'benchmark_identity'} or digest not in identities:
                raise ValueError('unknown or duplicate benchmark identity reference')
            used.add(digest)
            return identities[digest]
        if 'identity_schema_version' in value:
            raise ValueError('inline benchmark identity duplicates run evidence')
        return {key: expand(item) for key, item in value.items()}

    result = expand({
        key: item for key, item in artifact.items() if key != 'benchmark_evidence'
    })
    if used != set(identities):
        raise ValueError('benchmark evidence contains an unreferenced process identity')
    return result


def expand_run_manifest(manifest: dict[str, Any], path: Path) -> dict[str, Any]:
    admission, digest = run_admission(manifest, path)
    return expand_artifact(manifest, admission, digest)
