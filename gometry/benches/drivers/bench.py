"""Run the bounded smoke or release benchmark manifest."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import signal
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path, PurePosixPath
from typing import Any

_BENCHES = Path(__file__).resolve().parents[1]
GOMETRY_ROOT = _BENCHES.parent
_SUPPORT = _BENCHES / 'support'
_DRIVERS = _BENCHES / 'drivers'
_PYTHON = _BENCHES / 'python'
_TOOLS = GOMETRY_ROOT / 'tools'
for _path in (_SUPPORT, _DRIVERS, _PYTHON, _TOOLS):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from _bench_pairs import pair_units
from _bench_registry import (
    ALL_PUBLIC_OPERATIONS,
    PROFILES,
    PAIR_WORKER,
    RELEASE_OPERATIONS,
    SCALING_OPERATIONS,
    SUITES,
    Profile,
    ReleaseOperation,
    ScalingOperation,
    expand_filter_to_pairs,
    scaling_record,
    validate_canonical_symbols,
    validate_scaling_operations,
)
from bench_doctor import (
    _ENGINE_LIBRARY_TOKENS,
    _distribution_owned_paths,
    collect,
    collect_contention,
    select_benchmark_cpu,
    selected_cpu_governor,
)
import benchmark_runtime
from benchmark_identity import validate_producer_identity
import artifact_content
import release_artifact as artifact_release
import source_artifact
import operator


def _serialize_public_operations(
    operations: tuple[ReleaseOperation | ScalingOperation, ...],
    selected: dict[str, tuple[str, ...]],
) -> list[dict[str, object]]:
    """Ordered operation records for the run JSON (summarizer input)."""
    selected_names = {name for rows in selected.values() for name in rows}
    records: list[dict[str, object]] = []
    for op in operations:
        if not any(name in selected_names for name in op.rows):
            continue
        record: dict[str, object] = {
            'domain': op.domain,
            'label': op.label,
            'workload': op.workload,
            'suite': op.suite,
            'canonical_symbols': list(op.canonical_symbols),
            'gometry': op.gometry,
            'competitor': op.competitor,
            'competitor_label': op.competitor_label,
            'footnotes': list(getattr(op, 'footnotes', ())),
            'phase': op.phase,
        }
        record['scaling'] = scaling_record(op)
        records.append(record)
    return records


_OUTPUT_TAIL = 10_000
_RUN_MANIFEST_TYPE = 'gometry-benchmark-run'
_MANIFEST_SCHEMA_VERSION = 1
_REQUIRED_ENGINE_LIBRARY_TOKENS = {
    'shapely': ('geos',),
    'pyproj': ('proj',),
}
_EVIDENCE_ROOT = GOMETRY_ROOT / 'docs' / 'assets' / 'benchmark-runs'


def _stamp() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).strftime('%Y%m%dT%H%M%SZ')


def _rows(profile: Profile) -> dict[str, tuple[str, ...]]:
    return {suite: profile.rows(suite) for suite in SUITES}


def _select(
    rows: dict[str, tuple[str, ...]],
    value: str | None,
) -> dict[str, tuple[str, ...]]:
    if value is None:
        return rows
    requested = {name.strip() for name in value.split(',') if name.strip()}
    if not requested:
        raise SystemExit('--filter must contain at least one benchmark row name')
    # Selecting either raw member expands to the complete pair.
    requested = expand_filter_to_pairs(requested)
    available = {name for suite_rows in rows.values() for name in suite_rows}
    unknown = sorted(requested - available)
    if unknown:
        raise SystemExit(
            'benchmark filter is outside the selected manifest/profile: '
            + ', '.join(unknown)
        )
    return {
        suite: tuple(name for name in suite_rows if name in requested)
        for suite, suite_rows in rows.items()
        if any(name in requested for name in suite_rows)
    }


def _validate_release_manifest(profile: Profile) -> None:
    suite = 'competitors'
    try:
        validate_canonical_symbols(profile.operations)
        validate_scaling_operations(profile.all_operations)
    except ValueError as exc:
        raise SystemExit(
            f'{suite} release manifest has invalid canonical endpoint: {exc}'
        ) from exc
    unpaired = [
        unit[0]
        for unit in pair_units(profile.rows(suite), suite=suite)
        if len(unit) != 2
    ]
    if unpaired:
        raise SystemExit(
            f'{suite} release manifest has unpaired rows: ' + ', '.join(unpaired)
        )


def _validate_oracle_builders(selected: dict[str, tuple[str, ...]]) -> None:
    """Fail closed before timing/planning when a selected op has no builder.

    Manifest↔builder must be an exact bijection for every selected gometry
    row; orphans against the full public set are also rejected.
    """
    # Local import: builders register numpy-heavy public cases.
    from _bench_oracles import PUBLIC_CASE_BUILDERS, PUBLIC_SCALING_CASE_BUILDERS
    import _bench_public_cases as _public_cases  # ruff: ignore[unused-import]
    import _bench_scaling_cases as _scaling_cases  # ruff: ignore[unused-import]
    from bench_oracle import validate_builders

    gometry_names = [
        name
        for suite_rows in selected.values()
        for name in suite_rows
        if name.startswith('gometry.')
    ]
    by_name = {op.gometry: op for op in ALL_PUBLIC_OPERATIONS}
    by_any = {row: op for op in ALL_PUBLIC_OPERATIONS for row in op.rows}
    seen: set[str] = set()
    operations = []
    for name in gometry_names:
        op = by_name.get(name) or by_any.get(name)
        if op is None or op.gometry in seen:
            continue
        seen.add(op.gometry)
        operations.append(op)
    if not operations:
        # Full-manifest oracle path (--all) when no gometry rows selected.
        operations = list(RELEASE_OPERATIONS)
    base = [op for op in operations if not isinstance(op, ScalingOperation)]
    scaling = [op for op in operations if isinstance(op, ScalingOperation)]
    validate_builders(base, PUBLIC_CASE_BUILDERS)
    if scaling:
        validate_builders(
            scaling,
            PUBLIC_SCALING_CASE_BUILDERS,
            public_operations=tuple(SCALING_OPERATIONS),
        )


def _oracle_command(
    selected: dict[str, tuple[str, ...]], *, output: Path | None = None
) -> list[str]:
    """Build the single oracle subprocess command for selected logical ops."""
    gometry_names = [
        name
        for suite_rows in selected.values()
        for name in suite_rows
        if name.startswith('gometry.')
    ]
    # Deduplicate while preserving order
    seen: set[str] = set()
    ordered: list[str] = []
    for name in gometry_names:
        if name not in seen:
            seen.add(name)
            ordered.append(name)
    command = [
        sys.executable,
        'benches/python/bench_oracle.py',
    ]
    if not ordered:
        command.append('--all')
    else:
        command.extend(('--operations', ','.join(ordered)))
    if output is not None:
        command.extend(('--output', str(output)))
    return command


def _benchmark_environment(
    subject: dict[str, Any],
    package_root: Path,
    *,
    invocation_id: str,
    control: bool,
    side: str | None = None,
    row: str | None = None,
) -> dict[str, str]:
    environment = benchmark_runtime.environment_for_subject(
        subject,
        package_root,
        invocation_id=invocation_id,
        control=control,
    )
    names = {
        'PYTHONPATH',
        'GOMETRY_BENCH_SUBJECT_PATH',
        'GOMETRY_BENCH_SUBJECT_SHA256',
        'GOMETRY_BENCH_PACKAGE_ROOT',
        'GOMETRY_BENCH_INVOCATION_ID',
        'GOMETRY_BENCH_CONTROL',
    }
    if side is not None:
        environment['GOMETRY_BENCH_SIDE'] = side
        names.add('GOMETRY_BENCH_SIDE')
    if row is not None:
        environment['GOMETRY_BENCH_ROW'] = row
        names.add('GOMETRY_BENCH_ROW')
    return {name: environment[name] for name in names if name in environment}


def _paired_command(
    *,
    gometry: str,
    pair_id: str,
    block_id: str,
    lead: str,
    output: Path,
    profile: Profile,
    cpu: int,
    receipt_binding: dict[str, Any],
) -> list[str]:
    command = [
        'taskset',
        '--cpu-list',
        str(cpu),
        sys.executable,
        f'benches/python/{PAIR_WORKER}',
        '--gometry',
        gometry,
        '--pair-id',
        pair_id,
        '--block-id',
        block_id,
        '--lead',
        lead,
        *profile.paired_sampling_args,
        '--output',
        str(output),
    ]
    command.extend((
        '--gometry-receipt',
        receipt_binding['path'],
        '--receipt-sha256',
        receipt_binding['receipt_sha256'],
    ))
    return command


def _run(
    command: list[str],
    *,
    environment: dict[str, str] | None,
    timeout: int,
    benchmark_subject: dict[str, Any] | None = None,
    package_root: Path | None = None,
    case_path: Path | None = None,
    result_path: Path | None = None,
) -> dict[str, Any]:
    env = os.environ | (environment or {})
    env.pop('RUSTC_WRAPPER', None)
    started = time.monotonic()
    if env.get('GOMETRY_BENCH_CONTROL') == '1':
        if (
            benchmark_subject is None
            or package_root is None
            or case_path is None
            or result_path is None
        ):
            raise SystemExit(
                'controlled resource benchmark lacks its admitted subject or result path'
            )
        controlled = benchmark_runtime.run_controlled_process(
            command,
            case=case_path,
            side='A',
            subject=benchmark_subject,
            package_root=package_root,
            timeout=timeout,
            environment=env,
            result_path=result_path,
        )
        return {
            'command': command,
            'returncode': 0,
            'timeout': False,
            'elapsed_seconds': time.monotonic() - started,
            'stdout_tail': controlled['stdout_tail'],
            'stderr_tail': controlled['stderr_tail'],
            'case_process': controlled['case_process'],
            'producer_identity': controlled['producer_identity'],
            'measurement_done_monotonic_ns': controlled[
                'measurement_done_monotonic_ns'
            ],
        }
    process = subprocess.Popen(
        command,
        cwd=GOMETRY_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
        text=True,
    )
    timed_out = False
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        stdout, stderr = _stop_process_group(process)
    except KeyboardInterrupt:
        _stop_process_group(process)
        raise
    return {
        'command': command,
        'returncode': 124 if timed_out else process.returncode,
        'timeout': timed_out,
        'elapsed_seconds': time.monotonic() - started,
        'stdout_tail': stdout[-_OUTPUT_TAIL:],
        'stderr_tail': stderr[-_OUTPUT_TAIL:],
    }


def _stop_process_group(process: subprocess.Popen[str]) -> tuple[str, str]:
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        return process.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        return process.communicate()


def _benchmark_plans(
    *,
    profile_name: str,
    profile: Profile,
    selected: dict[str, tuple[str, ...]],
    output_dir: Path,
    timestamp: str,
    cpu: int,
    receipt_binding: dict[str, Any],
    package_root: Path,
) -> tuple[list[dict[str, Any]], dict[str, list[Path]]]:
    plans: list[dict[str, Any]] = []
    outputs: dict[str, list[Path]] = {suite: [] for suite in selected}
    for suite, rows in selected.items():
        units = pair_units(rows, suite=suite)
        scheduled = []
        for number, unit in enumerate(units, start=1):
            if len(unit) != 2:
                raise SystemExit(f'public benchmark unit is not paired: {unit!r}')
            for block_number in range(profile.paired_blocks):
                lead = 'ab' if block_number % 2 == 0 else 'ba'
                scheduled.append((
                    f'pair-{number:02d}-block-{block_number + 1:02d}-{lead}',
                    unit,
                ))
        for label, scheduled_rows in scheduled:
            run_label = f'{suite}-{label}'
            output = output_dir / f'{timestamp}-{run_label}-{profile_name}.json'
            outputs[suite].append(output)
            command: list[str]
            environment: dict[str, str]
            if len(scheduled_rows) == 2 and label.startswith('pair-'):
                pair_number, block_spec = label.removeprefix('pair-').split(
                    '-block-', 1
                )
                _, lead = block_spec.split('-', 1)
                invocation_id = uuid.uuid4().hex
                command = _paired_command(
                    gometry=scheduled_rows[0],
                    pair_id=f'{suite}-pair-{pair_number}',
                    block_id=run_label,
                    lead=lead,
                    output=output,
                    profile=profile,
                    cpu=cpu,
                    receipt_binding=receipt_binding,
                )
                environment = _benchmark_environment(
                    receipt_binding,
                    package_root,
                    invocation_id=invocation_id,
                    control=False,
                )
            else:
                raise SystemExit(f'unsupported public benchmark schedule: {label}')
            plans.append({
                'kind': 'paired-block',
                'suite': suite,
                'label': run_label,
                'rows': scheduled_rows,
                'environment': environment,
                'command': command,
                'output': str(output),
            })
    return plans, outputs


def _resource_plans(
    *,
    output_dir: Path,
    timestamp: str,
    cpu: int,
    receipt_binding: dict[str, Any],
    package_root: Path,
) -> list[dict[str, Any]]:
    plans: list[dict[str, Any]] = []
    case_path = GOMETRY_ROOT / 'benches' / 'cases' / 'case_resource_tail.py'
    for mode in ('bearing', 'features', 'intersects', 'mixed'):
        output = output_dir / f'{timestamp}-resource-{mode}-release.json'
        invocation_id = uuid.uuid4().hex
        environment = _benchmark_environment(
            receipt_binding,
            package_root,
            invocation_id=invocation_id,
            control=True,
            side='A',
            row=str(case_path.resolve()),
        )
        plans.append({
            'kind': 'resource',
            'suite': 'resources',
            'label': f'resource-{mode}',
            'rows': (),
            'environment': environment,
            'case_path': str(case_path),
            'command': [
                'taskset',
                '--cpu-list',
                str(cpu),
                sys.executable,
                'benches/cases/case_resource_tail.py',
                '--mode',
                mode,
                '--output',
                str(output),
            ],
            'output': str(output),
        })
    return plans


def _resource_summaries(
    plans: list[dict[str, Any]],
    *,
    subject: dict[str, Any],
) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for plan in plans:
        output = Path(plan['output'])
        if plan['kind'] != 'resource' or not output.is_file():
            continue
        payload = json.loads(output.read_text(encoding='utf-8'))
        identity = payload.get('producer_identity')
        case_process = payload.get('case_process')
        case_path = Path(plan['case_path']).resolve()
        expected_case = {
            'path': str(case_path),
            'sha256': _file_sha256(case_path),
        }
        process_pid = (
            case_process.get('pid') if isinstance(case_process, dict) else None
        )
        process_invocation = (
            case_process.get('invocation_id')
            if isinstance(case_process, dict)
            else None
        )
        if (
            not isinstance(identity, dict)
            or not isinstance(case_process, dict)
            or case_process.get('case') != expected_case
            or not isinstance(process_pid, int)
            or isinstance(process_pid, bool)
            or process_pid <= 0
            or not isinstance(process_invocation, str)
            or not process_invocation
            or case_process.get('identity_sha256')
            != benchmark_runtime.identity_sha256(identity)
        ):
            raise SystemExit('resource result is not bound to its measured process')
        admitted = benchmark_runtime.validate_identity_for_subject(
            identity,
            subject,
            side='A',
            row=str(case_path),
            verify_archive=False,
        )
        if admitted != identity:
            raise SystemExit('resource result identity differs from its subject')
        summaries.append({
            key: payload[key]
            for key in (
                'mode',
                'median_seconds',
                'p99_seconds',
                'p999_seconds',
                'rss_current_isolated_endpoint_delta_kib',
                'pss_current_isolated_endpoint_delta_kib',
                'rss_current_latency_endpoint_delta_kib',
                'pss_current_latency_endpoint_delta_kib',
                'python_peak_traced_bytes',
                'python_allocated_blocks_delta',
                'producer_identity',
                'case_process',
            )
        })
    return summaries


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json_object(path: Path, *, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f'{label} {path} is not valid JSON: {exc}') from exc
    if not isinstance(payload, dict):
        raise SystemExit(f'{label} {path} must contain a JSON object')
    return payload


def _generated_harness_output(relative: str, package_name: str) -> bool:
    path = PurePosixPath(relative)
    return (
        '__pycache__' in path.parts
        or path.suffix in {'.pyc', '.pyo'}
        or path.parts[:3] == (package_name, 'benches', 'results')
    )


def _benchmark_harness_matches_commit(
    commit: str,
    *,
    package_root: Path = GOMETRY_ROOT,
) -> bool:
    """Compare benchmark inputs with their recorded tree, not its later commits."""
    repository = package_root.parent
    package_name = package_root.name
    try:
        diff = subprocess.run(
            [
                'git',
                'diff',
                '--quiet',
                '--no-ext-diff',
                '--no-renames',
                '--ignore-submodules=none',
                commit,
                '--',
                f'{package_name}/benches',
                f':(exclude){package_name}/benches/results',
            ],
            cwd=repository,
            check=False,
            capture_output=True,
        )
        if diff.returncode not in (0, 1):
            raise SystemExit(
                'publication rejected: cannot compare benchmark harness content '
                f'with recorded commit {commit}: '
                f'{diff.stderr.decode("utf-8", errors="replace").strip()}'
            )
        if diff.returncode:
            return False
        untracked = subprocess.run(
            [
                'git',
                'ls-files',
                '--others',
                '--full-name',
                '-z',
                '--',
                f'{package_name}/benches',
            ],
            cwd=repository,
            check=True,
            capture_output=True,
        )
        untracked_paths = [
            item.decode('utf-8') for item in untracked.stdout.split(b'\0') if item
        ]
    except (OSError, subprocess.CalledProcessError, UnicodeDecodeError) as error:
        raise SystemExit(
            f'publication rejected: cannot inspect benchmark harness content: {error}'
        ) from error
    return not any(
        not _generated_harness_output(relative, package_name)
        for relative in untracked_paths
    )


def _confined_run_artifact(
    run_directory: Path,
    relative: str,
    *,
    label: str,
) -> tuple[str, Path]:
    """Resolve one manifest path without allowing it to escape the run."""
    if not isinstance(relative, str) or not relative:
        raise SystemExit(
            f'publication rejected: {label} path must be a non-empty relative path'
        )
    candidate = PurePosixPath(relative)
    if candidate.is_absolute() or '..' in candidate.parts:
        raise SystemExit(
            f'publication rejected: {label} path escapes the run directory: {relative}'
        )
    normalized = candidate.as_posix()
    path = run_directory / Path(*candidate.parts)
    try:
        resolved = path.resolve(strict=True)
        resolved.relative_to(run_directory.resolve())
    except (OSError, RuntimeError, ValueError) as error:
        raise SystemExit(
            f'publication rejected: {label} path is missing or escapes the run '
            f'directory: {relative}'
        ) from error
    if not resolved.is_file():
        raise SystemExit(f'publication rejected: {label} is not a file: {relative}')
    return normalized, resolved


def _stage_run_evidence(
    run_manifest: Path,
    manifest_bytes: bytes,
    result: dict[str, Any],
    evidence_root: Path,
) -> Path:
    """Copy a run and its listed artifacts into a private publication stage."""
    artifacts = result.get('artifacts')
    if not isinstance(artifacts, list):
        raise SystemExit('publication rejected: run artifact list is missing')
    run_directory = run_manifest.resolve().parent
    sources: dict[str, Path] = {}
    for artifact in artifacts:
        relative, source = _confined_run_artifact(
            run_directory, artifact, label='run artifact'
        )
        if relative == 'run.json' or relative.startswith('receipts/'):
            raise SystemExit(
                f'publication rejected: reserved evidence path: {relative}'
            )
        sources[relative] = source
    oracle = result.get('oracle_artifact')
    if not isinstance(oracle, str) or not oracle:
        raise SystemExit('publication rejected: external oracle artifact is missing')
    relative_oracle, oracle_path = _confined_run_artifact(
        run_directory, oracle, label='external oracle artifact'
    )
    if relative_oracle == 'run.json' or relative_oracle.startswith('receipts/'):
        raise SystemExit(
            f'publication rejected: reserved evidence path: {relative_oracle}'
        )
    sources[relative_oracle] = oracle_path

    evidence_root.mkdir(parents=True, exist_ok=True)
    stage: Path | None = None
    try:
        stage = Path(tempfile.mkdtemp(prefix='.pending-', dir=evidence_root))
        (stage / 'run.json').write_bytes(manifest_bytes)
        for relative, source in sorted(sources.items()):
            destination = stage / Path(*PurePosixPath(relative).parts)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
    except OSError as error:
        if stage is not None:
            shutil.rmtree(stage, ignore_errors=True)
        raise SystemExit(
            f'publication rejected: cannot stage benchmark run evidence: {error}'
        ) from error
    except BaseException:
        if stage is not None:
            shutil.rmtree(stage, ignore_errors=True)
        raise
    if stage is None:
        raise AssertionError('benchmark evidence stage was not created')
    return stage


def _retain_producer_receipts(
    stage: Path,
    receipt_cache: dict[tuple[str, str], dict[str, Any]],
) -> None:
    """Copy validated producer receipts and index their recorded source paths."""
    receipt_directory = stage / 'receipts'
    receipt_directory.mkdir()
    records = []
    for (receipt_path, expected_digest), _receipt in sorted(receipt_cache.items()):
        try:
            payload = Path(receipt_path).read_bytes()
        except OSError as error:
            raise SystemExit(
                f'publication rejected: selected producer receipt is unavailable: '
                f'{receipt_path}: {error}'
            ) from error
        digest = hashlib.sha256(payload).hexdigest()
        if digest != expected_digest:
            raise SystemExit(
                f'publication rejected: selected producer receipt changed: '
                f'{receipt_path}'
            )
        relative = f'receipts/{digest}.json'
        destination = stage / relative
        if not destination.exists():
            destination.write_bytes(payload)
        records.append({
            'path': receipt_path,
            'sha256': digest,
            'bundle_path': relative,
        })
    index = {
        'schema_version': 1,
        'receipts': sorted(records, key=operator.itemgetter('path', 'sha256')),
    }
    (receipt_directory / 'index.json').write_text(
        json.dumps(index, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )


def _evidence_digest(directory: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(item for item in directory.rglob('*') if item.is_file()):
        digest.update(path.relative_to(directory).as_posix().encode('utf-8'))
        digest.update(b'\0')
        with path.open('rb') as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                digest.update(chunk)
        digest.update(b'\0')
    return digest.hexdigest()


def _evidence_bundle_matches(stage: Path, existing: Path) -> bool:
    if existing.is_symlink() or not existing.is_dir():
        return False
    staged_paths = list(stage.rglob('*'))
    existing_paths = list(existing.rglob('*'))
    if any(path.is_symlink() for path in existing_paths):
        return False
    staged_files = {
        path.relative_to(stage).as_posix(): path
        for path in staged_paths
        if path.is_file()
    }
    existing_files = {
        path.relative_to(existing).as_posix(): path
        for path in existing_paths
        if path.is_file()
    }
    return staged_files.keys() == existing_files.keys() and all(
        _file_sha256(staged_files[relative]) == _file_sha256(existing_files[relative])
        for relative in staged_files
    )


def _publish_evidence_bundle(stage: Path) -> str:
    digest = _evidence_digest(stage)
    target = stage.parent / digest
    if target.exists() or target.is_symlink():
        if not _evidence_bundle_matches(stage, target):
            raise SystemExit(
                'publication rejected: existing benchmark evidence bundle '
                f'{digest} does not match its content address'
            )
        shutil.rmtree(stage)
    else:
        try:
            stage.rename(target)
        except OSError as error:
            if not _evidence_bundle_matches(stage, target):
                raise SystemExit(
                    f'publication rejected: cannot publish benchmark evidence '
                    f'bundle {digest}: {error}'
                ) from error
            shutil.rmtree(stage)
    return digest


def _write_json_once(path: Path, payload: dict[str, Any], *, label: str) -> None:
    """Write a manifest once; an existing path is never replaced."""
    try:
        with path.open('x', encoding='utf-8') as stream:
            json.dump(payload, stream, indent=2, sort_keys=True)
            stream.write('\n')
    except FileExistsError as exc:
        raise SystemExit(f'{label} already exists: {path}') from exc


def _publication_metadata(result: dict[str, Any]) -> dict[str, Any]:
    doctor = result.get('doctor')
    metadata = doctor.get('metadata') if isinstance(doctor, dict) else None
    if not isinstance(metadata, dict):
        raise SystemExit('publication rejected: benchmark identity metadata is missing')
    required = (
        'harness_commit',
        'source_inputs_sha256',
        'native_source_inputs_sha256',
        'python_executable',
        'python',
        'python_version',
        'python_build',
        'python_implementation',
        'python_debug',
        'python_cache_tag',
        'extension',
        'uv_lock',
        'packages',
        'cpu_model',
        'cpu_governor',
        'cpu_affinity',
        'isolated_cpus',
        'selected_cpu',
        'selected_cpu_source',
        'selected_cpu_governor',
        'cpu_frequency_state',
        'cpu_topology',
        'cache_topology',
        'numa_topology',
        'allowed_memory_nodes',
        'memory_policy',
        'memory_placement',
        'cgroup_v2',
        'loaded_native_libraries',
        'native_library_identity',
    )
    missing = [key for key in required if key not in metadata]
    if missing:
        raise SystemExit(
            'publication rejected: missing benchmark identity metadata: '
            + ', '.join(missing)
        )
    extension = metadata['extension']
    if (
        not isinstance(extension, dict)
        or not extension.get('path')
        or not extension.get('sha256')
        or not extension.get('size_bytes')
        or not isinstance(extension.get('build_info'), dict)
    ):
        raise SystemExit('publication rejected: extension path/digest/size is missing')
    build_info = extension['build_info']
    if (
        build_info.get('profile_family') != 'release'
        or build_info.get('opt_level') != '3'
    ):
        raise SystemExit(
            'publication rejected: extension effective build facts are not release optimized'
        )
    if build_info.get('debug_assertions') is not False:
        raise SystemExit('publication rejected: extension has debug assertions enabled')
    if not isinstance(metadata['uv_lock'], dict):
        raise SystemExit('publication rejected: uv.lock identity metadata is malformed')
    lock = metadata['uv_lock']
    lock_path = lock.get('path')
    lock_digest = lock.get('sha256')
    if any(value is not None for value in lock.values()):
        if (
            not isinstance(lock_path, str)
            or not lock_path
            or not isinstance(lock_digest, str)
            or len(lock_digest) != 64
            or not isinstance(lock.get('size_bytes'), int)
            or lock.get('size_bytes', 0) <= 0
        ):
            raise SystemExit(
                'publication rejected: dependency lock identity is malformed'
            )
        lock_file = Path(lock_path)
        if not lock_file.is_file() or _file_sha256(lock_file) != lock_digest:
            raise SystemExit(
                'publication rejected: dependency lock identity is stale or unavailable'
            )
    if not metadata.get('harness_commit') or not metadata.get('source_inputs_sha256'):
        raise SystemExit(
            'publication rejected: harness commit/input identity is missing'
        )
    if not metadata.get('native_source_inputs_sha256'):
        raise SystemExit('publication rejected: native source identity is missing')
    packages = metadata['packages']
    required_packages = {
        'gometry',
        'numpy',
        'shapely',
        'pyproj',
        'h3',
        'geopandas',
        'pyarrow',
        'mercantile',
        'pygeohash',
    }
    if not isinstance(packages, dict):
        raise SystemExit('publication rejected: package version inventory is missing')
    missing_versions = [
        package
        for package in sorted(required_packages)
        if not isinstance(packages.get(package), dict)
        or packages[package].get('available') is not True
        or not packages[package].get('version')
        or not isinstance(packages[package].get('module'), dict)
        or not isinstance(packages[package].get('distribution'), dict)
        or packages[package]['distribution'].get('record_available') is not True
    ]
    if missing_versions:
        raise SystemExit(
            'publication rejected: missing package versions: '
            + ', '.join(missing_versions)
        )
    if not metadata.get('geos_version'):
        raise SystemExit('publication rejected: GEOS version is missing')
    if not metadata.get('proj_version'):
        raise SystemExit('publication rejected: PROJ version is missing')
    native_capability = metadata['native_library_identity']
    if (
        not isinstance(native_capability, dict)
        or native_capability.get('available') is not True
        or not native_capability.get('source')
    ):
        raise SystemExit(
            'publication rejected: loaded native-library identity is unavailable '
            '(native comparison evidence is not publishable)'
        )
    hard_failures = doctor.get('hard_failures', ()) if isinstance(doctor, dict) else ()
    if hard_failures:
        raise SystemExit(
            'publication rejected: benchmark identity has hard failures: '
            + '; '.join(str(value) for value in hard_failures)
        )
    return metadata


def _validate_publication_result(result: dict[str, Any]) -> dict[str, Any]:
    if result.get('profile') != 'release':
        raise SystemExit('publication rejected: only a release profile is publishable')
    if result.get('plan_only') or not result.get('full_manifest'):
        raise SystemExit('publication rejected: filtered or plan-only run')
    if result.get('filter') is not None:
        raise SystemExit('publication rejected: filtered run')
    if not result.get('oracle_ok'):
        raise SystemExit('publication rejected: oracle did not pass')
    if not result.get('commands_complete'):
        raise SystemExit('publication rejected: run is incomplete')
    if not result.get('publishable'):
        raise SystemExit('publication rejected: run is not marked publishable')
    row_counts = result.get('row_counts')
    if (
        not isinstance(row_counts, dict)
        or not all(isinstance(value, int) for value in row_counts.values())
        or sum(row_counts.values()) != sum(len(op.rows) for op in ALL_PUBLIC_OPERATIONS)
    ):
        raise SystemExit(
            'publication rejected: release does not contain the complete paired row matrix'
        )
    operations = result.get('public_operations')
    if not isinstance(operations, list) or len(operations) != len(
        ALL_PUBLIC_OPERATIONS
    ):
        raise SystemExit(
            'publication rejected: release does not contain the complete operation matrix'
        )
    for operation in operations:
        if not isinstance(operation, dict) or not operation.get('competitor'):
            raise SystemExit(
                'publication rejected: operation matrix is not fully paired'
            )
    sampling = result.get('sampling')
    if not isinstance(sampling, dict):
        raise SystemExit('publication rejected: paired sampling matrix is missing')
    if sampling.get('measured_blocks') != 6 or sampling.get('lead_orders') != [
        'ab',
        'ba',
        'ab',
        'ba',
        'ab',
        'ba',
    ]:
        raise SystemExit(
            'publication rejected: release requires exactly six paired blocks '
            '(three ab and three ba)'
        )
    return _publication_metadata(result)


def _validate_oracle_artifact(result: dict[str, Any], run_directory: Path) -> None:
    """Require the untimed oracle to be a distinct, complete process artifact."""
    value = result.get('oracle_artifact')
    if not isinstance(value, str) or not value:
        raise SystemExit('publication rejected: external oracle artifact is missing')
    path = run_directory / value
    if not path.is_file():
        raise SystemExit('publication rejected: external oracle artifact is missing')
    payload = _read_json_object(path, label='external oracle artifact')
    expected = [
        operation['gometry']
        for operation in result.get('public_operations', ())
        if isinstance(operation, dict) and isinstance(operation.get('gometry'), str)
    ]
    if (
        payload.get('schema_version') != 1
        or payload.get('artifact_type') != 'gometry-public-oracle'
        or payload.get('state_scope') != 'external-fresh-process'
        or payload.get('operations') != expected
        or payload.get('identities') != [f'public:{name}:oracle' for name in expected]
        or payload.get('verified') != len(expected)
    ):
        raise SystemExit(
            'publication rejected: external oracle artifact is incomplete or mismatched'
        )


def _validate_block_identity(
    identity: dict[str, Any],
    metadata: dict[str, Any],
    *,
    receipt_cache: dict[tuple[str, str], dict[str, Any]],
    source_commit_cache: dict[str | None, bool],
    current_python_members: dict[str, str],
) -> None:
    """Check one measured arm against the run's recorded build and dependencies."""
    try:
        validate_producer_identity(identity)
    except (TypeError, ValueError) as error:
        raise SystemExit(
            f'publication rejected: paired block producer identity is invalid: {error}'
        ) from error
    expected_commit = metadata['harness_commit']
    expected_source = metadata['source_inputs_sha256']
    expected_extension_record = metadata['extension']
    expected_extension = expected_extension_record['sha256']
    expected_lock = metadata['uv_lock']
    expected_python = {
        key: metadata[key]
        for key in (
            'python_executable',
            'python_version',
            'python_implementation',
            'python_cache_tag',
            'python_build',
            'python_debug',
            'python_prefix',
            'python_ext_suffix',
            'python_gil_disabled',
            'python_gil_enabled',
        )
    }
    expected_packages = metadata['packages']
    if expected_commit and identity.get('harness_commit') != expected_commit:
        raise SystemExit('publication rejected: paired block harness identity mismatch')
    if expected_source and identity.get('source_inputs_sha256') != expected_source:
        raise SystemExit('publication rejected: paired block source identity mismatch')
    observed_extension = identity.get('extension')
    observed_extension_digest = (
        observed_extension.get('sha256')
        if isinstance(observed_extension, dict)
        else identity.get('extension_sha256')
    )
    if expected_extension and observed_extension_digest != expected_extension:
        raise SystemExit(
            'publication rejected: paired block extension identity mismatch'
        )
    if (
        isinstance(expected_extension_record, dict)
        and isinstance(observed_extension, dict)
        and observed_extension.get('build_info')
        != expected_extension_record.get('build_info')
    ):
        raise SystemExit(
            'publication rejected: paired block effective build identity mismatch'
        )
    if isinstance(expected_lock, dict) and identity.get('uv_lock') != expected_lock:
        raise SystemExit(
            'publication rejected: paired block dependency lock identity mismatch'
        )
    observed_python = identity.get('python')
    if isinstance(observed_python, dict):
        if observed_python.get('resolved_executable') != expected_python.get(
            'python_executable'
        ):
            raise SystemExit(
                'publication rejected: paired block Python executable identity mismatch'
            )
        if observed_python.get('version') != expected_python.get('python_version'):
            raise SystemExit(
                'publication rejected: paired block Python build identity mismatch'
            )
        if observed_python.get('implementation') != expected_python.get(
            'python_implementation'
        ):
            raise SystemExit(
                'publication rejected: paired block Python implementation identity mismatch'
            )
        if observed_python.get('cache_tag') != expected_python.get('python_cache_tag'):
            raise SystemExit(
                'publication rejected: paired block Python cache identity mismatch'
            )
        for field, expected_field in (
            ('prefix', 'python_prefix'),
            ('ext_suffix', 'python_ext_suffix'),
            ('gil_disabled', 'python_gil_disabled'),
            ('gil_enabled', 'python_gil_enabled'),
        ):
            if observed_python.get(field) != expected_python.get(expected_field):
                raise SystemExit(
                    f'publication rejected: paired block Python {field} mismatch'
                )
        observed_build = observed_python.get('build')
        if isinstance(observed_build, dict):
            if observed_build.get('build') != expected_python.get('python_build'):
                raise SystemExit(
                    'publication rejected: paired block Python build tuple mismatch'
                )
            if observed_build.get('debug') != expected_python.get('python_debug'):
                raise SystemExit(
                    'publication rejected: paired block Python debug identity mismatch'
                )
    loaded = identity.get('loaded_native_libraries')
    if not isinstance(loaded, list):
        raise SystemExit(
            'publication rejected: paired block loaded native-library inventory is missing'
        )
    loaded_by_path = {
        item.get('path'): item
        for item in loaded
        if isinstance(item, dict) and isinstance(item.get('path'), str)
    }
    implementation = identity.get('implementation')
    if not isinstance(implementation, dict):
        raise SystemExit(
            'publication rejected: paired block implementation attribution is missing'
        )
    source_commit = implementation.get('source_commit')
    source_receipt = implementation.get('source_receipt')
    source_wheel = implementation.get('source_wheel')
    runtime_binding = identity.get('runtime_binding')
    if not isinstance(runtime_binding, dict):
        raise SystemExit('publication rejected: measured runtime binding is missing')
    if runtime_binding.get('profile') != artifact_release.RELEASE_PROFILE:
        raise SystemExit('publication rejected: DEV producer is not release evidence')
    receipt_path = runtime_binding.get('path')
    receipt_sha = runtime_binding.get('sha256')
    if not isinstance(receipt_path, str) or not isinstance(receipt_sha, str):
        raise SystemExit('publication rejected: runtime receipt reference is malformed')
    key = (receipt_path, receipt_sha)
    receipt = receipt_cache.get(key)
    if receipt is None:
        try:
            receipt = benchmark_runtime.load_subject(Path(receipt_path))
        except (AssertionError, OSError, TypeError, ValueError) as error:
            raise SystemExit(
                f'publication rejected: runtime subject receipt is invalid: {error}'
            ) from error
        if receipt.get('receipt_sha256') != receipt_sha:
            raise SystemExit('publication rejected: runtime subject receipt changed')
        receipt_cache[key] = receipt
    package = identity.get('packages', {}).get('gometry', {})
    distribution = package.get('distribution') if isinstance(package, dict) else None
    try:
        expected_binding = artifact_release.match_benchmark_receipt(
            receipt,
            identity.get('extension', {}),
            distribution,
            runtime_binding=runtime_binding,
            verify_archive=False,
        )
    except (AssertionError, OSError, TypeError, ValueError) as error:
        raise SystemExit(
            f'publication rejected: runtime binding does not match subject: {error}'
        ) from error
    if implementation.get('runtime') == 'gometry':
        expected_wheel = {
            'filename': expected_binding['wheel_filename'],
            'sha256': expected_binding['wheel_sha256'],
            'extension_member': expected_binding['extension_member'],
            'extension_sha256': expected_binding['extension_sha256'],
        }
        if source_commit != expected_binding['source_commit']:
            raise SystemExit(
                'publication rejected: producer commit differs from selected subject'
            )
        if source_commit is None:
            if source_receipt is not None or source_wheel is not None:
                raise SystemExit(
                    'publication rejected: unattributed producer has receipt fields'
                )
        elif (
            source_receipt != {'path': receipt_path, 'sha256': receipt_sha}
            or source_wheel != expected_wheel
        ):
            raise SystemExit(
                'publication rejected: source attribution is not bound to selected wheel'
            )
        if source_commit not in source_commit_cache:
            source_commit_cache[source_commit] = (
                source_artifact._producer_commit_is_bound(
                    source_commit,
                    GOMETRY_ROOT,
                )
            )
        if not source_commit_cache[source_commit]:
            raise SystemExit(
                'publication rejected: measured Gometry producer inputs differ '
                'from the recorded source commit'
            )
        if runtime_binding.get('python_members') != current_python_members:
            raise SystemExit(
                'publication rejected: measured Gometry Python package members '
                'differ from the current source tree'
            )
    elif (
        source_commit is not None
        or source_receipt is not None
        or source_wheel is not None
    ):
        raise SystemExit(
            'publication rejected: comparator implementation has Gometry source attribution'
        )
    implementation_row = implementation.get('row')
    if not isinstance(implementation_row, str):
        raise SystemExit(
            'publication rejected: paired block implementation row is missing'
        )
    package_name = (
        'gometry'
        if implementation.get('runtime') == 'gometry'
        else implementation_row.split('.', 1)[0]
    )
    expected_package = expected_packages.get(package_name)
    if isinstance(expected_package, dict):
        if implementation.get('module') != expected_package.get('module'):
            raise SystemExit(
                f'publication rejected: paired block implementation module identity mismatch for {package_name}'
            )
        if implementation.get('distribution') != expected_package.get('distribution'):
            raise SystemExit(
                f'publication rejected: paired block implementation distribution identity mismatch for {package_name}'
            )
        native = implementation.get('native_libraries')
        if not isinstance(native, list) or any(
            not isinstance(item, dict)
            or item.get('path') not in loaded_by_path
            or any(
                item.get(key) != loaded_by_path[item['path']].get(key)
                for key in ('sha256', 'size_bytes')
            )
            for item in native
        ):
            raise SystemExit(
                f'publication rejected: paired block native-library identity mismatch for {package_name}'
            )
        if implementation.get('runtime') == 'gometry':
            expected_extension_file = (
                {
                    key: expected_extension_record[key]
                    for key in ('path', 'sha256', 'size_bytes')
                }
                if isinstance(expected_extension_record, dict)
                else None
            )
            if expected_extension_file is None or native != [expected_extension_file]:
                raise SystemExit(
                    'publication rejected: paired block gometry native identity mismatch'
                )
        else:
            distribution = implementation.get('distribution')
            owned_paths = (
                _distribution_owned_paths(distribution)
                if isinstance(distribution, dict)
                else None
            )
            if owned_paths is None:
                raise SystemExit(
                    f'publication rejected: paired block distribution ownership is unavailable for {package_name}'
                )
            module = implementation.get('module')
            module_path = module.get('path') if isinstance(module, dict) else None
            engine_tokens = _ENGINE_LIBRARY_TOKENS.get(package_name, ())
            for item in native:
                path = item.get('path')
                if not isinstance(path, str) or not (
                    path in owned_paths
                    or path == module_path
                    or any(token in Path(path).name.lower() for token in engine_tokens)
                ):
                    raise SystemExit(
                        f'publication rejected: paired block native-library ownership mismatch for {package_name}'
                    )
            for token in _REQUIRED_ENGINE_LIBRARY_TOKENS.get(package_name, ()):
                loaded_engine = [
                    path for path in loaded_by_path if token in Path(path).name.lower()
                ]
                attributed_engine = [
                    item
                    for item in native
                    if token in Path(str(item.get('path'))).name.lower()
                ]
                if not loaded_engine:
                    raise SystemExit(
                        f'publication rejected: required {token} engine is not loaded for {package_name}'
                    )
                if not attributed_engine:
                    raise SystemExit(
                        f'publication rejected: required {token} engine is not attributed for {package_name}'
                    )
        if implementation.get('native_library_status') == 'identified' and not native:
            raise SystemExit(
                f'publication rejected: paired block native-library identity is unavailable for {package_name}'
            )
        if implementation.get('native_library_status') == 'unavailable':
            raise SystemExit(
                f'publication rejected: paired block native-library capability is unavailable for {package_name}'
            )
    observed_packages = identity.get('packages')
    if not isinstance(observed_packages, dict) or not isinstance(
        expected_packages, dict
    ):
        raise SystemExit(
            'publication rejected: paired block dependency identity is missing'
        )
    # A block attests the packages its pair loaded; each must be the one the
    # run recorded.
    fields = ('available', 'version', 'module', 'distribution')
    for name, observed_package in observed_packages.items():
        package = expected_packages.get(name)
        if (
            not isinstance(package, dict)
            or not isinstance(observed_package, dict)
            or {key: package.get(key) for key in fields}
            != {key: observed_package.get(key) for key in fields}
        ):
            raise SystemExit(
                f'publication rejected: paired block dependency identity mismatch for {name}'
            )


def _render_publication(
    result: dict[str, Any],
    metadata: dict[str, Any],
    run_directory: Path,
    *,
    current_python_members: dict[str, str],
) -> tuple[str, dict[tuple[str, str], dict[str, Any]]]:
    """Validate every measured block of one release run and render its table."""
    support = _SUPPORT
    if str(support) not in sys.path:
        sys.path.insert(0, str(support))
    from summarize_bench import (
        PRACTICAL_EQUIVALENCE_MARGIN,
        _build_rows,
        _load_all_blocks,
        _parse_public_operations,
        load_run_artifacts,
        _run_environment,
        render,
        validate_exact_paired_matrix,
    )

    artifacts = load_run_artifacts(result, run_directory / 'run.json')
    validate_exact_paired_matrix(result, artifacts)
    _validate_oracle_artifact(result, run_directory)
    stats = _load_all_blocks(artifacts)
    receipt_cache: dict[tuple[str, str], dict[str, Any]] = {}
    source_commit_cache: dict[str | None, bool] = {}
    for operation_stats in stats.values():
        for block in operation_stats.blocks:
            for identity in block.arm_identities.values():
                _validate_block_identity(
                    identity,
                    metadata,
                    receipt_cache=receipt_cache,
                    source_commit_cache=source_commit_cache,
                    current_python_members=current_python_members,
                )
    sampling = result['sampling']
    rows = _build_rows(
        _parse_public_operations(result['public_operations']),
        stats,
        equivalence_margin=PRACTICAL_EQUIVALENCE_MARGIN,
        expected_blocks=sampling['measured_blocks'],
        expected_leads=tuple(sampling['lead_orders']),
    )
    lines = render(rows, publishable=True, environment=_run_environment(result))
    return '\n'.join(lines) + '\n', receipt_cache


def publish_run(run_manifest: Path, *, current_dir: Path | None = None) -> Path:
    """Validate one complete release run and publish its table and evidence."""
    try:
        manifest_bytes = run_manifest.read_bytes()
        result = json.loads(manifest_bytes)
    except (OSError, json.JSONDecodeError) as error:
        raise SystemExit(
            f'run manifest {run_manifest} is not valid JSON: {error}'
        ) from error
    if not isinstance(result, dict):
        raise SystemExit(f'run manifest {run_manifest} must contain a JSON object')
    if (
        result.get('manifest_type') != _RUN_MANIFEST_TYPE
        or result.get('schema_version') != _MANIFEST_SCHEMA_VERSION
    ):
        raise SystemExit(f'{run_manifest} is not a gometry benchmark run manifest')
    metadata = _validate_publication_result(result)
    try:
        current_inputs = artifact_content.source_input_digests(GOMETRY_ROOT)
        current_python_members = artifact_content.python_member_hashes(
            GOMETRY_ROOT / 'python' / 'gometry'
        )
    except (AssertionError, OSError, ValueError) as error:
        raise SystemExit(
            f'publication rejected: cannot identify current Gometry source inputs: '
            f'{error}'
        ) from error
    if (
        metadata['source_inputs_sha256'] != current_inputs['source_inputs_sha256']
        or metadata['native_source_inputs_sha256']
        != current_inputs['native_source_inputs_sha256']
    ):
        raise SystemExit(
            'publication rejected: measured Gometry source inputs differ from '
            'the current source tree'
        )
    harness_commit = metadata['harness_commit']
    if (
        not isinstance(harness_commit, str)
        or len(harness_commit) not in (40, 64)
        or any(character not in '0123456789abcdef' for character in harness_commit)
    ):
        raise SystemExit(
            'publication rejected: benchmark harness commit identity is malformed'
        )
    if not _benchmark_harness_matches_commit(harness_commit):
        raise SystemExit(
            'publication rejected: benchmark harness inputs differ from the '
            'recorded harness commit'
        )
    stage = _stage_run_evidence(
        run_manifest,
        manifest_bytes,
        result,
        _EVIDENCE_ROOT,
    )
    summary_stage: Path | None = None
    try:
        summary, receipt_cache = _render_publication(
            result,
            metadata,
            stage,
            current_python_members=current_python_members,
        )
        _retain_producer_receipts(stage, receipt_cache)
        bundle_digest = _publish_evidence_bundle(stage)
        stage = None  # The complete bundle now owns this directory.
        summary += (
            '\n<!-- --8<-- [start:evidence] -->\n\n'
            f'[Raw measurements](../assets/benchmark-runs/{bundle_digest}/run.json)'
            ' · [Build provenance]'
            f'(../assets/benchmark-runs/{bundle_digest}/receipts/index.json).\n\n'
            '<!-- --8<-- [end:evidence] -->\n'
        )
    finally:
        if stage is not None:
            shutil.rmtree(stage, ignore_errors=True)
    if current_dir is None:
        current_dir = GOMETRY_ROOT / 'benches' / 'results' / 'current'
    current_dir.mkdir(parents=True, exist_ok=True)
    target = current_dir / 'summary.md'
    summary_stage = current_dir / f'.{target.name}.{uuid.uuid4().hex}.tmp'
    try:
        summary_stage.write_text(summary, encoding='utf-8')
        summary_stage.replace(target)
    except OSError as error:
        raise SystemExit(
            f'publication rejected: cannot write summary: {error}'
        ) from error
    finally:
        if summary_stage.exists():
            summary_stage.unlink()
    return target


def _print_manifest() -> None:
    for profile_name, profile in PROFILES.items():
        print(f'[{profile_name}]')
        for suite in SUITES:
            print(f'{suite}:')
            for row in profile.rows(suite):
                print(f'  {row}')


def _validate_run_environment(
    profile: str,
    *,
    plan_only: bool,
    hard_failures: list[str],
) -> None:
    if profile != 'release' or plan_only or not hard_failures:
        return
    details = '\n'.join(f'- {failure}' for failure in hard_failures)
    raise SystemExit(
        'release benchmark environment is not publishable:\n'
        f'{details}\n'
        'resolve every hard release identity failure before timing'
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Run the bounded gometry smoke or release benchmark manifest.'
    )
    parser.add_argument('--profile', choices=PROFILES, default='smoke')
    parser.add_argument('--plan-only', action='store_true')
    parser.add_argument(
        '--cpu',
        type=int,
        help='CPU to pin (default: a kernel-isolated CPU when available)',
    )
    parser.add_argument(
        '--filter',
        help='comma-separated exact row names from the selected profile',
    )
    parser.add_argument(
        '--output-dir',
        type=Path,
        default=GOMETRY_ROOT / 'target' / 'bench' / 'results',
    )
    parser.add_argument(
        '--publish',
        type=Path,
        metavar='RUN_MANIFEST',
        help='validate one complete release run, retain its evidence, and write '
        'benches/results/current/summary.md',
    )
    parser.add_argument(
        '--current-dir',
        type=Path,
        help='directory receiving summary.md for --publish (default: '
        'benches/results/current)',
    )
    parser.add_argument(
        '--gometry-receipt',
        type=Path,
        help='source-attributed wheel or quality runtime receipt '
        '(default: this worktree quality receipt)',
    )
    parser.add_argument(
        '--gometry-package-root',
        type=Path,
        help='installed package root when benchmarking a wheel',
    )
    parser.add_argument('--list', action='store_true')
    args = parser.parse_args()
    if args.list:
        _print_manifest()
        return

    if args.publish is not None:
        print(f'wrote {publish_run(args.publish, current_dir=args.current_dir)}')
        return
    receipt_binding = benchmark_runtime.load_subject(args.gometry_receipt)
    package_root = benchmark_runtime.package_root_for_subject(
        receipt_binding,
        args.gometry_package_root,
    )
    output_dir = args.output_dir
    if not output_dir.is_absolute():
        output_dir = GOMETRY_ROOT / output_dir

    profile = PROFILES[args.profile]
    if args.profile == 'release':
        _validate_release_manifest(profile)
    selected = _select(_rows(profile), args.filter)
    # Builder bijection is pre-timing for smoke and release (incl. plan-only).
    _validate_oracle_builders(selected)
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = _stamp()
    run_directory = output_dir / f'{timestamp}-{args.profile}'
    try:
        run_directory.mkdir()
    except FileExistsError as exc:
        raise SystemExit(
            f'measurement output already exists: {run_directory}; '
            'choose a new --output-dir'
        ) from exc
    raw_directory = run_directory / 'raw'
    raw_directory.mkdir()
    cpu, cpu_selection = select_benchmark_cpu(args.cpu)
    plans, _outputs = _benchmark_plans(
        profile_name=args.profile,
        profile=profile,
        selected=selected,
        output_dir=raw_directory,
        timestamp=timestamp,
        cpu=cpu,
        receipt_binding=receipt_binding,
        package_root=package_root,
    )
    if args.profile == 'release' and args.filter is None:
        plans.extend(
            _resource_plans(
                output_dir=raw_directory,
                timestamp=timestamp,
                cpu=cpu,
                receipt_binding=receipt_binding,
                package_root=package_root,
            )
        )
    # Oracle runs once after env/manifest validation and before timing.
    oracle_output = raw_directory / f'{timestamp}-oracle-{args.profile}.json'
    oracle_plan: dict[str, Any] = {
        'kind': 'oracle',
        'suite': 'oracle',
        'label': 'oracle',
        'rows': (),
        'environment': None,
        'command': _oracle_command(selected, output=oracle_output),
        'output': str(oracle_output),
    }
    # Doctor package requirements follow the selected public operations.
    selected_ops = tuple(
        op
        for op in profile.all_operations
        if any(
            name in {n for rows in selected.values() for n in rows} for name in op.rows
        )
    )
    source_identity = receipt_binding.get('source')
    doctor = collect(
        operations=selected_ops,
        expected_source_inputs_sha256=(
            source_identity.get('source_inputs_sha256')
            if isinstance(source_identity, dict)
            else None
        ),
        expected_native_source_inputs_sha256=(
            source_identity.get('native_source_inputs_sha256')
            if isinstance(source_identity, dict)
            else None
        ),
    )
    doctor['metadata']['selected_cpu'] = cpu
    doctor['metadata']['selected_cpu_source'] = cpu_selection
    frequency_state = doctor['metadata']['cpu_frequency_state']
    selected_governor = (
        selected_cpu_governor(frequency_state, cpu)
        if str(cpu) in frequency_state
        else None
    )
    doctor['metadata']['selected_cpu_governor'] = selected_governor
    hard_failures: list[str] = [str(value) for value in doctor.get('hard_failures', ())]
    # Paired AB/BA blocks control shared host drift; the timing CPU's frequency
    # policy and isolation are recorded host facts, not preconditions.
    if selected_governor not in {None, 'performance'}:
        doctor['warnings'].append(
            f'selected benchmark CPU governor is {selected_governor!r}, '
            f"not 'performance': cpu={cpu}"
        )
    if cpu_selection.endswith('not-isolated'):
        doctor['warnings'].append(
            f'timing CPU is not kernel-isolated: cpu={cpu}, selection={cpu_selection}'
        )
    _validate_run_environment(
        args.profile,
        plan_only=args.plan_only,
        hard_failures=hard_failures,
    )
    doctor['hard_failures'] = hard_failures
    commands: list[dict[str, Any]] = []
    started = time.monotonic()
    oracle_ok = False
    if not args.plan_only:
        oracle_result = _run(
            oracle_plan['command'],
            environment=None,
            timeout=min(profile.command_timeout, profile.total_timeout),
        )
        oracle_result['suite'] = 'oracle'
        oracle_result['label'] = 'oracle'
        oracle_result['kind'] = 'oracle'
        commands.append(oracle_result)
        if oracle_result['returncode']:
            # Failed oracle: no measurement commands, publishable=false, nonzero.
            oracle_ok = False
        else:
            oracle_ok = True
            for plan in plans:
                remaining = profile.total_timeout - (time.monotonic() - started)
                if remaining <= 0:
                    commands.append({
                        'suite': plan['suite'],
                        'label': plan['label'],
                        'command': plan['command'],
                        'returncode': 124,
                        'timeout': True,
                        'elapsed_seconds': 0.0,
                        'stdout_tail': '',
                        'stderr_tail': 'whole-run timeout exhausted',
                    })
                    break
                env = dict(plan['environment'] or {})
                env['GOMETRY_BENCH_ORACLE_OK'] = '1'
                result = _run(
                    plan['command'],
                    environment=env,
                    timeout=min(profile.command_timeout, max(1, int(remaining))),
                    benchmark_subject=(
                        receipt_binding if plan.get('kind') == 'resource' else None
                    ),
                    package_root=(
                        package_root if plan.get('kind') == 'resource' else None
                    ),
                    case_path=(
                        Path(plan['case_path'])
                        if plan.get('kind') == 'resource'
                        else None
                    ),
                    result_path=(
                        Path(plan['output']) if plan.get('kind') == 'resource' else None
                    ),
                )
                result['suite'] = plan['suite']
                result['label'] = plan['label']
                result['kind'] = plan.get('kind', 'paired-block')
                commands.append(result)
                if result['returncode']:
                    break
    else:
        # plan-only: surface oracle + timing/resource plans
        plans = [oracle_plan, *plans]
        oracle_ok = True

    contention_after = (
        collect_contention()
        if args.profile == 'release' and not args.plan_only
        else {'metadata': {}, 'warnings': []}
    )
    # Keep the external oracle artifact separate from timing/resource plans.
    timing_plans = [plan for plan in plans if plan.get('kind') != 'oracle']
    expected_commands = 1 + len(timing_plans)  # oracle + timing/resource
    if args.plan_only:
        commands_complete = True
    else:
        commands_complete = (
            oracle_ok
            and len(commands) == expected_commands
            and all(command['returncode'] == 0 for command in commands)
        )
    artifact_plans = ([] if args.plan_only else [oracle_plan]) + timing_plans
    artifacts = [
        Path(plan['output']).relative_to(run_directory).as_posix()
        for plan in artifact_plans
        if plan.get('output') and Path(plan['output']).is_file()
    ]
    expected_artifacts = sum(1 for plan in artifact_plans if plan.get('output'))
    publishable = (
        args.profile == 'release'
        and args.filter is None
        and not args.plan_only
        and oracle_ok
        and commands_complete
        and len(artifacts) == expected_artifacts
        and not doctor['hard_failures']
    )

    public_operations = _serialize_public_operations(profile.all_operations, selected)
    sampling = {
        'worker': PAIR_WORKER,
        'measured_blocks': profile.paired_blocks,
        'lead_orders': [
            'ab' if index % 2 == 0 else 'ba' for index in range(profile.paired_blocks)
        ],
        'warmup_iterations': int(profile.paired_sampling_args[1]),
        'repetitions': int(profile.paired_sampling_args[3]),
        'independent_process_blocks': True,
        'state_scope': 'shared-process-prepared-fixture',
        'phase': 'warm',
        'oracle_state_scope': 'external-fresh-process',
    }
    result = {
        'manifest_type': _RUN_MANIFEST_TYPE,
        'schema_version': _MANIFEST_SCHEMA_VERSION,
        'profile': args.profile,
        'timestamp': timestamp,
        'plan_only': args.plan_only,
        'full_manifest': args.filter is None,
        'filter': args.filter,
        'publishable': publishable,
        'sampling': sampling,
        'row_timeout_seconds': profile.row_timeout,
        'command_timeout_seconds': profile.command_timeout,
        'total_timeout_seconds': profile.total_timeout,
        'row_counts': {suite: len(rows) for suite, rows in selected.items()},
        # Ordered editorial metadata for summarize_bench (never re-inferred).
        'public_operations': public_operations,
        'elapsed_seconds': time.monotonic() - started,
        'doctor': doctor,
        'contention_after': contention_after,
        'planned_commands': (
            [oracle_plan, *timing_plans] if not args.plan_only else plans
        ),
        'commands': commands,
        'commands_complete': commands_complete,
        'oracle_ok': oracle_ok if not args.plan_only else None,
        'oracle_artifact': (
            oracle_output.relative_to(run_directory).as_posix()
            if oracle_output.is_file()
            else None
        ),
        'artifacts': artifacts,
        'resources': (
            []
            if args.plan_only
            else _resource_summaries(plans, subject=receipt_binding)
        ),
    }
    output = run_directory / 'run.json'
    _write_json_once(output, result, label='run manifest')
    print(f'wrote {output}')
    if (
        (not args.plan_only and not oracle_ok)
        or any(command['returncode'] for command in commands)
        or (
            args.profile == 'release'
            and not args.plan_only
            and args.filter is None
            and not publishable
        )
    ):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
