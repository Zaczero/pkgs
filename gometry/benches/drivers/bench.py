"""Run the bounded smoke or release benchmark manifest."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

_BENCHES = Path(__file__).resolve().parents[1]
GOMETRY_ROOT = _BENCHES.parent
_SUPPORT = _BENCHES / 'support'
_DRIVERS = _BENCHES / 'drivers'
_PYTHON = _BENCHES / 'python'
for _path in (_SUPPORT, _DRIVERS, _PYTHON):
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


def _paired_matrix_identity(
    operations: list[dict[str, Any]], sampling: dict[str, Any]
) -> tuple[list[dict[str, str]], str]:
    leads = sampling.get('lead_orders')
    if not isinstance(leads, list):
        raise SystemExit('paired sampling matrix has no lead orders')
    matrix: list[dict[str, str]] = []
    for number, operation in enumerate(operations, start=1):
        pair_id = f'{operation["suite"]}-pair-{number:02d}'
        for block_number, lead in enumerate(leads, start=1):
            block_id = f'{pair_id}-block-{block_number:02d}-{lead}'
            matrix.append({
                'gometry': operation['gometry'],
                'competitor': operation['competitor'],
                'pair_id': pair_id,
                'block_id': block_id,
                'lead': lead,
            })
    encoded = json.dumps(matrix, separators=(',', ':'), sort_keys=True).encode('utf-8')
    return matrix, hashlib.sha256(encoded).hexdigest()


_OUTPUT_TAIL = 10_000
_RUN_MANIFEST_TYPE = 'gometry-benchmark-run'
_ANALYSIS_MANIFEST_TYPE = 'gometry-benchmark-analysis'
_MANIFEST_SCHEMA_VERSION = 1
_REQUIRED_ENGINE_LIBRARY_TOKENS = {
    'shapely': ('geos',),
    'pyproj': ('proj',),
}


def _stamp() -> str:
    utc = getattr(dt, 'UTC', dt.timezone.utc)  # noqa: UP017 — Python 3.10 compatibility
    return dt.datetime.now(utc).replace(microsecond=0).strftime('%Y%m%dT%H%M%SZ')


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
    import _bench_public_cases as _public_cases  # noqa: F401
    import _bench_scaling_cases as _scaling_cases  # noqa: F401
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
        '.venv/bin/python',
        'benches/python/bench_oracle.py',
    ]
    if not ordered:
        command.append('--all')
    else:
        command.extend(('--operations', ','.join(ordered)))
    if output is not None:
        command.extend(('--output', str(output)))
    return command


def _paired_command(
    *,
    gometry: str,
    competitor: str,
    profile_name: str,
    pair_id: str,
    block_id: str,
    lead: str,
    output: Path,
    profile: Profile,
    cpu: int,
    run_id: str,
    phase: str,
) -> list[str]:
    return [
        'taskset',
        '--cpu-list',
        str(cpu),
        '.venv/bin/python',
        f'benches/python/{PAIR_WORKER}',
        '--gometry',
        gometry,
        '--competitor',
        competitor,
        '--pair-id',
        pair_id,
        '--block-id',
        block_id,
        '--lead',
        lead,
        '--profile',
        profile_name,
        '--run-id',
        run_id,
        '--phase',
        phase,
        *profile.paired_sampling_args,
        '--output',
        str(output),
    ]


def _run(
    command: list[str],
    *,
    environment: dict[str, str] | None,
    timeout: int,
) -> dict[str, Any]:
    env = os.environ | (environment or {})
    env.pop('RUSTC_WRAPPER', None)
    started = time.monotonic()
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
            environment = {'GOMETRY_BENCH_PROFILE': profile_name}
            command: list[str]
            if len(scheduled_rows) == 2 and label.startswith('pair-'):
                gometry, competitor = scheduled_rows
                pair_number, block_spec = label.removeprefix('pair-').split(
                    '-block-', 1
                )
                _block_number, lead = block_spec.split('-', 1)
                command = _paired_command(
                    gometry=gometry,
                    competitor=competitor,
                    profile_name=profile_name,
                    pair_id=f'{suite}-pair-{pair_number}',
                    block_id=run_label,
                    lead=lead,
                    output=output,
                    profile=profile,
                    cpu=cpu,
                    run_id=f'{timestamp}-{profile_name}',
                    phase=getattr(
                        next(
                            op for op in profile.all_operations if op.gometry == gometry
                        ),
                        'phase',
                        'warm',
                    ),
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
    *, output_dir: Path, timestamp: str, cpu: int
) -> list[dict[str, Any]]:
    plans: list[dict[str, Any]] = []
    for mode in ('bearing', 'features', 'intersects', 'mixed'):
        output = output_dir / f'{timestamp}-resource-{mode}-release.json'
        plans.append({
            'kind': 'resource',
            'suite': 'resources',
            'label': f'resource-{mode}',
            'rows': (),
            'environment': None,
            'command': [
                'taskset',
                '--cpu-list',
                str(cpu),
                '.venv/bin/python',
                'benches/cases/case_resource_tail.py',
                '--mode',
                mode,
                '--output',
                str(output),
            ],
            'output': str(output),
        })
    return plans


def _resource_summaries(plans: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for plan in plans:
        output = Path(plan['output'])
        if plan['kind'] != 'resource' or not output.is_file():
            continue
        payload = json.loads(output.read_text(encoding='utf-8'))
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


def _write_json_once(path: Path, payload: dict[str, Any], *, label: str) -> None:
    """Write a manifest once; an existing path is never replaced."""
    try:
        with path.open('x', encoding='utf-8') as stream:
            json.dump(payload, stream, indent=2, sort_keys=True)
            stream.write('\n')
    except FileExistsError as exc:
        raise SystemExit(f'{label} already exists: {path}') from exc


def _manifest_path(value: Path | str, *, label: str) -> Path:
    path = Path(value)
    if not path.is_file():
        raise SystemExit(f'{label} does not exist: {path}')
    return path.resolve()


def _manifest_relative(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError as exc:
        raise SystemExit(
            f'benchmark artifact must be below its run directory: {path}'
        ) from exc


def _manifest_artifact_records(
    result: dict[str, Any], *, run_directory: Path
) -> list[dict[str, Any]]:
    raw = result.get('artifacts')
    if not isinstance(raw, list):
        raise SystemExit('run manifest rejected: artifact list is malformed')
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, str):
            raise SystemExit('run manifest rejected: artifact path is not a string')
        path = Path(item)
        if not path.is_file():
            raise SystemExit(f'run manifest rejected: missing artifact {item}')
        relative = _manifest_relative(path, run_directory)
        if relative in seen:
            raise SystemExit(f'run manifest rejected: duplicate artifact {relative}')
        seen.add(relative)
        records.append({
            'path': relative,
            'sha256': _file_sha256(path),
            'size_bytes': path.stat().st_size,
        })
    return records


def _resolve_manifest_artifact(manifest_path: Path, value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        resolved = path.resolve()
    else:
        resolved = (manifest_path.parent / path).resolve()
    try:
        resolved.relative_to(manifest_path.parent.resolve())
    except ValueError as exc:
        raise SystemExit(
            f'run manifest {manifest_path} references an artifact outside its directory: '
            f'{value}'
        ) from exc
    return resolved


def _resolve_analysis_file(analysis_path: Path, value: str, *, label: str) -> Path:
    path = (analysis_path.parent / value).resolve()
    try:
        path.relative_to(analysis_path.parent.resolve())
    except ValueError as exc:
        raise SystemExit(
            f'publication rejected: {label} is outside its manifest'
        ) from exc
    return _manifest_path(path, label=label)


def _verified_run_artifacts(
    manifest_path: Path, result: dict[str, Any]
) -> tuple[list[Path], list[dict[str, Any]]]:
    raw_records = result.get('raw_artifacts')
    if not isinstance(raw_records, list) or not raw_records:
        raise SystemExit('run manifest rejected: raw artifact digests are missing')
    records: list[dict[str, Any]] = []
    by_path: dict[str, dict[str, Any]] = {}
    for index, item in enumerate(raw_records):
        if not isinstance(item, dict):
            raise SystemExit(
                f'run manifest rejected: raw artifact {index} is not an object'
            )
        path_value = item.get('path')
        digest = item.get('sha256')
        size = item.get('size_bytes')
        if (
            not isinstance(path_value, str)
            or not isinstance(digest, str)
            or not isinstance(size, int)
            or size < 0
        ):
            raise SystemExit(
                f'run manifest rejected: raw artifact {index} is malformed'
            )
        path = _resolve_manifest_artifact(manifest_path, path_value)
        key = path.relative_to(manifest_path.parent.resolve()).as_posix()
        if key in by_path:
            raise SystemExit(f'run manifest rejected: duplicate raw artifact {key}')
        if not path.is_file():
            raise SystemExit(
                f'run manifest rejected: missing raw artifact {path_value}'
            )
        actual_size = path.stat().st_size
        actual_digest = _file_sha256(path)
        if actual_size != size or actual_digest != digest:
            raise SystemExit(
                f'run manifest rejected: raw artifact digest mismatch for {path_value}'
            )
        record = dict(item)
        record['path'] = key
        by_path[key] = record
        records.append(record)

    declared = result.get('artifacts')
    if not isinstance(declared, list) or not declared:
        raise SystemExit('run manifest rejected: artifact list is missing')
    declared_paths = []
    for item in declared:
        if not isinstance(item, str):
            raise SystemExit('run manifest rejected: artifact path is not a string')
        path = _resolve_manifest_artifact(manifest_path, item)
        key = path.relative_to(manifest_path.parent.resolve()).as_posix()
        if key not in by_path:
            raise SystemExit(f'run manifest rejected: artifact is not digested: {item}')
        declared_paths.append(path)
    if len(declared_paths) != len(set(declared_paths)):
        raise SystemExit('run manifest rejected: duplicate declared artifacts')
    if set(declared_paths) != {
        _resolve_manifest_artifact(manifest_path, str(record['path']))
        for record in records
    }:
        raise SystemExit(
            'run manifest rejected: declared artifacts do not match digests'
        )

    return [
        _resolve_manifest_artifact(manifest_path, str(record['path']))
        for record in records
    ], records


def _publication_metadata(result: dict[str, Any]) -> dict[str, Any]:
    doctor = result.get('doctor')
    metadata = doctor.get('metadata') if isinstance(doctor, dict) else None
    if not isinstance(metadata, dict):
        raise SystemExit('publication rejected: benchmark identity metadata is missing')
    required = (
        'git_commit',
        'source_inputs_sha256',
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
    if (
        not isinstance(lock_path, str)
        or not lock_path
        or not isinstance(lock_digest, str)
        or len(lock_digest) != 64
        or not isinstance(lock.get('size_bytes'), int)
        or lock.get('size_bytes', 0) <= 0
    ):
        raise SystemExit(
            'publication rejected: publishable evidence requires a lock file identity'
        )
    lock_file = Path(lock_path)
    if not lock_file.is_file() or _file_sha256(lock_file) != lock_digest:
        raise SystemExit(
            'publication rejected: dependency lock identity is stale or unavailable'
        )
    if not metadata.get('git_commit') or not metadata.get('source_inputs_sha256'):
        raise SystemExit(
            'publication rejected: source commit/input identity is missing'
        )
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


def _normalize_artifact_paths(summary: dict[str, Any], mapping: dict[str, str]) -> None:
    for operation in summary.get('operations', ()):
        for block in operation.get('blocks', ()):
            old = block.get('artifact')
            if isinstance(old, str):
                block['artifact'] = mapping.get(old, old)


def _validate_publication_result(
    result: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
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
    return _publication_metadata(result), operations


def _validate_oracle_artifact(result: dict[str, Any], artifacts: list[Path]) -> None:
    """Require the untimed oracle to be a distinct, complete process artifact."""
    value = result.get('oracle_artifact')
    if not isinstance(value, str) or not value:
        raise SystemExit('analysis rejected: external oracle artifact is missing')
    run_directory = artifacts[0].resolve().parent.parent if artifacts else None
    if run_directory is None:
        raise SystemExit('analysis rejected: no run artifacts for external oracle')
    path = Path(value)
    if not path.is_absolute():
        path = run_directory / path
    try:
        path = path.resolve()
        path.relative_to(run_directory.resolve())
    except ValueError as exc:
        raise SystemExit(
            'analysis rejected: external oracle artifact is outside the run'
        ) from exc
    if not path.is_file():
        raise SystemExit('analysis rejected: external oracle artifact is missing')
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
            'analysis rejected: external oracle artifact is incomplete or mismatched'
        )


def _analyze_result(
    result: dict[str, Any], artifacts: list[Path], *, artifact_mapping: dict[str, str]
) -> tuple[dict[str, Any], str]:
    """Run the existing deterministic normalization path for one run."""
    support = _SUPPORT
    if str(support) not in sys.path:
        sys.path.insert(0, str(support))
    from summarize_bench import (
        _build_rows,
        _load_all_blocks,
        _parse_public_operations,
        normalized_summary_payload,
        render,
        validate_exact_paired_matrix,
    )

    validate_exact_paired_matrix(result, artifacts)
    _validate_oracle_artifact(result, artifacts)

    stats = _load_all_blocks(
        artifacts,
        expected_profile=str(result.get('profile')),
        expected_run_id=str(result.get('run_id')),
    )
    doctor = result.get('doctor')
    metadata = doctor.get('metadata', {}) if isinstance(doctor, dict) else {}
    expected_source = metadata.get('source_inputs_sha256')
    extension = metadata.get('extension') if isinstance(metadata, dict) else None
    expected_extension = (
        extension.get('sha256') if isinstance(extension, dict) else None
    )
    expected_lock = metadata.get('uv_lock') if isinstance(metadata, dict) else None
    expected_python = (
        {
            key: metadata.get(key)
            for key in (
                'python_executable',
                'python_version',
                'python_implementation',
                'python_cache_tag',
                'python_build',
                'python_debug',
            )
        }
        if isinstance(metadata, dict)
        else {}
    )
    expected_packages = (
        metadata.get('packages', {}) if isinstance(metadata, dict) else {}
    )
    for operation_stats in stats.values():
        for block in operation_stats.blocks:
            identities = block.arm_identities or {'A': block.producer_identity}
            for identity in identities.values():
                _validate_analysis_identity(
                    identity,
                    expected_commit=metadata.get('git_commit')
                    if isinstance(metadata, dict)
                    else None,
                    expected_source=expected_source,
                    expected_extension=expected_extension,
                    expected_extension_record=extension,
                    expected_lock=expected_lock,
                    expected_python=expected_python,
                    expected_packages=expected_packages,
                )
    summary = normalized_summary_payload(result, stats)
    _normalize_artifact_paths(summary, artifact_mapping)
    parsed_operations = _parse_public_operations(result['public_operations'])
    sampling = result.get('sampling')
    expected_blocks = (
        sampling.get('measured_blocks') if isinstance(sampling, dict) else None
    )
    expected_leads = sampling.get('lead_orders') if isinstance(sampling, dict) else None
    rows = _build_rows(
        parsed_operations,
        stats,
        equivalence_margin=0.03,
        expected_blocks=expected_blocks if isinstance(expected_blocks, int) else None,
        expected_leads=tuple(expected_leads)
        if isinstance(expected_leads, list)
        else None,
    )
    summary_md = (
        '\n'.join(
            render(
                rows,
                markdown=True,
                publishable=True,
                environment=summary.get('environment'),
            )
        )
        + '\n'
    )
    return summary, summary_md


def _validate_analysis_identity(
    identity: dict[str, Any],
    *,
    expected_commit: Any,
    expected_source: Any,
    expected_extension: Any,
    expected_extension_record: Any,
    expected_lock: Any,
    expected_python: dict[str, Any],
    expected_packages: Any,
) -> None:
    """Check one arm against the doctor identity before normalization."""
    if expected_commit and identity.get('git_commit') != expected_commit:
        raise SystemExit('analysis rejected: paired block commit identity mismatch')
    if expected_source and identity.get('source_inputs_sha256') != expected_source:
        raise SystemExit('analysis rejected: paired block source identity mismatch')
    observed_extension = identity.get('extension')
    observed_extension_digest = (
        observed_extension.get('sha256')
        if isinstance(observed_extension, dict)
        else identity.get('extension_sha256')
    )
    if expected_extension and observed_extension_digest != expected_extension:
        raise SystemExit('analysis rejected: paired block extension identity mismatch')
    if (
        isinstance(expected_extension_record, dict)
        and isinstance(observed_extension, dict)
        and observed_extension.get('build_info')
        != expected_extension_record.get('build_info')
    ):
        raise SystemExit(
            'analysis rejected: paired block effective build identity mismatch'
        )
    if isinstance(expected_lock, dict) and identity.get('uv_lock') != expected_lock:
        raise SystemExit(
            'analysis rejected: paired block dependency lock identity mismatch'
        )
    observed_python = identity.get('python')
    if isinstance(observed_python, dict):
        if observed_python.get('executable') != expected_python.get(
            'python_executable'
        ):
            raise SystemExit(
                'analysis rejected: paired block Python executable identity mismatch'
            )
        if observed_python.get('version') != expected_python.get('python_version'):
            raise SystemExit(
                'analysis rejected: paired block Python build identity mismatch'
            )
        if observed_python.get('implementation') != expected_python.get(
            'python_implementation'
        ):
            raise SystemExit(
                'analysis rejected: paired block Python implementation identity mismatch'
            )
        if observed_python.get('cache_tag') != expected_python.get('python_cache_tag'):
            raise SystemExit(
                'analysis rejected: paired block Python cache identity mismatch'
            )
        observed_build = observed_python.get('build')
        if isinstance(observed_build, dict):
            if observed_build.get('build') != expected_python.get('python_build'):
                raise SystemExit(
                    'analysis rejected: paired block Python build tuple mismatch'
                )
            if observed_build.get('debug') != expected_python.get('python_debug'):
                raise SystemExit(
                    'analysis rejected: paired block Python debug identity mismatch'
                )
    loaded = identity.get('loaded_native_libraries')
    if not isinstance(loaded, list):
        raise SystemExit(
            'analysis rejected: paired block loaded native-library inventory is missing'
        )
    loaded_by_path = {
        item.get('path'): item
        for item in loaded
        if isinstance(item, dict) and isinstance(item.get('path'), str)
    }
    implementation = identity.get('implementation')
    if not isinstance(implementation, dict):
        raise SystemExit(
            'analysis rejected: paired block implementation attribution is missing'
        )
    implementation_row = implementation.get('row')
    if not isinstance(implementation_row, str):
        raise SystemExit(
            'analysis rejected: paired block implementation row is missing'
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
                f'analysis rejected: paired block implementation module identity mismatch for {package_name}'
            )
        if implementation.get('distribution') != expected_package.get('distribution'):
            raise SystemExit(
                f'analysis rejected: paired block implementation distribution identity mismatch for {package_name}'
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
                f'analysis rejected: paired block native-library identity mismatch for {package_name}'
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
                    'analysis rejected: paired block gometry native identity mismatch'
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
                    f'analysis rejected: paired block distribution ownership is unavailable for {package_name}'
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
                        f'analysis rejected: paired block native-library ownership mismatch for {package_name}'
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
                        f'analysis rejected: required {token} engine is not loaded for {package_name}'
                    )
                if not attributed_engine:
                    raise SystemExit(
                        f'analysis rejected: required {token} engine is not attributed for {package_name}'
                    )
        if implementation.get('native_library_status') == 'identified' and not native:
            raise SystemExit(
                f'analysis rejected: paired block native-library identity is unavailable for {package_name}'
            )
        if implementation.get('native_library_status') == 'unavailable':
            raise SystemExit(
                f'analysis rejected: paired block native-library capability is unavailable for {package_name}'
            )
    observed_packages = identity.get('packages')
    if not isinstance(observed_packages, dict) or not isinstance(
        expected_packages, dict
    ):
        raise SystemExit(
            'analysis rejected: paired block dependency identity is missing'
        )
    for name, package in expected_packages.items():
        expected_version = package.get('version') if isinstance(package, dict) else None
        observed_package = observed_packages.get(name)
        if isinstance(package, dict) and isinstance(observed_package, dict):
            expected_identity = {
                key: package.get(key)
                for key in ('available', 'version', 'module', 'distribution')
            }
            observed_identity = {
                key: observed_package.get(key)
                for key in ('available', 'version', 'module', 'distribution')
            }
            if expected_identity != observed_identity:
                raise SystemExit(
                    f'analysis rejected: paired block dependency identity mismatch for {name}'
                )
        elif expected_version and observed_package != expected_version:
            raise SystemExit(
                f'analysis rejected: paired block dependency identity mismatch for {name}'
            )


def analyze_run(
    run_manifest: Path | str,
    *,
    output_dir: Path | None = None,
) -> Path:
    """Validate one immutable run and write one immutable analysis manifest."""
    manifest_path = _manifest_path(run_manifest, label='run manifest')
    result = _read_json_object(manifest_path, label='run manifest')
    if result.get('manifest_type') != _RUN_MANIFEST_TYPE:
        raise SystemExit(f'{manifest_path} is not a gometry benchmark run manifest')
    if result.get('schema_version') != _MANIFEST_SCHEMA_VERSION:
        raise SystemExit(f'run manifest {manifest_path} has an unsupported schema')
    _validate_publication_result(result)
    artifacts, records = _verified_run_artifacts(manifest_path, result)
    record_paths = {str(record['path']): str(record['path']) for record in records}
    artifact_mapping = {
        str(path): record_paths[path.relative_to(manifest_path.parent).as_posix()]
        for path in artifacts
    }
    summary, summary_md = _analyze_result(
        result, artifacts, artifact_mapping=artifact_mapping
    )

    run_id = result.get('run_id')
    if not isinstance(run_id, str) or not run_id:
        raise SystemExit(f'run manifest {manifest_path} lacks run identity')
    root = output_dir
    if root is None:
        root = (
            manifest_path.parent.parent
            if manifest_path.parent.name == run_id
            else manifest_path.parent
        )
    root.mkdir(parents=True, exist_ok=True)
    analysis_directory = root / f'{run_id}-analysis'
    try:
        analysis_directory.mkdir()
    except FileExistsError as exc:
        raise SystemExit(
            f'analysis output already exists: {analysis_directory}; '
            'choose a new --output-dir'
        ) from exc
    summary_path = analysis_directory / 'summary.json'
    summary_md_path = analysis_directory / 'summary.md'
    _write_json_once(summary_path, summary, label='analysis summary')
    summary_md_path.write_text(summary_md, encoding='utf-8')
    analysis = {
        'manifest_type': _ANALYSIS_MANIFEST_TYPE,
        'schema_version': _MANIFEST_SCHEMA_VERSION,
        'analysis_id': f'{run_id}-analysis',
        'source_run_manifest': str(manifest_path),
        'source_run_sha256': _file_sha256(manifest_path),
        'source_run_id': run_id,
        'profile': result.get('profile'),
        'summary': {
            'path': 'summary.json',
            'sha256': _file_sha256(summary_path),
            'size_bytes': summary_path.stat().st_size,
        },
        'summary_markdown': {
            'path': 'summary.md',
            'sha256': _file_sha256(summary_md_path),
            'size_bytes': summary_md_path.stat().st_size,
        },
    }
    analysis_path = analysis_directory / 'analysis.json'
    _write_json_once(analysis_path, analysis, label='analysis manifest')
    return analysis_path


def _load_analysis(
    analysis_manifest: Path | str,
) -> tuple[
    Path, dict[str, Any], Path, dict[str, Any], list[Path], list[dict[str, Any]]
]:
    analysis_path = _manifest_path(analysis_manifest, label='analysis manifest')
    analysis = _read_json_object(analysis_path, label='analysis manifest')
    if analysis.get('manifest_type') != _ANALYSIS_MANIFEST_TYPE:
        raise SystemExit(
            f'{analysis_path} is not a gometry benchmark analysis manifest'
        )
    if analysis.get('schema_version') != _MANIFEST_SCHEMA_VERSION:
        raise SystemExit(f'analysis manifest {analysis_path} has an unsupported schema')
    source_value = analysis.get('source_run_manifest')
    if not isinstance(source_value, str):
        raise SystemExit('publication rejected: analysis source run is missing')
    source_path = Path(source_value)
    if not source_path.is_absolute():
        source_path = analysis_path.parent / source_path
    source_path = _manifest_path(source_path, label='source run manifest')
    expected_source_digest = analysis.get('source_run_sha256')
    if not isinstance(expected_source_digest, str):
        raise SystemExit('publication rejected: analysis source identity is missing')
    if _file_sha256(source_path) != expected_source_digest:
        raise SystemExit('publication rejected: source run manifest identity is stale')
    result = _read_json_object(source_path, label='run manifest')
    if result.get('manifest_type') != _RUN_MANIFEST_TYPE:
        raise SystemExit('publication rejected: analysis source is not a run manifest')
    if analysis.get('source_run_id') != result.get('run_id'):
        raise SystemExit('publication rejected: analysis/source run identity mismatch')
    if analysis.get('analysis_id') != f'{result.get("run_id")}-analysis':
        raise SystemExit('publication rejected: analysis identity mismatch')
    if analysis.get('profile') != result.get('profile'):
        raise SystemExit('publication rejected: analysis profile identity mismatch')
    summary_info = analysis.get('summary')
    if not isinstance(summary_info, dict) or not isinstance(
        summary_info.get('path'), str
    ):
        raise SystemExit('publication rejected: analysis summary identity is missing')
    summary_path = _resolve_analysis_file(
        analysis_path, summary_info['path'], label='analysis summary'
    )
    if (
        not isinstance(summary_info.get('sha256'), str)
        or _file_sha256(summary_path) != summary_info['sha256']
        or summary_path.stat().st_size != summary_info.get('size_bytes')
    ):
        raise SystemExit('publication rejected: analysis summary identity is stale')
    summary = _read_json_object(summary_path, label='analysis summary')
    if summary.get('profile') != result.get('profile'):
        raise SystemExit('publication rejected: analysis summary profile mismatch')
    summary_run = summary.get('run')
    doctor = result.get('doctor')
    metadata = doctor.get('metadata', {}) if isinstance(doctor, dict) else {}
    if (
        not isinstance(summary_run, dict)
        or summary_run.get('timestamp') != result.get('timestamp')
        or summary_run.get('git_commit') != metadata.get('git_commit')
    ):
        raise SystemExit('publication rejected: analysis summary/run identity mismatch')
    artifacts, records = _verified_run_artifacts(source_path, result)
    record_paths = {str(record['path']): str(record['path']) for record in records}
    artifact_mapping = {
        str(path): record_paths[path.relative_to(source_path.parent).as_posix()]
        for path in artifacts
    }
    recalculated, _recalculated_markdown = _analyze_result(
        result, artifacts, artifact_mapping=artifact_mapping
    )
    if recalculated != summary:
        raise SystemExit(
            'publication rejected: analysis summary does not match source evidence'
        )
    return analysis_path, analysis, source_path, result, artifacts, records


def _publication_path_exists(path: Path) -> bool:
    """Include broken symlinks when checking an owned publication path."""
    return path.exists() or path.is_symlink()


def _remove_publication_tree(path: Path) -> None:
    """Remove only a publication directory owned by this transaction."""
    if not _publication_path_exists(path):
        return
    if path.is_symlink() or not path.is_dir():
        path.unlink()
    else:
        shutil.rmtree(path)


def _reserve_publication_sibling(parent: Path, prefix: str) -> Path:
    """Reserve a unique sibling name without creating a replacement target."""
    reserved = Path(tempfile.mkdtemp(prefix=prefix, dir=parent))
    try:
        reserved.rmdir()
    except BaseException:
        try:
            reserved.rmdir()
        except OSError:
            pass
        raise
    return reserved


def publish_current_run(
    analysis_manifest: Path | str,
    *,
    output_dir: Path | None = None,
    current_dir: Path | None = None,
) -> Path:
    """Publish one explicitly named, validated analysis manifest."""
    (
        analysis_path,
        analysis,
        source_path,
        result,
        artifacts,
        _source_records,
    ) = _load_analysis(analysis_manifest)
    metadata, _operations = _validate_publication_result(result)
    if output_dir is None:
        output_dir = GOMETRY_ROOT / 'target' / 'bench' / 'results'
    if current_dir is None:
        current_dir = GOMETRY_ROOT / 'benches' / 'results' / 'current'
    output_dir = Path(output_dir)
    current_dir = Path(current_dir)
    if _publication_path_exists(current_dir) and (
        current_dir.is_symlink() or not current_dir.is_dir()
    ):
        raise SystemExit(
            f'publication rejected: current path is not a directory: {current_dir}'
        )
    output_dir.mkdir(parents=True, exist_ok=True)
    current_dir.parent.mkdir(parents=True, exist_ok=True)

    summary_info = analysis.get('summary')
    if not isinstance(summary_info, dict):
        raise SystemExit('publication rejected: analysis summary is missing')
    summary = _read_json_object(
        _resolve_analysis_file(
            analysis_path, str(summary_info['path']), label='analysis summary'
        ),
        label='analysis summary',
    )
    markdown_info = analysis.get('summary_markdown')
    if not isinstance(markdown_info, dict) or not isinstance(
        markdown_info.get('path'), str
    ):
        raise SystemExit('publication rejected: analysis Markdown is missing')
    summary_md_path = _resolve_analysis_file(
        analysis_path, markdown_info['path'], label='analysis Markdown'
    )
    if (
        not summary_md_path.is_file()
        or not isinstance(markdown_info.get('sha256'), str)
        or _file_sha256(summary_md_path) != markdown_info['sha256']
    ):
        raise SystemExit('publication rejected: analysis Markdown identity is stale')
    summary_md = summary_md_path.read_text(encoding='utf-8')

    # The final publication path owns the rename boundary.  Staging under
    # current_dir.parent keeps both publication renames on one filesystem even
    # when evidence is supplied from a custom output_dir elsewhere.
    stage = Path(
        tempfile.mkdtemp(
            prefix=f'.{current_dir.name}.publication-stage-',
            dir=current_dir.parent,
        )
    )
    stage_published = False
    backup: Path | None = None
    current_moved = False
    try:
        raw_dir = stage / 'raw'
        raw_dir.mkdir()
        mapping: dict[str, str] = {}
        raw_records: list[dict[str, Any]] = []
        for artifact in artifacts:
            destination = raw_dir / artifact.name
            shutil.copy2(artifact, destination)
            relative = str(Path('raw') / artifact.name)
            mapping[str(artifact)] = relative
            try:
                source_relative = artifact.relative_to(source_path.parent).as_posix()
            except ValueError:
                source_relative = ''
            if source_relative:
                mapping[source_relative] = relative
            raw_records.append({
                'path': relative,
                'sha256': _file_sha256(destination),
                'size_bytes': destination.stat().st_size,
            })
        _normalize_artifact_paths(summary, mapping)
        summary['raw_artifacts'] = raw_records
        identity = {
            'schema_version': 1,
            'profile': 'release',
            'run_id': result.get('run_id'),
            'analysis_id': analysis.get('analysis_id'),
            'run_manifest_sha256': _file_sha256(source_path),
            'analysis_manifest_sha256': _file_sha256(analysis_path),
            'git_commit': metadata['git_commit'],
            'git_dirty_build_inputs': metadata.get('git_dirty_build_inputs'),
            'clean_source_build_inputs': not bool(
                metadata.get('git_dirty_build_inputs')
            ),
            'source_inputs_sha256': metadata['source_inputs_sha256'],
            'uv_lock': metadata['uv_lock'],
            'python': {
                key: metadata[key]
                for key in (
                    'python_executable',
                    'python',
                    'python_version',
                    'python_build',
                    'python_implementation',
                    'python_debug',
                    'python_cache_tag',
                )
            },
            'extension': metadata['extension'],
            'packages': metadata['packages'],
            'geos_version': metadata.get('geos_version'),
            'proj_version': metadata.get('proj_version'),
            'platform': metadata.get('platform'),
            'kernel': metadata.get('kernel'),
            'cpu': {
                key: metadata[key]
                for key in (
                    'cpu_model',
                    'cpu_governor',
                    'cpu_affinity',
                    'isolated_cpus',
                    'selected_cpu',
                    'selected_cpu_source',
                    'selected_cpu_governor',
                    'cpu_frequency_state',
                )
            },
            'raw_artifacts': raw_records,
        }
        (stage / 'identity.json').write_text(
            json.dumps(identity, indent=2, sort_keys=True) + '\n', encoding='utf-8'
        )
        (stage / 'summary.json').write_text(
            json.dumps(summary, indent=2, sort_keys=True) + '\n', encoding='utf-8'
        )
        (stage / 'summary.md').write_text(summary_md, encoding='utf-8')

        # Build and validate the complete replacement before touching the
        # tracked publication directory.
        required = (
            stage / 'identity.json',
            stage / 'summary.json',
            stage / 'summary.md',
            raw_dir,
        )
        if not all(path.exists() for path in required) or not any(raw_dir.iterdir()):
            raise SystemExit('publication rejected: staged output is incomplete')
        if _publication_path_exists(current_dir):
            backup = _reserve_publication_sibling(
                current_dir.parent,
                f'.{current_dir.name}.publication-backup-',
            )
            try:
                current_dir.rename(backup)
            except BaseException:
                current_moved = not _publication_path_exists(
                    current_dir
                ) and _publication_path_exists(backup)
                raise
            current_moved = True
        try:
            stage.rename(current_dir)
        except BaseException as replacement_error:
            if current_moved and backup is not None:
                if _publication_path_exists(current_dir):
                    raise RuntimeError(
                        'publication replacement path became occupied; '
                        f'previous evidence remains at {backup}'
                    ) from replacement_error
                try:
                    backup.rename(current_dir)
                except BaseException as restore_error:
                    raise RuntimeError(
                        'publication replacement failed and previous evidence '
                        f'could not be restored from {backup}'
                    ) from restore_error
                current_moved = False
            raise
        stage_published = True
        if backup is not None:
            old_backup = backup
            backup = None
            current_moved = False
            # The new current is committed before the old tree is cleaned.
            shutil.rmtree(old_backup)
    except BaseException:
        if not stage_published and _publication_path_exists(stage):
            try:
                _remove_publication_tree(stage)
            except OSError:
                pass
        if (
            current_moved
            and backup is not None
            and _publication_path_exists(backup)
            and not _publication_path_exists(current_dir)
        ):
            try:
                backup.rename(current_dir)
            except BaseException as restore_error:
                raise RuntimeError(
                    'publication failed and previous evidence could not be '
                    f'restored from {backup}'
                ) from restore_error
        raise
    return current_dir


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
        help='CPU to pin; release evidence requires a kernel-isolated CPU',
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
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        '--analyze',
        type=Path,
        metavar='RUN_MANIFEST',
        help='validate and normalize one exact immutable run manifest',
    )
    mode.add_argument(
        '--publish',
        type=Path,
        metavar='ANALYSIS_MANIFEST',
        help='publish one exact validated analysis manifest',
    )
    parser.add_argument(
        '--current-dir',
        type=Path,
        help='tracked current directory for --publish (tests or an explicit checkout)',
    )
    parser.add_argument('--list', action='store_true')
    args = parser.parse_args()
    if args.list:
        _print_manifest()
        return

    output_dir = args.output_dir
    if not output_dir.is_absolute():
        output_dir = GOMETRY_ROOT / output_dir
    if args.analyze is not None:
        analysis_path = analyze_run(args.analyze, output_dir=output_dir)
        print(f'wrote {analysis_path}')
        return
    if args.publish is not None:
        current_dir = args.current_dir
        if current_dir is not None and not current_dir.is_absolute():
            current_dir = GOMETRY_ROOT / current_dir
        published = publish_current_run(
            args.publish,
            output_dir=output_dir,
            current_dir=current_dir,
        )
        print(f'published {published}')
        return

    profile = PROFILES[args.profile]
    if args.profile == 'release':
        _validate_release_manifest(profile)
    selected = _select(_rows(profile), args.filter)
    # Builder bijection is pre-timing for smoke and release (incl. plan-only).
    _validate_oracle_builders(selected)
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = _stamp()
    run_id = f'{timestamp}-{args.profile}'
    run_directory = output_dir / run_id
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
    )
    if args.profile == 'release' and args.filter is None:
        plans.extend(
            _resource_plans(output_dir=raw_directory, timestamp=timestamp, cpu=cpu)
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
    doctor = collect(operations=selected_ops)
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
    if args.profile == 'release' and selected_governor not in {None, 'performance'}:
        hard_failures.append(
            f'selected benchmark CPU governor is {selected_governor!r}, '
            f"not 'performance': cpu={cpu}"
        )
    if args.profile == 'release' and cpu_selection.endswith('not-isolated'):
        hard_failures.append(
            'release timing CPU is not kernel-isolated: '
            f'cpu={cpu}, selection={cpu_selection}'
        )
    lock = doctor['metadata'].get('uv_lock')
    if args.profile == 'release' and (
        not isinstance(lock, dict)
        or not lock.get('path')
        or not lock.get('sha256')
        or not isinstance(lock.get('size_bytes'), int)
        or lock.get('size_bytes', 0) <= 0
    ):
        hard_failures.append(
            'publishable release evidence requires an actual dependency lock file identity'
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
    if args.plan_only:
        # plans already includes oracle_plan at front
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
        plan['output']
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
    paired_matrix, paired_matrix_sha256 = _paired_matrix_identity(
        public_operations, sampling
    )
    result = {
        'manifest_type': _RUN_MANIFEST_TYPE,
        'schema_version': _MANIFEST_SCHEMA_VERSION,
        'run_id': run_id,
        'profile': args.profile,
        'timestamp': timestamp,
        'plan_only': args.plan_only,
        'full_manifest': args.filter is None,
        'filter': args.filter,
        'publishable': publishable,
        'sampling_args': profile.sampling_args,
        'paired_sampling_args': profile.paired_sampling_args,
        'sampling': sampling,
        'paired_matrix': paired_matrix,
        'paired_matrix_sha256': paired_matrix_sha256,
        'row_timeout_seconds': profile.row_timeout,
        'command_timeout_seconds': profile.command_timeout,
        'total_timeout_seconds': profile.total_timeout,
        'row_counts': {suite: len(rows) for suite, rows in selected.items()},
        'benchmark_names': sorted({
            name for rows in selected.values() for name in rows
        }),
        # Ordered editorial metadata for summarize_bench (never re-inferred).
        'public_operations': public_operations,
        'elapsed_seconds': time.monotonic() - started,
        'doctor': doctor,
        'contention_after': contention_after,
        'selected_cpu': cpu,
        'selected_cpu_source': cpu_selection,
        'planned_commands': (
            [oracle_plan, *timing_plans] if not args.plan_only else plans
        ),
        'commands': commands,
        'commands_complete': commands_complete,
        'oracle_ok': oracle_ok if not args.plan_only else None,
        'oracle_artifact': (
            _manifest_relative(oracle_output, run_directory)
            if oracle_output.is_file()
            else None
        ),
        'artifacts': artifacts,
        'resources': [] if args.plan_only else _resource_summaries(plans),
    }
    raw_records = _manifest_artifact_records(result, run_directory=run_directory)
    result['raw_artifacts'] = raw_records
    result['artifacts'] = [record['path'] for record in raw_records]
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
