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

from _bench_pairs import pair_orderings, pair_units
from _bench_registry import (
    CHUNK_SIZE,
    PROFILES,
    SCRIPTS,
    SUITES,
    Profile,
    ReleaseOperation,
    expand_filter_to_pairs,
)
from bench_doctor import (
    collect,
    collect_contention,
    select_benchmark_cpu,
    selected_cpu_governor,
)


def _serialize_public_operations(
    operations: tuple[ReleaseOperation, ...],
    selected: dict[str, tuple[str, ...]],
) -> list[dict[str, object]]:
    """Ordered operation records for the run JSON (summarizer input)."""
    selected_names = {name for rows in selected.values() for name in rows}
    records: list[dict[str, object]] = []
    for op in operations:
        if not any(name in selected_names for name in op.rows):
            continue
        records.append({
            'domain': op.domain,
            'label': op.label,
            'workload': op.workload,
            'suite': op.suite,
            'gometry': op.gometry,
            'competitor': op.competitor,
            'competitor_label': op.competitor_label,
            'footnotes': list(op.footnotes),
        })
    return records


_OUTPUT_TAIL = 10_000


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


def _chunks(rows: tuple[str, ...]) -> tuple[tuple[str, ...], ...]:
    return tuple(
        rows[index : index + CHUNK_SIZE] for index in range(0, len(rows), CHUNK_SIZE)
    )


def _validate_release_manifest(profile: Profile) -> None:
    suite = 'competitors'
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
    from _bench_oracles import PUBLIC_CASE_BUILDERS
    from _bench_registry import RELEASE_OPERATIONS
    import _bench_public_cases as _public_cases  # noqa: F401
    from bench_oracle import validate_builders

    gometry_names = [
        name
        for suite_rows in selected.values()
        for name in suite_rows
        if name.startswith('gometry.')
    ]
    by_name = {op.gometry: op for op in RELEASE_OPERATIONS}
    by_any = {row: op for op in RELEASE_OPERATIONS for row in op.rows}
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
    validate_builders(operations, PUBLIC_CASE_BUILDERS)


def _oracle_command(selected: dict[str, tuple[str, ...]]) -> list[str]:
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
    if not ordered:
        return [
            '.venv/bin/python',
            'benches/python/bench_oracle.py',
            '--all',
        ]
    return [
        '.venv/bin/python',
        'benches/python/bench_oracle.py',
        '--operations',
        ','.join(ordered),
    ]


def _pyperf_command(
    script: str,
    output: Path,
    profile: Profile,
    sampling_args: tuple[str, ...],
    cpu: int,
) -> list[str]:
    return [
        '.venv/bin/python',
        f'benches/python/{script}',
        *sampling_args,
        '--affinity',
        str(cpu),
        '--timeout',
        str(profile.row_timeout),
        '--inherit-environ',
        'GOMETRY_BENCH_FILTER,GOMETRY_BENCH_PROFILE,GOMETRY_BENCH_ORCHESTRATED,'
        'GOMETRY_BENCH_ORACLE_OK,GOMETRY_BENCH_PAIR_ID,GOMETRY_BENCH_LEAD',
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


def _pyperf_summaries(paths: list[Path]) -> list[dict[str, Any]]:
    if not paths:
        return []
    if str(_SUPPORT) not in sys.path:
        sys.path.insert(0, str(_SUPPORT))
    from summarize_bench import _load_all_stats

    stats = _load_all_stats([path for path in paths if path.is_file()])
    return [
        {
            'name': name,
            'mean_process_median_seconds': value.mean,
            'median_process_median_seconds': value.median,
            'stdev_process_median_seconds': value.stdev,
            'processes': [
                {
                    'artifact': process.artifact,
                    'process_index': process.process_index,
                    'pair_id': process.pair_id,
                    'lead': process.lead,
                    'median_seconds': process.median,
                    'values': list(process.values),
                }
                for process in value.processes
            ],
        }
        for name, value in sorted(stats.items())
    ]


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
        comparative = profile_name == 'release'
        if comparative:
            units = pair_units(rows, suite=suite)
            scheduled = []
            for number, unit in enumerate(units, start=1):
                if len(unit) == 1:
                    scheduled.append((f'row-{number:02d}', unit, profile.sampling_args))
                    continue
                scheduled.extend(
                    (
                        f'pair-{number:02d}-{lead}',
                        ordering,
                        profile.paired_sampling_args,
                    )
                    for lead, ordering in zip(
                        ('ab', 'ba'),
                        pair_orderings(unit, suite=suite),
                        strict=True,
                    )
                )
        else:
            chunks = _chunks(rows)
            scheduled = [
                (
                    'all' if len(chunks) == 1 else f'chunk-{number:02d}',
                    chunk,
                    profile.sampling_args,
                )
                for number, chunk in enumerate(chunks, start=1)
            ]
        for label, scheduled_rows, sampling_args in scheduled:
            run_label = f'{suite}-{label}'
            output = output_dir / f'{timestamp}-{run_label}-{profile_name}.json'
            outputs[suite].append(output)
            environment = {
                'GOMETRY_BENCH_FILTER': ','.join(scheduled_rows),
                'GOMETRY_BENCH_PROFILE': profile_name,
                'GOMETRY_BENCH_ORCHESTRATED': '1',
            }
            if label.startswith('pair-'):
                pair_number, lead = label.removeprefix('pair-').split('-', 1)
                environment['GOMETRY_BENCH_PAIR_ID'] = f'{suite}-pair-{pair_number}'
                environment['GOMETRY_BENCH_LEAD'] = lead
            plans.append({
                'kind': 'pyperf',
                'suite': suite,
                'label': run_label,
                'rows': scheduled_rows,
                'environment': environment,
                'command': _pyperf_command(
                    SCRIPTS[suite], output, profile, sampling_args, cpu
                ),
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
                'rss_peak_kib',
                'rss_peak_growth_kib',
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
        or extension.get('debug_assertion_marker_count') is None
        or not extension.get('profile_evidence_method')
        or not extension.get('profile_evidence')
    ):
        raise SystemExit('publication rejected: extension path/digest/size is missing')
    if extension.get('debug_build') is not False or not extension.get('tied_to_checkout'):
        raise SystemExit('publication rejected: extension is debug or not tied to checkout')
    if not isinstance(metadata['uv_lock'], dict):
        raise SystemExit('publication rejected: uv.lock identity metadata is malformed')
    if not metadata.get('git_commit') or not metadata.get('source_inputs_sha256'):
        raise SystemExit('publication rejected: source commit/input identity is missing')
    packages = metadata['packages']
    required_packages = {
        'gometry', 'numpy', 'pyperf', 'shapely', 'pyproj', 'h3',
        'geopandas', 'pyarrow', 'mercantile', 'pygeohash',
    }
    if not isinstance(packages, dict):
        raise SystemExit('publication rejected: package version inventory is missing')
    missing_versions = [
        package for package in sorted(required_packages)
        if not isinstance(packages.get(package), dict)
        or not packages[package].get('version')
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
    hard_failures = doctor.get('hard_failures', ())
    if hard_failures:
        raise SystemExit(
            'publication rejected: benchmark identity has hard failures: '
            + '; '.join(str(value) for value in hard_failures)
        )
    return metadata


def _publication_artifacts(result: dict[str, Any]) -> list[Path]:
    raw = result.get('pyperf_artifacts')
    if not isinstance(raw, list) or not raw:
        raise SystemExit('publication rejected: no pyperf artifacts are declared')
    artifacts: list[Path] = []
    for item in raw:
        if not isinstance(item, str):
            raise SystemExit('publication rejected: artifact path is not a string')
        path = Path(item)
        if not path.is_file():
            raise SystemExit(f'publication rejected: missing artifact {item}')
        artifacts.append(path)
    if len({path.name for path in artifacts}) != len(artifacts):
        raise SystemExit('publication rejected: raw artifact names are not unique')
    return artifacts


def _normalize_artifact_paths(summary: dict[str, Any], mapping: dict[str, str]) -> None:
    for operation in summary.get('operations', ()):
        for block in operation.get('blocks', ()):
            old = block.get('artifact')
            if isinstance(old, str):
                block['artifact'] = mapping.get(old, old)
                block['block_id'] = (
                    f'{block["artifact"]}#process-{block.get("process_index", 0)}'
                )


def publish_current_run(
    result: dict[str, Any],
    *,
    output_dir: Path | None = None,
    current_dir: Path | None = None,
) -> Path:
    """Atomically publish one validated full release run.

    Staging lives below ``target`` (ignored); the destination is the tracked
    ``benches/results/current`` directory.  No destination mutation occurs
    until identity, matrix, oracle, artifacts, and normalized statistics have
    all passed validation.
    """
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
    if not isinstance(row_counts, dict) or sum(row_counts.values()) != 74:
        raise SystemExit('publication rejected: release does not contain exactly 74 rows')
    operations = result.get('public_operations')
    if not isinstance(operations, list) or len(operations) != 37:
        raise SystemExit('publication rejected: release does not contain exactly 37 operations')
    for operation in operations:
        if not isinstance(operation, dict) or not operation.get('competitor'):
            raise SystemExit('publication rejected: operation matrix is not fully paired')
    metadata = _publication_metadata(result)
    artifacts = _publication_artifacts(result)
    if output_dir is None:
        output_dir = GOMETRY_ROOT / 'target' / 'bench' / 'results'
    if current_dir is None:
        current_dir = GOMETRY_ROOT / 'benches' / 'results' / 'current'
    output_dir.mkdir(parents=True, exist_ok=True)
    current_dir.parent.mkdir(parents=True, exist_ok=True)

    # Import lazily so plan/list commands do not load pyperf or NumPy support.
    support = _SUPPORT
    if str(support) not in sys.path:
        sys.path.insert(0, str(support))
    from summarize_bench import _load_all_stats, normalized_summary_payload, render, _build_rows, _parse_public_operations

    stats = _load_all_stats(artifacts)
    summary = normalized_summary_payload(result, stats)
    parsed_operations = _parse_public_operations(result['public_operations'])
    rows = _build_rows(parsed_operations, stats, noise_band=0.03)
    summary_md = '\n'.join(
        render(
            rows,
            markdown=True,
            publishable=True,
            environment=summary.get('environment'),
        )
    ) + '\n'

    stage = Path(tempfile.mkdtemp(prefix='.publication-', dir=output_dir))
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
            'git_commit': metadata['git_commit'],
            'git_dirty_build_inputs': metadata.get('git_dirty_build_inputs'),
            'clean_source_build_inputs': not bool(metadata.get('git_dirty_build_inputs')),
            'source_inputs_sha256': metadata['source_inputs_sha256'],
            'uv_lock': metadata['uv_lock'],
            'python': {
                key: metadata[key]
                for key in (
                    'python_executable', 'python', 'python_version', 'python_build',
                    'python_implementation', 'python_debug', 'python_cache_tag',
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
                    'cpu_model', 'cpu_governor', 'cpu_affinity',
                    'isolated_cpus', 'selected_cpu', 'selected_cpu_source',
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

        # Ensure the normalized transaction is complete before replacing the
        # current directory.  Missing one required file is never a partial
        # publication.
        required = (stage / 'identity.json', stage / 'summary.json', stage / 'summary.md', raw_dir)
        if not all(path.exists() for path in required) or not any(raw_dir.iterdir()):
            raise SystemExit('publication rejected: staged output is incomplete')
        backup = current_dir.with_name(f'.{current_dir.name}.previous')
        if backup.exists():
            shutil.rmtree(backup)
        moved_old = False
        try:
            if current_dir.exists():
                current_dir.rename(backup)
                moved_old = True
            stage.rename(current_dir)
        except Exception:
            if moved_old and backup.exists() and not current_dir.exists():
                backup.rename(current_dir)
            raise
        if backup.exists():
            shutil.rmtree(backup)
    except Exception:
        if stage.exists():
            shutil.rmtree(stage)
        raise
    return current_dir


def _markdown(result: dict[str, Any]) -> str:
    lines = [
        '# Gometry benchmark run',
        '',
        f'- profile: `{result["profile"]}`',
        f'- timestamp: `{result["timestamp"]}`',
        f'- plan only: `{result["plan_only"]}`',
        f'- full manifest: `{result["full_manifest"]}`',
        f'- publishable: `{result["publishable"]}`',
        f'- selected rows: `{sum(result["row_counts"].values())}`',
        f'- total timeout: `{result["total_timeout_seconds"]} s`',
        f'- elapsed: `{result["elapsed_seconds"]:.3f} s`',
        '',
        '## Commands',
        '',
    ]
    command_rows = (
        result['planned_commands'] if result['plan_only'] else result['commands']
    )
    for command in command_rows:
        rendered = ' '.join(command['command'])
        if result['plan_only']:
            lines.append(f'- `{command["label"]}`: `{rendered}`')
        else:
            lines.append(
                f'- `{command["label"]}`: `{rendered}` -> '
                f'`{command["returncode"]}` ({command["elapsed_seconds"]:.3f} s)'
            )
    if result['pyperf']:
        lines.extend(('', '## Results', ''))
        for suite, summaries in result['pyperf'].items():
            lines.extend((
                f'### {suite}',
                '',
                '| Benchmark | Median of process medians | Process spread | Processes |',
                '|---|---:|---:|---:|',
            ))
            lines.extend(
                (
                    f'| `{row["name"]}` | '
                    f'{row["median_process_median_seconds"]:.6g} s | '
                    f'{row["stdev_process_median_seconds"]:.6g} s | '
                    f'{len(row["processes"])} |'
                )
                for row in summaries
            )
            lines.append('')
    if result['resources']:
        lines.extend((
            '',
            '## Gometry-only resource probes',
            '',
            'These probes are not competitor comparison evidence.',
            '',
            '| Mode | Median | p99 | Python peak allocation |',
            '|---|---:|---:|---:|',
        ))
        lines.extend(
            f'| `{row["mode"]}` | {row["median_seconds"]:.6g} s | '
            f'{row["p99_seconds"]:.6g} s | '
            f'{row["python_peak_traced_bytes"]} B |'
            for row in result['resources']
        )
    warnings = result['doctor']['warnings']
    if warnings:
        lines.extend(('', '## Environment warnings', ''))
        lines.extend(f'- {warning}' for warning in warnings)
    postflight_warnings = result['contention_after']['warnings']
    if postflight_warnings:
        lines.extend(('', '## Postflight contention warnings', ''))
        lines.extend(f'- {warning}' for warning in postflight_warnings)
    hard_failures = result['doctor'].get('hard_failures', ())
    if hard_failures:
        lines.extend(('', '## Release identity failures', ''))
        lines.extend(f'- {failure}' for failure in hard_failures)
    return '\n'.join(lines) + '\n'


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
    parser.add_argument('--list', action='store_true')
    args = parser.parse_args()
    if args.list:
        _print_manifest()
        return

    profile = PROFILES[args.profile]
    if args.profile == 'release':
        _validate_release_manifest(profile)
    selected = _select(_rows(profile), args.filter)
    # Builder bijection is pre-timing for smoke and release (incl. plan-only).
    _validate_oracle_builders(selected)
    output_dir = args.output_dir
    if not output_dir.is_absolute():
        output_dir = GOMETRY_ROOT / output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = _stamp()
    cpu, cpu_selection = select_benchmark_cpu(args.cpu)
    plans, outputs = _benchmark_plans(
        profile_name=args.profile,
        profile=profile,
        selected=selected,
        output_dir=output_dir,
        timestamp=timestamp,
        cpu=cpu,
    )
    if args.profile == 'release' and args.filter is None:
        plans.extend(
            _resource_plans(output_dir=output_dir, timestamp=timestamp, cpu=cpu)
        )
    # Oracle runs once after env/manifest validation and before timing.
    oracle_plan: dict[str, Any] = {
        'kind': 'oracle',
        'suite': 'oracle',
        'label': 'oracle',
        'rows': (),
        'environment': None,
        'command': _oracle_command(selected),
        'output': None,
    }
    # Doctor package requirements follow the selected public operations.
    selected_ops = tuple(
        op
        for op in profile.operations
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
    hard_failures = list(doctor.get('hard_failures', ()))
    if args.profile == 'release' and selected_governor not in {None, 'performance'}:
        hard_failures.append(
            f"selected benchmark CPU governor is {selected_governor!r}, "
            f"not 'performance': cpu={cpu}"
        )
    if args.profile == 'release' and cpu_selection.endswith('not-isolated'):
        hard_failures.append(
            'release timing CPU is not kernel-isolated: '
            f'cpu={cpu}, selection={cpu_selection}'
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
            # Failed oracle: zero pyperf commands, publishable=false, nonzero.
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
                result['kind'] = plan.get('kind', 'pyperf')
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
    # Oracle has no pyperf artifact; count timing/resource plans separately.
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
    artifacts = [
        plan['output']
        for plan in timing_plans
        if plan.get('output') and Path(plan['output']).is_file()
    ]
    pyperf_artifacts = [
        plan['output']
        for plan in timing_plans
        if plan.get('kind') == 'pyperf'
        and plan.get('output')
        and Path(plan['output']).is_file()
    ]
    expected_artifacts = sum(1 for plan in timing_plans if plan.get('output'))
    publishable = (
        args.profile == 'release'
        and args.filter is None
        and not args.plan_only
        and oracle_ok
        and commands_complete
        and len(artifacts) == expected_artifacts
        and not doctor['hard_failures']
    )

    result = {
        'profile': args.profile,
        'timestamp': timestamp,
        'plan_only': args.plan_only,
        'full_manifest': args.filter is None,
        'filter': args.filter,
        'publishable': publishable,
        'sampling_args': profile.sampling_args,
        'paired_sampling_args': profile.paired_sampling_args,
        'row_timeout_seconds': profile.row_timeout,
        'command_timeout_seconds': profile.command_timeout,
        'total_timeout_seconds': profile.total_timeout,
        'row_counts': {suite: len(rows) for suite, rows in selected.items()},
        'benchmark_names': sorted({
            name for rows in selected.values() for name in rows
        }),
        # Ordered editorial metadata for summarize_bench (never re-inferred).
        'public_operations': _serialize_public_operations(profile.operations, selected),
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
        'artifacts': artifacts,
        'pyperf_artifacts': pyperf_artifacts,
        'pyperf': {}
        if args.plan_only
        else {suite: _pyperf_summaries(paths) for suite, paths in outputs.items()},
        'resources': [] if args.plan_only else _resource_summaries(plans),
    }
    output = output_dir / f'{timestamp}-{args.profile}.json'
    report = output_dir / f'{timestamp}-{args.profile}.md'
    output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + '\n', encoding='utf-8'
    )
    report.write_text(_markdown(result), encoding='utf-8')
    if publishable:
        publish_current_run(result, output_dir=output_dir)
    print(f'wrote {output}')
    print(f'wrote {report}')
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
