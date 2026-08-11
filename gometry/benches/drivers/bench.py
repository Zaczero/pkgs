"""Run the bounded smoke or release benchmark manifest."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import signal
import statistics
import subprocess
import sys
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
from bench_doctor import collect, collect_contention, select_benchmark_cpu


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
    for suite in ('competitors', 'real_world'):
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
        'GOMETRY_BENCH_ORACLE_OK',
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
    samples: dict[str, list[float]] = {}
    for path in paths:
        if not path.is_file():
            continue
        payload = json.loads(path.read_text(encoding='utf-8'))
        for benchmark in payload.get('benchmarks', []):
            metadata = benchmark.get('metadata') or payload.get('metadata', {})
            name = metadata.get('name', 'unknown')
            samples.setdefault(name, []).extend(
                value
                for run in benchmark.get('runs', [])
                for value in run.get('values', [])
            )
    return [
        {
            'name': name,
            'mean': statistics.fmean(values),
            'median': statistics.median(values),
            'stdev': statistics.stdev(values) if len(values) > 1 else 0.0,
            'samples': len(values),
        }
        for name, values in sorted(samples.items())
        if values
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
        comparative = profile_name == 'release' and suite != 'gometry'
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
            plans.append({
                'kind': 'pyperf',
                'suite': suite,
                'label': run_label,
                'rows': scheduled_rows,
                'environment': {
                    'GOMETRY_BENCH_FILTER': ','.join(scheduled_rows),
                    'GOMETRY_BENCH_PROFILE': profile_name,
                    'GOMETRY_BENCH_ORCHESTRATED': '1',
                },
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
                '| Benchmark | Mean | Median | Std dev | Samples |',
                '|---|---:|---:|---:|---:|',
            ))
            lines.extend(
                (
                    f'| `{row["name"]}` | {row["mean"]:.6g} s | '
                    f'{row["median"]:.6g} s | {row["stdev"]:.6g} s | '
                    f'{row["samples"]} |'
                )
                for row in summaries
            )
            lines.append('')
    if result['resources']:
        lines.extend((
            '',
            '## Latency and resource probes',
            '',
            '| Mode | Median | p99 | p99.9 | Peak RSS | RSS growth | Python peak |',
            '|---|---:|---:|---:|---:|---:|---:|',
        ))
        lines.extend(
            f'| `{row["mode"]}` | {row["median_seconds"]:.6g} s | '
            f'{row["p99_seconds"]:.6g} s | {row["p999_seconds"]:.6g} s | '
            f'{row["rss_peak_kib"]} KiB | '
            f'{row["rss_peak_growth_kib"]} KiB | '
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
    return '\n'.join(lines) + '\n'


def _print_manifest() -> None:
    for profile_name, profile in PROFILES.items():
        print(f'[{profile_name}]')
        for suite in SUITES:
            print(f'{suite}:')
            for row in profile.rows(suite):
                print(f'  {row}')


def _validate_run_environment(
    profile: str, *, plan_only: bool, warnings: list[str]
) -> None:
    if profile != 'release' or plan_only or not warnings:
        return
    details = '\n'.join(f'- {warning}' for warning in warnings)
    raise SystemExit(
        'release benchmark environment is not publishable:\n'
        f'{details}\n'
        'resolve every warning before running the release profile'
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
    # Doctor package requirements follow the selected public operations so an
    # internal-only dependency (e.g. s2sphere) never blocks the public S2-only row.
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
    if args.profile == 'release' and cpu_selection.endswith('not-isolated'):
        doctor['warnings'].append(
            'release timing CPU is not kernel-isolated: '
            f'cpu={cpu}, selection={cpu_selection}'
        )
    _validate_run_environment(
        args.profile,
        plan_only=args.plan_only,
        warnings=doctor['warnings'],
    )
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
    expected_artifacts = sum(1 for plan in timing_plans if plan.get('output'))
    publishable = (
        args.profile == 'release'
        and args.filter is None
        and not args.plan_only
        and oracle_ok
        and commands_complete
        and len(artifacts) == expected_artifacts
        and not doctor['warnings']
        and not contention_after['warnings']
    )

    result = {
        'profile': args.profile,
        'timestamp': timestamp,
        'plan_only': args.plan_only,
        'full_manifest': args.filter is None,
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
        'oracle_ok': oracle_ok if not args.plan_only else None,
        'artifacts': artifacts,
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
