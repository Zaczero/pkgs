"""Balanced blocked A/B harness for native performance changes.

Each measured block runs both builds, alternating the lead order ``A/B`` then
``B/A``. A fixed seed chooses the first leader and drives the bootstrap, so the
entire comparison is reproducible without grouping all runs from one build.

The case script prints one validated typed observation JSON object on stdout.
It must perform equivalent work and validate its output before printing the
measured value.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
from itertools import pairwise
import json
import math
import os
import platform
import random
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

GOMETRY_ROOT = Path(__file__).resolve().parents[2]
_SUPPORT = GOMETRY_ROOT / 'benches' / 'support'
_TOOLS = GOMETRY_ROOT / 'tools'
for _path in (_SUPPORT, _TOOLS):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from bench_doctor import collect, collect_contention, select_benchmark_cpu
from benchmark_statistics import (
    bootstrap_interval,
    bootstrap_median_interval,
    percentile,
)
import benchmark_runtime
from benchmark_identity import (
    IDENTITY_SCHEMA_VERSION,
    common_identity,
    validate_producer_identity,
)
from benchmark_observation import metric_spec, measurement_summary, validate_observation

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

DEFAULT_SEED = 20_260_709
MIN_MEASURED_ROUNDS = 9
IDENTIFY_TIMEOUT_SECONDS = 30
CASE_TIMEOUT_SECONDS = 300
PERF_BINARY = 'perf'
DEFAULT_PERF_EVENTS = (
    'instructions',
    'cycles',
    'cache-references',
    'cache-misses',
    'branches',
    'branch-misses',
)
MIN_COUNTER_COVERAGE = 0.90
PERF_CONTROL_TIMEOUT_SECONDS = 30


def expand_perf_events(events: Sequence[str] | None = None) -> tuple[str, ...]:
    """Normalize requested perf events, using the focused default set."""
    if not events:
        return DEFAULT_PERF_EVENTS
    expanded = tuple(
        part.strip() for item in events for part in item.split(',') if part.strip()
    )
    if not expanded:
        return DEFAULT_PERF_EVENTS
    if len(set(expanded)) != len(expanded):
        raise ValueError(f'duplicate perf event in {expanded!r}')
    return expanded


def perf_version(perf_binary: str = PERF_BINARY) -> dict[str, Any]:
    """Return the exact perf version output, failing closed when unavailable."""
    command = [perf_binary, '--version']
    try:
        result = subprocess.run(
            command,
            cwd=GOMETRY_ROOT,
            capture_output=True,
            text=True,
            check=False,
            timeout=IDENTIFY_TIMEOUT_SECONDS,
            env={**os.environ, 'LC_ALL': 'C', 'LANG': 'C'},
        )
    except (FileNotFoundError, OSError) as error:
        raise SystemExit(
            f'perf counters requested but {perf_binary!r} cannot execute: {error}'
        ) from error
    output = '\n'.join(
        part for part in (result.stdout.strip(), result.stderr.strip()) if part
    )
    if result.returncode != 0 or not output:
        raise SystemExit(
            f'perf counters requested but {perf_binary!r} --version failed '
            f'(returncode={result.returncode}, output={output!r})'
        )
    return {'command': command, 'version': output}


def _parse_observation_stdout(case: Path, stdout: str, stderr: str) -> dict[str, Any]:
    try:
        payload = json.loads(stdout)
        observation = payload['observation']
        validate_observation(observation)
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise SystemExit(
            f'{case}: expected one typed observation JSON object on stdout, '
            f'got {stdout!r} (stderr: {stderr[-400:]!r})'
        ) from error
    return observation


def _run_case_process(
    command: list[str],
    *,
    case: Path,
    side: str,
    subject: dict[str, Any],
    package_root: Path,
    timeout: int = CASE_TIMEOUT_SECONDS,
    environment: dict[str, str] | None = None,
    pass_fds: tuple[int, ...] = (),
) -> dict[str, Any]:
    result = benchmark_runtime.run_controlled_process(
        command,
        case=case,
        side=side,
        subject=subject,
        package_root=package_root,
        timeout=timeout,
        environment=environment,
        pass_fds=pass_fds,
    )
    return {
        'observation': result['observation'],
        'started_monotonic_ns': result['started_monotonic_ns'],
        'ended_monotonic_ns': result['ended_monotonic_ns'],
        'provenance_before': result['producer_identity'],
        'provenance_after': result['producer_identity'],
        'command': command,
        'case_stdout': result['case_stdout'],
        'case_stderr': result['case_stderr'],
        'case_process': result['case_process'],
    }


def run_case(
    python: str,
    case: Path,
    extra: list[str],
    *,
    receipt_binding: dict[str, Any],
    package_root: Path,
    side: str,
    environment: dict[str, str] | None = None,
) -> dict[str, Any]:
    return _run_case_process(
        [python, str(case), *extra],
        case=case,
        side=side,
        subject=receipt_binding,
        package_root=package_root,
        environment=environment,
    )


def _event_base(event: str) -> str:
    """Drop perf's user/kernel modifiers while retaining the event identity."""
    name = event.strip()
    if ':' not in name:
        return name
    base, modifiers = name.split(':', 1)
    if modifiers and all(part and part.isalpha() for part in modifiers.split(':')):
        return base
    return name


def _event_matches(requested: str, observed: str) -> bool:
    return requested == observed or _event_base(requested) == _event_base(observed)


def _number(token: str | None) -> int | float | None:
    if token is None:
        return None
    value = token.strip()
    if not value or value.startswith('<'):
        return None
    value = value.replace(',', '')
    try:
        parsed = float(value.rstrip('%'))
    except ValueError:
        return None
    if not math.isfinite(parsed):
        return None
    return int(parsed) if parsed.is_integer() else parsed


def _status_token(token: str | None) -> str | None:
    if token is None:
        return None
    normalized = token.strip().lower().replace(' ', '-')
    if normalized == '<not-supported>':
        return 'unsupported'
    if normalized == '<not-counted>':
        return 'not-counted'
    return None


def _counter_entry(
    requested: str,
    *,
    status: str,
    observed: str | None = None,
    raw_value: float | None = None,
    raw_value_text: str | None = None,
    unit: str | None = None,
    runtime: float | None = None,
    coverage: float | None = None,
    scaling: float | None = None,
    error: str | None = None,
    raw_line: str | None = None,
) -> dict[str, Any]:
    return {
        'requested_event': requested,
        'event': observed,
        'raw_value': raw_value,
        'raw_value_text': raw_value_text,
        'counter_value': raw_value,
        'unit': unit,
        'runtime': runtime,
        'coverage': coverage,
        'scaling': scaling,
        'status': status,
        'error': error,
        'raw_line': raw_line,
    }


def _counter_from_fields(
    requested: str,
    fields: list[str],
    event_index: int,
    raw_line: str,
    minimum_coverage: float,
) -> dict[str, Any]:
    value_text = fields[event_index - 2].strip() if event_index >= 2 else ''
    unit = fields[event_index - 1].strip() if event_index >= 1 else None
    runtime_text = (
        fields[event_index + 1].strip() if len(fields) > event_index + 1 else ''
    )
    coverage_text = (
        fields[event_index + 2].strip() if len(fields) > event_index + 2 else ''
    )
    coverage_value = _number(coverage_text)
    coverage = float(coverage_value) / 100.0 if coverage_value is not None else None
    runtime = _number(runtime_text)
    status = _status_token(value_text)
    if status is not None:
        return _counter_entry(
            requested,
            status=status,
            observed=fields[event_index].strip(),
            raw_value_text=value_text,
            unit=unit,
            runtime=runtime,
            coverage=coverage,
            raw_line=raw_line,
        )
    value = _number(value_text)
    if value is None:
        return _counter_entry(
            requested,
            status='error',
            observed=fields[event_index].strip(),
            raw_value_text=value_text,
            unit=unit,
            error=f'counter value is not numeric: {value_text!r}',
            raw_line=raw_line,
        )
    if value < 0:
        return _counter_entry(
            requested,
            status='error',
            observed=fields[event_index].strip(),
            raw_value=value,
            raw_value_text=value_text,
            unit=unit,
            runtime=runtime,
            coverage=coverage,
            error=f'counter value is negative: {value_text!r}',
            raw_line=raw_line,
        )
    status = 'ok'
    error = None
    scaling = None
    if coverage is not None:
        if coverage < 0.0 or coverage > 1.0:
            status = 'error'
            error = f'perf reported invalid running coverage: {coverage:.3f}'
        elif coverage == 0.0:
            status = 'not-counted'
            error = 'perf reported zero running coverage'
        elif coverage < minimum_coverage:
            status = 'multiplexed'
            error = (
                f'perf running coverage {coverage:.3f} is below the '
                f'{minimum_coverage:.3f} minimum'
            )
        else:
            scaling = 1.0 / coverage
    return _counter_entry(
        requested,
        status=status,
        observed=fields[event_index].strip(),
        raw_value=value,
        raw_value_text=value_text,
        unit=unit,
        runtime=runtime,
        coverage=coverage,
        scaling=scaling,
        error=error,
        raw_line=raw_line,
    )


def _human_counter_fields(requested: str, line: str) -> dict[str, Any] | None:
    tokens = line.split('#', 1)[0].split()
    for event_index, token in enumerate(tokens):
        if not _event_matches(requested, token):
            continue
        prefix = tokens[:event_index]
        if not prefix:
            return _counter_entry(
                requested,
                status='error',
                observed=token,
                error='human perf output has no counter value',
                raw_line=line,
            )
        if prefix[0].startswith('<'):
            value_text = prefix[0]
            for part in prefix[1:]:
                value_text = f'{value_text} {part}'
                if part.endswith('>'):
                    break
        else:
            value_text = prefix[0]
        status = _status_token(value_text)
        if status is not None:
            return _counter_entry(
                requested,
                status=status,
                observed=token,
                raw_value_text=value_text,
                raw_line=line,
            )
        value = _number(value_text)
        if value is None:
            return _counter_entry(
                requested,
                status='error',
                observed=token,
                raw_value_text=value_text,
                error=f'counter value is not numeric: {value_text!r}',
                raw_line=line,
            )
        if value < 0:
            return _counter_entry(
                requested,
                status='error',
                observed=token,
                raw_value=value,
                raw_value_text=value_text,
                error=f'counter value is negative: {value_text!r}',
                raw_line=line,
            )
        unit = prefix[1] if len(prefix) > 1 else ''
        return _counter_entry(
            requested,
            status='ok',
            observed=token,
            raw_value=value,
            raw_value_text=value_text,
            unit=unit,
            raw_line=line,
        )
    return None


def parse_perf_output(
    output: str,
    requested_events: Sequence[str],
    *,
    minimum_coverage: float = MIN_COUNTER_COVERAGE,
) -> dict[str, dict[str, Any]]:
    """Parse perf CSV or human output without hiding incomplete counters."""
    requested = expand_perf_events(requested_events)
    entries: dict[str, dict[str, Any]] = {}
    for line in output.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith('#'):
            continue
        try:
            fields = next(csv.reader([line]))
        except csv.Error:
            fields = []
        matched = False
        for event_index, field in enumerate(fields):
            for event in requested:
                if not _event_matches(event, field.strip()):
                    continue
                matched = True
                entry = _counter_from_fields(
                    event, fields, event_index, line, minimum_coverage
                )
                if event in entries:
                    entries[event] = _counter_entry(
                        event,
                        status='error',
                        observed=entry.get('event'),
                        error='perf emitted duplicate rows for the requested event',
                        raw_line=f'{entries[event].get("raw_line")}\n{line}',
                    )
                else:
                    entries[event] = entry
                break
            if matched:
                break
        if not matched:
            for event in requested:
                if event in entries:
                    continue
                entry = _human_counter_fields(event, line)
                if entry is not None:
                    entries[event] = entry
    for event in requested:
        entries.setdefault(
            event,
            _counter_entry(
                event,
                status='missing',
                error='perf output did not contain the requested event',
            ),
        )
    return entries


def _validate_counters(
    counters: dict[str, dict[str, Any]], events: Sequence[str]
) -> None:
    failures = {}
    for event in events:
        entry = counters.get(event)
        if entry is None:
            failures[event] = {'status': 'missing'}
        elif (
            entry.get('status') != 'ok'
            or entry.get('raw_value') is None
            or entry.get('coverage') is None
        ):
            failures[event] = {
                'status': entry.get('status'),
                'coverage': entry.get('coverage'),
                'error': entry.get('error'),
            }
    if failures:
        raise SystemExit(
            f'perf counters are not comparable; refusing result: {failures}'
        )


def _perf_control_command(
    perf_binary: str, events: Sequence[str], log_fd: int
) -> list[str]:
    command = [
        perf_binary,
        'stat',
        '--all-user',
        '-x,',
        '--no-big-num',
        '--log-fd',
        str(log_fd),
        '-e',
        ('{' + ','.join(events) + '}') if len(events) > 1 else events[0],
        '--',
        sys.executable,
        '-c',
        'x = 0\nfor i in range(2000000):\n    x += i\nprint(x)',
    ]
    return command


def _run_perf_control(
    events: Sequence[str],
    *,
    perf_binary: str = PERF_BINARY,
    minimum_coverage: float = MIN_COUNTER_COVERAGE,
) -> dict[str, Any]:
    requested = expand_perf_events(events)
    with tempfile.TemporaryFile() as perf_log:
        command = _perf_control_command(perf_binary, requested, perf_log.fileno())
        try:
            result = subprocess.run(
                command,
                cwd=GOMETRY_ROOT,
                capture_output=True,
                text=True,
                check=False,
                timeout=PERF_CONTROL_TIMEOUT_SECONDS,
                pass_fds=(perf_log.fileno(),),
                env={**os.environ, 'LC_ALL': 'C', 'LANG': 'C'},
            )
        except (FileNotFoundError, OSError) as error:
            return {
                'status': 'unavailable',
                'events': list(requested),
                'error': f'perf cannot execute: {error}',
                'command': command,
            }
        except subprocess.TimeoutExpired:
            return {
                'status': 'unavailable',
                'events': list(requested),
                'error': 'perf positive control timed out',
                'command': command,
            }
        perf_log.seek(0)
        output = perf_log.read().decode(errors='replace')
    counters = parse_perf_output(output, requested, minimum_coverage=minimum_coverage)
    failures = validate_positive_control(counters, requested)
    return {
        'status': 'ok' if result.returncode == 0 and not failures else 'rejected',
        'events': list(requested),
        'counters': counters,
        'failures': failures,
        'command': command,
        'stderr': result.stderr[-400:],
    }


def validate_positive_control(
    counters: dict[str, dict[str, Any]], events: Sequence[str]
) -> dict[str, Any]:
    """Return every requested event that did not move with usable coverage."""
    failures: dict[str, Any] = {}
    for event in expand_perf_events(events):
        entry = counters.get(event)
        if entry is None:
            failures[event] = {'status': 'missing'}
            continue
        raw = entry.get('raw_value')
        if entry.get('status') != 'ok' or entry.get('coverage') is None:
            failures[event] = {
                'status': entry.get('status'),
                'coverage': entry.get('coverage'),
                'error': entry.get('error'),
            }
        elif not isinstance(raw, (int, float)) or raw <= 0:
            failures[event] = {
                'status': 'nonmoving',
                'raw_value': raw,
                'error': 'positive-control counter did not move',
            }
    return failures


def check_perf_capability(
    events: Sequence[str],
    *,
    perf_binary: str = PERF_BINARY,
    minimum_coverage: float = MIN_COUNTER_COVERAGE,
) -> dict[str, Any]:
    """Find usable event groups using a moving execution positive control."""
    requested = expand_perf_events(events)
    try:
        version = perf_version(perf_binary)
    except SystemExit as error:
        return {
            'status': 'capability-null',
            'reason': str(error),
            'events': list(requested),
            'event_groups': [],
            'controls': [],
        }
    groups: list[list[str]] = []
    controls: list[dict[str, Any]] = []
    for event in requested:
        single = _run_perf_control(
            [event], perf_binary=perf_binary, minimum_coverage=minimum_coverage
        )
        controls.append(single)
        if single['status'] != 'ok':
            return {
                'status': 'capability-null',
                'reason': f'event {event!r} failed its positive control',
                'events': list(requested),
                'event_groups': [],
                'controls': controls,
                'perf': version,
            }
        if not groups:
            groups.append([event])
            continue
        candidate = [*groups[-1], event]
        grouped = _run_perf_control(
            candidate, perf_binary=perf_binary, minimum_coverage=minimum_coverage
        )
        controls.append(grouped)
        if grouped['status'] == 'ok':
            groups[-1] = candidate
        else:
            # The event itself passed alone, so begin a new minimally usable
            # group instead of accepting a multiplexed wider group.
            groups.append([event])
    return {
        'status': 'available',
        'events': list(requested),
        'event_groups': groups,
        'usable_group_capacity': max(map(len, groups), default=0),
        'controls': controls,
        'perf': version,
    }


def _run_case_with_perf_group(
    python: str,
    case: Path,
    extra: list[str],
    events: Sequence[str],
    *,
    receipt_binding: dict[str, Any],
    package_root: Path,
    side: str,
    perf_binary: str = PERF_BINARY,
    minimum_coverage: float = MIN_COUNTER_COVERAGE,
) -> dict[str, Any]:
    """Run one validated case under one non-multiplexed perf event group."""
    requested = expand_perf_events(events)
    with tempfile.TemporaryFile() as perf_log:
        log_fd = perf_log.fileno()
        command = [
            perf_binary,
            'stat',
            '--all-user',
            '-x,',
            '--no-big-num',
            '--log-fd',
            str(log_fd),
        ]
        command.extend((
            '-e',
            ('{' + ','.join(requested) + '}') if len(requested) > 1 else requested[0],
        ))
        command.extend(('--', python, str(case), *extra))
        try:
            result = _run_case_process(
                command,
                case=case,
                side=side,
                subject=receipt_binding,
                package_root=package_root,
                environment={**os.environ, 'LC_ALL': 'C', 'LANG': 'C'},
                pass_fds=(log_fd,),
            )
        except subprocess.TimeoutExpired as error:
            raise SystemExit(
                f'{case}: perf-wrapped case exceeded the '
                f'{CASE_TIMEOUT_SECONDS}s per-block timeout'
            ) from error
        except (FileNotFoundError, OSError) as error:
            raise SystemExit(
                f'perf counters requested but {perf_binary!r} cannot execute: {error}'
            ) from error
        perf_log.seek(0)
        perf_output = perf_log.read().decode(errors='replace')
    counters = parse_perf_output(
        perf_output, requested, minimum_coverage=minimum_coverage
    )
    _validate_counters(counters, requested)
    result.update({
        'counters': counters,
        'command': command,
        'perf_output': perf_output,
        'perf_stderr': result['case_stderr'],
    })
    return result


def _observation_identity(observation: dict[str, Any]) -> dict[str, Any]:
    return {
        name: observation.get(name)
        for name in (
            'schema_version',
            'observation_type',
            'useful_work',
            'phase',
            'warmup',
            'calibration',
            'oracle',
        )
    }


def run_case_with_perf(
    python: str,
    case: Path,
    extra: list[str],
    events: Sequence[str],
    *,
    receipt_binding: dict[str, Any],
    package_root: Path,
    side: str,
    perf_binary: str = PERF_BINARY,
    minimum_coverage: float = MIN_COUNTER_COVERAGE,
    event_groups: Sequence[Sequence[str]] | None = None,
) -> dict[str, Any]:
    """Run each usable perf group in its own process invocation.

    Group counters are merged only after every invocation proves the same
    oracle, useful work, and producer identity.  The timing retained here is
    instrumented timing and is never reused as uninstrumented release latency.
    """
    requested = expand_perf_events(events)
    groups = tuple(event_groups or tuple((event,) for event in requested))
    flattened = [event for group in groups for event in group]
    if set(flattened) != set(requested) or len(flattened) != len(set(flattened)):
        raise SystemExit('perf event groups do not cover the requested events exactly')
    results = [
        _run_case_with_perf_group(
            python,
            case,
            extra,
            group,
            receipt_binding=receipt_binding,
            package_root=package_root,
            side=side,
            perf_binary=perf_binary,
            minimum_coverage=minimum_coverage,
        )
        for group in groups
    ]
    first = results[0]
    first_observation = first['observation']
    first_before = first['provenance_before']
    first_after = first['provenance_after']
    counters: dict[str, dict[str, Any]] = {}
    commands: list[list[str]] = []
    case_processes: list[dict[str, Any]] = []
    for _group, result in zip(groups, results, strict=True):
        if _observation_identity(result['observation']) != _observation_identity(
            first_observation
        ):
            raise SystemExit(
                'perf split groups produced mismatched observation identity'
            )
        if (
            result['provenance_before'] != first_before
            or result['provenance_after'] != first_after
        ):
            raise SystemExit('perf split groups produced mismatched producer identity')
        for event, counter in result['counters'].items():
            if event in counters:
                raise SystemExit(f'perf split groups duplicated counter {event!r}')
            counters[event] = counter
        commands.append(result['command'])
        case_processes.append(result['case_process'])
    merged = dict(first)
    merged['counters'] = counters
    merged['commands'] = commands
    merged['event_groups'] = [list(group) for group in groups]
    merged['case_processes'] = case_processes
    return merged


def blocked_orders(blocks: int, seed: int) -> tuple[tuple[str, str], ...]:
    """Return balanced AB/BA lead order; the seed selects the first leader."""
    first_is_a = bool(random.Random(seed).getrandbits(1))
    return tuple(
        ('A', 'B') if first_is_a == (index % 2 == 0) else ('B', 'A')
        for index in range(blocks)
    )


def run_blocks(
    sides: dict[str, str],
    case: Path,
    extra: list[str],
    *,
    warmup: int,
    rounds: int,
    seed: int,
    receipt_bindings: dict[str, dict[str, Any] | None] | None = None,
    package_roots: dict[str, Path] | None = None,
    runner: Callable[[str, Path, list[str]], dict[str, Any]] = run_case,
    clock_ns: Callable[[], int] = time.monotonic_ns,
) -> dict[str, Any]:
    samples: dict[str, list[float]] = {'A': [], 'B': []}
    producer_by_side: dict[str, dict[str, Any]] = {}
    blocks: list[dict[str, Any]] = []
    for block_index, order in enumerate(blocked_orders(warmup + rounds, seed)):
        measured = block_index >= warmup
        block_started = clock_ns()
        arms: dict[str, Any] = {}
        for side in order:
            if runner is run_case:
                if receipt_bindings is None or package_roots is None:
                    raise SystemExit('A/B timing requires an admitted subject per side')
                arms[side] = run_case(
                    sides[side],
                    case,
                    extra,
                    receipt_binding=receipt_bindings[side],
                    package_root=package_roots[side],
                    side=side,
                )
            else:
                arms[side] = runner(sides[side], case, extra)
            arm = arms[side]
            before = arm.get('provenance_before')
            after = arm.get('provenance_after')
            if (
                isinstance(before, dict)
                and before.get('identity_schema_version') == IDENTITY_SCHEMA_VERSION
            ):
                validate_producer_identity(before)
                if (
                    before.get('arm') != {'side': side, 'row': str(case.resolve())}
                    or after != before
                    or arm['observation'].get('producer_identity') != before
                ):
                    raise SystemExit(
                        f'{case}: measured case identity has the wrong arm attribution'
                    )
                common = common_identity(before)
                if side in producer_by_side and producer_by_side[side] != common:
                    raise SystemExit(f'{case}: producer identity changed across blocks')
                producer_by_side[side] = common
        block_ended = clock_ns()
        metric, unit = _validate_pair_arms(arms, case)
        _validate_arm_order(
            arms,
            order,
            case,
            block_started=block_started,
            block_ended=block_ended,
        )
        for side in order:
            value = _observation_estimate(arms[side]['observation'])
            if measured:
                samples[side].append(value)
            tag = 'measure' if measured else 'warmup'
            print(
                f'  [{tag} block={block_index} lead={order[0]}] {side}: '
                f'{metric}={value:.9g} {unit}',
                file=sys.stderr,
            )
        blocks.append({
            'index': block_index,
            'lead_order': list(order),
            'measured': measured,
            'metric': metric,
            'unit': unit,
            'timestamps_monotonic_ns': {
                'block_started': block_started,
                'block_ended': block_ended,
            },
            'arms': arms,
        })
    return {'samples': samples, 'blocks': blocks}


def run_blocks_with_perf(
    sides: dict[str, str],
    case: Path,
    extra: list[str],
    *,
    events: Sequence[str],
    warmup: int,
    rounds: int,
    seed: int,
    receipt_bindings: dict[str, dict[str, Any] | None] | None = None,
    package_roots: dict[str, Path] | None = None,
    runner: Callable[
        [str, Path, list[str], Sequence[str]], dict[str, Any]
    ] = run_case_with_perf,
    clock_ns: Callable[[], int] = time.monotonic_ns,
    event_groups: Sequence[Sequence[str]] | None = None,
) -> dict[str, Any]:
    """Run the typed blocked schedule and retain counter evidence per block."""
    requested = expand_perf_events(events)
    samples: dict[str, list[float]] = {'A': [], 'B': []}
    producer_by_side: dict[str, dict[str, Any]] = {}
    blocks: list[dict[str, Any]] = []
    for block_index, order in enumerate(blocked_orders(warmup + rounds, seed)):
        measured = block_index >= warmup
        block_started = clock_ns()
        block: dict[str, Any] = {
            'index': block_index,
            'lead_order': list(order),
            'measured': measured,
        }
        arms: dict[str, Any] = {}
        for side in order:
            if event_groups is None:
                if runner is run_case_with_perf:
                    if receipt_bindings is None or package_roots is None:
                        raise SystemExit(
                            'perf A/B timing requires an admitted subject per side'
                        )
                    result = run_case_with_perf(
                        sides[side],
                        case,
                        extra,
                        requested,
                        receipt_binding=receipt_bindings[side],
                        package_root=package_roots[side],
                        side=side,
                    )
                else:
                    result = runner(sides[side], case, extra, requested)
            elif runner is run_case_with_perf:
                if receipt_bindings is None or package_roots is None:
                    raise SystemExit(
                        'perf A/B timing requires an admitted subject per side'
                    )
                result = run_case_with_perf(
                    sides[side],
                    case,
                    extra,
                    requested,
                    receipt_binding=receipt_bindings[side],
                    package_root=package_roots[side],
                    side=side,
                    event_groups=event_groups,
                )
            else:
                result = runner(
                    sides[side],
                    case,
                    extra,
                    requested,
                    event_groups=event_groups,
                )
            before = result.get('provenance_before')
            after = result.get('provenance_after')
            if (
                isinstance(before, dict)
                and before.get('identity_schema_version') == IDENTITY_SCHEMA_VERSION
            ):
                validate_producer_identity(before)
                if (
                    before.get('arm') != {'side': side, 'row': str(case.resolve())}
                    or after != before
                    or result['observation'].get('producer_identity') != before
                ):
                    raise SystemExit(
                        f'{case}: measured perf case identity has wrong arm attribution'
                    )
                common = common_identity(before)
                if side in producer_by_side and producer_by_side[side] != common:
                    raise SystemExit(
                        f'{case}: perf producer identity changed across blocks'
                    )
                producer_by_side[side] = common
            _validate_counters(result['counters'], requested)
            arms[side] = result
        metric, unit = _validate_pair_arms(arms, case)
        block_ended = clock_ns()
        _validate_arm_order(
            arms,
            order,
            case,
            block_started=block_started,
            block_ended=block_ended,
        )
        block['arms'] = arms
        block['metric'], block['unit'] = metric, unit
        block['timestamps_monotonic_ns'] = {
            'block_started': block_started,
            'block_ended': block_ended,
        }
        for side in order:
            value = _observation_estimate(arms[side]['observation'])
            if measured:
                samples[side].append(value)
            tag = 'measure' if measured else 'warmup'
            print(
                f'  [{tag} block={block_index} lead={order[0]}] '
                f'{side}: {metric}={value:.9g} {unit}',
                file=sys.stderr,
            )
        block['counter_ratios'] = {
            event: _finite_ratio(
                block['arms']['A']['counters'][event]['counter_value'],
                block['arms']['B']['counters'][event]['counter_value'],
            )
            for event in requested
        }
        blocks.append(block)
    return {'samples': samples, 'blocks': blocks, 'events': list(requested)}


def validate_block_schedule(
    blocks: Sequence[dict[str, Any]], *, minimum_measured: int = 3
) -> None:
    """Require an alternating, balanced lead schedule over independent blocks."""
    measured = [block for block in blocks if block.get('measured')]
    if len(measured) < minimum_measured:
        raise SystemExit(
            f'paired schedule requires at least {minimum_measured} measured blocks'
        )
    leads = [tuple(block.get('lead_order', ())) for block in measured]
    if any(lead not in {('A', 'B'), ('B', 'A')} for lead in leads):
        raise SystemExit('paired schedule contains an invalid lead order')
    if {lead[0] for lead in leads} != {'A', 'B'}:
        raise SystemExit('paired schedule must exercise both lead orders')
    if any(left[0] == right[0] for left, right in pairwise(leads)):
        raise SystemExit('paired schedule must alternate lead orders')
    if (
        abs(
            sum(lead[0] == 'A' for lead in leads)
            - sum(lead[0] == 'B' for lead in leads)
        )
        > 1
    ):
        raise SystemExit('paired schedule lead orders are unbalanced')


def compare_blocks(
    blocks: Sequence[dict[str, Any]], seed: int, *, metric: str, unit: str
) -> dict[str, Any]:
    """Compare estimates kept in their producing paired blocks."""
    validate_block_schedule(blocks)
    measured = [block for block in blocks if block.get('measured')]
    ratios: list[float] = []
    for block in measured:
        _validate_block_timing(block, Path('<paired-block>'))
        baseline_observation = block['arms']['A']['observation']
        candidate_observation = block['arms']['B']['observation']
        baseline_metric, baseline_unit, baseline, _raw = measurement_summary(
            baseline_observation
        )
        candidate_metric, candidate_unit, candidate, _raw = measurement_summary(
            candidate_observation
        )
        if (baseline_metric, baseline_unit) != (metric, unit) or (
            candidate_metric,
            candidate_unit,
        ) != (metric, unit):
            raise SystemExit('paired blocks have inconsistent typed metric semantics')
        if candidate <= 0.0:
            raise ValueError('paired block candidate estimate must be positive')
        ratios.append(baseline / candidate)
    return compare_paired_ratios(ratios, seed, metric=metric, unit=unit)


def _observation_estimate(observation: dict[str, Any]) -> float:
    _metric, _unit, estimate, _raw = measurement_summary(observation)
    return estimate


def _validate_pair_arms(arms: dict[str, Any], case: Path) -> tuple[str, str]:
    if set(arms) != {'A', 'B'}:
        raise SystemExit(f'{case}: paired block must contain exactly A and B arms')
    left = arms['A']
    right = arms['B']
    try:
        left_observation = left['observation']
        right_observation = right['observation']
        left_metric, left_unit, _, _ = measurement_summary(left_observation)
        right_metric, right_unit, _, _ = measurement_summary(right_observation)
    except (KeyError, TypeError, ValueError) as exc:
        raise SystemExit(f'{case}: paired observation is malformed: {exc}') from exc
    if (left_metric, left_unit) != (right_metric, right_unit):
        raise SystemExit(
            f'{case}: arms have mismatched typed metrics: '
            f'{left_metric}/{left_unit} vs {right_metric}/{right_unit}'
        )
    for name in ('useful_work', 'phase', 'warmup', 'calibration', 'oracle'):
        if left_observation.get(name) != right_observation.get(name):
            raise SystemExit(f'{case}: arms have mismatched {name} semantics')
    for side in ('A', 'B'):
        arm = arms[side]
        before = arm.get('provenance_before')
        after = arm.get('provenance_after')
        if (
            not isinstance(before, dict)
            or not isinstance(after, dict)
            or before != after
        ):
            raise SystemExit(
                f'{case}: {side} producer identity is incomplete or changed'
            )
        started = arm.get('started_monotonic_ns')
        ended = arm.get('ended_monotonic_ns')
        if (
            not isinstance(started, int)
            or not isinstance(ended, int)
            or started > ended
        ):
            raise SystemExit(f'{case}: {side} monotonic timestamps are invalid')
    return left_metric, left_unit


def _validate_arm_order(
    arms: dict[str, Any],
    order: tuple[str, str],
    case: Path,
    *,
    block_started: int | None = None,
    block_ended: int | None = None,
) -> None:
    first, second = order
    first_started = arms[first]['started_monotonic_ns']
    first_ended = arms[first]['ended_monotonic_ns']
    second_started = arms[second]['started_monotonic_ns']
    second_ended = arms[second]['ended_monotonic_ns']
    bounds = (
        block_started is None
        or block_ended is None
        or block_started
        <= first_started
        <= first_ended
        <= second_started
        <= second_ended
        <= block_ended
    )
    if not bounds:
        raise SystemExit(
            f'{case}: observed arm timestamps overlap or contradict lead order'
        )


def _validate_block_timing(block: dict[str, Any], case: Path) -> None:
    order = tuple(block.get('lead_order', ()))
    timestamps = block.get('timestamps_monotonic_ns')
    arms = block.get('arms')
    if (
        order not in {('A', 'B'), ('B', 'A')}
        or not isinstance(timestamps, dict)
        or not isinstance(arms, dict)
    ):
        raise SystemExit(f'{case}: paired block timing metadata is incomplete')
    block_started = timestamps.get('block_started')
    block_ended = timestamps.get('block_ended')
    if not isinstance(block_started, int) or not isinstance(block_ended, int):
        raise SystemExit(f'{case}: paired block timestamps are invalid')
    _validate_arm_order(
        arms,
        order,
        case,
        block_started=block_started,
        block_ended=block_ended,
    )


def _finite_ratio(numerator: float, denominator: float) -> float | None:
    if denominator == 0:
        return None
    ratio = numerator / denominator
    return ratio if math.isfinite(ratio) else None


def summarize_counter_values(values: Sequence[float], seed: int) -> dict[str, Any]:
    if not values:
        raise ValueError('counter summary requires at least one sample')
    numeric = [float(value) for value in values]
    q1 = percentile(numeric, 0.25)
    q3 = percentile(numeric, 0.75)
    ci_low, ci_high = bootstrap_median_interval(numeric, seed=seed)
    return {
        'samples': list(values),
        'median': statistics.median(values),
        'iqr': q3 - q1,
        'p50': percentile(numeric, 0.50),
        'max_block': max(values),
        'median_ci95': [ci_low, ci_high],
    }


def summarize_counter_ratios(
    blocks: Sequence[dict[str, Any]],
    events: Sequence[str],
    seed: int,
) -> dict[str, Any]:
    """Summarize only measured, paired blocks and preserve undefined ratios."""
    requested = expand_perf_events(events)
    measured = [block for block in blocks if block['measured']]
    if not measured:
        raise ValueError('counter summary requires at least one measured block')
    summary: dict[str, Any] = {
        'primary_event': (
            'instructions' if 'instructions' in requested else requested[0]
        ),
        'A': {},
        'B': {},
        'ratios': {},
    }
    for event_index, event in enumerate(requested):
        values_a = [
            block['arms']['A']['counters'][event]['counter_value'] for block in measured
        ]
        values_b = [
            block['arms']['B']['counters'][event]['counter_value'] for block in measured
        ]
        if any(value is None for value in (*values_a, *values_b)):
            raise ValueError(f'counter {event!r} has an incomplete measured block')
        summary['A'][event] = summarize_counter_values(
            values_a, seed ^ (event_index * 2 + 1)
        )
        summary['B'][event] = summarize_counter_values(
            values_b, seed ^ (event_index * 2 + 2)
        )
        median_ratio = _finite_ratio(
            summary['A'][event]['median'], summary['B'][event]['median']
        )
        if any(value == 0 for value in values_b):
            summary['ratios'][event] = {
                'status': 'undefined-zero-denominator',
                'a_over_b': None,
                'ci95': None,
            }
            continue
        ci_low, ci_high = bootstrap_ratio_ci(
            values_a, values_b, seed ^ (event_index * 2 + 3)
        )
        summary['ratios'][event] = {
            'status': 'ok',
            'a_over_b': median_ratio,
            'ci95': [ci_low, ci_high],
        }
    return summary


def bootstrap_ratio_ci(
    baseline: Sequence[float], candidate: Sequence[float], seed: int
) -> tuple[float, float]:
    """Bootstrap the ratio while preserving each blocked A/B pair.

    The harness measures A and B in the same block specifically so ambient
    drift cancels. Resampling the sides independently discards that design and
    can manufacture variance (or confidence) from frequency/thermal drift.
    """
    if len(baseline) != len(candidate):
        raise ValueError('paired bootstrap requires equal sample counts')

    def ratio(indexes: Sequence[int]) -> float:
        med_b = statistics.median(candidate[index] for index in indexes)
        if not med_b:
            return float('inf')
        return statistics.median(baseline[index] for index in indexes) / med_b

    return bootstrap_interval(len(baseline), ratio, seed=seed)


def compare_paired_ratios(
    ratios: Sequence[float],
    seed: int,
    *,
    metric: str,
    unit: str,
) -> dict[str, Any]:
    """Estimate the arm ratio from one observation pair per independent block."""
    expected_unit, _, _, direction = metric_spec(metric)
    if unit != expected_unit or direction != 'lower_is_better':
        raise ValueError(f'{metric} requires typed unit {expected_unit!r}')
    if not ratios:
        raise ValueError('typed comparison requires at least one paired block')
    numeric = [float(value) for value in ratios]
    if any(not math.isfinite(value) or value <= 0.0 for value in numeric):
        raise ValueError('paired block ratios must be finite and positive')
    log_ratios = [math.log(value) for value in numeric]
    ratio = math.exp(statistics.median(log_ratios))
    log_ci = bootstrap_median_interval(log_ratios, seed=seed)
    ratio_ci = (math.exp(log_ci[0]), math.exp(log_ci[1]))
    differences = [value - 1.0 for value in numeric]
    difference = statistics.median(differences)
    spread = percentile(differences, 0.75) - percentile(differences, 0.25)
    significant = ratio_ci[0] > 1.0 or ratio_ci[1] < 1.0
    return {
        'measurement': {'metric': metric, 'unit': unit, 'direction': direction},
        'relative_ratio_a_over_b': ratio,
        'relative_ratio_ci95': ratio_ci,
        'paired_difference': {'median': difference, 'iqr': spread, 'unit': 'ratio'},
        'paired_ratios': numeric,
        'significant': significant,
    }


def format_directional_verdict(ratio: float, *, metric: str, unit: str) -> str:
    """Describe candidate B's direction from the stored A/B ratio."""
    if ratio > 1.0:
        return f'Candidate B is {ratio:.3f}x faster than A for {metric} ({unit})'
    if ratio < 1.0:
        return f'Candidate B is {1.0 / ratio:.3f}x slower than A for {metric} ({unit})'
    return f'Candidate B matches A for {metric} ({unit}); A/B ratio is 1.000x'


def summarize(
    values: Sequence[float], seed: int, *, metric: str, unit: str
) -> dict[str, Any]:
    if not values:
        raise ValueError('observation summary requires at least one measured block')
    median = statistics.median(values)
    q1 = percentile(values, 0.25)
    q3 = percentile(values, 0.75)
    ci_low, ci_high = bootstrap_median_interval(values, seed=seed)
    return {
        'metric': metric,
        'unit': unit,
        'raw_estimates': list(values),
        'median': median,
        'iqr': q3 - q1,
        'p50': percentile(values, 0.50),
        'max_block': max(values),
        'median_ci95': [ci_low, ci_high],
    }


def pin_cpu(requested: int | None) -> dict[str, Any]:
    before = sorted(os.sched_getaffinity(0))
    cpu, source = select_benchmark_cpu(requested)
    os.sched_setaffinity(0, {cpu})
    base = Path(f'/sys/devices/system/cpu/cpu{cpu}/cpufreq')

    def read(name: str) -> str | None:
        path = base / name
        return path.read_text().strip() if path.exists() else None

    return {
        'cpu': cpu,
        'selection': source,
        'affinity_before': before,
        'affinity_after': sorted(os.sched_getaffinity(0)),
        'governor': read('scaling_governor'),
        'frequency_khz': read('scaling_cur_freq'),
        'driver': read('scaling_driver'),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--a',
        required=True,
        help='Python or wrapper for side A (baseline build)',
    )
    parser.add_argument(
        '--b',
        required=True,
        help='Python or wrapper for side B (candidate build)',
    )
    parser.add_argument(
        '--a-receipt',
        type=Path,
        help='quality receipt, release manifest, or runner-wheel receipt for side A',
    )
    parser.add_argument(
        '--a-package-root',
        type=Path,
        help='installed package root for a release subject on side A',
    )
    parser.add_argument(
        '--b-receipt',
        type=Path,
        help='quality receipt, release manifest, or runner-wheel receipt for side B',
    )
    parser.add_argument(
        '--b-package-root',
        type=Path,
        help='installed package root for a release subject on side B',
    )
    parser.add_argument(
        '--case',
        required=True,
        type=Path,
        help='case script printing one typed observation JSON object',
    )
    parser.add_argument('--rounds', type=int, default=MIN_MEASURED_ROUNDS)
    parser.add_argument('--warmup', type=int, default=2)
    parser.add_argument('--seed', type=int, default=DEFAULT_SEED)
    parser.add_argument(
        '--cpu', type=int, help='CPU to pin; defaults to an isolated CPU if available'
    )
    parser.add_argument(
        '--json-out', type=Path, help='write the complete evidence record as JSON'
    )
    parser.add_argument(
        '--perf',
        action='store_true',
        help='collect optional user-mode perf counters around every case run',
    )
    parser.add_argument(
        '--perf-event',
        dest='perf_events',
        action='append',
        help='perf event to collect; repeat or comma-separate (implies --perf)',
    )
    parser.add_argument(
        'extra', nargs='*', help='extra args forwarded to the case script'
    )
    args = parser.parse_args()
    if args.rounds < MIN_MEASURED_ROUNDS:
        parser.error(f'--rounds must be at least {MIN_MEASURED_ROUNDS}')
    if args.warmup < 1:
        parser.error('--warmup must be at least 1')
    if not args.case.is_file():
        parser.error(f'case does not exist: {args.case}')

    receipt_bindings = {
        side: benchmark_runtime.load_subject(path)
        for side, path in (('A', args.a_receipt), ('B', args.b_receipt))
    }
    package_roots = {
        side: benchmark_runtime.package_root_for_subject(
            receipt_bindings[side],
            explicit,
        )
        for side, explicit in (
            ('A', args.a_package_root),
            ('B', args.b_package_root),
        )
    }
    perf_events = (
        expand_perf_events(args.perf_events) if args.perf or args.perf_events else None
    )
    cpu = pin_cpu(args.cpu)
    doctor_before = collect()
    if cpu['selection'].endswith('not-isolated'):
        print(
            'WARNING: no kernel-isolated CPU was available; record is not release evidence',
            file=sys.stderr,
        )
    perf_run: dict[str, Any] | None = None
    perf_info: dict[str, Any] | None = None
    perf_capability: dict[str, Any] | None = None
    if perf_events is None:
        block_run = run_blocks(
            {'A': args.a, 'B': args.b},
            args.case,
            args.extra,
            warmup=args.warmup,
            rounds=args.rounds,
            seed=args.seed,
            receipt_bindings=receipt_bindings,
            package_roots=package_roots,
        )
    else:
        perf_capability = check_perf_capability(perf_events)
        if perf_capability['status'] != 'available':
            capability_record = {
                'schema_version': 1,
                'observation_type': 'typed-paired-comparison',
                'case': str(args.case),
                'case_sha256': hashlib.sha256(args.case.read_bytes()).hexdigest(),
                'python': {'A': args.a, 'B': args.b},
                'perf': {
                    'enabled': True,
                    'status': 'capability-null',
                    'events': list(perf_events),
                    'capability': perf_capability,
                },
                'publishable': False,
            }
            if args.json_out is not None:
                args.json_out.parent.mkdir(parents=True, exist_ok=True)
                args.json_out.write_text(
                    json.dumps(capability_record, indent=2, sort_keys=True) + '\n'
                )
            print(
                'PERF CAPABILITY NULL: '
                f'{perf_capability.get("reason", "requested events are unavailable")}',
                file=sys.stderr,
            )
            return 2
        perf_info = perf_capability['perf']
        perf_run = run_blocks_with_perf(
            {'A': args.a, 'B': args.b},
            args.case,
            args.extra,
            events=perf_events,
            warmup=args.warmup,
            rounds=args.rounds,
            seed=args.seed,
            receipt_bindings=receipt_bindings,
            package_roots=package_roots,
            event_groups=perf_capability['event_groups'],
        )
        block_run = perf_run
    samples = block_run['samples']
    blocks = block_run['blocks']
    measured_blocks = [block for block in blocks if block['measured']]
    if not measured_blocks:
        raise SystemExit('typed A/B run produced no measured blocks')
    metric = measured_blocks[0]['metric']
    unit = measured_blocks[0]['unit']
    summary_a = summarize(samples['A'], args.seed ^ 0xA, metric=metric, unit=unit)
    summary_b = summarize(samples['B'], args.seed ^ 0xB, metric=metric, unit=unit)
    comparison = compare_blocks(blocks, args.seed ^ 0xAB, metric=metric, unit=unit)
    ratio = comparison['relative_ratio_a_over_b']
    ratio_ci = comparison['relative_ratio_ci95']
    paired_delta = comparison['paired_difference']['median']
    noise = comparison['paired_difference']['iqr']
    delta = abs(paired_delta)
    significant = comparison['significant']
    verdict = (
        format_directional_verdict(ratio, metric=metric, unit=unit)
        if significant
        else 'INCONCLUSIVE (paired log-ratio bootstrap CI includes 1.0; no directional result)'
    )
    timing_semantics = (
        'perf-instrumented-diagnostic' if perf_run is not None else 'uninstrumented'
    )
    if perf_run is not None:
        verdict = f'PERF-INSTRUMENTED DIAGNOSTIC: {verdict}'
    record = {
        'schema_version': 1,
        'observation_type': 'typed-paired-comparison',
        'seed': args.seed,
        'warmup_blocks': args.warmup,
        'measured_blocks': args.rounds,
        'lead_orders': blocked_orders(args.warmup + args.rounds, args.seed),
        'case': str(args.case),
        'case_sha256': hashlib.sha256(args.case.read_bytes()).hexdigest(),
        'extra': args.extra,
        'python': {'A': args.a, 'B': args.b},
        'build': {
            side: common_identity(measured_blocks[0]['arms'][side]['provenance_before'])
            for side in ('A', 'B')
        },
        'environment': {
            'cpu': cpu,
            'platform': platform.platform(),
            'python': sys.version,
            'load_average': os.getloadavg(),
            'bench_doctor_before': doctor_before,
            'contention_after': collect_contention(exclude_pgids={os.getpgrp()}),
        },
        'measurement': {'metric': metric, 'unit': unit, 'direction': 'lower_is_better'},
        'timing_semantics': timing_semantics,
        'release_latency_eligible': perf_run is None,
        'A': summary_a,
        'B': summary_b,
        'relative_ratio_a_over_b': ratio,
        'relative_ratio_ci95': ratio_ci,
        'paired_difference': {'median': paired_delta, 'iqr': noise, 'unit': 'ratio'},
        'paired_ratios': comparison['paired_ratios'],
        'significant': significant,
        'verdict': verdict,
        'blocks': blocks,
    }
    if perf_run is not None and perf_info is not None and perf_events is not None:
        counter_summary = summarize_counter_ratios(
            perf_run['blocks'],
            perf_events,
            args.seed ^ 0xC0,
        )
        record['perf'] = {
            'enabled': True,
            'status': 'available',
            'binary': PERF_BINARY,
            'version': perf_info['version'],
            'version_command': perf_info['command'],
            'events': list(perf_events),
            'all_user': True,
            'format': 'csv',
            'locale': {'LC_ALL': 'C', 'LANG': 'C'},
            'minimum_coverage': MIN_COUNTER_COVERAGE,
            'capability': perf_capability,
            'summary': counter_summary,
            'commands': [
                command
                for block in perf_run['blocks']
                for side in ('A', 'B')
                for command in block['arms'][side].get(
                    'commands', [block['arms'][side]['command']]
                )
            ],
            'timing_semantics': 'instrumented-perf-run; not uninstrumented release latency',
        }
    for label, summary in (('A (baseline)', summary_a), ('B (candidate)', summary_b)):
        ci = summary['median_ci95']
        print(
            f'{label}: metric={metric} unit={unit} median={summary["median"]:.9g} '
            f'IQR={summary["iqr"]:.9g} max_block={summary["max_block"]:.9g} '
            f'95%CI=[{ci[0]:.9f}, {ci[1]:.9f}]'
        )
    print(
        f'relative_ratio A/B={ratio:.4f} 95%CI=[{ratio_ci[0]:.4f}, {ratio_ci[1]:.4f}] '
        f'|difference|={delta:.9g} ratio paired_spread={noise:.9g} ratio'
    )
    print(f'VERDICT: {verdict}')
    if perf_run is not None and perf_events is not None:
        primary_event = record['perf']['summary']['primary_event']
        primary = record['perf']['summary']['ratios'][primary_event]
        if primary['status'] == 'ok':
            print(
                f'PERF {primary_event} A/B={primary["a_over_b"]:.4f} '
                f'95%CI=[{primary["ci95"][0]:.4f}, {primary["ci95"][1]:.4f}]'
            )
        else:
            print(f'PERF {primary_event} A/B=undefined ({primary["status"]})')
    if args.json_out is not None:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(record, indent=2, sort_keys=True) + '\n')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
