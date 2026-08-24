"""Balanced blocked A/B harness for native performance changes.

Each measured block runs both builds, alternating the lead order ``A/B`` then
``B/A``. A fixed seed chooses the first leader and drives the bootstrap, so the
entire comparison is reproducible without grouping all runs from one build.

The case script prints one elapsed-seconds float on stdout. It must perform
equivalent work and validate its output before printing the measured value.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import platform
import random
import statistics
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

from bench_doctor import collect, collect_contention, select_benchmark_cpu

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

GOMETRY_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SEED = 20_260_709
MIN_MEASURED_ROUNDS = 9
BOOTSTRAP_SAMPLES = 10_000
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


def expand_perf_events(events: Sequence[str] | None = None) -> tuple[str, ...]:
    """Normalize requested perf events, using the focused default set."""
    if not events:
        return DEFAULT_PERF_EVENTS
    expanded = tuple(
        part.strip()
        for item in events
        for part in item.split(',')
        if part.strip()
    )
    if not expanded:
        return DEFAULT_PERF_EVENTS
    if len(set(expanded)) != len(expanded):
        raise ValueError(f'duplicate perf event in {expanded!r}')
    return expanded


def identify_build(python: str) -> dict[str, Any]:
    code = """
import hashlib, json
from pathlib import Path
import gometry._lib as lib
path = Path(lib.__file__).resolve()
print(json.dumps({
    'extension_path': str(path),
    'extension_sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
    'extension_size_bytes': path.stat().st_size,
}))
"""
    result = subprocess.run(
        [python, '-c', code],
        cwd=GOMETRY_ROOT,
        capture_output=True,
        text=True,
        check=True,
        timeout=IDENTIFY_TIMEOUT_SECONDS,
    )
    return json.loads(result.stdout)


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


def _parse_elapsed_stdout(case: Path, stdout: str, stderr: str) -> float:
    try:
        value = float(stdout.strip().splitlines()[-1])
    except (IndexError, ValueError) as error:
        raise SystemExit(
            f'{case}: expected a float on stdout, got {stdout!r} '
            f'(stderr: {stderr[-400:]!r})'
        ) from error
    if not math.isfinite(value) or value < 0.0:
        raise SystemExit(
            f'{case}: elapsed seconds must be finite and non-negative, got {value}'
        )
    return value


def run_case(python: str, case: Path, extra: list[str]) -> float:
    try:
        result = subprocess.run(
            [python, str(case), *extra],
            cwd=GOMETRY_ROOT,
            capture_output=True,
            text=True,
            check=True,
            timeout=CASE_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as error:
        raise SystemExit(
            f'{case}: exceeded the {CASE_TIMEOUT_SECONDS}s per-block timeout'
        ) from error
    return _parse_elapsed_stdout(case, result.stdout, result.stderr)


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
        'value': raw_value,
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
    runtime_text = fields[event_index + 1].strip() if len(fields) > event_index + 1 else ''
    coverage_text = fields[event_index + 2].strip() if len(fields) > event_index + 2 else ''
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
            or entry.get('value') is None
            or entry.get('coverage') is None
        ):
            failures[event] = {
                'status': entry.get('status'),
                'coverage': entry.get('coverage'),
                'error': entry.get('error'),
            }
    if failures:
        raise SystemExit(f'perf counters are not comparable; refusing result: {failures}')


def run_case_with_perf(
    python: str,
    case: Path,
    extra: list[str],
    events: Sequence[str],
    *,
    perf_binary: str = PERF_BINARY,
    minimum_coverage: float = MIN_COUNTER_COVERAGE,
) -> dict[str, Any]:
    """Run one validated case under perf, keeping perf output out of stdout."""
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
        for event in requested:
            command.extend(('-e', event))
        command.extend(('--', python, str(case), *extra))
        try:
            result = subprocess.run(
                command,
                cwd=GOMETRY_ROOT,
                capture_output=True,
                text=True,
                check=False,
                timeout=CASE_TIMEOUT_SECONDS,
                pass_fds=(log_fd,),
                env={**os.environ, 'LC_ALL': 'C', 'LANG': 'C'},
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
    if result.returncode != 0:
        raise SystemExit(
            f'perf command failed with returncode={result.returncode}; '
            f'case stdout={result.stdout!r}; process stderr={result.stderr[-400:]!r}; '
            f'perf output={perf_output!r}'
        )
    elapsed = _parse_elapsed_stdout(case, result.stdout, result.stderr)
    _validate_counters(counters, requested)
    return {
        'elapsed_seconds': elapsed,
        'counters': counters,
        'command': command,
        'perf_output': perf_output,
        'case_stdout': result.stdout,
        'perf_stderr': result.stderr,
    }


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
    runner: Callable[[str, Path, list[str]], float] = run_case,
) -> dict[str, list[float]]:
    samples: dict[str, list[float]] = {'A': [], 'B': []}
    for block_index, order in enumerate(blocked_orders(warmup + rounds, seed)):
        measured = block_index >= warmup
        for side in order:
            value = runner(sides[side], case, extra)
            if measured:
                samples[side].append(value)
            tag = 'measure' if measured else 'warmup'
            print(
                f'  [{tag} block={block_index} lead={order[0]}] {side}: {value:.9f}s',
                file=sys.stderr,
            )
    return samples


def run_blocks_with_perf(
    sides: dict[str, str],
    case: Path,
    extra: list[str],
    *,
    events: Sequence[str],
    warmup: int,
    rounds: int,
    seed: int,
    runner: Callable[[str, Path, list[str], Sequence[str]], dict[str, Any]] = run_case_with_perf,
) -> dict[str, Any]:
    """Run the existing blocked schedule and retain counter evidence per block."""
    requested = expand_perf_events(events)
    samples: dict[str, list[float]] = {'A': [], 'B': []}
    blocks: list[dict[str, Any]] = []
    for block_index, order in enumerate(blocked_orders(warmup + rounds, seed)):
        measured = block_index >= warmup
        block: dict[str, Any] = {
            'index': block_index,
            'lead_order': list(order),
            'measured': measured,
        }
        for side in order:
            result = runner(sides[side], case, extra, requested)
            _validate_counters(result['counters'], requested)
            value = result['elapsed_seconds']
            if measured:
                samples[side].append(value)
            block[side] = result
            tag = 'measure' if measured else 'warmup'
            print(
                f'  [{tag} block={block_index} lead={order[0]}] '
                f'{side}: {value:.9f}s',
                file=sys.stderr,
            )
        block['counter_ratios'] = {
            event: _finite_ratio(
                block['A']['counters'][event]['value'],
                block['B']['counters'][event]['value'],
            )
            for event in requested
        }
        blocks.append(block)
    return {'samples': samples, 'blocks': blocks, 'events': list(requested)}


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
    ci_low, ci_high = bootstrap_median_ci(numeric, seed)
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
        values_a = [block['A']['counters'][event]['value'] for block in measured]
        values_b = [block['B']['counters'][event]['value'] for block in measured]
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


def percentile(values: Sequence[float], quantile: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError('percentile requires at least one value')
    position = quantile * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def bootstrap_median_ci(
    values: Sequence[float], seed: int, *, samples: int = BOOTSTRAP_SAMPLES
) -> tuple[float, float]:
    rng = random.Random(seed)
    size = len(values)
    medians = [
        statistics.median(values[rng.randrange(size)] for _ in range(size))
        for _ in range(samples)
    ]
    return percentile(medians, 0.025), percentile(medians, 0.975)


def bootstrap_ratio_ci(
    baseline: Sequence[float],
    candidate: Sequence[float],
    seed: int,
    *,
    samples: int = BOOTSTRAP_SAMPLES,
) -> tuple[float, float]:
    """Bootstrap the ratio while preserving each blocked A/B pair.

    The harness measures A and B in the same block specifically so ambient
    drift cancels. Resampling the sides independently discards that design and
    can manufacture variance (or confidence) from frequency/thermal drift.
    """
    if len(baseline) != len(candidate):
        raise ValueError('paired bootstrap requires equal sample counts')
    if not baseline:
        raise ValueError('paired bootstrap requires at least one sample')
    rng = random.Random(seed)
    ratios = []
    size = len(baseline)
    for _ in range(samples):
        indexes = [rng.randrange(size) for _ in range(size)]
        med_a = statistics.median(baseline[index] for index in indexes)
        med_b = statistics.median(candidate[index] for index in indexes)
        ratios.append(med_a / med_b if med_b else float('inf'))
    return percentile(ratios, 0.025), percentile(ratios, 0.975)


def summarize(values: Sequence[float], seed: int) -> dict[str, Any]:
    median = statistics.median(values)
    q1 = percentile(values, 0.25)
    q3 = percentile(values, 0.75)
    ci_low, ci_high = bootstrap_median_ci(values, seed)
    return {
        'samples': list(values),
        'median_seconds': median,
        'iqr_seconds': q3 - q1,
        'p50_seconds': percentile(values, 0.50),
        'max_block_seconds': max(values),
        'median_ci95_seconds': [ci_low, ci_high],
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
    parser.add_argument('--a', required=True, help='Python of side A (baseline build)')
    parser.add_argument('--b', required=True, help='Python of side B (candidate build)')
    parser.add_argument(
        '--case', required=True, type=Path, help='case script printing one float'
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

    perf_events = (
        expand_perf_events(args.perf_events)
        if args.perf or args.perf_events
        else None
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
    if perf_events is None:
        samples = run_blocks(
            {'A': args.a, 'B': args.b},
            args.case,
            args.extra,
            warmup=args.warmup,
            rounds=args.rounds,
            seed=args.seed,
        )
    else:
        perf_info = perf_version()
        perf_run = run_blocks_with_perf(
            {'A': args.a, 'B': args.b},
            args.case,
            args.extra,
            events=perf_events,
            warmup=args.warmup,
            rounds=args.rounds,
            seed=args.seed,
        )
        samples = perf_run['samples']
    summary_a = summarize(samples['A'], args.seed ^ 0xA)
    summary_b = summarize(samples['B'], args.seed ^ 0xB)
    median_a = summary_a['median_seconds']
    median_b = summary_b['median_seconds']
    ratio = median_a / median_b if median_b else float('inf')
    ratio_ci = bootstrap_ratio_ci(samples['A'], samples['B'], args.seed ^ 0xAB)
    paired_deltas = [a - b for a, b in zip(samples['A'], samples['B'], strict=True)]
    paired_delta = statistics.median(paired_deltas)
    noise = percentile(paired_deltas, 0.75) - percentile(paired_deltas, 0.25)
    delta = abs(paired_delta)
    significant = delta > noise and (ratio_ci[0] > 1.0 or ratio_ci[1] < 1.0)
    verdict = (
        f'B is {ratio:.3f}x of A ({"faster" if ratio > 1.0 else "SLOWER"})'
        if significant
        else 'NOISE (paired-delta IQR / bootstrap CI does not exclude equivalence)'
    )
    record = {
        'schema_version': 1,
        'seed': args.seed,
        'warmup_blocks': args.warmup,
        'measured_blocks': args.rounds,
        'lead_orders': blocked_orders(args.warmup + args.rounds, args.seed),
        'case': str(args.case),
        'case_sha256': hashlib.sha256(args.case.read_bytes()).hexdigest(),
        'extra': args.extra,
        'python': {'A': args.a, 'B': args.b},
        'build': {'A': identify_build(args.a), 'B': identify_build(args.b)},
        'environment': {
            'cpu': cpu,
            'platform': platform.platform(),
            'python': sys.version,
            'load_average': os.getloadavg(),
            'bench_doctor_before': doctor_before,
            'contention_after': collect_contention(exclude_pgids={os.getpgrp()}),
        },
        'A': summary_a,
        'B': summary_b,
        'ratio_a_over_b': ratio,
        'ratio_ci95': ratio_ci,
        'paired_delta_median_seconds': paired_delta,
        'noise_floor_seconds': noise,
        'significant': significant,
        'verdict': verdict,
    }
    if perf_run is not None and perf_info is not None and perf_events is not None:
        counter_summary = summarize_counter_ratios(
            perf_run['blocks'], perf_events, args.seed ^ 0xC0,
        )
        record['blocks'] = perf_run['blocks']
        record['perf'] = {
            'enabled': True,
            'binary': PERF_BINARY,
            'version': perf_info['version'],
            'version_command': perf_info['command'],
            'events': list(perf_events),
            'all_user': True,
            'format': 'csv',
            'locale': {'LC_ALL': 'C', 'LANG': 'C'},
            'minimum_coverage': MIN_COUNTER_COVERAGE,
            'summary': counter_summary,
            'commands': [
                block[side]['command']
                for block in perf_run['blocks']
                for side in ('A', 'B')
            ],
        }
    for label, summary in (('A (baseline)', summary_a), ('B (candidate)', summary_b)):
        ci = summary['median_ci95_seconds']
        print(
            f'{label}: median={summary["median_seconds"]:.9f}s '
            f'IQR={summary["iqr_seconds"]:.9f}s max_block={summary["max_block_seconds"]:.9f}s '
            f'95%CI=[{ci[0]:.9f}, {ci[1]:.9f}]'
        )
    print(
        f'ratio A/B={ratio:.4f} 95%CI=[{ratio_ci[0]:.4f}, {ratio_ci[1]:.4f}] '
        f'|delta|={delta:.9f}s noise_floor={noise:.9f}s'
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
