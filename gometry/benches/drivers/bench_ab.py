"""Balanced blocked A/B harness for native performance changes.

Each measured block runs both builds, alternating the lead order ``A/B`` then
``B/A``. A fixed seed chooses the first leader and drives the bootstrap, so the
entire comparison is reproducible without grouping all runs from one build.

The case script prints one elapsed-seconds float on stdout. It must perform
equivalent work and validate its output before printing the measured value.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import random
import statistics
import subprocess
import sys
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
    try:
        value = float(result.stdout.strip().splitlines()[-1])
    except (IndexError, ValueError) as error:
        raise SystemExit(
            f'{case}: expected a float on stdout, got {result.stdout!r} '
            f'(stderr: {result.stderr[-400:]!r})'
        ) from error
    if not math.isfinite(value) or value < 0.0:
        raise SystemExit(
            f'{case}: elapsed seconds must be finite and non-negative, got {value}'
        )
    return value


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
        'extra', nargs='*', help='extra args forwarded to the case script'
    )
    args = parser.parse_args()
    if args.rounds < MIN_MEASURED_ROUNDS:
        parser.error(f'--rounds must be at least {MIN_MEASURED_ROUNDS}')
    if args.warmup < 1:
        parser.error('--warmup must be at least 1')
    if not args.case.is_file():
        parser.error(f'case does not exist: {args.case}')

    cpu = pin_cpu(args.cpu)
    doctor_before = collect()
    if cpu['selection'].endswith('not-isolated'):
        print(
            'WARNING: no kernel-isolated CPU was available; record is not release evidence',
            file=sys.stderr,
        )
    samples = run_blocks(
        {'A': args.a, 'B': args.b},
        args.case,
        args.extra,
        warmup=args.warmup,
        rounds=args.rounds,
        seed=args.seed,
    )
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
    if args.json_out is not None:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(record, indent=2, sort_keys=True) + '\n')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
