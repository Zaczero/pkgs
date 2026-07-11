"""Paired component benchmark for the WebSocket masking kernels.

The Rust driver exercises every payload length across all four masking phases
and all byte offsets modulo 32. This wrapper compiles it for the package's
``x86-64-v2`` floor, runs balanced AB/BA process order on one CPU, and reports
paired bootstrap intervals. Lower nanoseconds per call is better.
"""

from __future__ import annotations

import argparse
import json
import math
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

try:
    from .compare import DEFAULT_SEED, blocked_orders, paired_comparison
except ImportError:  # Direct script execution.
    from compare import DEFAULT_SEED, blocked_orders, paired_comparison

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / 'bench' / 'mask_kernel.rs'
LENGTH_WEIGHTS = {
    '1': 0.02,
    '2': 0.02,
    '8': 0.04,
    '16': 0.08,
    '32': 0.10,
    '64': 0.10,
    '125': 0.14,
    '1500': 0.20,
    '2048': 0.08,
    '4096': 0.08,
    '8192': 0.05,
    '16384': 0.09,
}
PERF_EVENTS = (
    'cycles',
    'instructions',
    'branches',
    'branch-misses',
    'cache-references',
    'cache-misses',
    'L1-dcache-loads',
    'L1-dcache-load-misses',
)


def run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=True, text=True, capture_output=True)


def compile_driver(output: Path) -> None:
    run([
        'rustc',
        '--edition=2024',
        '-O',
        '-C',
        'target-cpu=x86-64-v2',
        '-C',
        'codegen-units=1',
        '-C',
        'lto=fat',
        str(SOURCE),
        '-o',
        str(output),
    ])


def measure(
    binary: Path, kernel: str, cpu: int, *, scale: float = 1.0
) -> dict[str, float]:
    result = run([
        'taskset',
        '-c',
        str(cpu),
        str(binary),
        kernel,
        str(scale),
    ])
    payload = json.loads(result.stdout)
    if payload['kernel'] != kernel:
        raise RuntimeError(f'kernel identity mismatch: {payload!r}')
    return payload['cells']


def weighted(cells: dict[str, float]) -> float:
    return sum(cells[length] * weight for length, weight in LENGTH_WEIGHTS.items())


def compare_kernels(
    binary: Path,
    *,
    cpu: int,
    trials: int,
    warmups: int,
    seed: int,
) -> dict[str, Any]:
    samples = {length: {'control': [], 'candidate': []} for length in LENGTH_WEIGHTS}
    control_weighted: list[float] = []
    candidate_weighted: list[float] = []
    role_orders = blocked_orders(warmups + trials, seed)
    orders = tuple(
        tuple('legacy' if role == 'control' else 'production' for role in order)
        for order in role_orders
    )

    for block_index, order in enumerate(orders):
        block = {kernel: measure(binary, kernel, cpu) for kernel in order}
        if block_index < warmups:
            continue
        control_weighted.append(weighted(block['legacy']))
        candidate_weighted.append(weighted(block['production']))
        for length in LENGTH_WEIGHTS:
            samples[length]['control'].append(block['legacy'][length])
            samples[length]['candidate'].append(block['production'][length])

    return {
        'lead_orders': [list(order) for order in orders],
        'weighted': paired_comparison(
            control_weighted,
            candidate_weighted,
            seed=seed,
            higher_is_better=False,
        ),
        'cells': {
            length: paired_comparison(
                values['control'],
                values['candidate'],
                seed=seed + int(length),
                higher_is_better=False,
            )
            for length, values in samples.items()
        },
    }


def parse_perf_stat(stderr: str, event: str) -> tuple[int, float]:
    """Return an unscaled raw count and running percentage from perf -x output."""
    for line in stderr.splitlines():
        fields = line.split(';')
        if len(fields) < 5 or fields[2] != event:
            continue
        if fields[0].startswith('<'):
            raise RuntimeError(f'{event} is unavailable: {fields[0]}')
        count = int(fields[0].replace(',', ''))
        running_percent = float(fields[4])
        if not math.isfinite(running_percent) or running_percent < 99.9:
            raise RuntimeError(
                f'{event} was multiplexed (running {running_percent:.2f}%)'
            )
        return count, running_percent
    raise RuntimeError(f'perf did not report {event}: {stderr[-4096:]}')


def perf_count(
    binary: Path, kernel: str, cpu: int, event: str, scale: float
) -> dict[str, float | int]:
    result = run([
        'perf',
        'stat',
        '-x',
        ';',
        '-e',
        event,
        '--',
        'taskset',
        '-c',
        str(cpu),
        str(binary),
        kernel,
        str(scale),
    ])
    count, running_percent = parse_perf_stat(result.stderr, event)
    return {'count': count, 'running_percent': running_percent}


def available_perf_events() -> tuple[tuple[str, ...], dict[str, str]]:
    available: list[str] = []
    unavailable: dict[str, str] = {}
    for event in PERF_EVENTS:
        result = subprocess.run(
            ['perf', 'stat', '-x', ';', '-e', event, '--', 'true'],
            check=False,
            text=True,
            capture_output=True,
        )
        try:
            parse_perf_stat(result.stderr, event)
        except (RuntimeError, ValueError) as error:
            unavailable[event] = str(error)
        else:
            available.append(event)
    return tuple(available), unavailable


def collect_perf(
    binary: Path,
    *,
    cpu: int,
    trials: int,
    seed: int,
    scale: float,
) -> dict[str, Any]:
    events, unavailable = available_perf_events()
    orders = blocked_orders(trials, seed)
    samples: dict[str, dict[str, list[int]]] = {
        event: {'control': [], 'candidate': []} for event in events
    }
    raw: list[dict[str, Any]] = []
    kernels = {'control': 'legacy', 'candidate': 'production'}
    for event in events:
        for block_index, order in enumerate(orders):
            block: dict[str, Any] = {
                'event': event,
                'block': block_index,
                'lead_order': list(order),
                'runs': {},
            }
            for role in order:
                evidence = perf_count(binary, kernels[role], cpu, event, scale)
                block['runs'][role] = evidence
                samples[event][role].append(int(evidence['count']))
            raw.append(block)
    return {
        'scale': scale,
        'trials': trials,
        'unavailable': unavailable,
        'raw': raw,
        'comparison': {
            event: paired_comparison(
                values['control'],
                values['candidate'],
                seed=seed + index,
                higher_is_better=False,
            )
            for index, (event, values) in enumerate(samples.items())
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--cpu', type=int, default=15)
    parser.add_argument('--trials', type=int, default=15)
    parser.add_argument('--warmups', type=int, default=2)
    parser.add_argument('--seed', type=int, default=DEFAULT_SEED)
    parser.add_argument('--perf', action='store_true')
    parser.add_argument('--perf-trials', type=int, default=5)
    parser.add_argument('--perf-scale', type=float, default=0.1)
    parser.add_argument('--summary', action='store_true')
    args = parser.parse_args()
    if args.trials < 5 or args.perf_trials < 5:
        parser.error('--trials and --perf-trials must be at least 5')
    if args.warmups < 1:
        parser.error('--warmups must be at least 1')
    if not 0.0 < args.perf_scale <= 1.0:
        parser.error('--perf-scale must be in (0, 1]')
    if args.perf and shutil.which('perf') is None:
        parser.error('--perf requested, but perf is unavailable')

    with tempfile.TemporaryDirectory(prefix='h2corn-mask-kernel-') as directory:
        binary = Path(directory) / 'mask-kernel'
        compile_driver(binary)
        report = {
            'cpu': args.cpu,
            'trials': args.trials,
            'warmups': args.warmups,
            'seed': args.seed,
            'weights': LENGTH_WEIGHTS,
            'comparison': compare_kernels(
                binary,
                cpu=args.cpu,
                trials=args.trials,
                warmups=args.warmups,
                seed=args.seed,
            ),
        }
        if args.perf:
            report['perf'] = collect_perf(
                binary,
                cpu=args.cpu,
                trials=args.perf_trials,
                seed=args.seed,
                scale=args.perf_scale,
            )
    if args.summary:
        summary = {
            'cpu': report['cpu'],
            'trials': report['trials'],
            'comparison': {
                'weighted': {
                    key: report['comparison']['weighted'][key]
                    for key in (
                        'control_median',
                        'candidate_median',
                        'paired_delta_median_percent',
                        'paired_delta_ci95_percent',
                        'empirical_noise_floor_percent',
                        'significant',
                    )
                },
                'cells': {
                    length: {
                        key: cell[key]
                        for key in (
                            'control_median',
                            'candidate_median',
                            'paired_delta_median_percent',
                            'paired_delta_ci95_percent',
                        )
                    }
                    for length, cell in report['comparison']['cells'].items()
                },
            },
        }
        if 'perf' in report:
            summary['perf'] = {
                'trials': report['perf']['trials'],
                'scale': report['perf']['scale'],
                'unavailable': report['perf']['unavailable'],
                'comparison': {
                    event: {
                        key: comparison[key]
                        for key in (
                            'control_median',
                            'candidate_median',
                            'paired_delta_median_percent',
                            'paired_delta_ci95_percent',
                            'empirical_noise_floor_percent',
                            'significant',
                        )
                    }
                    for event, comparison in report['perf']['comparison'].items()
                },
            }
        report = summary
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
