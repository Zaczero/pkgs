"""Paired component benchmark for the WebSocket masking kernels.

The Rust driver exercises every payload length across all four masking phases
and all byte offsets modulo 32. An optional chunk size applies one key across
the complete logical payload while carrying phase between chunks. This wrapper
compiles the driver for the package's ``x86-64-v2`` floor, discards complete
warmup blocks, runs balanced AB/BA process order on one CPU, and reports paired
bootstrap intervals. Lower nanoseconds per logical payload is better.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypedDict

try:
    from bench.compare import (
        DEFAULT_SEED,
        PairedComparison,
        blocked_orders,
        paired_comparison,
    )
    from bench.system import (
        BenchmarkSystemState,
        capture_system_state,
        physical_core_keys,
        pin_process_threads,
    )
except ModuleNotFoundError:  # Direct ``python bench/mask_kernel_compare.py``.
    from compare import (  # type: ignore[import-not-found, no-redef]
        DEFAULT_SEED,
        PairedComparison,
        blocked_orders,
        paired_comparison,
    )
    from system import (  # type: ignore[import-not-found, no-redef]
        BenchmarkSystemState,
        capture_system_state,
        physical_core_keys,
        pin_process_threads,
    )

if TYPE_CHECKING:
    from collections.abc import Sequence

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / 'bench' / 'mask_kernel.rs'
PRODUCTION_SOURCE = ROOT / 'src' / 'websocket' / 'codec' / 'mask.rs'
DEFAULT_TRIALS = 16
DEFAULT_WARMUPS = 2
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


class MaskRunEvidence(TypedDict):
    kernel: str
    cells: dict[str, float]


class MaskBlockEvidence(TypedDict):
    block: int
    retained: bool
    lead_order: list[str]
    runs: dict[str, MaskRunEvidence]


class MaskComparison(TypedDict):
    control: str
    candidate: str
    workload: str
    chunk_size: int
    lead_orders: list[list[str]]
    blocks: list[MaskBlockEvidence]
    weighted: PairedComparison
    cells: dict[str, PairedComparison]


class MaskHostEvidence(TypedDict):
    measurement_cpu: int
    management_cpu: int | None
    system: BenchmarkSystemState


class MaskProvenance(TypedDict):
    sources: dict[str, str]
    binary_path: str
    git_head: str | None


class MaskReport(TypedDict):
    cpu: int
    management_cpu: int | None
    trials: int
    warmups: int
    seed: int
    weights: dict[str, float]
    host: MaskHostEvidence
    provenance: MaskProvenance
    comparison: MaskComparison


@dataclass(frozen=True, slots=True)
class MaskBenchmarkHost:
    measurement_cpu: int
    management_cpu: int | None
    system: BenchmarkSystemState


def run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=True, text=True, capture_output=True)


def _compile_command(output: Path) -> list[str]:
    return [
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
    ]


def compile_driver(output: Path) -> None:
    run(_compile_command(output))


def measure(
    binary: Path,
    kernel: str,
    cpu: int,
    workload: str,
    chunk_size: int,
) -> dict[str, float]:
    result = run([
        'taskset',
        '--cpu-list',
        str(cpu),
        str(binary),
        kernel,
        '1.0',
        workload,
        str(chunk_size),
    ])
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError(
            f'mask driver returned invalid JSON: {result.stdout[-2_000:]!r}'
        ) from error
    if (
        not isinstance(payload, dict)
        or payload.get('kernel') != kernel
        or payload.get('workload') != workload
        or payload.get('chunk_size') != chunk_size
    ):
        raise RuntimeError(f'kernel identity mismatch: {payload!r}')
    cells = payload.get('cells')
    if not isinstance(cells, dict) or set(cells) != set(LENGTH_WEIGHTS):
        raise RuntimeError(f'mask driver returned incomplete cells: {cells!r}')
    normalized: dict[str, float] = {}
    for length in LENGTH_WEIGHTS:
        value = cells[length]
        if not isinstance(value, int | float) or not math.isfinite(value) or value <= 0:
            raise RuntimeError(
                f'mask driver returned invalid {length}-byte cell: {value!r}'
            )
        normalized[length] = float(value)
    return normalized


def weighted(cells: dict[str, float]) -> float:
    return sum(cells[length] * weight for length, weight in LENGTH_WEIGHTS.items())


def _capture_mask_system(
    measurement_cpu: int, management_cpu: int | None
) -> BenchmarkSystemState:
    return capture_system_state(
        (measurement_cpu,),
        None,
        None if management_cpu is None else (management_cpu,),
    )


def _validate_pinned_host(
    system: BenchmarkSystemState,
    measurement_cpu: int,
    management_cpu: int,
) -> None:
    measurement = system['server']
    management = system['management']
    if measurement is None or management is None:
        raise RuntimeError('mask benchmark CPU topology is incomplete')
    selected = [*measurement['topology'], *management['topology']]
    offline = [entry['cpu'] for entry in selected if not entry['online']]
    if offline:
        raise RuntimeError(f'mask benchmark CPUs must be online: {offline!r}')
    allowed = system['cgroup_allowed_cpus']
    if allowed is None:
        raise RuntimeError('mask benchmarking requires cgroup CPU evidence')
    disallowed = sorted({measurement_cpu, management_cpu} - set(allowed))
    if disallowed:
        raise RuntimeError(
            f'mask benchmark CPUs are outside the cgroup allowance: {disallowed!r}'
        )
    if system['allowed_cpus'] != [management_cpu]:
        raise RuntimeError(
            'mask benchmark driver is not pinned to its management CPU: '
            f'{system["allowed_cpus"]!r}'
        )
    masks = system['thread_affinity_masks']
    if not masks or any(mask['cpus'] != [management_cpu] for mask in masks):
        raise RuntimeError(
            f'every mask benchmark-driver thread must use the management CPU: {masks!r}'
        )
    if physical_core_keys(measurement) & physical_core_keys(management):
        raise RuntimeError(
            'measurement and management CPUs overlap on one physical core'
        )


def prepare_host(
    measurement_cpu: int,
    management_cpu: int | None,
) -> MaskBenchmarkHost:
    if management_cpu is not None:
        pin_process_threads((management_cpu,))
    system = _capture_mask_system(measurement_cpu, management_cpu)
    if management_cpu is not None:
        _validate_pinned_host(system, measurement_cpu, management_cpu)
    return MaskBenchmarkHost(
        measurement_cpu=measurement_cpu,
        management_cpu=management_cpu,
        system=system,
    )


def _measure_kernel(
    binary: Path,
    kernel: str,
    workload: str,
    chunk_size: int,
    host: MaskBenchmarkHost,
) -> MaskRunEvidence:
    return {
        'kernel': kernel,
        'cells': measure(
            binary,
            kernel,
            host.measurement_cpu,
            workload,
            chunk_size,
        ),
    }


def compare_kernels(
    binary: Path,
    *,
    control: str,
    candidate: str,
    workload: str,
    chunk_size: int,
    host: MaskBenchmarkHost,
    trials: int,
    warmups: int,
    seed: int,
) -> MaskComparison:
    samples = {length: {'control': [], 'candidate': []} for length in LENGTH_WEIGHTS}
    control_weighted: list[float] = []
    candidate_weighted: list[float] = []
    role_orders = blocked_orders(warmups + trials, seed)
    kernels = {'control': control, 'candidate': candidate}
    blocks: list[MaskBlockEvidence] = []

    for block_index, role_order in enumerate(role_orders):
        block: MaskBlockEvidence = {
            'block': block_index,
            'retained': block_index >= warmups,
            'lead_order': [kernels[role] for role in role_order],
            'runs': {},
        }
        for role in role_order:
            block['runs'][role] = _measure_kernel(
                binary,
                kernels[role],
                workload,
                chunk_size,
                host,
            )
        blocks.append(block)
        if not block['retained']:
            continue
        control_cells = block['runs']['control']['cells']
        candidate_cells = block['runs']['candidate']['cells']
        control_weighted.append(weighted(control_cells))
        candidate_weighted.append(weighted(candidate_cells))
        for length in LENGTH_WEIGHTS:
            samples[length]['control'].append(control_cells[length])
            samples[length]['candidate'].append(candidate_cells[length])

    return {
        'control': control,
        'candidate': candidate,
        'workload': workload,
        'chunk_size': chunk_size,
        'lead_orders': [block['lead_order'] for block in blocks],
        'blocks': blocks,
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


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _git_head() -> str | None:
    try:
        result = subprocess.run(
            ['git', 'rev-parse', 'HEAD'],
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=True,
            timeout=10.0,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    return result.stdout.strip() if result.returncode == 0 else None


def capture_provenance(binary: Path) -> MaskProvenance:
    return {
        'sources': {
            'driver': _sha256(SOURCE),
            'production': _sha256(PRODUCTION_SOURCE),
        },
        'binary_path': str(binary.resolve()),
        'git_head': _git_head(),
    }


_SUMMARY_REPORT_KEYS = (
    'cpu',
    'management_cpu',
    'trials',
    'warmups',
)
_SUMMARY_COMPARISON_KEYS = ('control', 'candidate', 'workload', 'chunk_size')
_SUMMARY_WEIGHTED_KEYS = (
    'control_median',
    'candidate_median',
    'paired_delta_median_percent',
    'paired_delta_ci95_percent',
    'paired_delta_iqr_percent',
    'directionally_stable_above_iqr',
)
_SUMMARY_CELL_KEYS = (
    'control_median',
    'candidate_median',
    'paired_delta_median_percent',
    'paired_delta_ci95_percent',
)


def _summary(report: MaskReport) -> dict[str, Any]:
    """Project the report onto its summary key set, dropping raw blocks."""
    report_data: dict[str, Any] = dict(report)
    comparison: dict[str, Any] = dict(report['comparison'])
    return {
        **{key: report_data[key] for key in _SUMMARY_REPORT_KEYS},
        'comparison': {
            **{key: comparison[key] for key in _SUMMARY_COMPARISON_KEYS},
            'weighted': {
                key: comparison['weighted'][key] for key in _SUMMARY_WEIGHTED_KEYS
            },
            'cells': {
                length: {key: cell[key] for key in _SUMMARY_CELL_KEYS}
                for length, cell in comparison['cells'].items()
            },
        },
        'host': report_data['host'],
        'provenance': report_data['provenance'],
    }


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        '--cpu',
        type=int,
        required=True,
        help='measurement CPU; no default because it is host-topology-specific',
    )
    parser.add_argument(
        '--management-cpu',
        type=int,
        help='pin every harness thread here and validate its topology',
    )
    parser.add_argument('--trials', type=int, default=DEFAULT_TRIALS)
    parser.add_argument('--warmups', type=int, default=DEFAULT_WARMUPS)
    parser.add_argument('--seed', type=int, default=DEFAULT_SEED)
    parser.add_argument(
        '--control-kernel',
        choices=('legacy', 'eager-key'),
        default='eager-key',
    )
    parser.add_argument(
        '--candidate-kernel', choices=('production',), default='production'
    )
    parser.add_argument(
        '--state-workload', choices=('cartesian', 'phase0'), default='cartesian'
    )
    parser.add_argument(
        '--chunk-size',
        type=int,
        default=0,
        help='reuse one mask key across chunks of this size; 0 masks each payload whole',
    )
    parser.add_argument('--summary', action='store_true')
    parser.add_argument('--output', type=Path)
    return parser


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = create_parser()
    args = parser.parse_args(argv)
    if args.trials < 6 or args.trials % 2:
        parser.error('--trials must be an even number of at least 6')
    if args.warmups < 1:
        parser.error('--warmups must be at least 1')
    if args.cpu < 0:
        parser.error('--cpu must be non-negative')
    if args.management_cpu is not None and args.management_cpu < 0:
        parser.error('--management-cpu must be non-negative')
    if args.management_cpu == args.cpu:
        parser.error('--cpu and --management-cpu must differ')
    if args.chunk_size < 0:
        parser.error('--chunk-size must be non-negative')
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    for executable in ('rustc', 'taskset'):
        if shutil.which(executable) is None:
            raise SystemExit(f'{executable} is required for the mask benchmark')

    try:
        with tempfile.TemporaryDirectory(prefix='h2corn-mask-kernel-') as directory:
            binary = Path(directory) / 'mask-kernel'
            compile_driver(binary)
            host = prepare_host(args.cpu, args.management_cpu)
            provenance = capture_provenance(binary)
            comparison = compare_kernels(
                binary,
                control=args.control_kernel,
                candidate=args.candidate_kernel,
                workload=args.state_workload,
                chunk_size=args.chunk_size,
                host=host,
                trials=args.trials,
                warmups=args.warmups,
                seed=args.seed,
            )
            host_evidence: MaskHostEvidence = {
                'measurement_cpu': host.measurement_cpu,
                'management_cpu': host.management_cpu,
                'system': host.system,
            }
            report: MaskReport = {
                'cpu': args.cpu,
                'management_cpu': args.management_cpu,
                'trials': args.trials,
                'warmups': args.warmups,
                'seed': args.seed,
                'weights': LENGTH_WEIGHTS,
                'host': host_evidence,
                'provenance': provenance,
                'comparison': comparison,
            }
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        raise SystemExit(f'mask benchmark failed: {error}') from error

    serialized = json.dumps(_summary(report) if args.summary else report, indent=2)
    if args.output is None:
        print(serialized)
    else:
        args.output.write_text(serialized + '\n')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
