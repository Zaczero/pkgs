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
import sys
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, TypedDict

try:
    from bench.compare import (
        DEFAULT_SEED,
        PairedComparison,
        blocked_orders,
        paired_comparison,
    )
    from bench.system import (
        MAX_AMBIENT_CPU_UTILIZATION,
        MAX_AMBIENT_SINGLE_CPU_UTILIZATION,
        AmbientCpuProbe,
        BenchmarkSystemState,
        CpuActivity,
        CpuActivityMonitor,
        benchmark_system_state_matches,
        capture_ambient_cpu_probe,
        capture_system_state,
        physical_core_capacity,
        physical_core_keys,
        pin_process_threads,
        validate_ambient_cpu,
        validate_interference_cpu,
    )
except ModuleNotFoundError:  # Direct ``python bench/mask_kernel_compare.py``.
    from compare import (  # type: ignore[import-not-found, no-redef]
        DEFAULT_SEED,
        PairedComparison,
        blocked_orders,
        paired_comparison,
    )
    from system import (  # type: ignore[import-not-found, no-redef]
        MAX_AMBIENT_CPU_UTILIZATION,
        MAX_AMBIENT_SINGLE_CPU_UTILIZATION,
        AmbientCpuProbe,
        BenchmarkSystemState,
        CpuActivity,
        CpuActivityMonitor,
        benchmark_system_state_matches,
        capture_ambient_cpu_probe,
        capture_system_state,
        physical_core_capacity,
        physical_core_keys,
        pin_process_threads,
        validate_ambient_cpu,
        validate_interference_cpu,
    )

if TYPE_CHECKING:
    from collections.abc import Sequence

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / 'bench' / 'mask_kernel.rs'
PRODUCTION_SOURCE = ROOT / 'src' / 'websocket' / 'codec' / 'mask.rs'
SCHEMA_VERSION = 3
DEFAULT_TRIALS = 16
DEFAULT_WARMUPS = 2
DEFAULT_AMBIENT_CPU_PROBE_SECONDS = 1.0
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

EvidenceMode = Literal[
    'pinned-noise-gated',
    'diagnostic-pinned-noisy',
    'diagnostic-unmanaged',
]


class FileIdentity(TypedDict):
    path: str
    sha256: str
    size_bytes: int


class ToolIdentity(TypedDict):
    executable: str
    executable_sha256: str
    version: str


class MaskRunEvidence(TypedDict):
    kernel: str
    cells: dict[str, float]
    interference_cpu_during: CpuActivity | None


class MaskBlockEvidence(TypedDict):
    block: int
    retained: bool
    lead_order: list[str]
    ambient_cpu_before: AmbientCpuProbe | None
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
    mode: EvidenceMode
    measurement_cpu: int
    management_cpu: int | None
    interference_cpus: list[int]
    system_before: BenchmarkSystemState
    system_after: BenchmarkSystemState


class MaskProvenance(TypedDict):
    captured_at: str
    compile_command: list[str]
    rustc: ToolIdentity
    sources: dict[str, FileIdentity]
    binary: FileIdentity
    git_head: str | None
    git_status_short: list[str] | None


class MaskReport(TypedDict):
    schema_version: int
    evidence_mode: EvidenceMode
    cpu: int
    management_cpu: int | None
    trials: int
    warmups: int
    seed: int
    weights: dict[str, float]
    ambient_cpu_probe_seconds: float
    maximum_ambient_cpu_utilization: float
    maximum_ambient_single_cpu_utilization: float
    host: MaskHostEvidence
    provenance: MaskProvenance
    comparison: MaskComparison


@dataclass(frozen=True, slots=True)
class MaskBenchmarkHost:
    mode: EvidenceMode
    measurement_cpu: int
    management_cpu: int | None
    system: BenchmarkSystemState
    measurement_physical_cpus: tuple[int, ...]
    measurement_llc_cpus: tuple[int, ...]
    interference_cpus: tuple[int, ...]
    interference_physical_core_count: int


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


def compile_driver(output: Path) -> list[str]:
    command = _compile_command(output)
    run(command)
    return command


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
    for entry in measurement['topology']:
        frequency = entry['frequency']
        if frequency['scaling_driver'] is None:
            raise RuntimeError('measurement CPU scaling driver is unavailable')
        if frequency['scaling_governor'] != 'performance':
            raise RuntimeError(
                'measurement CPU must use the performance governor, got '
                f'{frequency["scaling_governor"]!r}'
            )
        preference = frequency['energy_performance_preference']
        if preference not in {None, 'performance'}:
            raise RuntimeError(
                'measurement CPU energy preference must be performance, got '
                f'{preference!r}'
            )


def evidence_mode(
    management_cpu: int | None,
    maximum_ambient_cpu_utilization: float,
    maximum_ambient_single_cpu_utilization: float,
) -> EvidenceMode:
    """Classify the noise-evidence grade of this mask benchmark run.

    Deliberately distinct from compare.host_noise_mode: the measurement CPU
    is always pinned here, so the missing role is the *management* CPU
    ('diagnostic-unmanaged'), not the pinning itself ('diagnostic-unpinned').
    Both classifiers share the limits declared in bench.system.
    """
    if management_cpu is None:
        return 'diagnostic-unmanaged'
    if (
        maximum_ambient_cpu_utilization > MAX_AMBIENT_CPU_UTILIZATION
        or maximum_ambient_single_cpu_utilization > MAX_AMBIENT_SINGLE_CPU_UTILIZATION
    ):
        return 'diagnostic-pinned-noisy'
    return 'pinned-noise-gated'


def prepare_host(
    measurement_cpu: int,
    management_cpu: int | None,
    maximum_ambient_cpu_utilization: float = MAX_AMBIENT_CPU_UTILIZATION,
    maximum_ambient_single_cpu_utilization: float = (
        MAX_AMBIENT_SINGLE_CPU_UTILIZATION
    ),
) -> MaskBenchmarkHost:
    if management_cpu is not None:
        pin_process_threads((management_cpu,))
    system = _capture_mask_system(measurement_cpu, management_cpu)
    if management_cpu is not None:
        _validate_pinned_host(system, measurement_cpu, management_cpu)
    online = set(system['online_cpus'])
    measurement = system['server']
    if measurement is None:
        raise RuntimeError('measurement CPU topology is missing')
    topology = measurement['topology'][0]
    physical_cpus = tuple(sorted(online & set(topology['thread_siblings'])))
    llc_cpus = tuple(sorted(online & set(topology['last_level_cache']['shared_cpus'])))
    assigned = {measurement_cpu}
    if management_cpu is not None:
        assigned.add(management_cpu)
    interference_cpus = tuple(sorted(online - assigned))
    if management_cpu is not None and not interference_cpus:
        raise RuntimeError('noise gating requires at least one unassigned logical CPU')
    return MaskBenchmarkHost(
        mode=evidence_mode(
            management_cpu,
            maximum_ambient_cpu_utilization,
            maximum_ambient_single_cpu_utilization,
        ),
        measurement_cpu=measurement_cpu,
        management_cpu=management_cpu,
        system=system,
        measurement_physical_cpus=physical_cpus,
        measurement_llc_cpus=llc_cpus,
        interference_cpus=interference_cpus,
        interference_physical_core_count=(
            physical_core_capacity(interference_cpus) if interference_cpus else 0
        ),
    )


def _verify_host_identity(host: MaskBenchmarkHost) -> BenchmarkSystemState:
    current = _capture_mask_system(host.measurement_cpu, host.management_cpu)
    if not benchmark_system_state_matches(host.system, current):
        raise RuntimeError(
            'frozen mask benchmark host identity changed; refusing mixed evidence'
        )
    return current


def _capture_ambient_cpu(
    host: MaskBenchmarkHost, duration_seconds: float
) -> AmbientCpuProbe:
    measurement = host.system['server']
    if measurement is None:
        raise RuntimeError('measurement CPU topology is missing')
    online = tuple(host.system['online_cpus'])
    return capture_ambient_cpu_probe(
        online,
        host.measurement_physical_cpus,
        host.measurement_llc_cpus,
        host.measurement_llc_cpus,
        online_physical_core_count=host.system['online_physical_core_count'],
        server_physical_core_count=physical_core_capacity(
            host.measurement_physical_cpus
        ),
        server_llc_physical_core_count=physical_core_capacity(
            host.measurement_llc_cpus
        ),
        load_llc_physical_core_count=physical_core_capacity(host.measurement_llc_cpus),
        duration_seconds=duration_seconds,
    )


def _measure_with_host_evidence(
    binary: Path,
    kernel: str,
    workload: str,
    chunk_size: int,
    host: MaskBenchmarkHost,
    *,
    maximum_aggregate: float,
    maximum_single_cpu: float,
) -> MaskRunEvidence:
    if host.management_cpu is None:
        return {
            'kernel': kernel,
            'cells': measure(
                binary,
                kernel,
                host.measurement_cpu,
                workload,
                chunk_size,
            ),
            'interference_cpu_during': None,
        }
    monitor = CpuActivityMonitor(
        host.interference_cpus,
        host.interference_physical_core_count,
    )
    monitor.start()
    try:
        cells = measure(
            binary,
            kernel,
            host.measurement_cpu,
            workload,
            chunk_size,
        )
    finally:
        activity = monitor.stop()
    validate_interference_cpu(
        activity, max_aggregate=maximum_aggregate, max_single=maximum_single_cpu
    )
    return {
        'kernel': kernel,
        'cells': cells,
        'interference_cpu_during': activity,
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
    ambient_cpu_probe_seconds: float,
    maximum_ambient_cpu_utilization: float,
    maximum_ambient_single_cpu_utilization: float,
) -> MaskComparison:
    samples = {length: {'control': [], 'candidate': []} for length in LENGTH_WEIGHTS}
    control_weighted: list[float] = []
    candidate_weighted: list[float] = []
    role_orders = blocked_orders(warmups + trials, seed)
    kernels = {'control': control, 'candidate': candidate}
    blocks: list[MaskBlockEvidence] = []

    for block_index, role_order in enumerate(role_orders):
        _verify_host_identity(host)
        ambient: AmbientCpuProbe | None = None
        if host.management_cpu is not None:
            ambient = _capture_ambient_cpu(host, ambient_cpu_probe_seconds)
            validate_ambient_cpu(
                ambient,
                max_aggregate=maximum_ambient_cpu_utilization,
                max_single=maximum_ambient_single_cpu_utilization,
            )
        block: MaskBlockEvidence = {
            'block': block_index,
            'retained': block_index >= warmups,
            'lead_order': [kernels[role] for role in role_order],
            'ambient_cpu_before': ambient,
            'runs': {},
        }
        for role in role_order:
            block['runs'][role] = _measure_with_host_evidence(
                binary,
                kernels[role],
                workload,
                chunk_size,
                host,
                maximum_aggregate=maximum_ambient_cpu_utilization,
                maximum_single_cpu=maximum_ambient_single_cpu_utilization,
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


def _file_identity(path: Path) -> FileIdentity:
    resolved = path.resolve(strict=True)
    return {
        'path': str(resolved),
        'sha256': _sha256(resolved),
        'size_bytes': resolved.stat().st_size,
    }


def _rustc_identity() -> ToolIdentity:
    executable = shutil.which('rustc')
    if executable is None:
        raise RuntimeError('rustc is unavailable')
    invoked = Path(executable).absolute()
    version = run([str(invoked), '--version', '--verbose']).stdout.strip()
    sysroot = Path(run([str(invoked), '--print', 'sysroot']).stdout.strip())
    compiler = (sysroot / 'bin' / 'rustc').resolve(strict=True)
    return {
        'executable': str(compiler),
        'executable_sha256': _sha256(compiler),
        'version': version,
    }


def _git(command: list[str]) -> str | None:
    try:
        result = subprocess.run(
            ['git', *command],
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=True,
            timeout=10.0,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    return result.stdout if result.returncode == 0 else None


def capture_provenance(binary: Path, compile_command: list[str]) -> MaskProvenance:
    head = _git(['rev-parse', 'HEAD'])
    status = _git([
        'status',
        '--short',
        '--',
        str(SOURCE.relative_to(ROOT)),
        str(PRODUCTION_SOURCE.relative_to(ROOT)),
    ])
    return {
        'captured_at': datetime.now(UTC).isoformat(),
        'compile_command': compile_command,
        'rustc': _rustc_identity(),
        'sources': {
            'driver': _file_identity(SOURCE),
            'production': _file_identity(PRODUCTION_SOURCE),
        },
        'binary': _file_identity(binary),
        'git_head': head.strip() if head is not None else None,
        'git_status_short': status.splitlines() if status is not None else None,
    }


_SUMMARY_REPORT_KEYS = (
    'schema_version',
    'evidence_mode',
    'cpu',
    'management_cpu',
    'trials',
    'warmups',
    'ambient_cpu_probe_seconds',
    'maximum_ambient_cpu_utilization',
    'maximum_ambient_single_cpu_utilization',
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
        help='pin every harness thread here and enable topology/noise evidence gates',
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
    parser.add_argument(
        '--ambient-cpu-probe-seconds',
        type=float,
        default=DEFAULT_AMBIENT_CPU_PROBE_SECONDS,
    )
    parser.add_argument(
        '--max-ambient-cpu-utilization',
        type=float,
        default=MAX_AMBIENT_CPU_UTILIZATION,
        help=(
            'maximum aggregate foreign CPU use in physical-core equivalents; '
            'SMT activity can reach 2, which retains rather than rejects it'
        ),
    )
    parser.add_argument(
        '--max-ambient-single-cpu-utilization',
        type=float,
        default=MAX_AMBIENT_SINGLE_CPU_UTILIZATION,
        help=(
            'maximum foreign utilization of one logical CPU; use 1 to retain '
            'but not reject full-core activity on a known noisy host'
        ),
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
    if args.ambient_cpu_probe_seconds <= 0:
        parser.error('--ambient-cpu-probe-seconds must be positive')
    if not 0 < args.max_ambient_cpu_utilization <= 2:
        parser.error('--max-ambient-cpu-utilization must be in (0, 2]')
    if not 0 < args.max_ambient_single_cpu_utilization <= 1:
        parser.error('--max-ambient-single-cpu-utilization must be in (0, 1]')
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    for executable in ('rustc', 'taskset'):
        if shutil.which(executable) is None:
            raise SystemExit(f'{executable} is required for the mask benchmark')

    try:
        with tempfile.TemporaryDirectory(prefix='h2corn-mask-kernel-') as directory:
            binary = Path(directory) / 'mask-kernel'
            compile_command = compile_driver(binary)
            host = prepare_host(
                args.cpu,
                args.management_cpu,
                args.max_ambient_cpu_utilization,
                args.max_ambient_single_cpu_utilization,
            )
            provenance = capture_provenance(binary, compile_command)
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
                ambient_cpu_probe_seconds=args.ambient_cpu_probe_seconds,
                maximum_ambient_cpu_utilization=args.max_ambient_cpu_utilization,
                maximum_ambient_single_cpu_utilization=(
                    args.max_ambient_single_cpu_utilization
                ),
            )
            system_after = _verify_host_identity(host)
            host_evidence: MaskHostEvidence = {
                'mode': host.mode,
                'measurement_cpu': host.measurement_cpu,
                'management_cpu': host.management_cpu,
                'interference_cpus': list(host.interference_cpus),
                'system_before': host.system,
                'system_after': system_after,
            }
            report: MaskReport = {
                'schema_version': SCHEMA_VERSION,
                'evidence_mode': host.mode,
                'cpu': args.cpu,
                'management_cpu': args.management_cpu,
                'trials': args.trials,
                'warmups': args.warmups,
                'seed': args.seed,
                'weights': LENGTH_WEIGHTS,
                'ambient_cpu_probe_seconds': args.ambient_cpu_probe_seconds,
                'maximum_ambient_cpu_utilization': args.max_ambient_cpu_utilization,
                'maximum_ambient_single_cpu_utilization': (
                    args.max_ambient_single_cpu_utilization
                ),
                'host': host_evidence,
                'provenance': provenance,
                'comparison': comparison,
            }
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        raise SystemExit(f'mask benchmark failed: {error}') from error

    if host.mode == 'diagnostic-unmanaged':
        print(
            'mask benchmark is DIAGNOSTIC_UNMANAGED; pass --management-cpu for '
            'topology, policy, ambient, and during-run CPU gates',
            file=sys.stderr,
        )
    elif host.mode == 'diagnostic-pinned-noisy':
        print(
            'mask benchmark is DIAGNOSTIC_PINNED_NOISY; CPU roles and probes are '
            'retained, but relaxed interference thresholds are not evidence-grade',
            file=sys.stderr,
        )
    serialized = json.dumps(_summary(report) if args.summary else report, indent=2)
    if args.output is None:
        print(serialized)
    else:
        args.output.write_text(serialized + '\n')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
