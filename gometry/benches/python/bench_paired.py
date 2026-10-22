"""Run both members of one public benchmark pair in one process block."""

from __future__ import annotations

import argparse
import importlib
import json
import math
import os
import platform
import sys
import time
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

_PYTHON = Path(__file__).resolve().parent
_BENCHES = _PYTHON.parent
_SUPPORT = _BENCHES / 'support'
_DRIVERS = _BENCHES / 'drivers'
_TOOLS = _PYTHON.parents[1] / 'tools'
for _path in (_PYTHON, _SUPPORT, _DRIVERS, _TOOLS):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from _gatelib import prepend_tools_import_paths

prepend_tools_import_paths()

from benchroom.sampling import next_batch_size

from _bench_oracles import (
    PUBLIC_CASE_BUILDERS,
    PUBLIC_SCALING_CASE_BUILDERS,
    public_oracle_identity,
)
from _bench_registry import operation_for_row, scaling_record
from bench_doctor import (
    packages_required_by_operations,
    implementation_identity,
)
import benchmark_runtime
from benchmark_identity import (
    IDENTITY_SCHEMA_VERSION,
    identity_for_arm,
)
from benchmark_observation import make_observation

_OUTPUT_SINK = 0
PAIR_ARTIFACT_SCHEMA_VERSION = 1
MIN_SAMPLE_NS = 1_000_000
MAX_CALIBRATION_BATCHES = 20


def _time_batch(
    call: Callable[[], object], iterations: int, clock_ns: Callable[[], int]
) -> int:
    started = clock_ns()
    for _ in range(iterations):
        # Discard every result here so each call includes its own release,
        # including the last call. No result escapes into another sample.
        call()
    return clock_ns() - started


def _arm_identity(
    identity: dict[str, Any], *, side: str, row: str, extension: dict[str, Any]
) -> dict[str, Any]:
    """Keep common facts intact while attributing one implementation to an arm."""
    if identity.get('identity_schema_version') != IDENTITY_SCHEMA_VERSION:
        # Unit tests may inject a deliberately tiny identity provider; real
        # worker output always comes from producer_identity above.
        return identity
    packages = identity.get('packages')
    package_name = 'gometry' if row.startswith('gometry.') else row.split('.', 1)[0]
    package_info = packages.get(package_name) if isinstance(packages, dict) else None
    implementation = implementation_identity(
        row,
        extension,
        package_info=package_info if isinstance(package_info, dict) else None,
        loaded_native_libraries=identity.get('loaded_native_libraries'),
    )
    return identity_for_arm(
        identity,
        side=side,
        row=row,
        runtime=implementation['runtime'],
        module=implementation['module'],
        distribution=implementation['distribution'],
        native_libraries=implementation['native_libraries'],
        native_library_status=implementation['native_library_status'],
        source_commit=(
            identity.get('source_commit')
            if implementation['runtime'] == 'gometry'
            else None
        ),
        source_receipt=(
            identity.get('source_receipt')
            if implementation['runtime'] == 'gometry'
            else None
        ),
        source_wheel=(
            identity.get('source_wheel')
            if implementation['runtime'] == 'gometry'
            else None
        ),
    )


def _consume_output(value: object) -> None:
    """Keep each result observable without serializing the result itself."""
    global _OUTPUT_SINK
    if value is None:
        raise RuntimeError('paired benchmark callable returned no useful output')
    try:
        size = len(value)  # type: ignore[arg-type]
    except TypeError:
        size = 1
    _OUTPUT_SINK = (_OUTPUT_SINK + size + id(value)) & ((1 << 63) - 1)


def _measure_arm(
    *,
    row: str,
    call: Callable[[], object],
    clock_ns: Callable[[], int],
    producer_identity: dict[str, Any] | None,
    warmups: int,
    repetitions: int,
    phase: str,
    useful_work: dict[str, Any],
    output_validator: Callable[[object], None] | None,
    oracle_identity: str,
    min_sample_ns: int,
) -> dict[str, Any]:
    if not callable(call):
        raise SystemExit(f'paired block callable for {row} is not callable')
    if phase == 'warm' and warmups < 1:
        raise SystemExit(f'warm phase for {row} requires explicit untimed prewarming')
    if phase == 'first-use':
        raise SystemExit(
            'first-use cannot be paired in one shared process: the first arm warms '
            'state before the second arm is invoked'
        )
    for _ in range(warmups):
        _consume_output(call())
    iterations = 1
    calibration_iterations = 0
    for _ in range(MAX_CALIBRATION_BATCHES):
        elapsed = _time_batch(call, iterations, clock_ns)
        calibration_iterations += iterations
        next_iterations = next_batch_size(
            iterations,
            elapsed / 1_000_000_000.0,
            minimum_seconds=min_sample_ns / 1_000_000_000.0,
        )
        if next_iterations == iterations:
            break
        iterations = next_iterations
    else:
        raise SystemExit(f'paired batch calibration did not converge for {row}')
    started = clock_ns()
    batch_seconds = [
        _time_batch(call, iterations, clock_ns) / 1_000_000_000.0
        for _ in range(repetitions)
    ]
    ended = clock_ns()
    last_value = call()
    _consume_output(last_value)
    if output_validator is not None:
        try:
            output_validator(last_value)
        except Exception as exc:
            raise SystemExit(
                f'paired block output validation failed for {row}: {exc}'
            ) from exc
    observation = make_observation(
        metric='duration',
        raw_values=[elapsed / iterations for elapsed in batch_seconds],
        useful_work=useful_work,
        oracle_identity=oracle_identity,
        phase=phase,
        warmup_iterations=warmups,
        calibration_iterations=calibration_iterations,
        calibration_strategy='bounded-doubling',
        batch={
            'iterations': iterations,
            'raw_seconds': batch_seconds,
            'result_lifetime': 'released-inside-timer',
        },
    )
    if producer_identity is not None:
        observation['producer_identity'] = producer_identity
    arm_identity = producer_identity if producer_identity is not None else {}
    return {
        'row': row,
        'started_monotonic_ns': started,
        'ended_monotonic_ns': ended,
        'provenance_before': arm_identity,
        'provenance_after': arm_identity,
        'observation': observation,
    }


def run_block(
    *,
    operation: Any,
    block_id: str,
    pair_id: str,
    lead: str,
    warmups: int,
    repetitions: int,
    clock_ns: Callable[[], int] = time.perf_counter_ns,
    identity: Callable[[], dict[str, Any]],
    completion: Callable[[], dict[str, Any]] | None = None,
    builder: Callable[[], Any] | None = None,
    min_sample_ns: int = MIN_SAMPLE_NS,
) -> dict[str, Any]:
    """Build and execute one complete paired block."""
    if lead not in {'ab', 'ba'}:
        raise SystemExit(f'paired block lead must be ab or ba, got {lead!r}')
    if warmups < 0 or repetitions < 1:
        raise SystemExit(
            'paired block warmups must be non-negative and repetitions positive'
        )
    if min_sample_ns < 1:
        raise SystemExit('minimum sample duration must be positive')
    phase = operation.phase
    fixture_started = time.perf_counter()
    invocation_id = os.environ.get('GOMETRY_BENCH_INVOCATION_ID') or uuid.uuid4().hex
    if phase == 'first-use':
        raise SystemExit(
            'first-use cannot be paired in one shared process: the first arm warms '
            'state before the second arm is invoked'
        )
    if builder is None:
        # Import the workload catalog only after the worker has passed CLI and
        # oracle-gate checks; importing it constructs native-backed fixtures.
        import _bench_public_cases as _public_cases  # ruff: ignore[unused-import]
        import _bench_scaling_cases as _scaling_cases  # ruff: ignore[unused-import]

        case_builder = (
            PUBLIC_SCALING_CASE_BUILDERS.get(operation.gometry)
            if hasattr(operation, 'curve_id')
            else PUBLIC_CASE_BUILDERS.get(operation.gometry)
        )
    else:
        case_builder = builder
    if case_builder is None:
        raise SystemExit(f'missing public paired case builder: {operation.gometry}')
    case = case_builder()
    pair = getattr(case, 'pair', None)
    if pair is None or not callable(getattr(pair, 'gometry_call', None)):
        raise SystemExit(f'public paired case is malformed: {operation.gometry}')
    if not callable(getattr(pair, 'competitor_call', None)):
        raise SystemExit(
            f'public paired case lacks competitor callable: {operation.gometry}'
        )
    oracle_identity = public_oracle_identity(operation)
    useful_work = getattr(case, 'useful_work', None)
    if not isinstance(useful_work, dict):
        useful_work = {'count': 1, 'unit': 'equivalent-case-invocation'}
    output_validator = getattr(case, 'output_validator', None)
    arms = {
        'A': (operation.gometry, pair.gometry_call),
        'B': (operation.competitor, pair.competitor_call),
    }
    order = ('A', 'B') if lead == 'ab' else ('B', 'A')
    # Competitor callables import lazily; load every package the pair needs
    # first so the attested native libraries cannot change mid-block.
    for package in sorted(packages_required_by_operations((operation,))):
        importlib.import_module(package)
    fixture_ended = time.perf_counter()
    common_before = identity()
    block_started = clock_ns()
    if common_before.get('identity_schema_version') == IDENTITY_SCHEMA_VERSION:
        arm_identity_before = {
            side: _arm_identity(
                common_before,
                side=side,
                row=arm[0],
                extension=common_before.get('extension', {}),
            )
            for side, arm in arms.items()
        }
    else:
        arm_identity_before = {'A': {}, 'B': {}}
    measurement_started = time.perf_counter()
    measured = {
        side: _measure_arm(
            row=arms[side][0],
            call=arms[side][1],
            clock_ns=clock_ns,
            producer_identity=arm_identity_before[side],
            warmups=warmups,
            repetitions=repetitions,
            phase=phase,
            useful_work=useful_work,
            output_validator=output_validator,
            oracle_identity=oracle_identity,
            min_sample_ns=min_sample_ns,
        )
        for side in order
    }
    measurement_ended = time.perf_counter()
    common_after = completion() if completion is not None else identity()
    if common_before != common_after:
        raise SystemExit(
            'paired block producer identity changed during the timed block'
        )
    if common_after.get('identity_schema_version') == IDENTITY_SCHEMA_VERSION:
        arm_identity_after = {
            side: _arm_identity(
                common_after,
                side=side,
                row=arm[0],
                extension=common_after.get('extension', {}),
            )
            for side, arm in arms.items()
        }
    else:
        arm_identity_after = {'A': {}, 'B': {}}
    block_ended = clock_ns()
    for side in ('A', 'B'):
        measured[side]['provenance_after'] = arm_identity_after[side]
        measured[side]['observation']['producer_identity'] = arm_identity_before[side]
        case_process = {
            'pid': os.getpid(),
            'invocation_id': invocation_id,
            'row': arms[side][0],
            'identity_sha256': benchmark_runtime.identity_sha256(
                arm_identity_before[side]
            ),
        }
        measured[side]['case_process'] = case_process
        measured[side]['observation']['case_process'] = case_process
    return {
        'schema_version': PAIR_ARTIFACT_SCHEMA_VERSION,
        'artifact_type': 'gometry-paired-block',
        'block_id': block_id,
        'pair_id': pair_id,
        'operation': {
            'gometry': operation.gometry,
            'competitor': operation.competitor,
            'canonical_symbols': list(operation.canonical_symbols),
            'phase': phase,
            'scaling': scaling_record(operation),
        },
        'lead_order': list(order),
        'state_scope': 'shared-process-prepared-fixture',
        'process': {
            'pid': os.getpid(),
            'invocation_id': invocation_id,
            'platform': platform.platform(),
            'executable': str(Path(sys.executable).resolve()),
        },
        'timestamps_monotonic_ns': {
            'block_started': block_started,
            'block_ended': block_ended,
        },
        'sampling': {
            'warmup_iterations': warmups,
            'repetitions': repetitions,
            'calibration_strategy': 'bounded-doubling',
            'min_sample_ns': min_sample_ns,
            'phase': phase,
            'scaling': scaling_record(operation),
        },
        'arms': measured,
        'phase_seconds': {
            'fixture_and_competitor_import': fixture_ended - fixture_started,
            'measurement': measurement_ended - measurement_started,
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--gometry', required=True)
    parser.add_argument(
        '--gometry-receipt',
        type=Path,
        help=argparse.SUPPRESS,
    )
    parser.add_argument('--receipt-sha256', help=argparse.SUPPRESS)
    parser.add_argument('--pair-id', required=True)
    parser.add_argument('--block-id', required=True)
    parser.add_argument('--lead', choices=('ab', 'ba'), required=True)
    parser.add_argument('--warmups', type=int, required=True)
    parser.add_argument('--repetitions', type=int, required=True)
    parser.add_argument(
        '--min-sample-time',
        type=float,
        default=MIN_SAMPLE_NS / 1_000_000_000,
        help='Target this many seconds per batch in untimed calibration (default: %(default)s)',
    )
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    if not math.isfinite(args.min_sample_time) or args.min_sample_time < 1e-9:
        parser.error('--min-sample-time must be finite and at least 1 ns')
    if os.environ.get('GOMETRY_BENCH_ORACLE_OK') != '1':
        raise SystemExit(
            'paired benchmark timing requires GOMETRY_BENCH_ORACLE_OK=1 '
            '(the driver oracle must pass first)'
        )
    if (args.gometry_receipt is None) != (args.receipt_sha256 is None):
        parser.error('--gometry-receipt and --receipt-sha256 must be supplied together')
    runtime = benchmark_runtime.admit_runtime(
        args.gometry_receipt,
        expected_receipt_sha256=args.receipt_sha256,
        package_root=os.environ.get('GOMETRY_BENCH_PACKAGE_ROOT'),
    )
    operation = operation_for_row(args.gometry)
    if operation is None or operation.gometry != args.gometry:
        raise SystemExit(f'unknown gometry public row: {args.gometry}')
    artifact = run_block(
        operation=operation,
        block_id=args.block_id,
        pair_id=args.pair_id,
        lead=args.lead,
        warmups=args.warmups,
        repetitions=args.repetitions,
        identity=runtime.collect_identity,
        completion=runtime.complete,
        min_sample_ns=int(args.min_sample_time * 1_000_000_000),
    )
    artifact['phase_seconds'].update(runtime.phase_seconds or {})
    if runtime.run_admission is not None:
        from benchmark_evidence import compact_artifact

        artifact = compact_artifact(
            artifact, runtime.subject['run_admission']['sha256']
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x', encoding='utf-8') as stream:
        json.dump(artifact, stream, indent=2, sort_keys=True)
        stream.write('\n')
    print(f'wrote {args.output}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
