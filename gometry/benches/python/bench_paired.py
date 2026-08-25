"""Run both members of one public benchmark pair in one process block."""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
import platform
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

_PYTHON = Path(__file__).resolve().parent
_BENCHES = _PYTHON.parent
_SUPPORT = _BENCHES / 'support'
_DRIVERS = _BENCHES / 'drivers'
for _path in (_PYTHON, _SUPPORT, _DRIVERS):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from _bench_oracles import OracleContext, PUBLIC_CASE_BUILDERS, public_oracle_identity
from _bench_registry import operation_for_row
from benchmark_observation import make_observation

_DEPENDENCIES = (
    'gometry', 'numpy', 'shapely', 'pyproj', 'h3', 'geopandas', 'pyarrow',
    'mercantile', 'pygeohash',
)
_OUTPUT_SINK = 0


def _package_identity() -> dict[str, str | None]:
    versions: dict[str, str | None] = {}
    for name in _DEPENDENCIES:
        try:
            versions[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            versions[name] = None
    try:
        import shapely
        geos = getattr(shapely, 'geos_version_string', None)
    except ImportError:
        geos = None
    try:
        import pyproj
        proj = getattr(pyproj, 'proj_version_str', None)
    except ImportError:
        proj = None
    versions['GEOS'] = geos
    versions['PROJ'] = proj
    return versions


def producer_identity() -> dict[str, Any]:
    """Capture immutable producer facts, not transient host load."""
    import gometry._lib as lib
    from bench_doctor import _extension_identity, _source_inputs_digest, _uv_lock_identity

    extension = _extension_identity()
    source_digest = _source_inputs_digest()
    build_info = extension.get('build_info')
    if (
        isinstance(build_info, dict)
        and source_digest is not None
        and build_info.get('source_inputs_sha256') != source_digest
    ):
        raise SystemExit(
            'paired block producer identity is stale: embedded source input digest '
            f'{build_info.get("source_inputs_sha256")!r} != live {source_digest!r}'
        )
    if not isinstance(getattr(lib, '__build_info__', None), dict):
        raise SystemExit('paired block producer identity lacks embedded build info')
    return {
        'source_inputs_sha256': source_digest,
        'extension': extension,
        'uv_lock': _uv_lock_identity(),
        'packages': _package_identity(),
        'python': {
            'executable': sys.executable,
            'version': sys.version,
            'implementation': platform.python_implementation(),
            'cache_tag': sys.implementation.cache_tag,
        },
    }


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
    identity: Callable[[], dict[str, Any]],
    warmups: int,
    repetitions: int,
    oracle_identity: str,
) -> dict[str, Any]:
    if not callable(call):
        raise SystemExit(f'paired block callable for {row} is not callable')
    identity_before = identity()
    for _ in range(warmups):
        _consume_output(call())
    started = clock_ns()
    raw_seconds: list[float] = []
    for _ in range(repetitions):
        arm_started = clock_ns()
        _consume_output(call())
        arm_ended = clock_ns()
        raw_seconds.append((arm_ended - arm_started) / 1_000_000_000.0)
    ended = clock_ns()
    identity_after = identity()
    if identity_before != identity_after:
        raise SystemExit(f'paired block producer identity changed during {row}')
    return {
        'row': row,
        'started_monotonic_ns': started,
        'ended_monotonic_ns': ended,
        'provenance_before': identity_before,
        'provenance_after': identity_after,
        'observation': make_observation(
            metric='duration',
            raw_values=raw_seconds,
            useful_work={'count': 1, 'unit': 'equivalent-case-invocation'},
            oracle_identity=oracle_identity,
            phase='warm',
            warmup_iterations=warmups,
            calibration_iterations=repetitions,
        ),
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
    identity: Callable[[], dict[str, Any]] = producer_identity,
    builder: Callable[[], Any] | None = None,
) -> dict[str, Any]:
    """Build and execute one complete paired block."""
    if lead not in {'ab', 'ba'}:
        raise SystemExit(f'paired block lead must be ab or ba, got {lead!r}')
    if warmups < 0 or repetitions < 1:
        raise SystemExit('paired block warmups must be non-negative and repetitions positive')
    if builder is None:
        # Import the workload catalog only after the worker has passed CLI and
        # oracle-gate checks; importing it constructs native-backed fixtures.
        import _bench_public_cases as _public_cases  # noqa: F401
        case_builder = PUBLIC_CASE_BUILDERS.get(operation.gometry)
    else:
        case_builder = builder
    if case_builder is None:
        raise SystemExit(f'missing public paired case builder: {operation.gometry}')
    oracle_case = case_builder()
    pair = getattr(oracle_case, 'pair', None)
    if pair is None or not callable(getattr(pair, 'gometry_call', None)):
        raise SystemExit(f'public paired case is malformed: {operation.gometry}')
    if not callable(getattr(pair, 'competitor_call', None)):
        raise SystemExit(f'public paired case lacks competitor callable: {operation.gometry}')
    oracle_identity = public_oracle_identity(operation)
    verify = getattr(oracle_case, 'verify', None)
    if not callable(verify):
        raise SystemExit(f'public paired case lacks untimed oracle verification: {operation.gometry}')
    try:
        verify(OracleContext(
            operation=operation,
            kind=getattr(oracle_case, 'kind', 'oracle'),
            unit=getattr(oracle_case, 'unit', None),
            oracle_identity=oracle_identity,
        ))
    except Exception as exc:
        raise SystemExit(
            f'public paired preflight oracle failed for {operation.gometry}: {exc}'
        ) from exc
    # The timed case is freshly built so oracle mutations cannot share its state.
    case = case_builder()
    pair = getattr(case, 'pair', None)
    if pair is None or not callable(getattr(pair, 'gometry_call', None)):
        raise SystemExit(f'public timed paired case is malformed: {operation.gometry}')
    if not callable(getattr(pair, 'competitor_call', None)):
        raise SystemExit(f'public timed paired case lacks competitor callable: {operation.gometry}')
    arms = {
        'A': (operation.gometry, pair.gometry_call),
        'B': (operation.competitor, pair.competitor_call),
    }
    order = ('A', 'B') if lead == 'ab' else ('B', 'A')
    block_started = clock_ns()
    measured = {
        side: _measure_arm(
            row=arms[side][0],
            call=arms[side][1],
            clock_ns=clock_ns,
            identity=identity,
            warmups=warmups,
            repetitions=repetitions,
            oracle_identity=oracle_identity,
        )
        for side in order
    }
    block_ended = clock_ns()
    return {
        'schema_version': 1,
        'artifact_type': 'gometry-paired-block',
        'block_id': block_id,
        'pair_id': pair_id,
        'operation': {
            'gometry': operation.gometry,
            'competitor': operation.competitor,
            'canonical_symbols': list(operation.canonical_symbols),
        },
        'lead_order': list(order),
        'state_scope': 'shared-process-prepared-fixture',
        'process': {'pid': os.getpid(), 'platform': platform.platform()},
        'timestamps_monotonic_ns': {
            'block_started': block_started,
            'block_ended': block_ended,
        },
        'sampling': {
            'warmup_iterations': warmups,
            'repetitions': repetitions,
            'calibration_strategy': 'fixed-repetitions',
        },
        'arms': measured,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--gometry', required=True)
    parser.add_argument('--competitor', required=True)
    parser.add_argument('--pair-id', required=True)
    parser.add_argument('--block-id', required=True)
    parser.add_argument('--lead', choices=('ab', 'ba'), required=True)
    parser.add_argument('--warmups', type=int, required=True)
    parser.add_argument('--repetitions', type=int, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    if os.environ.get('GOMETRY_BENCH_ORACLE_OK') != '1':
        raise SystemExit(
            'paired benchmark timing requires GOMETRY_BENCH_ORACLE_OK=1 '
            '(the driver oracle must pass first)'
        )
    operation = operation_for_row(args.gometry)
    if operation is None or operation.gometry != args.gometry:
        raise SystemExit(f'unknown gometry public row: {args.gometry}')
    if operation.competitor != args.competitor:
        raise SystemExit(
            f'competitor does not match manifest for {args.gometry}: '
            f'{args.competitor!r} != {operation.competitor!r}'
        )
    artifact = run_block(
        operation=operation,
        block_id=args.block_id,
        pair_id=args.pair_id,
        lead=args.lead,
        warmups=args.warmups,
        repetitions=args.repetitions,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x', encoding='utf-8') as stream:
        json.dump(artifact, stream, indent=2, sort_keys=True)
        stream.write('\n')
    print(f'wrote {args.output}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
