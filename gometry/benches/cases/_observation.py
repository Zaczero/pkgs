"""Small stdout adapter for typed native A/B case observations."""

from __future__ import annotations

import json
import os
import sys
import time
from hashlib import sha256
from pathlib import Path
from typing import Any

_SUPPORT = Path(__file__).resolve().parents[1] / 'support'
if str(_SUPPORT) not in sys.path:
    sys.path.insert(0, str(_SUPPORT))

from benchmark_runtime import (
    BenchmarkRuntime,
    admit_runtime,
    identity_sha256,
    validate_identity_for_subject,
)
from benchmark_observation import make_observation, validate_observation

_RUNTIME: BenchmarkRuntime | None = None
_IDENTITY: dict[str, Any] | None = None
_COMMON_IDENTITY: dict[str, Any] | None = None
_INVOCATION_ID: str | None = None
_CASE_FACTS: dict[str, Any] | None = None
_MEASUREMENT_AUTHORIZED = False
_MEASUREMENT_FINISHED = False
_PREPARATION_STARTED: float | None = None
_MEASUREMENT_STARTED: float | None = None


def bootstrap() -> None:
    """Admit the explicitly selected package before the case imports Gometry."""
    global _RUNTIME, _INVOCATION_ID, _PREPARATION_STARTED
    if _RUNTIME is not None:
        raise AssertionError('focused case runtime was bootstrapped twice')
    receipt_path = os.environ.get('GOMETRY_BENCH_SUBJECT_PATH')
    receipt_sha256 = os.environ.get('GOMETRY_BENCH_SUBJECT_SHA256')
    package_root = os.environ.get('GOMETRY_BENCH_PACKAGE_ROOT')
    if (receipt_path is None) != (receipt_sha256 is None):
        raise SystemExit('benchmark subject path and digest must be supplied together')
    if os.environ.get('GOMETRY_BENCH_CONTROL') == '1':
        _INVOCATION_ID = os.environ.get('GOMETRY_BENCH_INVOCATION_ID')
        if not _INVOCATION_ID:
            raise SystemExit('benchmark invocation identity is missing')
    _RUNTIME = admit_runtime(
        None if receipt_path is None else Path(receipt_path),
        expected_receipt_sha256=receipt_sha256,
        package_root=package_root,
    )
    _PREPARATION_STARTED = time.perf_counter()


def prepare_measurement() -> dict[str, Any]:
    """Report admitted case identity and wait for driver permission before timing."""
    global \
        _IDENTITY, \
        _COMMON_IDENTITY, \
        _CASE_FACTS, \
        _MEASUREMENT_AUTHORIZED, \
        _MEASUREMENT_STARTED
    if _RUNTIME is None:
        raise AssertionError('focused case did not admit its runtime before imports')
    if _MEASUREMENT_FINISHED:
        raise AssertionError('focused case cannot restart a completed measurement')
    if _MEASUREMENT_AUTHORIZED:
        assert _IDENTITY is not None
        return _IDENTITY
    if _INVOCATION_ID is None:
        raise SystemExit('focused benchmark timing requires the driver control channel')
    if _PREPARATION_STARTED is not None and _RUNTIME.phase_seconds is not None:
        _RUNTIME.phase_seconds['case_import_and_preparation'] = (
            time.perf_counter() - _PREPARATION_STARTED
        )
    _COMMON_IDENTITY = _RUNTIME.collect_identity()
    side = os.environ.get('GOMETRY_BENCH_SIDE')
    row = os.environ.get('GOMETRY_BENCH_ROW')
    if side not in {'A', 'B'} or not row:
        raise SystemExit('benchmark arm attribution is missing from the driver')
    _IDENTITY = validate_identity_for_subject(
        _COMMON_IDENTITY,
        _RUNTIME.subject,
        side=side,
        row=row,
        verify_archive=False,
    )
    case_path = Path(sys.argv[0]).resolve()
    _CASE_FACTS = {
        'path': str(case_path),
        'sha256': sha256(case_path.read_bytes()).hexdigest(),
    }
    ready = {
        'type': 'ready',
        'invocation_id': _INVOCATION_ID,
        'pid': os.getpid(),
        'case': _CASE_FACTS,
        'identity': _IDENTITY,
    }
    print(
        json.dumps({'benchmark_control': ready}, sort_keys=True),
        flush=True,
    )
    try:
        response = json.loads(sys.stdin.readline())
    except (json.JSONDecodeError, OSError) as error:
        raise SystemExit(
            f'benchmark driver permission was unreadable: {error}'
        ) from error
    if (
        not isinstance(response, dict)
        or response.get('command') != 'measure'
        or response.get('invocation_id') != _INVOCATION_ID
    ):
        raise SystemExit('benchmark driver did not authorize this case invocation')
    _MEASUREMENT_AUTHORIZED = True
    _MEASUREMENT_STARTED = time.perf_counter()
    return _IDENTITY


def finish_measurement() -> dict[str, Any]:
    """Recheck admitted files and report completion outside case timing."""
    global _MEASUREMENT_FINISHED
    if (
        _RUNTIME is None
        or _IDENTITY is None
        or _COMMON_IDENTITY is None
        or _CASE_FACTS is None
        or _INVOCATION_ID is None
        or not _MEASUREMENT_AUTHORIZED
    ):
        raise AssertionError('focused case completed without driver authorization')
    if _MEASUREMENT_FINISHED:
        raise AssertionError('focused case measurement completion was reported twice')
    if _MEASUREMENT_STARTED is not None and _RUNTIME.phase_seconds is not None:
        _RUNTIME.phase_seconds['measurement'] = (
            time.perf_counter() - _MEASUREMENT_STARTED
        )
    identity_digest = identity_sha256(_IDENTITY)
    measurement_done = {
        'type': 'measurement-done',
        'invocation_id': _INVOCATION_ID,
        'pid': os.getpid(),
        'case': _CASE_FACTS,
        'identity_sha256': identity_digest,
    }
    print(
        json.dumps({'benchmark_control': measurement_done}, sort_keys=True),
        flush=True,
    )
    try:
        measurement_ack = json.loads(sys.stdin.readline())
    except (json.JSONDecodeError, OSError) as error:
        raise SystemExit(
            f'benchmark driver completion acknowledgement failed: {error}'
        ) from error
    if (
        not isinstance(measurement_ack, dict)
        or measurement_ack.get('command') != 'measurement-done-ack'
        or measurement_ack.get('invocation_id') != _INVOCATION_ID
    ):
        raise SystemExit('benchmark driver did not acknowledge measured work')
    identity = _RUNTIME.complete()
    if identity is not _COMMON_IDENTITY:
        raise AssertionError('focused case identity changed after driver permission')
    case_path = Path(_CASE_FACTS['path'])
    if sha256(case_path.read_bytes()).hexdigest() != _CASE_FACTS['sha256']:
        raise AssertionError('focused case source changed during measurement')
    completed = {
        'type': 'done',
        'invocation_id': _INVOCATION_ID,
        'pid': os.getpid(),
        'case': _CASE_FACTS,
        'identity_sha256': identity_digest,
    }
    print(
        json.dumps({'benchmark_control': completed}, sort_keys=True),
        flush=True,
    )
    _MEASUREMENT_FINISHED = True
    return {
        'pid': os.getpid(),
        'invocation_id': _INVOCATION_ID,
        'case': _CASE_FACTS,
        'identity_sha256': completed['identity_sha256'],
    }


def result_evidence(payload: dict[str, Any]) -> dict[str, Any]:
    """Attach process phase costs and share admitted run evidence on disk."""
    if _RUNTIME is None:
        raise AssertionError('benchmark result has no admitted runtime')
    payload['phase_seconds'] = dict(_RUNTIME.phase_seconds or {})
    if _RUNTIME.run_admission is None:
        return payload
    from benchmark_evidence import compact_artifact

    return compact_artifact(payload, _RUNTIME.subject['run_admission']['sha256'])


def emit(
    *,
    metric: str,
    raw_values: list[float],
    useful_work: dict[str, Any],
    oracle_identity: str,
    phase: str = 'warm',
    warmup_iterations: int = 0,
    calibration_iterations: int = 0,
    calibration_strategy: str = 'fixed-repetitions',
) -> None:
    if not _MEASUREMENT_AUTHORIZED:
        raise SystemExit(
            'focused case observation was emitted before driver permission'
        )
    if _IDENTITY is None:
        raise AssertionError('focused case has no measured-process identity')
    observation = make_observation(
        metric=metric,
        raw_values=raw_values,
        useful_work=useful_work,
        oracle_identity=oracle_identity,
        phase=phase,
        warmup_iterations=warmup_iterations,
        calibration_iterations=calibration_iterations,
        calibration_strategy=calibration_strategy,
    )
    observation['producer_identity'] = _IDENTITY
    validate_observation(observation)
    observation['case_process'] = finish_measurement()
    print(json.dumps(result_evidence({'observation': observation}), sort_keys=True))
