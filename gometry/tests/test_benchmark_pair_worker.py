"""Paired worker controls independent of native benchmark workloads."""

from __future__ import annotations

import importlib.util
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

GOMETRY_ROOT = Path(__file__).resolve().parents[1]
_SUPPORT = GOMETRY_ROOT / 'benches' / 'support'
_PYTHON = GOMETRY_ROOT / 'benches' / 'python'
for path in (_SUPPORT, _PYTHON):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

_SPEC = importlib.util.spec_from_file_location(
    'gometry_bench_paired_test', _PYTHON / 'bench_paired.py'
)
assert _SPEC and _SPEC.loader
worker = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = worker
_SPEC.loader.exec_module(worker)


@dataclass(frozen=True)
class _Pair:
    gometry_call: object
    competitor_call: object


@dataclass(frozen=True)
class _Case:
    pair: _Pair

    def verify(self, _context: object) -> None:
        self.pair.gometry_call()
        self.pair.competitor_call()


@dataclass(frozen=True)
class _Operation:
    gometry: str = 'gometry.fake/1'
    competitor: str = 'shapely.fake/1'
    canonical_symbols: tuple[str, ...] = ('area',)


def _clock() -> object:
    value = 0

    def next_value() -> int:
        nonlocal value
        value += 10
        return value

    return next_value


def _identity() -> dict[str, object]:
    return {'source_inputs_sha256': 'source', 'extension': {'sha256': 'extension'}}


def test_one_fresh_process_block_contains_both_arms_and_consumes_output() -> None:
    operation = _Operation()
    seen: list[str] = []

    def left() -> list[int]:
        seen.append('A')
        return [1]

    def right() -> list[int]:
        seen.append('B')
        return [2]

    artifact = worker.run_block(
        operation=operation,
        block_id='pair-1-block-1-ab',
        pair_id='pair-1',
        lead='ab',
        warmups=1,
        repetitions=2,
        clock_ns=_clock(),
        identity=_identity,
        builder=lambda: _Case(_Pair(left, right)),
    )
    assert artifact['lead_order'] == ['A', 'B']
    assert set(artifact['arms']) == {'A', 'B'}
    assert seen == ['A', 'B', 'A', 'A', 'A', 'B', 'B', 'B']
    assert artifact['arms']['A']['observation']['measurement']['unit'] == 'seconds'
    assert artifact['arms']['A']['observation']['useful_work']['unit'] == 'equivalent-case-invocation'
    assert artifact['arms']['A']['observation']['oracle']['identity'] == (
        'public:gometry.fake/1:oracle-v1'
    )
    assert artifact['arms']['A']['observation']['oracle'] == artifact['arms']['B']['observation']['oracle']


def test_callable_mutation_fails_before_measured_acceptance() -> None:
    calls = 0

    def clock() -> int:
        nonlocal calls
        calls += 1
        return calls

    operation = _Operation()
    with pytest.raises(SystemExit, match='lacks competitor callable'):
        worker.run_block(
            operation=operation,
            block_id='pair-1-block-1-ab',
            pair_id='pair-1',
            lead='ab',
            warmups=1,
            repetitions=2,
            clock_ns=clock,
            identity=_identity,
            builder=lambda: _Case(_Pair(lambda: [1], None)),
        )
    assert calls == 0


def test_result_mutation_fails_untimed_oracle_before_timing() -> None:
    clock_calls = 0
    timed_calls = 0

    def clock() -> int:
        nonlocal clock_calls
        clock_calls += 1
        return clock_calls

    def gometry_call() -> list[int]:
        nonlocal timed_calls
        timed_calls += 1
        return [1]

    def competitor_call() -> list[int]:
        nonlocal timed_calls
        timed_calls += 1
        return [2]

    class _OracleCase(_Case):
        def verify(self, _context: object) -> None:
            if self.pair.gometry_call() != self.pair.competitor_call():
                raise ValueError('result mutation detected')

    with pytest.raises(SystemExit, match='preflight oracle failed'):
        worker.run_block(
            operation=_Operation(),
            block_id='pair-1-block-1-ab',
            pair_id='pair-1',
            lead='ab',
            warmups=1,
            repetitions=2,
            clock_ns=clock,
            identity=_identity,
            builder=lambda: _OracleCase(_Pair(gometry_call, competitor_call)),
        )
    assert timed_calls == 2
    assert clock_calls == 0


def test_block_snapshots_common_identity_once_before_and_after_both_arms() -> None:
    calls = 0

    def identity() -> dict[str, object]:
        nonlocal calls
        calls += 1
        return {'source_inputs_sha256': 'source', 'extension': {'sha256': 'extension'}}

    artifact = worker.run_block(
        operation=_Operation(),
        block_id='pair-1-block-1-ab',
        pair_id='pair-1',
        lead='ab',
        warmups=0,
        repetitions=1,
        clock_ns=_clock(),
        identity=identity,
        builder=lambda: _Case(_Pair(lambda: [1], lambda: [2])),
    )
    assert calls == 2
    assert artifact['arms']['A']['provenance_before'] == artifact['arms']['A']['provenance_after']
