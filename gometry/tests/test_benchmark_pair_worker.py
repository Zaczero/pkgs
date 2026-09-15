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
    phase: str = 'warm'


def _clock() -> object:
    value = 0

    def next_value() -> int:
        nonlocal value
        value += 10
        return value

    return next_value


def _identity() -> dict[str, object]:
    return {'source_inputs_sha256': 'source', 'extension': {'sha256': 'extension'}}


def _file_identity(path: str) -> dict[str, object]:
    return {'path': path, 'sha256': '0' * 64, 'size_bytes': 1}


def _complete_identity() -> dict[str, object]:
    source = '0' * 64
    extension = {
        **_file_identity('/checkout/gometry.so'),
        'build_info': {'source_inputs_sha256': source},
    }

    def package(name: str) -> dict[str, object]:
        return {
            'available': True,
            'version': '1',
            'module': _file_identity(f'/site/{name}/__init__.py'),
            'distribution': {
                'name': name,
                'version': '1',
                'location': '/site',
                'metadata': _file_identity(f'/site/{name}.dist-info/METADATA'),
                'record': _file_identity(f'/site/{name}.dist-info/RECORD'),
                'record_available': True,
            },
            'native_libraries': [],
        }

    return {
        'identity_schema_version': 1,
        'git_commit': 'test-commit',
        'source_inputs_sha256': source,
        'extension': extension,
        'uv_lock': None,
        'packages': {'gometry': package('gometry'), 'shapely': package('shapely')},
        'loaded_native_libraries': [_file_identity('/checkout/gometry.so')],
        'python': {
            'executable': '/python',
            'version': '3.13 test',
            'implementation': 'CPython',
            'cache_tag': 'cpython-313',
            'build': {
                'implementation': 'CPython',
                'version': '3.13',
                'build': ['test', 'build'],
                'debug': False,
            },
        },
    }


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
    assert seen == ['A', 'A', 'A', 'B', 'B', 'B']
    assert artifact['arms']['A']['observation']['measurement']['unit'] == 'seconds'
    assert (
        artifact['arms']['A']['observation']['useful_work']['unit']
        == 'equivalent-case-invocation'
    )
    assert artifact['arms']['A']['observation']['oracle']['identity'] == (
        'public:gometry.fake/1:oracle'
    )
    assert (
        artifact['arms']['A']['observation']['oracle']
        == artifact['arms']['B']['observation']['oracle']
    )


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


def test_local_oracle_is_not_run_in_timed_worker() -> None:
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
            raise AssertionError(
                'the timed worker must use the external oracle artifact'
            )

    artifact = worker.run_block(
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
    assert set(artifact['arms']) == {'A', 'B'}
    assert timed_calls == 6
    assert clock_calls > 0


def test_warm_phase_requires_explicit_prewarms() -> None:
    with pytest.raises(SystemExit, match='explicit untimed prewarming'):
        worker.run_block(
            operation=_Operation(),
            block_id='pair-1-block-1-ab',
            pair_id='pair-1',
            lead='ab',
            warmups=0,
            repetitions=1,
            clock_ns=_clock(),
            identity=_identity,
            builder=lambda: _Case(_Pair(lambda: [1], lambda: [1])),
        )


def test_first_use_is_not_claimed_for_a_shared_process_pair() -> None:
    with pytest.raises(SystemExit, match='first-use cannot be paired'):
        worker.run_block(
            operation=_Operation(phase='first-use'),
            block_id='pair-1-block-1-ab',
            pair_id='pair-1',
            lead='ab',
            phase='first-use',
            warmups=0,
            repetitions=1,
            clock_ns=_clock(),
            identity=_identity,
            builder=lambda: _Case(_Pair(lambda: [1], lambda: [1])),
        )


def test_useful_output_mutation_fails_after_timing_before_artifact_acceptance() -> None:
    class _ValidatedCase(_Case):
        output_validator = staticmethod(
            lambda _value: (_ for _ in ()).throw(ValueError('mutated useful output'))
        )

    with pytest.raises(SystemExit, match='output validation failed'):
        worker.run_block(
            operation=_Operation(),
            block_id='pair-1-block-1-ab',
            pair_id='pair-1',
            lead='ab',
            warmups=1,
            repetitions=1,
            clock_ns=_clock(),
            identity=_identity,
            builder=lambda: _ValidatedCase(_Pair(lambda: [1], lambda: [1])),
        )


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
        warmups=1,
        repetitions=1,
        clock_ns=_clock(),
        identity=identity,
        builder=lambda: _Case(_Pair(lambda: [1], lambda: [2])),
    )
    assert calls == 2
    assert (
        artifact['arms']['A']['provenance_before']
        == artifact['arms']['A']['provenance_after']
    )


def test_complete_arm_identity_uses_rows_from_tuple_backed_arms(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation = _Operation()
    rows: list[str] = []

    def implementation_identity(
        row: str,
        extension: dict[str, object],
        *,
        package_info: dict[str, object] | None = None,
        loaded_native_libraries: list[dict[str, object]] | None = None,
    ) -> dict[str, object]:
        rows.append(row)
        package = package_info or {}
        runtime = 'gometry' if row.startswith('gometry.') else 'comparator'
        native = (
            [{key: extension[key] for key in ('path', 'sha256', 'size_bytes')}]
            if runtime == 'gometry'
            else []
        )
        return {
            'runtime': runtime,
            'row': row,
            'module': package.get('module'),
            'distribution': package.get('distribution'),
            'native_libraries': native,
            'native_library_status': 'identified' if native else 'not-applicable',
        }

    monkeypatch.setattr(worker, 'implementation_identity', implementation_identity)
    artifact = worker.run_block(
        operation=operation,
        block_id='pair-1-block-1-ab',
        pair_id='pair-1',
        lead='ab',
        warmups=1,
        repetitions=1,
        clock_ns=_clock(),
        identity=_complete_identity,
        builder=lambda: _Case(_Pair(lambda: [1], lambda: [2])),
    )

    assert rows == [operation.gometry, operation.competitor] * 2
    assert (
        artifact['arms']['A']['provenance_before']['implementation']['row']
        == operation.gometry
    )
    assert (
        artifact['arms']['B']['provenance_before']['implementation']['row']
        == operation.competitor
    )
