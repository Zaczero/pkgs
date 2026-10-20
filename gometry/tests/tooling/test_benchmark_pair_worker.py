"""Paired worker controls independent of native benchmark workloads."""

from __future__ import annotations

import importlib.util
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

GOMETRY_ROOT = Path(__file__).resolve().parents[2]
_SUPPORT = GOMETRY_ROOT / 'benches' / 'support'
_PYTHON = GOMETRY_ROOT / 'benches' / 'python'
for path in (_SUPPORT, _PYTHON):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

# Resolve benchmark support only after selecting the repository import paths.
from benchmark_identity import (  # ruff: ignore[module-import-not-at-top-of-file]
    identity_for_arm,
    validate_producer_identity,
)

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
    facade = _file_identity('/site/gometry/__init__.py')
    distribution = {
        'name': 'gometry',
        'version': '1',
        'location': '/site',
        'metadata': _file_identity('/site/gometry.dist-info/METADATA'),
        'record': _file_identity('/site/gometry.dist-info/RECORD'),
        'record_available': True,
    }
    extension = {
        **_file_identity('/site/gometry/_lib.cpython-314.so'),
        'build_info': {'source_inputs_sha256': source},
    }

    def package(name: str) -> dict[str, object]:
        return {
            'available': True,
            'version': '1',
            'module': (
                facade
                if name == 'gometry'
                else _file_identity(f'/site/{name}/__init__.py')
            ),
            'distribution': (
                distribution
                if name == 'gometry'
                else {
                    'name': name,
                    'version': '1',
                    'location': '/site',
                    'metadata': _file_identity(f'/site/{name}.dist-info/METADATA'),
                    'record': _file_identity(f'/site/{name}.dist-info/RECORD'),
                    'record_available': True,
                }
            ),
            'native_libraries': [],
        }

    runtime_binding = {
        'kind': 'release',
        'path': '/checkout/release-manifest.json',
        'sha256': '0' * 64,
        'profile': 'release-dist',
        'package_root': '/site',
        'facade': facade,
        'extension': extension,
        'wheel': {
            'path': '/checkout/gometry-1.whl',
            'filename': 'gometry-1.whl',
            'archive': _file_identity('/checkout/gometry-1.whl'),
            'extension_member': 'gometry/_lib.cpython-314.so',
            'extension_sha256': extension['sha256'],
        },
        'distribution': distribution,
        'python_members': {
            'gometry/__init__.py': facade['sha256'],
            'gometry/_lib.pyi': '0' * 64,
        },
    }
    return {
        'identity_schema_version': 1,
        'harness_commit': 'test-commit',
        'source_inputs_sha256': source,
        'extension': extension,
        'uv_lock': None,
        'packages': {'gometry': package('gometry'), 'shapely': package('shapely')},
        'loaded_native_libraries': [_file_identity('/checkout/gometry.so')],
        'runtime_binding': runtime_binding,
        'python': {
            'executable': '/python',
            'resolved_executable': '/python',
            'prefix': '/venv',
            'version': '3.13 test',
            'implementation': 'CPython',
            'cache_tag': 'cpython-313',
            'ext_suffix': '.cpython-313.so',
            'gil_disabled': False,
            'gil_enabled': True,
            'build': {
                'implementation': 'CPython',
                'version': '3.13',
                'build': ['test', 'build'],
                'debug': False,
            },
        },
    }


def test_schema_two_identity_is_rejected_without_local_upgrade() -> None:
    identity = _complete_identity()
    identity['identity_schema_version'] = 2

    with pytest.raises(ValueError, match='unsupported identity schema version'):
        validate_producer_identity(identity)


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
        min_sample_ns=1,
        clock_ns=_clock(),
        identity=_identity,
        builder=lambda: _Case(_Pair(left, right)),
    )
    assert artifact['lead_order'] == ['A', 'B']
    assert set(artifact['arms']) == {'A', 'B'}
    assert seen == ['A'] * 5 + ['B'] * 5
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
        min_sample_ns=1,
        identity=_identity,
        builder=lambda: _OracleCase(_Pair(gometry_call, competitor_call)),
    )
    assert set(artifact['arms']) == {'A', 'B'}
    assert timed_calls == 10
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
            min_sample_ns=1,
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
            min_sample_ns=1,
            clock_ns=_clock(),
            identity=_identity,
            builder=lambda: _ValidatedCase(_Pair(lambda: [1], lambda: [1])),
        )


def test_batches_normalize_creation_and_release_cost_for_every_call() -> None:
    now = 100
    calls = 0
    releases = 0

    class Result:
        def __del__(self) -> None:
            nonlocal now, releases
            now += 200
            releases += 1

    def call() -> Result:
        nonlocal now, calls
        now += 100
        calls += 1
        return Result()

    arm = worker._measure_arm(
        row='gometry.fake/1',
        call=call,
        clock_ns=lambda: now,
        producer_identity=None,
        warmups=1,
        repetitions=3,
        phase='warm',
        useful_work={'count': 1, 'unit': 'invocation'},
        output_validator=None,
        oracle_identity='public:gometry.fake/1:oracle',
        min_sample_ns=1_000,
    )
    observation = arm['observation']
    assert observation['calibration']['iterations'] == 7
    assert observation['batch']['iterations'] == 4
    assert observation['batch']['raw_seconds'] == [1.2e-6] * 3
    assert observation['measurement']['raw']['raw_seconds'] == [3e-7] * 3
    assert calls == 21
    assert releases == calls


def test_unresolved_batch_calibration_fails_instead_of_publishing_precision() -> None:
    with pytest.raises(SystemExit, match='calibration did not converge'):
        worker._measure_arm(
            row='gometry.fake/1',
            call=lambda: [1],
            clock_ns=lambda: 1,
            producer_identity=None,
            warmups=1,
            repetitions=1,
            phase='warm',
            useful_work={'count': 1, 'unit': 'invocation'},
            output_validator=None,
            oracle_identity='public:gometry.fake/1:oracle',
            min_sample_ns=1,
        )


@pytest.mark.parametrize('duration', ['0', 'nan', 'inf'])
def test_cli_rejects_invalid_batch_targets_before_runtime_admission(
    duration: str, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as error:
        worker.main([
            '--gometry',
            'gometry.fake/1',
            '--pair-id',
            'pair',
            '--block-id',
            'block',
            '--lead',
            'ab',
            '--warmups',
            '2',
            '--repetitions',
            '5',
            '--output',
            'unused.json',
            '--min-sample-time',
            duration,
        ])
    assert error.value.code == 2
    assert (
        '--min-sample-time must be finite and at least 1 ns' in capsys.readouterr().err
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
        min_sample_ns=1,
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
        min_sample_ns=1,
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


def test_source_commit_binding_and_null_attribution_require_consistent_receipts() -> (
    None
):
    common = _complete_identity()
    package = common['packages']['gometry']
    extension = common['extension']
    native = [{key: extension[key] for key in ('path', 'sha256', 'size_bytes')}]
    receipt = {
        'path': '/bench-evidence/target/quality-runtime/receipt.json',
        'sha256': '1' * 64,
    }
    wheel = {
        'filename': 'gometry-1.0-cp314-cp314-manylinux_x86_64.whl',
        'sha256': '2' * 64,
        'extension_member': 'gometry/_lib.cpython-314.so',
        'extension_sha256': extension['sha256'],
    }
    binding = common['runtime_binding']
    binding['path'] = receipt['path']
    binding['sha256'] = receipt['sha256']
    binding['wheel']['filename'] = wheel['filename']
    binding['wheel']['archive']['sha256'] = wheel['sha256']
    binding['wheel']['extension_member'] = wheel['extension_member']

    identity = identity_for_arm(
        common,
        side='A',
        row='gometry.op/1',
        runtime='gometry',
        module=package['module'],
        distribution=package['distribution'],
        native_libraries=native,
        native_library_status='identified',
        source_commit='3' * 40,
        source_receipt=receipt,
        source_wheel=wheel,
    )

    assert identity['implementation']['source_commit'] == '3' * 40
    assert identity['implementation']['source_wheel'] == wheel

    unbound = identity_for_arm(
        common,
        side='A',
        row='gometry.op/1',
        runtime='gometry',
        module=package['module'],
        distribution=package['distribution'],
        native_libraries=native,
        native_library_status='identified',
    )
    implementation = unbound['implementation']
    assert implementation['source_commit'] is None
    assert implementation['source_receipt'] is None
    assert implementation['source_wheel'] is None
    assert implementation['module'] == package['module']
    assert implementation['native_libraries'] == native

    with pytest.raises(ValueError, match='require a producer source commit'):
        identity_for_arm(
            common,
            side='A',
            row='gometry.op/1',
            runtime='gometry',
            module=package['module'],
            distribution=package['distribution'],
            native_libraries=native,
            native_library_status='identified',
            source_receipt=receipt,
        )
    with pytest.raises(ValueError, match='require a producer source commit'):
        identity_for_arm(
            common,
            side='A',
            row='gometry.op/1',
            runtime='gometry',
            module=package['module'],
            distribution=package['distribution'],
            native_libraries=native,
            native_library_status='identified',
            source_wheel=wheel,
        )

    with pytest.raises(ValueError, match='source wheel differs from runtime binding'):
        identity_for_arm(
            common,
            side='A',
            row='gometry.op/1',
            runtime='gometry',
            module=package['module'],
            distribution=package['distribution'],
            native_libraries=native,
            native_library_status='identified',
            source_commit='3' * 40,
            source_receipt=receipt,
            source_wheel={**wheel, 'extension_sha256': 'b' * 64},
        )
