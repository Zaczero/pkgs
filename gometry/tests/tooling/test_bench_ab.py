"""Typed native A/B protocol and blocked-sampling contracts."""

from __future__ import annotations

import importlib.util
import json
import math
import os
import shutil
import sys
from copy import deepcopy
from itertools import pairwise
from pathlib import Path
from typing import Any

import pytest

GOMETRY_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(GOMETRY_ROOT / 'benches' / 'support'))
sys.path.insert(0, str(GOMETRY_ROOT / 'tools'))
import benchmark_runtime  # noqa: E402
from benchmark_observation import make_observation, validate_observation  # noqa: E402


def _module():
    path = GOMETRY_ROOT / 'benches' / 'drivers' / 'bench_ab.py'
    name = 'gometry_bench_ab_typed_test'
    sys.path.insert(0, str(path.parent))
    try:
        spec = importlib.util.spec_from_file_location(name, path)
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.pop(0)


def _observation(bench_ab, metric: str, value: float) -> dict[str, Any]:
    return make_observation(
        metric=metric,
        raw_values=[value],
        useful_work=(
            {
                'numerator': {'count': 10, 'unit': 'large'},
                'denominator': {'count': 1, 'unit': 'small'},
            }
            if metric == 'scaling_ratio'
            else {'count': 10, 'unit': 'calls'}
        ),
        oracle_identity='test:native-observation-v1',
        warmup_iterations=1,
        calibration_iterations=1,
    )


def _arm(
    bench_ab,
    value: float,
    *,
    metric: str = 'duration',
    identity: str = 'A',
    started: int = 10,
    ended: int = 11,
) -> dict[str, Any]:
    return {
        'observation': _observation(bench_ab, metric, value),
        'started_monotonic_ns': started,
        'ended_monotonic_ns': ended,
        'provenance_before': {'extension': identity},
        'provenance_after': {'extension': identity},
    }


def _write_protocol_case(path: Path, marker: Path) -> None:
    support = GOMETRY_ROOT / 'benches' / 'cases'
    path.write_text(
        f"""
import sys
import time
from pathlib import Path
sys.path.insert(0, {str(support)!r})
from _observation import bootstrap, emit, prepare_measurement
if __name__ == '__main__':
    bootstrap()
import gometry as gm

def main():
    point = gm.Point(1.0, 2.0)
    prepare_measurement()
    Path({str(marker)!r}).write_text('timing permission granted')
    started = time.perf_counter()
    assert gm.equals_identical(point, point)
    elapsed = time.perf_counter() - started
    emit(
        metric='duration',
        raw_values=[elapsed],
        useful_work={{'count': 1, 'unit': 'geometry-comparisons'}},
        oracle_identity='case_protocol:identical-point',
        warmup_iterations=1,
        calibration_iterations=1,
    )

if __name__ == '__main__':
    main()
""",
        encoding='utf-8',
    )


def _write_python_wrapper(path: Path) -> None:
    path.write_text(
        """#!/bin/sh
printf '%s\\n' \"$*\" >> \"$GOMETRY_TEST_WRAPPER_LOG\"
if [ \"${1-}\" = \"-c\" ]; then
  export PYTHONPATH=\"$GOMETRY_BENCH_PACKAGE_ROOT\"
else
  export PYTHONPATH=\"$GOMETRY_TEST_MISSING:$GOMETRY_TEST_STALE\"
fi
exec \"$GOMETRY_TEST_REAL_PYTHON\" \"$@\"
""",
        encoding='utf-8',
    )
    path.chmod(0o755)


def _stale_install(source_root: Path, destination: Path) -> None:
    def link_file(source: str, target: str) -> str:
        try:
            os.link(source, target)
        except OSError:
            shutil.copy2(source, target)
        return target

    shutil.copytree(
        source_root / 'gometry',
        destination / 'gometry',
        copy_function=link_file,
    )
    for dist_info in source_root.glob('gometry*.dist-info'):
        shutil.copytree(
            dist_info,
            destination / dist_info.name,
            copy_function=link_file,
        )


def test_blocked_orders_are_seeded_balanced_and_alternate_leaders() -> None:
    bench_ab = _module()
    orders = bench_ab.blocked_orders(11, 20_260_709)
    assert orders == bench_ab.blocked_orders(11, 20_260_709)
    assert set(orders) == {('A', 'B'), ('B', 'A')}
    assert all(left[0] != right[0] for left, right in pairwise(orders))
    assert (
        abs(
            sum(order[0] == 'A' for order in orders)
            - sum(order[0] == 'B' for order in orders)
        )
        == 1
    )


def test_run_blocks_records_real_typed_pairs_and_excludes_warmups() -> None:
    bench_ab = _module()
    calls: list[str] = []
    tick = 0

    def clock() -> int:
        nonlocal tick
        tick += 1
        return tick

    def runner(python: str, _case: Path, _extra: list[str]) -> dict[str, Any]:
        calls.append(python)
        value = 1.1 if python == 'python-a' else 1.0
        started = clock()
        ended = clock()
        return _arm(bench_ab, value, identity=python, started=started, ended=ended)

    result = bench_ab.run_blocks(
        {'A': 'python-a', 'B': 'python-b'},
        GOMETRY_ROOT / 'benches' / 'cases' / 'case_import_wkb.py',
        [],
        warmup=2,
        rounds=9,
        seed=20_260_709,
        runner=runner,
        clock_ns=clock,
    )
    assert len(calls) == 22
    assert len(result['blocks']) == 11
    assert sum(block['measured'] for block in result['blocks']) == 9
    assert (
        result['blocks'][0]['arms']['A']['observation']['measurement']['unit']
        == 'seconds'
    )
    assert len(result['samples']['A']) == len(result['samples']['B']) == 9


def test_wrapper_selected_case_process_supplies_identity_without_a_probe(
    tmp_path: Path,
) -> None:
    bench_ab = _module()
    subject = benchmark_runtime.load_subject()
    package_root = benchmark_runtime.package_root_for_subject(subject)
    case = tmp_path / 'case_protocol.py'
    marker = tmp_path / 'timing-started'
    wrapper_log = tmp_path / 'wrapper.log'
    wrapper = tmp_path / 'python-wrapper'
    _write_protocol_case(case, marker)
    _write_python_wrapper(wrapper)
    environment = os.environ.copy()
    environment.update({
        'GOMETRY_TEST_WRAPPER_LOG': str(wrapper_log),
        'GOMETRY_TEST_REAL_PYTHON': sys.executable,
        'GOMETRY_TEST_STALE': str(package_root),
        'GOMETRY_TEST_MISSING': str(tmp_path / 'does-not-exist'),
    })

    result = bench_ab.run_case(
        str(wrapper),
        case,
        [],
        receipt_binding=subject,
        package_root=package_root,
        side='A',
        environment=environment,
    )

    identity = result['observation']['producer_identity']
    assert marker.read_text(encoding='utf-8') == 'timing permission granted'
    assert identity['python']['resolved_executable'] == str(
        Path(sys.executable).resolve()
    )
    assert identity['runtime_binding']['path'] == subject['path']
    assert result['case_process']['pid'] != os.getpid()
    assert len(wrapper_log.read_text(encoding='utf-8').splitlines()) == 1
    assert ' -c' not in wrapper_log.read_text(encoding='utf-8')


def test_missing_default_quality_receipt_fails_without_preparation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    absent = tmp_path / 'gometry' / 'target' / 'quality-runtime' / 'receipt.json'
    monkeypatch.setattr(benchmark_runtime, 'default_subject_path', lambda: absent)

    with pytest.raises(AssertionError, match='quality runtime receipt is unavailable'):
        benchmark_runtime.load_subject()


def test_missing_path_entry_does_not_fall_back_to_a_valid_installed_package(
    tmp_path: Path,
) -> None:
    bench_ab = _module()
    subject = benchmark_runtime.load_subject()
    package_root = benchmark_runtime.package_root_for_subject(subject)
    stale_root = tmp_path / 'stale-site-packages'
    stale_root.mkdir()
    _stale_install(package_root, stale_root)
    case = tmp_path / 'case_stale_fallback.py'
    marker = tmp_path / 'timing-started'
    wrapper_log = tmp_path / 'wrapper.log'
    wrapper = tmp_path / 'python-wrapper'
    _write_protocol_case(case, marker)
    _write_python_wrapper(wrapper)
    environment = os.environ.copy()
    environment.update({
        'GOMETRY_TEST_WRAPPER_LOG': str(wrapper_log),
        'GOMETRY_TEST_REAL_PYTHON': sys.executable,
        'GOMETRY_TEST_STALE': str(stale_root),
        'GOMETRY_TEST_MISSING': str(tmp_path / 'does-not-exist'),
    })

    with pytest.raises(SystemExit, match='does not select the admitted'):
        bench_ab.run_case(
            str(wrapper),
            case,
            [],
            receipt_binding=subject,
            package_root=package_root,
            side='A',
            environment=environment,
        )

    assert not marker.exists()
    assert len(wrapper_log.read_text(encoding='utf-8').splitlines()) == 1
    assert ' -c' not in wrapper_log.read_text(encoding='utf-8')


def test_driver_withholds_permission_for_an_identity_from_another_subject(
    tmp_path: Path,
) -> None:
    subject = benchmark_runtime.load_subject()
    root = GOMETRY_ROOT
    quality_receipt = subject['runtime_receipt']
    wheel_path = (root.parent / quality_receipt['wheel']['path']).resolve()
    admission = benchmark_runtime.runtime_artifact.inspect_runtime_inputs(
        root,
        expected_wheel=wheel_path,
        build_receipt=quality_receipt,
    )
    runtime = benchmark_runtime.runtime_artifact.verify_admitted_runtime(admission)
    session = benchmark_runtime.BenchmarkRuntime(
        subject=subject,
        admission=admission,
        runtime=runtime,
        receipt_path=Path(subject['path']),
        receipt_sha256=subject['receipt_sha256'],
        wheel_path=wheel_path,
        wheel_item=subject['wheel'],
        package_root=Path(subject['package_root']),
    )
    case = tmp_path / 'forged-ready.py'
    marker = tmp_path / 'permission-granted'
    common = session.collect_identity()
    identity = benchmark_runtime.validate_identity_for_subject(
        common,
        subject,
        side='A',
        row=str(case.resolve()),
        verify_archive=False,
    )
    identity['runtime_binding']['path'] = str(tmp_path / 'another-receipt.json')
    identity['runtime_binding']['sha256'] = 'f' * 64
    control_support = GOMETRY_ROOT / 'benches' / 'support'
    case.write_text(
        f"""
import hashlib
import json
import os
import sys
from pathlib import Path
sys.path.insert(0, {str(control_support)!r})
from benchmark_observation import make_observation

invocation = os.environ['GOMETRY_BENCH_INVOCATION_ID']
identity = json.loads(os.environ['GOMETRY_TEST_READY_IDENTITY'])
path = Path(sys.argv[0]).resolve()
case = {{
    'path': str(path),
    'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
}}
print(json.dumps({{'benchmark_control': {{
    'type': 'ready',
    'invocation_id': invocation,
    'pid': os.getpid(),
    'case': case,
    'identity': identity,
}}}}), flush=True)
json.loads(sys.stdin.readline())
Path({str(marker)!r}).write_text('permission granted')
identity_sha256 = hashlib.sha256(
    json.dumps(identity, sort_keys=True, separators=(',', ':')).encode()
).hexdigest()
print(json.dumps({{'benchmark_control': {{
    'type': 'done',
    'invocation_id': invocation,
    'pid': os.getpid(),
    'case': case,
    'identity_sha256': identity_sha256,
}}}}), flush=True)
observation = make_observation(
    metric='duration',
    raw_values=[1.0],
    useful_work={{'count': 1, 'unit': 'calls'}},
    oracle_identity='control-test:identity',
    warmup_iterations=1,
    calibration_iterations=1,
)
observation['producer_identity'] = identity
observation['case_process'] = {{
    'pid': os.getpid(),
    'invocation_id': invocation,
    'case': case,
    'identity_sha256': identity_sha256,
}}
print(json.dumps({{'observation': observation}}, sort_keys=True), flush=True)
""",
        encoding='utf-8',
    )
    environment = os.environ.copy()
    environment['GOMETRY_TEST_READY_IDENTITY'] = json.dumps(identity)

    with pytest.raises(SystemExit, match='differs from selected subject'):
        benchmark_runtime.run_controlled_process(
            [sys.executable, str(case)],
            case=case,
            side='A',
            subject=subject,
            package_root=Path(subject['package_root']),
            timeout=30,
            environment=environment,
        )

    assert not marker.exists()


@pytest.mark.parametrize('metric', ['duration', 'scaling_ratio'])
def test_stdout_parser_accepts_only_typed_metric_payload(metric: str) -> None:
    bench_ab = _module()
    observation = _observation(bench_ab, metric, 1.5)
    parsed = bench_ab._parse_observation_stdout(
        Path('case.py'), json.dumps({'observation': observation}) + '\n', ''
    )
    assert parsed['measurement']['metric'] == metric


def test_stdout_parser_rejects_float_and_ratio_as_seconds() -> None:
    bench_ab = _module()
    with pytest.raises(SystemExit, match='typed observation JSON'):
        bench_ab._parse_observation_stdout(Path('case.py'), '1.5\n', '')
    ratio = _observation(bench_ab, 'scaling_ratio', 1.5)
    ratio['measurement']['unit'] = 'seconds'
    with pytest.raises(ValueError, match='unit'):
        validate_observation(ratio)


def test_smooth_common_drift_is_inconclusive_for_a_a_blocks() -> None:
    bench_ab = _module()
    tick = 0
    call_index = 0

    def clock() -> int:
        nonlocal tick
        tick += 1
        return tick

    def runner(_side: str, _case: Path, _extra: list[str]) -> dict[str, Any]:
        nonlocal call_index
        started = clock()
        value = math.exp(call_index * 0.5)
        call_index += 1
        ended = clock()
        return _arm(bench_ab, value, started=started, ended=ended)

    run = bench_ab.run_blocks(
        {'A': 'same-a', 'B': 'same-b'},
        Path('synthetic-a-a.py'),
        [],
        warmup=1,
        rounds=6,
        seed=0,
        runner=runner,
        clock_ns=clock,
    )
    result = bench_ab.compare_blocks(
        run['blocks'], 123, metric='duration', unit='seconds'
    )
    measured = [block for block in run['blocks'] if block['measured']]
    unpaired_a = [
        bench_ab._observation_estimate(block['arms']['A']['observation'])
        for block in measured
    ]
    unpaired_b = [
        bench_ab._observation_estimate(block['arms']['B']['observation'])
        for block in measured
    ]
    assert bench_ab.statistics.median(unpaired_a) / bench_ab.statistics.median(
        unpaired_b
    ) != pytest.approx(1.0)
    assert result['relative_ratio_a_over_b'] == pytest.approx(1.0)
    assert result['significant'] is False
    broken_schedule = deepcopy(run['blocks'])
    for block in broken_schedule:
        if block['measured']:
            block['lead_order'] = ['A', 'B']
    with pytest.raises(SystemExit, match='both lead orders'):
        bench_ab.compare_blocks(broken_schedule, 123, metric='duration', unit='seconds')
    swapped_order = deepcopy(run['blocks'])
    for block in swapped_order:
        if block['measured']:
            block['lead_order'].reverse()
    with pytest.raises(SystemExit, match='timestamps'):
        bench_ab.compare_blocks(swapped_order, 123, metric='duration', unit='seconds')


@pytest.mark.parametrize(('seed', 'first_lead'), [(0, 'A'), (1, 'B')])
def test_constant_ten_percent_cost_is_recovered_for_both_leads(
    seed: int, first_lead: str
) -> None:
    bench_ab = _module()
    tick = 0

    def clock() -> int:
        nonlocal tick
        tick += 1
        return tick

    def runner(side: str, _case: Path, _extra: list[str]) -> dict[str, Any]:
        started = clock()
        ended = clock()
        value = 1.1 if side == 'python-a' else 1.0
        return _arm(bench_ab, value, identity=side, started=started, ended=ended)

    run = bench_ab.run_blocks(
        {'A': 'python-a', 'B': 'python-b'},
        Path('synthetic-cost.py'),
        [],
        warmup=1,
        rounds=4,
        seed=seed,
        runner=runner,
        clock_ns=clock,
    )
    measured = [block for block in run['blocks'] if block['measured']]
    assert run['blocks'][0]['lead_order'][0] == first_lead
    assert len(measured) >= 3
    result = bench_ab.compare_blocks(
        run['blocks'], 123, metric='duration', unit='seconds'
    )
    assert result['relative_ratio_a_over_b'] == pytest.approx(1.1)
    assert result['relative_ratio_ci95'] == pytest.approx((1.1, 1.1))


@pytest.mark.parametrize(
    ('ratio', 'expected'),
    [
        (1.25, 'Candidate B is 1.250x faster than A'),
        (0.8, 'Candidate B is 1.250x slower than A'),
        (1.0, 'Candidate B matches A'),
    ],
)
def test_directional_verdict_names_candidate_direction(
    ratio: float, expected: str
) -> None:
    bench_ab = _module()
    assert expected in bench_ab.format_directional_verdict(
        ratio, metric='duration', unit='seconds'
    )


def test_observation_requires_oracle_identity() -> None:
    bench_ab = _module()
    observation = _observation(bench_ab, 'duration', 1.0)
    del observation['oracle']['identity']
    with pytest.raises(ValueError, match='oracle'):
        validate_observation(observation)


def test_pair_semantics_reject_mismatched_metric_before_summary() -> None:
    bench_ab = _module()
    arms = {
        'A': _arm(bench_ab, 1.0, metric='duration'),
        'B': _arm(bench_ab, 1.0, metric='scaling_ratio'),
    }
    with pytest.raises(SystemExit, match='mismatched typed metrics'):
        bench_ab._validate_pair_arms(arms, Path('case.py'))


def test_perf_event_expansion_and_parser_retain_counter_identity() -> None:
    bench_ab = _module()
    assert bench_ab.expand_perf_events(['instructions, cycles', 'branches']) == (
        'instructions',
        'cycles',
        'branches',
    )
    parsed = bench_ab.parse_perf_output(
        '1234,,instructions:u,1000,100.00,,\n2000,,cycles,1000,50.00,,\n',
        ['instructions', 'cycles'],
    )
    assert parsed['instructions']['raw_value'] == 1234
    assert parsed['instructions']['coverage'] == 1.0
    assert parsed['cycles']['status'] == 'multiplexed'
    with pytest.raises(SystemExit, match='not comparable'):
        bench_ab._validate_counters(parsed, ('instructions', 'cycles'))


@pytest.mark.parametrize(
    'entry',
    [
        {'status': 'ok', 'raw_value': 0, 'coverage': 1.0},
        {'status': 'unsupported', 'raw_value': None, 'coverage': None},
        {'status': 'not-counted', 'raw_value': None, 'coverage': 0.0},
        {'status': 'multiplexed', 'raw_value': 10, 'coverage': 0.5},
    ],
)
def test_perf_positive_control_rejects_nonmoving_or_unusable_event(
    entry: dict[str, object],
) -> None:
    bench_ab = _module()
    failures = bench_ab.validate_positive_control(
        {'instructions': entry}, ('instructions',)
    )
    assert failures['instructions']


def test_perf_positive_control_accepts_supported_moving_event() -> None:
    bench_ab = _module()
    assert (
        bench_ab.validate_positive_control(
            {'instructions': {'status': 'ok', 'raw_value': 100, 'coverage': 1.0}},
            ('instructions',),
        )
        == {}
    )


def test_perf_capability_splits_at_failed_group_capacity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bench_ab = _module()
    monkeypatch.setattr(
        bench_ab, 'perf_version', lambda _binary='perf': {'version': 'test'}
    )

    def control(events, **_kwargs):
        if len(events) > 2:
            return {'status': 'rejected', 'events': list(events)}
        return {
            'status': 'ok',
            'events': list(events),
            'counters': {
                event: {'status': 'ok', 'raw_value': 1, 'coverage': 1.0}
                for event in events
            },
        }

    monkeypatch.setattr(bench_ab, '_run_perf_control', control)
    result = bench_ab.check_perf_capability(('instructions', 'cycles', 'branches'))
    assert result['status'] == 'available'
    assert result['event_groups'] == [['instructions', 'cycles'], ['branches']]
    assert result['usable_group_capacity'] == 2


def test_split_perf_groups_execute_separate_case_invocations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bench_ab = _module()
    calls: list[tuple[str, ...]] = []
    observation = _observation(bench_ab, 'duration', 1.0)
    provenance = {'build': 'same'}

    def fake_group(_python, _case, _extra, events, **_kwargs):
        group = tuple(events)
        calls.append(group)
        return {
            'observation': observation,
            'provenance_before': provenance,
            'provenance_after': provenance,
            'case_process': {'pid': len(calls), 'invocation_id': str(len(calls))},
            'counters': {
                event: {
                    'status': 'ok',
                    'raw_value': 10,
                    'coverage': 1.0,
                }
                for event in group
            },
            'command': ['perf', 'stat', '-e', ','.join(group)],
        }

    monkeypatch.setattr(bench_ab, '_run_case_with_perf_group', fake_group)
    result = bench_ab.run_case_with_perf(
        'python',
        Path('case.py'),
        [],
        ('instructions', 'cycles'),
        receipt_binding={'kind': 'quality'},
        package_root=Path('/site-packages'),
        side='A',
        event_groups=(('instructions',), ('cycles',)),
    )
    assert calls == [('instructions',), ('cycles',)]
    assert result['commands'][0][-1] == 'instructions'
    assert result['commands'][1][-1] == 'cycles'
    assert [record['pid'] for record in result['case_processes']] == [1, 2]
    assert [record['invocation_id'] for record in result['case_processes']] == [
        '1',
        '2',
    ]
    assert set(result['counters']) == {'instructions', 'cycles'}
