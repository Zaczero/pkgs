import json
import os
import stat
from errno import ESRCH
from pathlib import Path
from typing import cast

import pytest

from bench.system import (
    PAGE_SIZE,
    BenchmarkError,
    BenchmarkSystemState,
    CpuActivityMonitor,
    ProcessGroupResourceSampler,
    ProcessResourceSample,
    benchmark_system_state_matches,
    capture_ambient_cpu_probe,
    capture_cpu_set,
    capture_thread_affinity_masks,
    durable_json,
    parse_linux_cpu_list,
    physical_core_capacity,
    pin_process_threads,
    read_cpu_times,
    read_process_group_resources,
    validate_k6_result,
    validate_oha_result,
)


def test_system_identity_ignores_thread_count_churn_but_not_affinity_masks() -> None:
    expected = cast(
        'BenchmarkSystemState',
        {
            'thread_affinity_masks': [{'cpus': [0], 'thread_count': 7}],
            'kernel': 'test',
        },
    )
    actual = cast(
        'BenchmarkSystemState',
        {
            'thread_affinity_masks': [{'cpus': [0], 'thread_count': 3}],
            'kernel': 'test',
        },
    )
    assert benchmark_system_state_matches(expected, actual)

    actual['thread_affinity_masks'] = [{'cpus': [0, 1], 'thread_count': 3}]
    assert not benchmark_system_state_matches(expected, actual)


def _write(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value)


def _fake_cpu(
    root: Path,
    cpu: int,
    *,
    core: int,
    siblings: str,
    llc_id: int,
    llc_cpus: str,
) -> None:
    cpu_root = root / f'cpu{cpu}'
    _write(cpu_root / 'topology/physical_package_id', '0')
    _write(cpu_root / 'topology/die_id', '0')
    _write(cpu_root / 'topology/core_id', str(core))
    _write(cpu_root / 'topology/thread_siblings_list', siblings)
    _write(cpu_root / 'cache/index0/type', 'Data')
    _write(cpu_root / 'cache/index0/level', '1')
    _write(cpu_root / 'cache/index0/id', str(cpu))
    _write(cpu_root / 'cache/index0/shared_cpu_list', str(cpu))
    _write(cpu_root / 'cache/index3/type', 'Unified')
    _write(cpu_root / 'cache/index3/level', '3')
    _write(cpu_root / 'cache/index3/id', str(llc_id))
    _write(cpu_root / 'cache/index3/shared_cpu_list', llc_cpus)
    _write(cpu_root / 'cpufreq/scaling_governor', 'performance')
    _write(cpu_root / 'cpufreq/scaling_driver', 'test-driver')
    _write(cpu_root / 'cpufreq/energy_performance_preference', 'performance')


def test_linux_cpu_list_parser() -> None:
    assert parse_linux_cpu_list('0-2,4,8-9') == (0, 1, 2, 4, 8, 9)
    # The one shared parser deduplicates and sorts, matching the Linux sysfs
    # representation; duplicates and unordered input are accepted.
    assert parse_linux_cpu_list('2,2,4-5,4') == (2, 4, 5)
    with pytest.raises(ValueError):
        parse_linux_cpu_list('2-1')
    with pytest.raises(ValueError):
        parse_linux_cpu_list('1,,2')


def test_thread_affinity_capture_groups_every_stable_thread(
    tmp_path: Path, monkeypatch
) -> None:
    for task_id in (101, 102, 103):
        (tmp_path / str(task_id)).mkdir()
    affinities = {101: {0}, 102: {0}, 103: {2, 3}}
    monkeypatch.setattr(
        'bench.system.os.sched_getaffinity', lambda task_id: affinities[task_id]
    )

    assert capture_thread_affinity_masks(task_root=tmp_path) == [
        {'cpus': [0], 'thread_count': 2},
        {'cpus': [2, 3], 'thread_count': 1},
    ]


def test_thread_affinity_pinning_repeats_until_new_threads_are_stable(
    tmp_path: Path, monkeypatch
) -> None:
    (tmp_path / '101').mkdir()
    affinities = {101: {0, 1}}
    calls: list[int] = []

    def set_affinity(task_id: int, cpus: tuple[int, ...]) -> None:
        calls.append(task_id)
        affinities[task_id] = set(cpus)
        if task_id == 101 and 102 not in affinities:
            affinities[102] = {0, 1}
            (tmp_path / '102').mkdir()

    monkeypatch.setattr('bench.system.os.sched_setaffinity', set_affinity)
    monkeypatch.setattr(
        'bench.system.os.sched_getaffinity', lambda task_id: affinities[task_id]
    )

    assert pin_process_threads((3,), task_root=tmp_path) == [
        {'cpus': [3], 'thread_count': 2}
    ]
    assert calls.count(101) >= 2
    assert calls.count(102) >= 2
    assert affinities == {101: {3}, 102: {3}}


def test_thread_affinity_pinning_tolerates_threads_exiting_mid_pass(
    tmp_path: Path, monkeypatch
) -> None:
    for task_id in (101, 102):
        (tmp_path / str(task_id)).mkdir()
    affinities = {101: {0, 1}, 102: {0, 1}}

    def set_affinity(task_id: int, cpus: tuple[int, ...]) -> None:
        if task_id == 101 and task_id in affinities:
            affinities.pop(task_id)
            (tmp_path / str(task_id)).rmdir()
            raise ProcessLookupError(ESRCH, 'thread exited')
        affinities[task_id] = set(cpus)

    def get_affinity(task_id: int) -> set[int]:
        try:
            return affinities[task_id]
        except KeyError as error:
            raise ProcessLookupError(ESRCH, 'thread exited') from error

    monkeypatch.setattr('bench.system.os.sched_setaffinity', set_affinity)
    monkeypatch.setattr('bench.system.os.sched_getaffinity', get_affinity)

    assert pin_process_threads((3,), task_root=tmp_path) == [
        {'cpus': [3], 'thread_count': 1}
    ]


def test_cpu_activity_monitor_retains_aggregate_and_worst_window(
    monkeypatch,
) -> None:
    samples = iter([
        {0: {'total_ticks': 100, 'idle_ticks': 100}},
        {0: {'total_ticks': 200, 'idle_ticks': 200}},
        {0: {'total_ticks': 300, 'idle_ticks': 200}},
    ])
    monotonic = iter([0.0, 1.0, 2.0])
    monkeypatch.setattr('bench.system.read_cpu_times', lambda _cpus: next(samples))
    monkeypatch.setattr('bench.system.time.monotonic', lambda: next(monotonic))

    def capture_once(monitor: CpuActivityMonitor) -> None:
        monitor._capture_window()

    monkeypatch.setattr(CpuActivityMonitor, '_sample_loop', capture_once)
    monitor = CpuActivityMonitor((0,), 1)
    monitor.start()
    activity = monitor.stop()

    assert activity['physical_core_utilization'] == pytest.approx(0.5)
    assert [window['physical_core_utilization'] for window in activity['windows']] == [
        0.0,
        1.0,
    ]
    assert activity['maximum_window_physical_core_utilization'] == 1.0
    assert activity['maximum_window_cpu_utilization'] == 1.0


def test_cpu_activity_monitor_normalizes_short_busy_tail_to_fixed_interval(
    monkeypatch,
) -> None:
    samples = iter([
        {0: {'total_ticks': 100, 'idle_ticks': 100}},
        {0: {'total_ticks': 200, 'idle_ticks': 198}},
        {0: {'total_ticks': 234, 'idle_ticks': 225}},
    ])
    monotonic = iter([0.0, 1.0, 1.34])
    monkeypatch.setattr('bench.system.read_cpu_times', lambda _cpus: next(samples))
    monkeypatch.setattr('bench.system.time.monotonic', lambda: next(monotonic))
    monkeypatch.setattr(
        CpuActivityMonitor,
        '_sample_loop',
        lambda monitor: monitor._capture_window(),
    )

    monitor = CpuActivityMonitor((0,), 1)
    monitor.start()
    activity = monitor.stop()

    assert len(activity['windows']) == 2
    assert activity['windows'][-1]['elapsed_seconds'] == pytest.approx(0.34)
    assert activity['windows'][-1]['maximum_cpu_utilization'] == pytest.approx(7 / 34)
    assert activity['physical_core_utilization'] == pytest.approx(9 / 134)
    assert activity['maximum_raw_window_physical_core_utilization'] == pytest.approx(
        7 / 34
    )
    assert activity['maximum_raw_window_cpu_utilization'] == pytest.approx(7 / 34)
    assert activity['maximum_window_physical_core_utilization'] == pytest.approx(0.07)
    assert activity['maximum_window_cpu_utilization'] == pytest.approx(0.07)


def test_cpu_activity_monitor_rejects_genuinely_busy_partial_tail(monkeypatch) -> None:
    samples = iter([
        {0: {'total_ticks': 100, 'idle_ticks': 100}},
        {0: {'total_ticks': 200, 'idle_ticks': 200}},
        {0: {'total_ticks': 220, 'idle_ticks': 200}},
    ])
    monotonic = iter([0.0, 1.0, 1.2])
    monkeypatch.setattr('bench.system.read_cpu_times', lambda _cpus: next(samples))
    monkeypatch.setattr('bench.system.time.monotonic', lambda: next(monotonic))
    monkeypatch.setattr(
        CpuActivityMonitor,
        '_sample_loop',
        lambda monitor: monitor._capture_window(),
    )

    monitor = CpuActivityMonitor((0,), 1)
    monitor.start()
    activity = monitor.stop()

    assert activity['windows'][-1]['maximum_cpu_utilization'] == 1.0
    assert activity['maximum_window_physical_core_utilization'] == pytest.approx(0.2)
    assert activity['maximum_window_cpu_utilization'] == pytest.approx(0.2)


def test_cpu_set_counts_physical_cores_and_discovers_highest_cache(
    tmp_path: Path,
) -> None:
    _write(tmp_path / 'online', '0-3')
    _fake_cpu(tmp_path, 0, core=0, siblings='0,2', llc_id=0, llc_cpus='0-3')
    _fake_cpu(tmp_path, 1, core=1, siblings='1,3', llc_id=0, llc_cpus='0-3')
    _fake_cpu(tmp_path, 2, core=0, siblings='0,2', llc_id=0, llc_cpus='0-3')
    _fake_cpu(tmp_path, 3, core=1, siblings='1,3', llc_id=0, llc_cpus='0-3')

    state = capture_cpu_set((0, 1, 2, 3), sysfs_root=tmp_path)

    assert state['logical_cpu_count'] == 4
    assert state['physical_core_count'] == 2
    assert state['topology'][0]['thread_siblings'] == [0, 2]
    assert state['topology'][0]['last_level_cache'] == {
        'level': 3,
        'cache_id': 0,
        'cache_type': 'Unified',
        'shared_cpus': [0, 1, 2, 3],
    }
    assert physical_core_capacity((0, 2), sysfs_root=tmp_path) == 1


def test_cpu_topology_capture_rejects_missing_required_evidence(
    tmp_path: Path,
) -> None:
    _write(tmp_path / 'online', '0')
    _write(tmp_path / 'cpu0/topology/physical_package_id', '0')

    with pytest.raises(RuntimeError, match='thread sibling'):
        capture_cpu_set((0,), sysfs_root=tmp_path)


def test_ambient_cpu_probe_reports_system_and_role_utilization(
    tmp_path: Path, monkeypatch
) -> None:
    proc_stat = tmp_path / 'stat'
    proc_stat.write_text('cpu0 0 0 0 100 0 0 0 0 0 0\ncpu1 0 0 0 100 0 0 0 0 0 0\n')

    def advance(_duration: float) -> None:
        proc_stat.write_text(
            'cpu0 50 0 0 150 0 0 0 0 0 0\ncpu1 0 0 0 200 0 0 0 0 0 0\n'
        )

    monkeypatch.setattr('bench.system.time.sleep', advance)

    probe = capture_ambient_cpu_probe(
        (0, 1),
        (0,),
        (0,),
        (1,),
        online_physical_core_count=2,
        server_physical_core_count=1,
        server_llc_physical_core_count=1,
        load_llc_physical_core_count=1,
        duration_seconds=0.5,
        proc_stat=proc_stat,
    )

    assert probe['system_physical_core_utilization'] == pytest.approx(0.25)
    assert probe['server_physical_core_utilization'] == pytest.approx(0.5)
    assert probe['load_llc_physical_core_utilization'] == 0.0
    assert probe['maximum_cpu_utilization'] == pytest.approx(0.5)
    assert probe['per_cpu'][0] == {
        'cpu': 0,
        'total_ticks': 100,
        'active_ticks': 50,
        'utilization': 0.5,
    }


def test_cpu_time_reader_does_not_double_count_guest_ticks(tmp_path: Path) -> None:
    proc_stat = tmp_path / 'stat'
    proc_stat.write_text('cpu0 10 2 3 80 1 2 1 1 50 20\n')

    assert read_cpu_times((0,), proc_stat=proc_stat) == {
        0: {'total_ticks': 100, 'idle_ticks': 81}
    }


def test_process_group_resource_reader_aggregates_only_matching_group(
    tmp_path: Path,
) -> None:
    def write_process(
        pid: int,
        process_group: int,
        user_ticks: int,
        system_ticks: int,
        resident_pages: str,
    ) -> None:
        process = tmp_path / str(pid)
        process.mkdir()
        process.joinpath('stat').write_text(
            f'{pid} (worker with spaces) S 1 {process_group} 1 0 0 0 0 0 0 0 '
            f'{user_ticks} {system_ticks}\n'
        )
        process.joinpath('statm').write_text(f'100 {resident_pages}\n')

    write_process(101, 77, 20, 5, '3')
    write_process(102, 88, 900, 100, '200')
    # An RSS read racing a process exit keeps the cumulative CPU observation.
    write_process(103, 77, 7, 3, 'invalid')
    tmp_path.joinpath('not-a-pid').mkdir()

    samples = read_process_group_resources(77, tmp_path)

    assert samples == {
        101: {'cpu_ticks': 25, 'rss_bytes': 3 * PAGE_SIZE},
        103: {'cpu_ticks': 10, 'rss_bytes': 0},
    }


def test_process_group_sampler_tracks_exited_and_new_processes(monkeypatch) -> None:
    snapshots: list[dict[int, ProcessResourceSample]] = [
        {
            10: {'cpu_ticks': 100, 'rss_bytes': 1_000},
            11: {'cpu_ticks': 50, 'rss_bytes': 500},
        },
        {
            10: {'cpu_ticks': 130, 'rss_bytes': 1_200},
            12: {'cpu_ticks': 20, 'rss_bytes': 700},
        },
    ]
    monotonic = iter([10.0, 12.0])
    monkeypatch.setattr(
        'bench.system.read_process_group_resources',
        lambda _process_group: snapshots.pop(0),
    )
    monkeypatch.setattr('bench.system.time.monotonic', lambda: next(monotonic))
    monkeypatch.setattr('bench.system.os.sysconf', lambda _name: 100)

    sampler = ProcessGroupResourceSampler(77, interval=60.0)
    sampler.start()
    usage = sampler.stop()

    assert usage == {
        'elapsed_seconds': 2.0,
        'cpu_seconds': 0.5,
        'average_cpu_cores': 0.25,
        'peak_rss_bytes': 1_900,
        'peak_process_count': 2,
        'sample_count': 2,
        'sampling_interval_seconds': 60.0,
    }


def test_durable_json_fsyncs_file_and_directory_and_cleans_temp(
    tmp_path: Path, monkeypatch
) -> None:
    sync_kinds = []
    real_fsync = os.fsync

    def tracked_fsync(file_descriptor):
        sync_kinds.append(
            'directory' if stat.S_ISDIR(os.fstat(file_descriptor).st_mode) else 'file'
        )
        real_fsync(file_descriptor)

    monkeypatch.setattr('bench.system.os.fsync', tracked_fsync)
    output = tmp_path / 'nested/evidence.json'
    durable_json(output, {'status': 'running'})

    assert sync_kinds == ['file', 'directory']
    assert json.loads(output.read_text()) == {'status': 'running'}
    assert [path.name for path in output.parent.iterdir()] == ['evidence.json']


def test_oha_validation_requires_all_expected_responses() -> None:
    with pytest.raises(BenchmarkError, match='no JSON'):
        validate_oha_result(None)

    valid = {
        'summary': {'total': 1.0, 'successRate': 1},
        'statusCodeDistribution': {'200': 100},
        'errorDistribution': {},
    }
    validate_oha_result(valid)

    invalid = {**valid, 'statusCodeDistribution': {'200': 99, '500': 1}}
    with pytest.raises(BenchmarkError, match='status distribution'):
        validate_oha_result(invalid)

    bad_rate = {**valid, 'summary': {'total': 1.0, 'successRate': 1.5}}
    with pytest.raises(BenchmarkError, match='success rate is invalid'):
        validate_oha_result(bad_rate)

    timed_stop = {
        **valid,
        'summary': {'total': 1.0, 'successRate': 0.999},
        'errorDistribution': {'aborted due to deadline': 1},
    }
    validate_oha_result(timed_stop, max_deadline_aborts=1)


@pytest.mark.parametrize('shape', ['flat', 'values'])
def test_k6_validation_rejects_failed_handshakes_and_echoes(shape) -> None:
    def check(passes, fails, rate) -> dict:
        if shape == 'flat':
            return {'passes': passes, 'fails': fails, 'value': rate}
        return {'values': {'passes': passes, 'fails': fails, 'rate': rate}}

    valid = {
        'metrics': {
            'checks': check(200, 0, 1),
            'bench_echo_success': check(100, 0, 1),
        }
    }
    validate_k6_result(valid)

    with pytest.raises(BenchmarkError, match='handshake checks failed'):
        validate_k6_result({
            'metrics': {
                'checks': check(199, 1, 0.99),
                'bench_echo_success': check(100, 0, 1),
            }
        })

    with pytest.raises(BenchmarkError, match='echo checks failed'):
        validate_k6_result({
            'metrics': {
                'checks': check(200, 0, 1),
                'bench_echo_success': check(99, 1, 0.99),
            }
        })

    # A summary missing the success rate outright fails validation.
    missing_rate = check(200, 0, 1)
    rate_holder = missing_rate['values'] if shape == 'values' else missing_rate
    rate_holder.pop('rate' if shape == 'values' else 'value')
    with pytest.raises(BenchmarkError, match='handshake checks failed'):
        validate_k6_result({
            'metrics': {
                'checks': missing_rate,
                'bench_echo_success': check(100, 0, 1),
            }
        })
