import json
import subprocess
from contextlib import contextmanager
from dataclasses import replace

import pytest

import bench.bench as benchmark
from bench.bench import (
    BenchmarkError,
    CommandIdentity,
    HarnessConfig,
    LoadGeneratorUsage,
    LoadResult,
    Metrics,
    Scenario,
    _measure_load_headroom,
    _publish_artifacts,
    _receive_at_least,
    _relative_spread,
    _run_load_command,
    _run_scenario_warmup,
    _scenario_budget,
    _scenario_load_evidence,
    _scenario_stable,
    _validate_publication_harness,
    _validate_scenario_load,
    aggregate_metrics,
    balanced_orders,
    benchmark_scenarios,
    extract_k6_metrics,
    extract_oha_metrics,
    get_server_command,
    run_benchmarks,
    wait_for_worker_pids,
)
from bench.system import (
    BenchmarkSystemState,
    CpuSetState,
    CpuTopology,
)


class _StubResourceSampler:
    def __init__(self, _process_group_id):
        pass

    def start(self):
        pass

    def stop(self):
        return {
            'elapsed_seconds': 1.0,
            'cpu_seconds': 0.1,
            'average_cpu_cores': 0.1,
            'peak_rss_bytes': 1024,
            'peak_process_count': 1,
            'sample_count': 1,
            'sampling_interval_seconds': 0.05,
        }


def _cpu_topology(
    cpu: int,
    *,
    core: int | None = None,
    llc_id: int,
    llc_cpus: tuple[int, ...],
    siblings: tuple[int, ...] | None = None,
) -> CpuTopology:
    return {
        'cpu': cpu,
        'online': True,
        'physical_package_id': 0,
        'die_id': 0,
        'core_id': cpu if core is None else core,
        'thread_siblings': list((cpu,) if siblings is None else siblings),
        'last_level_cache': {
            'level': 3,
            'cache_id': llc_id,
            'cache_type': 'Unified',
            'shared_cpus': list(llc_cpus),
        },
        'frequency': {
            'scaling_governor': 'performance',
            'scaling_driver': 'test-driver',
            'energy_performance_preference': 'performance',
        },
    }


def _cpu_set(cpus: tuple[int, ...], *, llc_id: int) -> CpuSetState:
    return {
        'cpus': list(cpus),
        'logical_cpu_count': len(cpus),
        'physical_core_count': len(cpus),
        'topology': [_cpu_topology(cpu, llc_id=llc_id, llc_cpus=cpus) for cpu in cpus],
    }


def _publication_system(harness: HarnessConfig) -> BenchmarkSystemState:
    assert harness.load_cpus is not None
    assert harness.management_cpus is not None
    all_cpus = sorted(
        set((harness.server_cpus or ()) + harness.load_cpus + harness.management_cpus)
    )
    return {
        'summary': 'test system',
        'python_version': '3.14.0',
        'kernel': 'Linux test',
        'machine': 'x86_64',
        'cpu_model': 'test CPU',
        'online_cpus': all_cpus,
        'allowed_cpus': list(harness.management_cpus),
        'cgroup_allowed_cpus': all_cpus,
        'online_physical_core_count': len(all_cpus),
        'thread_affinity_masks': [
            {'cpus': list(harness.management_cpus), 'thread_count': 1}
        ],
        'boost_enabled': True,
        'kernel_command_line': 'test',
        'transparent_hugepages': '[never]',
        'server': None
        if harness.server_cpus is None
        else _cpu_set(harness.server_cpus, llc_id=0),
        'load_generator': _cpu_set(harness.load_cpus, llc_id=1),
        'management': _cpu_set(harness.management_cpus, llc_id=0),
    }


def _run_stubbed_main_suite(tmp_path, monkeypatch, outcome: str):
    command: CommandIdentity = {'argv': ['load'], 'environment': {}}
    usage: LoadGeneratorUsage = {
        'elapsed_seconds': 1.0,
        'cpu_seconds': 0.1,
        'logical_cpu_capacity': 1,
        'physical_core_capacity': 1,
        'logical_cpu_utilization': 0.1,
        'physical_core_utilization': 0.1,
        'physical_core_headroom': 0.9,
        'maximum_publish_physical_core_utilization': 0.85,
        'sufficient_headroom': True,
    }
    warmup = LoadResult(raw={'phase': 'warmup'}, command=command, usage=usage)
    measured = LoadResult(raw={'phase': 'measured'}, command=command, usage=usage)
    metrics: Metrics = {'rps': 100.0, 'latency_percentiles': {'p99': 0.001}}

    monkeypatch.setattr(benchmark, 'RUNS_DIRECTORY', tmp_path)
    monkeypatch.setattr(benchmark, 'SERVERS', {'h2corn': ['h2corn']})
    monkeypatch.setattr(benchmark, 'SERVER_PROFILES', {'h2corn': 'test'})
    monkeypatch.setattr(
        benchmark, 'benchmark_scenarios', lambda: [Scenario('cell', 1, 'h1')]
    )
    monkeypatch.setattr(benchmark, '_ensure_static_file_response_payload', lambda: None)
    monkeypatch.setattr(
        benchmark, 'capture_system_state', lambda *_args: {'summary': 'test'}
    )
    monkeypatch.setattr(benchmark, '_git_head', lambda: 'test')
    monkeypatch.setattr(benchmark, 'get_versions', dict)
    monkeypatch.setattr(benchmark.time, 'sleep', lambda _seconds: None)
    monkeypatch.setattr(benchmark, 'validate_response_contract', lambda *_args: {})
    monkeypatch.setattr(benchmark, '_run_scenario_warmup', lambda *_args: warmup)
    monkeypatch.setattr(benchmark, '_validate_scenario_load', lambda *_args: metrics)
    monkeypatch.setattr(
        benchmark, 'wait_for_worker_pids', lambda *_args, **_kwargs: (True, {41})
    )
    monkeypatch.setattr(benchmark, 'plot_results', lambda *_args, **_kwargs: None)

    @contextmanager
    def stub_server(*_args, **_kwargs):
        yield {
            'command': ['server'],
            'log': '',
            'worker_pids': [41],
            'process_group_id': 41,
        }

    def measured_load(*_args, **_kwargs):
        if outcome == 'interrupt':
            raise KeyboardInterrupt
        if outcome == 'error':
            raise BenchmarkError('measured load failed')
        return measured

    monkeypatch.setattr(benchmark, 'running_server', stub_server)
    monkeypatch.setattr(benchmark, 'ProcessGroupResourceSampler', _StubResourceSampler)
    monkeypatch.setattr(benchmark, '_run_scenario_load', measured_load)

    harness = HarnessConfig(min_trials=3, max_trials=4, settle_seconds=0)
    if outcome == 'interrupt':
        with pytest.raises(KeyboardInterrupt):
            run_benchmarks(harness=harness)
        return next(tmp_path.iterdir())
    return run_benchmarks(harness=harness)


def test_interrupted_run_leaves_no_partial_records(tmp_path, monkeypatch) -> None:
    run_directory = _run_stubbed_main_suite(tmp_path, monkeypatch, 'interrupt')

    identity = json.loads((run_directory / 'identity.json').read_text())
    assert identity['servers'] == ['h2corn']
    assert identity['git_head'] == 'test'
    assert not (run_directory / 'manifest.json').exists()
    assert not (run_directory / 'raw/benchmark_cell.json').exists()


@pytest.mark.parametrize(
    (
        'outcome',
        'expected_status',
        'expected_runs',
        'expected_trials',
        'expected_stopping_reason',
    ),
    [
        ('success', 'complete', 3, 3, 'stable'),
        ('error', 'failed', 1, 4, 'max-trials'),
    ],
)
def test_main_suite_writes_each_record_once_at_completion(
    tmp_path,
    monkeypatch,
    outcome,
    expected_status,
    expected_runs,
    expected_trials,
    expected_stopping_reason,
) -> None:
    run_directory = _run_stubbed_main_suite(tmp_path, monkeypatch, outcome)

    manifest = json.loads((run_directory / 'manifest.json').read_text())
    scenario = json.loads((run_directory / 'raw/benchmark_cell.json').read_text())
    assert manifest['status'] == expected_status
    assert manifest['scenarios'] == {'cell': expected_status}
    assert scenario['status'] == expected_status
    assert len(scenario['runs']) == expected_runs
    assert scenario['trials_run'] == expected_trials
    assert scenario['stopping_reason'] == expected_stopping_reason


def test_balanced_orders_rotate_every_server_through_every_position() -> None:
    names = ['a', 'b', 'c', 'd']
    orders = balanced_orders(names, 8, seed=7)

    assert all(sorted(order) == names for order in orders)
    assert all(
        {name: sum(order[position] == name for order in orders) for name in names}
        == dict.fromkeys(names, 2)
        for position in range(4)
    )
    assert all(
        orders[index + 1] == list(reversed(orders[index])) for index in range(0, 8, 2)
    )


@pytest.mark.parametrize('trials', [1, 3, 7])
def test_balanced_orders_accept_any_positive_trial_count(trials: int) -> None:
    names = ['a', 'b', 'c', 'd']
    orders = balanced_orders(names, trials, seed=7)

    assert len(orders) == trials
    assert all(sorted(order) == names for order in orders)
    if trials >= 3:
        assert orders[1] == list(reversed(orders[0]))
        assert orders[2] == orders[0][1:] + orders[0][:1]


def test_relative_spread_and_scenario_stability_use_active_rps_samples() -> None:
    stable: list[Metrics] = [
        {'rps': 99.0, 'latency_percentiles': {}},
        {'rps': 100.0, 'latency_percentiles': {}},
        {'rps': 101.0, 'latency_percentiles': {}},
    ]
    unstable: list[Metrics] = [
        {'rps': 90.0, 'latency_percentiles': {}},
        {'rps': 100.0, 'latency_percentiles': {}},
        {'rps': 110.0, 'latency_percentiles': {}},
    ]
    harness = HarnessConfig(min_trials=3, stable_relative_spread=0.03)

    assert _relative_spread([99.0, 100.0, 101.0]) == pytest.approx(0.01)
    assert _relative_spread([0.0, 0.0, 0.0]) == float('inf')
    assert not _scenario_stable({'a': stable, 'b': unstable}, {}, harness)
    assert _scenario_stable({'a': stable, 'b': unstable}, {'b': 'failed'}, harness)
    assert not _scenario_stable({'a': stable[:2]}, {}, harness)


@pytest.mark.parametrize(
    ('deadline', 'remaining_minimums', 'eligible', 'expected_deadline', 'duration'),
    [
        # Last scenario: full remaining budget, extension deadline = cap.
        (1_000.0, [10.5], ['a'], 1_000.0, '10000ms'),
        # Two equal-cost scenarios split the budget; extensions may run
        # until only the next scenario's minimum remains reserved.
        (220.0, [21.0, 21.0], ['a', 'b'], 199.0, '7500ms'),
        # A squeezed share shrinks to the floor, never below.
        (118.0, [21.0, 21.0], ['a', 'b'], 97.0, '1000ms'),
        # Cost-weighted: a four-server scenario earns twice the share of the
        # remaining two-server one (120 s remaining -> 80 s share).
        (220.0, [42.0, 21.0], ['a', 'b', 'c', 'd'], 199.0, '4166ms'),
    ],
)
def test_scenario_budget_weights_shares_and_reserves_minimums(
    monkeypatch,
    deadline: float,
    remaining_minimums: list[float],
    eligible: list[str],
    expected_deadline: float,
    duration: str,
) -> None:
    monkeypatch.setattr(benchmark.time, 'monotonic', lambda: 100.0)

    assert _scenario_budget(
        Scenario('cell', 1, 'h1'),
        HarnessConfig(duration='10s', min_trials=3),
        eligible,
        deadline,
        remaining_minimums,
    ) == (expected_deadline, duration)


def test_scenario_budget_keeps_websocket_cells_above_the_k6_floor(
    monkeypatch,
) -> None:
    monkeypatch.setattr(benchmark.time, 'monotonic', lambda: 100.0)

    # A share that would shrink an HTTP cell to the 1 s floor keeps a
    # WebSocket cell at the k6 floor so startup cost cannot dominate.
    _deadline, duration = _scenario_budget(
        Scenario('ws cell', 1, 'ws'),
        HarnessConfig(duration='3s', min_trials=3),
        ['a', 'b'],
        118.0,
        [33.0, 33.0],
    )
    assert duration == '3000ms'


def test_public_metrics_are_trial_medians_and_retain_samples() -> None:
    samples: list[Metrics] = [
        {'rps': 100.0, 'latency_percentiles': {'p50': 0.01, 'p99': 0.03}},
        {'rps': 300.0, 'latency_percentiles': {'p50': 0.03, 'p99': 0.09}},
        {'rps': 200.0, 'latency_percentiles': {'p50': 0.02, 'p99': 0.06}},
    ]

    assert aggregate_metrics(samples) == {
        'rps': 200.0,
        'rps_samples': [100.0, 300.0, 200.0],
        'rps_range': [100.0, 300.0],
        'latency_percentiles': {'p50': 0.02, 'p99': 0.06},
        'latency_percentile_samples': {
            'p50': [0.01, 0.03, 0.02],
            'p99': [0.03, 0.09, 0.06],
        },
        'latency_percentile_ranges': {
            'p50': [0.01, 0.03],
            'p99': [0.03, 0.09],
        },
    }


def test_extractors_preserve_p99_9() -> None:
    oha = extract_oha_metrics({
        'summary': {'requestsPerSec': 100},
        'latencyPercentiles': {'p50': 0.001, 'p99': 0.004, 'p99.9': 0.009},
    })
    assert oha == {
        'rps': 100.0,
        'latency_percentiles': {'p50': 0.001, 'p99': 0.004, 'p99.9': 0.009},
    }

    k6 = extract_k6_metrics({
        'metrics': {
            'checks': {'passes': 20, 'fails': 0, 'value': 1},
            'bench_echo_success': {'passes': 10, 'fails': 0, 'value': 1},
            'iterations': {'rate': 50},
            'ws_session_duration': {
                'med': 1,
                'p(99)': 4,
                'p(99.9)': 8,
            },
        }
    })
    assert k6 == {
        'rps': 50.0,
        'latency_percentiles': {'p50': 0.001, 'p99': 0.004, 'p99.9': 0.008},
    }


def test_worker_readiness_requires_every_distinct_process(monkeypatch) -> None:
    class Process:
        def poll(self):
            return None

    pids = iter([10, 10, 11, 12])
    monkeypatch.setattr('bench.bench._worker_pid_response', lambda _socket: next(pids))
    monkeypatch.setattr('bench.bench.time.sleep', lambda _seconds: None)

    ready, seen = wait_for_worker_pids(3, timeout=1, process=Process())

    assert ready
    assert seen == {10, 11, 12}


def _publication_tree(tmp_path, monkeypatch, *, complete: bool = True):
    results_directory = tmp_path / 'results'
    raw_directory = results_directory / 'raw'
    plot_directory = results_directory / 'plots'
    raw_directory.mkdir(parents=True)
    plot_directory.mkdir()
    (raw_directory / 'benchmark_stale.json').write_text('{"generation":"old"}\n')
    (raw_directory / 'run_manifest.json').write_text('{"generation":"old"}\n')
    (plot_directory / 'benchmark_stale.svg').write_text('<svg id="old"/>\n')

    run_directory = results_directory / 'runs' / 'candidate'
    (run_directory / 'raw').mkdir(parents=True)
    (run_directory / 'plots').mkdir()
    (run_directory / 'load').mkdir()
    (run_directory / 'raw' / 'benchmark_cell.json').write_text('{"generation":"new"}\n')
    if complete:
        (run_directory / 'plots' / 'benchmark_cell.svg').write_text('<svg id="new"/>\n')
    (run_directory / 'load' / 'evidence.json').write_text('{}\n')
    (results_directory / 'compare').mkdir()
    (results_directory / 'compare' / 'keep.json').write_text('{}\n')

    monkeypatch.setattr(benchmark, 'CANONICAL_RAW_DIRECTORY', raw_directory)
    monkeypatch.setattr(benchmark, 'CANONICAL_PLOT_DIRECTORY', plot_directory)
    monkeypatch.setattr(benchmark, 'RUNS_DIRECTORY', results_directory / 'runs')
    return results_directory, run_directory, raw_directory, plot_directory


def _manifest(status):
    return benchmark._manifest_record({}, {}, [], {}, status=status)


def test_publication_replaces_the_canonical_artifacts(tmp_path, monkeypatch) -> None:
    results, run, raw, plots = _publication_tree(tmp_path, monkeypatch)

    _publish_artifacts(run, ['cell'], _manifest('complete'))

    assert (raw / 'benchmark_cell.json').read_text() == '{"generation":"new"}\n'
    assert (plots / 'benchmark_cell.svg').read_text() == '<svg id="new"/>\n'
    assert json.loads((raw / 'run_manifest.json').read_text())['status'] == 'complete'
    # Stale canonical artifacts from earlier runs are removed; everything
    # outside the canonical raw/plots directories is untouched.
    assert sorted(path.name for path in raw.iterdir()) == [
        'benchmark_cell.json',
        'run_manifest.json',
    ]
    assert sorted(path.name for path in plots.iterdir()) == ['benchmark_cell.svg']
    assert (results / 'runs/candidate/raw/benchmark_cell.json').exists()
    assert (results / 'compare/keep.json').read_text() == '{}\n'


def test_incomplete_publication_preserves_canonical_artifacts(
    tmp_path, monkeypatch
) -> None:
    _results, run, raw, plots = _publication_tree(tmp_path, monkeypatch, complete=False)

    with pytest.raises(BenchmarkError, match='artifact is missing'):
        _publish_artifacts(run, ['cell'], _manifest('complete'))

    assert (raw / 'benchmark_stale.json').read_text() == '{"generation":"old"}\n'
    assert (plots / 'benchmark_stale.svg').read_text() == '<svg id="old"/>\n'


def test_publication_requires_a_complete_run(tmp_path, monkeypatch) -> None:
    _results, run, _raw, _plots = _publication_tree(tmp_path, monkeypatch)

    with pytest.raises(BenchmarkError, match='complete benchmark run'):
        _publish_artifacts(run, ['cell'], _manifest('failed'))


def test_harness_configuration_is_immutable() -> None:
    harness = HarnessConfig()
    with pytest.raises((AttributeError, TypeError)):
        harness.max_trials = 2  # type: ignore[misc]


def test_classic_uvicorn_profile_loads_websockets_only_for_websocket_cells() -> None:
    http = get_server_command('uvicorn', 1, 'h1')
    websocket = get_server_command('uvicorn', 1, 'ws')

    for command in (http, websocket):
        assert command[:2] == ['uvicorn', 'bench.bench_app:app']
        assert command[command.index('--loop') + 1] == 'asyncio'
        assert command[command.index('--http') + 1] == 'h11'
        assert '--no-proxy-headers' in command
        assert all('uvicorn[standard]' not in argument for argument in command)
    assert http[http.index('--ws') + 1] == 'none'
    assert websocket[websocket.index('--ws') + 1] == 'websockets'


@pytest.mark.parametrize(
    ('field', 'value'),
    [
        ('duration', '11s'),
        ('warmup_duration', '2s'),
        ('concurrency', 101),
        ('min_trials', 4),
        ('max_trials', 9),
        ('stable_relative_spread', 0.04),
        ('settle_seconds', 0.5),
        ('order_seed', 1),
        ('rate_limit_qps', 10_000),
        ('max_load_utilization', 0.99),
        ('max_load_scaling_gain', 0.05),
        ('load_generator_grace_seconds', 30.0),
    ],
)
def test_publication_rejects_noncanonical_methodology(field, value) -> None:
    canonical = HarnessConfig(
        server_cpus=(2, 3, 4, 5),
        load_cpus=(0, 1, 6, 7),
        management_cpus=(8,),
    )

    with pytest.raises(BenchmarkError, match=field):
        _validate_publication_harness(
            replace(canonical, **{field: value}), _publication_system(canonical)
        )


def test_publication_allows_host_derived_worker_count_and_time_budget() -> None:
    harness = HarnessConfig(
        server_cpus=(2, 3, 4, 5),
        load_cpus=(0, 1, 6, 7),
        management_cpus=(8,),
        load_worker_threads=8,
        time_budget_seconds=60.0,
    )

    _validate_publication_harness(harness, _publication_system(harness))


def test_publication_requires_a_pinned_instrument_with_load_headroom() -> None:
    # Servers stay unpinned by default; only the instrument roles are
    # required and validated.
    canonical = HarnessConfig(load_cpus=(0, 1, 6, 7), management_cpus=(8,))
    _validate_publication_harness(canonical, _publication_system(canonical))

    harness = HarnessConfig(load_cpus=(0, 1, 6), management_cpus=(8,))
    with pytest.raises(BenchmarkError, match='at least four load-generator CPUs'):
        _validate_publication_harness(harness, _publication_system(harness))
    harness = HarnessConfig(load_cpus=(0, 1, 6, 8), management_cpus=(8,))
    with pytest.raises(BenchmarkError, match='overlap: 8'):
        _validate_publication_harness(harness, _publication_system(harness))
    harness = HarnessConfig(load_cpus=(0, 1, 6, 7), management_cpus=None)
    with pytest.raises(BenchmarkError, match='explicit load-generator and management'):
        _validate_publication_harness(
            harness,
            _publication_system(
                HarnessConfig(load_cpus=(0, 1, 6, 7), management_cpus=(8,))
            ),
        )


def test_publication_rejects_uncontrolled_or_overlapping_cpu_topology() -> None:
    harness = HarnessConfig(
        load_cpus=(0, 1, 6, 7),
        management_cpus=(8,),
    )

    offline = _publication_system(harness)
    assert offline['load_generator'] is not None
    offline['load_generator']['topology'][0]['online'] = False
    with pytest.raises(BenchmarkError, match='online'):
        _validate_publication_harness(harness, offline)

    unpinned = _publication_system(harness)
    unpinned['allowed_cpus'] = list(unpinned['cgroup_allowed_cpus'] or [])
    with pytest.raises(BenchmarkError, match='pinned to the management CPU'):
        _validate_publication_harness(harness, unpinned)

    partially_unpinned = _publication_system(harness)
    partially_unpinned['thread_affinity_masks'].append({
        'cpus': [0, 1, 2],
        'thread_count': 31,
    })
    with pytest.raises(BenchmarkError, match='every benchmark harness thread'):
        _validate_publication_harness(harness, partially_unpinned)

    disallowed = _publication_system(harness)
    disallowed['cgroup_allowed_cpus'] = [0, 1, 2, 3, 4, 6, 7, 8]
    disallowed['cgroup_allowed_cpus'].remove(7)
    with pytest.raises(BenchmarkError, match='outside the cgroup CPU allowance'):
        _validate_publication_harness(harness, disallowed)

    split_llc = _publication_system(harness)
    assert split_llc['load_generator'] is not None
    split_llc['load_generator']['topology'][0]['last_level_cache']['cache_id'] = 2
    with pytest.raises(BenchmarkError, match='one last-level-cache domain'):
        _validate_publication_harness(harness, split_llc)

    physical_overlap = _publication_system(harness)
    assert physical_overlap['management'] is not None
    assert physical_overlap['load_generator'] is not None
    physical_overlap['load_generator']['topology'][0]['core_id'] = physical_overlap[
        'management'
    ]['topology'][0]['core_id']
    with pytest.raises(BenchmarkError, match='disjoint physical'):
        _validate_publication_harness(harness, physical_overlap)


def test_scenario_warmup_is_unmeasured_validated_and_fully_recorded(
    tmp_path, monkeypatch
) -> None:
    durations: list[str] = []
    raw = {
        'summary': {'requestsPerSec': 123.0, 'successRate': 1.0},
        'statusCodeDistribution': {'200': 10},
        'errorDistribution': {},
        'latencyPercentiles': {'p50': 0.001, 'p99': 0.003, 'p99.9': 0.005},
    }
    usage: LoadGeneratorUsage = {
        'elapsed_seconds': 0.75,
        'cpu_seconds': 1.0,
        'logical_cpu_capacity': 4,
        'physical_core_capacity': 3,
        'logical_cpu_utilization': 0.25,
        'physical_core_utilization': 1 / 3,
        'physical_core_headroom': 2 / 3,
        'maximum_publish_physical_core_utilization': 0.85,
        'sufficient_headroom': True,
    }
    load = LoadResult(
        raw=raw,
        command={
            'argv': ['oha', '-z', '750ms'],
            'environment': {'TOKIO_WORKER_THREADS': '16'},
        },
        usage=usage,
    )

    def fake_load(_scenario, harness, _socket_path, _summary_file):
        durations.append(harness.duration)
        return load

    monkeypatch.setattr('bench.bench._run_scenario_load', fake_load)
    scenario = Scenario('HTTP/1 warmup', 1, 'h1')
    harness = HarnessConfig(duration='10s', warmup_duration='750ms')

    warmup = _run_scenario_warmup(
        scenario,
        harness,
        None,
        tmp_path / 'warmup.json',
    )
    metrics = _validate_scenario_load(scenario, harness, warmup)

    assert durations == ['750ms']
    assert metrics == {
        'rps': 123.0,
        'latency_percentiles': {'p50': 0.001, 'p99': 0.003, 'p99.9': 0.005},
    }
    assert _scenario_load_evidence(warmup, metrics) == {
        'load_command': load.command,
        'load_generator_usage': usage,
        'raw': raw,
        'metrics': metrics,
    }


@pytest.mark.parametrize(
    ('cpu_seconds', 'physical_utilization', 'headroom'),
    [(1.8, 0.9, 0.1), (3.6, 1.8, 0.0)],
)
def test_load_generator_cpu_saturation_fails_headroom_gate(
    monkeypatch, cpu_seconds, physical_utilization, headroom
) -> None:
    cpu_times = iter([10.0, 10.0 + cpu_seconds])
    wall_times = iter([20.0, 21.0])
    monkeypatch.setattr('bench.bench._child_cpu_seconds', lambda: next(cpu_times))
    monkeypatch.setattr('bench.bench.time.monotonic', lambda: next(wall_times))
    monkeypatch.setattr('bench.bench.physical_core_capacity', lambda _cpus: 2)

    class Process:
        returncode = 0
        pid = 123

        def communicate(self, *, timeout):
            assert timeout == 25.0
            return '', ''

    monkeypatch.setattr(
        'bench.bench.subprocess.Popen', lambda *_args, **_kwargs: Process()
    )

    _result, usage = _run_load_command(
        ['load-generator'],
        HarnessConfig(
            duration='10s',
            load_cpus=(4, 5, 6, 7),
            max_load_utilization=0.85,
        ),
    )

    assert usage['logical_cpu_utilization'] == pytest.approx(cpu_seconds / 4)
    assert usage['physical_core_utilization'] == pytest.approx(physical_utilization)
    assert usage['physical_core_headroom'] == pytest.approx(headroom)
    assert usage['sufficient_headroom'] is False


def test_load_generator_timeout_is_duration_bounded_and_kills_group(
    monkeypatch,
) -> None:
    class Process:
        returncode = None
        pid = 321

        def communicate(self, *, timeout):
            assert timeout == pytest.approx(2.25)
            raise subprocess.TimeoutExpired(['load-generator'], timeout)

    process = Process()
    terminated = []
    monkeypatch.setattr(
        'bench.bench.subprocess.Popen', lambda *_args, **_kwargs: process
    )
    monkeypatch.setattr('bench.bench.terminate_process_group', terminated.append)

    with pytest.raises(BenchmarkError, match='bounded timeout'):
        _run_load_command(
            ['load-generator'],
            HarnessConfig(duration='250ms', load_generator_grace_seconds=2.0),
        )

    assert terminated == [process]


def test_websocket_exact_reader_rejects_eof() -> None:
    class EOFConnection:
        def recv(self, _size):
            return b''

    with pytest.raises(BenchmarkError, match='ended after 0/2 bytes'):
        _receive_at_least(
            EOFConnection(),  # type: ignore[arg-type]
            bytearray(),
            2,
            label='WebSocket test frame',
        )


@pytest.mark.parametrize(
    ('full_base_rps', 'expected_plateau'),
    [(120.0, False), (80.0, True), (100.0, True)],
)
def test_publication_headroom_requires_symmetric_interleaved_scaling_plateau(
    tmp_path, monkeypatch, full_base_rps, expected_plateau
) -> None:
    counters = {'reduced': 0, 'full': 0}

    @contextmanager
    def fake_server(*_args, **_kwargs):
        yield {
            'command': [],
            'log': '',
            'worker_pids': [],
            'process_group_id': 77,
        }

    def fake_load(_scenario, harness, _socket_path, _summary_file):
        variant = 'full' if harness.load_worker_threads == 16 else 'reduced'
        counters[variant] += 1
        rps = (full_base_rps if variant == 'full' else 100.0) + counters[variant]
        return LoadResult(
            raw={'rps': rps},
            command={
                'argv': [variant],
                'environment': {
                    'TOKIO_WORKER_THREADS': str(harness.load_worker_threads)
                },
            },
            usage={
                'elapsed_seconds': 1.0,
                'cpu_seconds': 1.0,
                'logical_cpu_capacity': len(harness.load_cpus),
                'physical_core_capacity': len(harness.load_cpus),
                'logical_cpu_utilization': 0.25,
                'physical_core_utilization': 0.25,
                'physical_core_headroom': 0.75,
                'maximum_publish_physical_core_utilization': 0.85,
                'sufficient_headroom': True,
            },
        )

    monkeypatch.setattr('bench.bench.running_server', fake_server)
    monkeypatch.setattr(
        'bench.bench.validate_response_contract', lambda *_args: {'status': 200}
    )
    monkeypatch.setattr('bench.bench._run_scenario_load', fake_load)
    monkeypatch.setattr(
        'bench.bench.validate_oha_result', lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(
        'bench.bench.extract_metrics',
        lambda raw, _kind: {'rps': raw['rps'], 'latency_percentiles': {}},
    )
    monkeypatch.setattr('bench.bench.time.sleep', lambda _seconds: None)
    monkeypatch.setattr(
        'bench.bench.wait_for_worker_pids',
        lambda *_args, **_kwargs: (True, set()),
    )

    harness = HarnessConfig(load_cpus=(4, 5, 6, 7), management_cpus=(8,))
    ladder = _measure_load_headroom(
        Scenario('cell', 1, 'h1'),
        'h2corn',
        harness,
        tmp_path,
    )

    assert ladder['warmup_order'] == ['reduced', 'full']
    assert ladder['order'] == ['reduced', 'full', 'full', 'reduced']
    assert ladder['load_cpus'] == [4, 5, 6, 7]
    assert ladder['reduced_worker_threads'] == 12
    assert ladder['full_worker_threads'] == 16
    assert ladder['maximum_publish_gain'] == 0.05
    assert ladder['all_runs_have_cpu_headroom'] is True
    assert ladder['plateau_observed'] is expected_plateau
    if full_base_rps > 100.0:
        assert ladder['full_vs_reduced_gain'] > 0.15
    elif full_base_rps < 100.0:
        assert ladder['full_vs_reduced_gain'] < -0.15
    else:
        assert ladder['full_vs_reduced_gain'] == 0.0
    assert len(ladder['paired_gain_samples']) == 2
    assert len(ladder['runs']) == 6


def test_headroom_propagates_load_failure(tmp_path, monkeypatch) -> None:
    harness = HarnessConfig(
        server_cpus=(0, 1, 2, 3),
        load_cpus=(4, 5, 6, 7),
        management_cpus=(8,),
    )

    @contextmanager
    def fake_server(*_args, **_kwargs):
        yield {
            'command': [],
            'log': '',
            'worker_pids': [],
            'process_group_id': 77,
        }

    monkeypatch.setattr('bench.bench.running_server', fake_server)
    monkeypatch.setattr(
        'bench.bench.validate_response_contract', lambda *_args: {'status': 200}
    )
    monkeypatch.setattr('bench.bench.time.sleep', lambda _seconds: None)
    monkeypatch.setattr('bench.bench.ProcessGroupResourceSampler', _StubResourceSampler)
    monkeypatch.setattr(
        'bench.bench._run_scenario_load',
        lambda *_args: (_ for _ in ()).throw(BenchmarkError('load failed')),
    )

    with pytest.raises(BenchmarkError, match='load failed'):
        _measure_load_headroom(
            Scenario('cell', 1, 'h1'),
            'h2corn',
            harness,
            tmp_path,
        )


def test_public_suite_keeps_only_single_worker_http2_multiplexing() -> None:
    multiplexed = [
        scenario for scenario in benchmark_scenarios() if scenario.http2_parallelism > 1
    ]

    assert [
        (scenario.workers, scenario.concurrency, scenario.http2_parallelism)
        for scenario in multiplexed
    ] == [(1, 10, 10)]


def test_publication_contract_cannot_be_bypassed_through_python_api() -> None:
    with pytest.raises(BenchmarkError, match='complete server'):
        run_benchmarks({'h2corn'}, {'h1'}, publish=True)
