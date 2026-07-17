import json
import subprocess
from contextlib import contextmanager
from dataclasses import replace

import pytest

import bench.bench as benchmark
from bench.bench import (
    BENCHMARK_PYTHON_MODULES,
    AmbientCpuError,
    BenchmarkError,
    CommandIdentity,
    GitIdentity,
    HarnessConfig,
    LoadGeneratorUsage,
    LoadResult,
    Metrics,
    RunSnapshotGate,
    Scenario,
    _artifact_identity,
    _ensure_static_file_response_payload,
    _equivalence_sign_test_p,
    _git_identity,
    _measure_load_headroom,
    _one_sided_sign_test_p,
    _publish_artifacts,
    _receive_at_least,
    _run_load_command,
    _run_scenario_warmup,
    _scenario_load_evidence,
    _validate_ambient_cpu,
    _validate_interference_cpu,
    _validate_publication_harness,
    _validate_scenario_load,
    aggregate_metrics,
    artifact_snapshot,
    balanced_orders,
    benchmark_scenarios,
    extract_k6_metrics,
    extract_oha_metrics,
    get_server_command,
    run_benchmarks,
    verify_artifact_snapshot,
    verify_run_snapshot,
    wait_for_worker_pids,
)
from bench.system import (
    AmbientCpuProbe,
    BenchmarkSystemState,
    CpuActivity,
    CpuSetState,
    CpuTopology,
)


def _quiet_probe(**overrides) -> AmbientCpuProbe:
    probe: AmbientCpuProbe = {
        'started_at': 'start',
        'completed_at': 'end',
        'elapsed_seconds': 1.0,
        'system_physical_core_utilization': 0.0,
        'server_physical_core_utilization': 0.0,
        'server_llc_physical_core_utilization': 0.0,
        'load_llc_physical_core_utilization': 0.0,
        'maximum_cpu_utilization': 0.0,
        'per_cpu': [],
    }
    probe.update(overrides)  # type: ignore[typeddict-item]
    return probe


def _quiet_activity(**overrides) -> CpuActivity:
    activity: CpuActivity = {
        'started_at': 'start',
        'completed_at': 'end',
        'elapsed_seconds': 1.0,
        'cpus': [9],
        'physical_core_count': 1,
        'total_ticks': 100,
        'active_ticks': 0,
        'physical_core_utilization': 0.0,
        'maximum_cpu_utilization': 0.0,
        'per_cpu': [],
        'interval_seconds': 1.0,
        'windows': [],
        'maximum_raw_window_physical_core_utilization': 0.0,
        'maximum_raw_window_cpu_utilization': 0.0,
        'maximum_window_physical_core_utilization': 0.0,
        'maximum_window_cpu_utilization': 0.0,
    }
    activity.update(overrides)  # type: ignore[typeddict-item]
    return activity


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


class _StubInterferenceMonitor:
    def start(self):
        pass

    def stop(self):
        return _quiet_activity()


def _snapshot_gate(harness: HarnessConfig, system: BenchmarkSystemState):
    """A real gate over an empty artifact set: cell checks are no-ops."""
    git: GitIdentity = {
        'git_head': None,
        'git_diff_sha256': None,
        'git_status_sha256': None,
        'git_untracked_sha256': None,
    }
    return RunSnapshotGate({}, git, system, harness, {}, set())


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
    assert harness.server_cpus is not None
    assert harness.load_cpus is not None
    assert harness.management_cpus is not None
    all_cpus = sorted(
        set(harness.server_cpus + harness.load_cpus + harness.management_cpus)
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
        'server': _cpu_set(harness.server_cpus, llc_id=0),
        'load_generator': _cpu_set(harness.load_cpus, llc_id=1),
        'management': _cpu_set(harness.management_cpus, llc_id=0),
    }


def _run_stubbed_main_suite(tmp_path, monkeypatch, outcome: str):
    command: CommandIdentity = {
        'argv': ['load'],
        'executable': None,
        'executable_sha256': None,
        'environment': {},
    }
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
    ambient = _quiet_probe()
    observations = []
    initial_manifests = []
    balanced_orders_impl = benchmark.balanced_orders

    monkeypatch.setattr(benchmark, 'RUNS_DIRECTORY', tmp_path)
    monkeypatch.setattr(benchmark, 'SERVERS', {'h2corn': ['h2corn']})
    monkeypatch.setattr(benchmark, 'SERVER_PROFILES', {'h2corn': 'test'})
    monkeypatch.setattr(
        benchmark, 'benchmark_scenarios', lambda: [Scenario('cell', 1, 'h1')]
    )
    # The Bonferroni family follows the stubbed scenario set.
    monkeypatch.setattr(benchmark, 'HEADROOM_FAMILY_SIZE', 1)

    def observe_initial_manifest(*args, **kwargs):
        run_directory = next(tmp_path.iterdir())
        initial_manifests.append(
            json.loads((run_directory / 'manifest.json').read_text())
        )
        return balanced_orders_impl(*args, **kwargs)

    monkeypatch.setattr(benchmark, 'balanced_orders', observe_initial_manifest)
    monkeypatch.setattr(benchmark, '_ensure_static_file_response_payload', lambda: None)

    def recover_before_creating_run() -> None:
        assert not any(tmp_path.iterdir())

    monkeypatch.setattr(benchmark, '_recover_publication', recover_before_creating_run)
    monkeypatch.setattr(
        benchmark, 'capture_system_state', lambda *_args: {'summary': 'test'}
    )
    monkeypatch.setattr(benchmark, '_validate_publication_harness', lambda *_args: None)
    monkeypatch.setattr(benchmark, 'artifact_snapshot', lambda *_args: {})
    monkeypatch.setattr(
        benchmark,
        '_git_identity',
        lambda: {
            'git_head': 'test',
            'git_diff_sha256': None,
            'git_status_sha256': None,
            'git_untracked_sha256': None,
        },
    )
    monkeypatch.setattr(benchmark, 'get_versions', dict)
    monkeypatch.setattr(benchmark, 'benchmark_provenance', lambda *_args: {})
    monkeypatch.setattr(benchmark, 'verify_run_snapshot', lambda *_args: None)
    monkeypatch.setattr(
        benchmark,
        'command_provenance',
        lambda argv: {**command, 'argv': argv},
    )
    monkeypatch.setattr(benchmark.time, 'sleep', lambda _seconds: None)
    monkeypatch.setattr(benchmark, 'validate_response_contract', lambda *_args: {})
    monkeypatch.setattr(benchmark, '_run_scenario_warmup', lambda *_args: warmup)
    monkeypatch.setattr(benchmark, '_validate_scenario_load', lambda *_args: metrics)
    monkeypatch.setattr(benchmark, '_capture_ambient_cpu', lambda *_args: ambient)
    monkeypatch.setattr(benchmark, '_validate_ambient_cpu', lambda *_args: None)
    monkeypatch.setattr(benchmark, '_validate_interference_cpu', lambda *_args: None)
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
        run_directory = next(tmp_path.iterdir())
        manifest = json.loads((run_directory / 'manifest.json').read_text())
        scenario = json.loads((run_directory / 'raw/benchmark_cell.json').read_text())
        observations.append((manifest, scenario))
        if outcome == 'interrupt':
            raise KeyboardInterrupt
        if outcome == 'error':
            raise BenchmarkError('measured load failed')
        return measured

    monkeypatch.setattr(benchmark, 'running_server', stub_server)
    monkeypatch.setattr(benchmark, 'ProcessGroupResourceSampler', _StubResourceSampler)
    monkeypatch.setattr(
        benchmark, '_interference_monitor', lambda *_args: _StubInterferenceMonitor()
    )
    monkeypatch.setattr(benchmark, '_run_scenario_load', measured_load)

    harness = HarnessConfig(trials=2, settle_seconds=0)
    if outcome == 'interrupt':
        with pytest.raises(KeyboardInterrupt):
            run_benchmarks(harness=harness, publish=True)
        run_directory = next(tmp_path.iterdir())
    else:
        run_directory = run_benchmarks(harness=harness)
    return run_directory, initial_manifests, observations, ambient


def test_main_suite_retains_pending_attempt_when_interrupted(
    tmp_path, monkeypatch
) -> None:
    run_directory, initial_manifests, observations, ambient = _run_stubbed_main_suite(
        tmp_path, monkeypatch, 'interrupt'
    )

    assert initial_manifests[0]['status'] == 'running'
    assert initial_manifests[0]['scenarios'] == {}
    running_manifest, pending_scenario = observations[0]
    assert running_manifest['status'] == 'running'
    assert running_manifest['scenarios'] == {'cell': 'running'}
    assert pending_scenario['status'] == 'running'
    assert pending_scenario['runs'] == []
    pending = pending_scenario['pending_attempt']
    assert pending['trial'] == 1
    assert pending['order'] == ['h2corn']
    assert pending['server'] == 'h2corn'
    assert pending['ambient_cpu_before'] == ambient
    assert pending['server_command']['argv'][-2:] == ['-w', '1']
    assert pending['warmup']['raw'] == {'phase': 'warmup'}
    assert pending['warmup']['metrics'] == {
        'rps': 100.0,
        'latency_percentiles': {'p99': 0.001},
    }

    failed_manifest = json.loads((run_directory / 'manifest.json').read_text())
    failed_scenario = json.loads(
        (run_directory / 'raw/benchmark_cell.json').read_text()
    )
    assert failed_manifest['status'] == 'failed'
    assert failed_manifest['error'] == 'KeyboardInterrupt'
    assert failed_manifest['scenarios'] == {'cell': 'failed'}
    assert failed_scenario['status'] == 'failed'
    assert failed_scenario['error'] == 'KeyboardInterrupt'
    assert failed_scenario['pending_attempt'] == pending_scenario['pending_attempt']


@pytest.mark.parametrize(
    ('outcome', 'expected_status', 'expected_runs'),
    [('success', 'complete', 2), ('error', 'failed', 1)],
)
def test_main_suite_clears_completed_pending_attempts(
    tmp_path, monkeypatch, outcome, expected_status, expected_runs
) -> None:
    run_directory, _initial_manifests, observations, _ambient = _run_stubbed_main_suite(
        tmp_path, monkeypatch, outcome
    )

    assert observations[0][0]['status'] == 'running'
    assert observations[0][1]['pending_attempt']['server'] == 'h2corn'
    manifest = json.loads((run_directory / 'manifest.json').read_text())
    scenario = json.loads((run_directory / 'raw/benchmark_cell.json').read_text())
    assert manifest['status'] == expected_status
    assert manifest['scenarios'] == {'cell': expected_status}
    assert scenario['status'] == expected_status
    assert scenario['pending_attempt'] is None
    assert len(scenario['runs']) == expected_runs


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


def test_balanced_orders_reject_inexact_or_odd_trial_counts() -> None:
    with pytest.raises(ValueError, match='even'):
        balanced_orders(['a', 'b'], 5)
    with pytest.raises(ValueError, match='twice'):
        balanced_orders(['a', 'b', 'c'], 8)


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


def test_artifact_snapshot_detects_executable_replacement(tmp_path) -> None:
    executable = tmp_path / 'server'
    executable.write_text('first')
    executable.chmod(0o755)
    command = {'server': [str(executable)]}
    expected = artifact_snapshot(command, set())
    executable.write_text('second')

    with pytest.raises(BenchmarkError, match='server_executable:server'):
        verify_artifact_snapshot(expected, command, set())


def test_artifact_snapshot_covers_competitor_and_dependency_source_trees() -> None:
    snapshot = artifact_snapshot({}, set())

    assert {
        key.removeprefix('python_module:')
        for key in snapshot
        if key.startswith('python_module:')
    } == set(BENCHMARK_PYTHON_MODULES)
    assert all(
        snapshot[f'python_module:{module}']['sha256'] is not None
        for module in BENCHMARK_PYTHON_MODULES
    )


def test_static_payload_is_canonical_and_frozen(tmp_path, monkeypatch) -> None:
    payload = tmp_path / 'payload.bin'
    payload.write_bytes(b'wrong but same size')
    monkeypatch.setattr('bench.bench.STATIC_FILE_RESPONSE_PATH', payload)

    _ensure_static_file_response_payload()
    expected = artifact_snapshot({}, set())
    payload.write_bytes(b'x' * payload.stat().st_size)

    with pytest.raises(BenchmarkError, match='static_file_payload'):
        verify_artifact_snapshot(expected, {}, set())


def test_artifact_snapshot_detects_python_module_tree_replacement(
    tmp_path, monkeypatch
) -> None:
    package = tmp_path / 'uvicorn'
    package.mkdir()
    source = package / '__init__.py'
    source.write_text('VERSION = 1\n')
    monkeypatch.setattr('bench.bench.BENCHMARK_PYTHON_MODULES', ('uvicorn',))
    monkeypatch.setattr(
        'bench.bench._module_artifact',
        lambda module: (
            _artifact_identity(package)
            if module == 'uvicorn'
            else {'path': module, 'sha256': None}
        ),
    )
    expected = artifact_snapshot({}, set())

    source.write_text('VERSION = 2\n')
    with pytest.raises(BenchmarkError, match='python_module:uvicorn'):
        verify_artifact_snapshot(expected, {}, set())


def test_git_identity_hashes_untracked_contents_but_excludes_results(
    tmp_path, monkeypatch
) -> None:
    subprocess.run(['git', 'init', '--quiet'], cwd=tmp_path, check=True)
    project = tmp_path / 'h2corn'
    project.mkdir()
    monkeypatch.chdir(project)

    initial = _git_identity()
    result = project / 'bench/results/raw/benchmark.json'
    result.parent.mkdir(parents=True)
    result.write_text('transient result\n')
    assert _git_identity() == initial

    (tmp_path / 'sibling-project.py').write_text('unrelated = True\n')
    assert _git_identity() == initial

    source = project / 'untracked.py'
    source.write_text('VALUE = 1\n')
    first_source = _git_identity()
    assert first_source['git_status_sha256'] != initial['git_status_sha256']
    assert first_source['git_untracked_sha256'] != initial['git_untracked_sha256']

    source.write_text('VALUE = 2\n')
    second_source = _git_identity()
    assert second_source['git_status_sha256'] == first_source['git_status_sha256']
    assert second_source['git_untracked_sha256'] != first_source['git_untracked_sha256']


def _publication_tree(tmp_path, monkeypatch, *, complete: bool = True):
    results_directory = tmp_path / 'results'
    raw_directory = results_directory / 'raw'
    plot_directory = results_directory / 'plots'
    raw_directory.mkdir(parents=True)
    plot_directory.mkdir()
    (raw_directory / 'benchmark_cell.json').write_text('{"generation":"old"}\n')
    (raw_directory / 'run_manifest.json').write_text('{"generation":"old"}\n')
    (plot_directory / 'benchmark_cell.svg').write_text('<svg id="old"/>\n')

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


def _visible_publication_generation(raw_directory, plot_directory) -> str:
    raw = (raw_directory / 'benchmark_cell.json').read_text()
    plot = (plot_directory / 'benchmark_cell.svg').read_text()
    manifest = json.loads((raw_directory / 'run_manifest.json').read_text())
    if (
        raw == '{"generation":"old"}\n'
        and plot == '<svg id="old"/>\n'
        and manifest == {'generation': 'old'}
    ):
        return 'old'
    if (
        raw == '{"generation":"new"}\n'
        and plot == '<svg id="new"/>\n'
        and manifest == {'status': 'complete'}
    ):
        return 'new'
    raise AssertionError(
        f'mixed publication generation: raw={raw!r}, plot={plot!r}, '
        f'manifest={manifest!r}'
    )


def _assert_publication_transaction_clean(results_directory) -> None:
    control_directory = results_directory.parent
    assert not list(
        control_directory.glob(f'{benchmark.PUBLICATION_TRANSACTION_PREFIX}*')
    )


def test_publication_atomically_replaces_the_complete_generation(
    tmp_path, monkeypatch
) -> None:
    results, run, raw, plots = _publication_tree(tmp_path, monkeypatch)

    _publish_artifacts(run, ['cell'], {'status': 'complete'})

    assert _visible_publication_generation(raw, plots) == 'new'
    assert not (raw / 'benchmark_cell.json').is_symlink()
    assert not (plots / 'benchmark_cell.svg').is_symlink()
    assert sorted(path.name for path in raw.iterdir()) == [
        benchmark.PUBLICATION_GENERATION_NAME,
        'benchmark_cell.json',
        'run_manifest.json',
    ]
    assert sorted(path.name for path in plots.iterdir()) == ['benchmark_cell.svg']
    # A directory-level generation swap must preserve all noncanonical evidence,
    # including nested directories named raw/plots inside completed run records.
    assert (results / 'runs/candidate/raw/benchmark_cell.json').read_text() == (
        '{"generation":"new"}\n'
    )
    assert (results / 'runs/candidate/plots/benchmark_cell.svg').read_text() == (
        '<svg id="new"/>\n'
    )
    assert (results / 'runs/candidate/load/evidence.json').read_text() == '{}\n'
    assert (results / 'compare/keep.json').read_text() == '{}\n'
    _assert_publication_transaction_clean(results)


@pytest.mark.parametrize(
    ('transition', 'expected_generation', 'raises'),
    [
        # Before the exchange, a failure leaves the canonical tree untouched.
        ('staged', 'old', True),
        # The exchange is the commit point: later failures still publish.
        ('exchanged', 'new', False),
        ('cleaned', 'new', False),
    ],
)
def test_publication_recovers_synchronous_failure_at_every_transition(
    tmp_path, monkeypatch, transition, expected_generation, raises
) -> None:
    results, run, raw, plots = _publication_tree(tmp_path, monkeypatch)

    def fail_at_transition(current: str) -> None:
        if current == transition:
            raise OSError(f'injected failure at {transition}')

    monkeypatch.setattr(benchmark, '_publication_checkpoint', fail_at_transition)
    if raises:
        with pytest.raises(OSError, match=f'injected failure at {transition}'):
            _publish_artifacts(run, ['cell'], {'status': 'complete'})
    else:
        _publish_artifacts(run, ['cell'], {'status': 'complete'})

    assert _visible_publication_generation(raw, plots) == expected_generation
    _assert_publication_transaction_clean(results)


class _SimulatedPublicationCrash(BaseException):
    pass


@pytest.mark.parametrize(
    ('transition', 'visible_generation'),
    [
        # Crash before the exchange: the canonical generation is untouched.
        ('staged', 'old'),
        # Crash after the exchange, before cleanup: the new generation is
        # fully visible and only an orphaned swapped-out tree remains.
        ('exchanged', 'new'),
        ('cleaned', 'new'),
    ],
)
def test_publication_crash_never_exposes_a_mixed_generation_and_recovers(
    tmp_path,
    monkeypatch,
    transition,
    visible_generation,
) -> None:
    results, run, raw, plots = _publication_tree(tmp_path, monkeypatch)

    def crash_at_transition(current: str) -> None:
        if current == transition:
            raise _SimulatedPublicationCrash(transition)

    monkeypatch.setattr(benchmark, '_publication_checkpoint', crash_at_transition)
    with pytest.raises(_SimulatedPublicationCrash, match=transition):
        _publish_artifacts(run, ['cell'], {'status': 'complete'})

    assert _visible_publication_generation(raw, plots) == visible_generation
    monkeypatch.setattr(benchmark, '_publication_checkpoint', lambda _current: None)
    benchmark._recover_publication()
    assert _visible_publication_generation(raw, plots) == visible_generation
    _assert_publication_transaction_clean(results)


def test_publication_recovery_refuses_a_stale_committed_generation(
    tmp_path, monkeypatch
) -> None:
    _results, run, raw, _plots = _publication_tree(tmp_path, monkeypatch)

    def crash_after_exchange(current: str) -> None:
        if current == 'exchanged':
            raise _SimulatedPublicationCrash(current)

    monkeypatch.setattr(benchmark, '_publication_checkpoint', crash_after_exchange)
    with pytest.raises(_SimulatedPublicationCrash):
        _publish_artifacts(run, ['cell'], {'status': 'complete'})
    (raw / 'benchmark_cell.json').write_text('{"generation":"tampered"}\n')

    monkeypatch.setattr(benchmark, '_publication_checkpoint', lambda _current: None)
    with pytest.raises(BenchmarkError, match='artifact is stale'):
        benchmark._recover_publication()


def test_incomplete_publication_preserves_canonical_artifacts(
    tmp_path, monkeypatch
) -> None:
    results, run, raw, plots = _publication_tree(tmp_path, monkeypatch, complete=False)

    with pytest.raises(BenchmarkError, match='artifact is missing'):
        _publish_artifacts(run, ['cell'], {'status': 'complete'})

    assert _visible_publication_generation(raw, plots) == 'old'
    _assert_publication_transaction_clean(results)


def test_harness_configuration_is_immutable() -> None:
    harness = HarnessConfig()
    with pytest.raises((AttributeError, TypeError)):
        harness.trials = 2  # type: ignore[misc]


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
        ('trials', 16),
        ('settle_seconds', 0.5),
        ('order_seed', 1),
        ('rate_limit_qps', 10_000),
        ('load_worker_threads', 8),
        ('max_load_utilization', 0.99),
        ('ambient_cpu_probe_seconds', 0.5),
        ('max_ambient_cpu_utilization', 0.2),
        ('max_ambient_single_cpu_utilization', 0.2),
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


def test_publication_requires_four_disjoint_server_cpus_and_load_headroom() -> None:
    canonical = HarnessConfig(
        server_cpus=(2, 3, 4, 5),
        load_cpus=(0, 1, 6, 7),
        management_cpus=(8,),
    )
    _validate_publication_harness(canonical, _publication_system(canonical))

    harness = HarnessConfig(
        server_cpus=(2, 3, 4),
        load_cpus=(0, 1, 6, 7),
        management_cpus=(8,),
    )
    with pytest.raises(BenchmarkError, match='exactly four server CPUs'):
        _validate_publication_harness(harness, _publication_system(harness))
    harness = HarnessConfig(
        server_cpus=(2, 3, 4, 5),
        load_cpus=(0, 1, 6),
        management_cpus=(8,),
    )
    with pytest.raises(BenchmarkError, match='at least four load-generator CPUs'):
        _validate_publication_harness(harness, _publication_system(harness))
    harness = HarnessConfig(
        server_cpus=(2, 3, 4, 5),
        load_cpus=(0, 1, 2, 6),
        management_cpus=(8,),
    )
    with pytest.raises(BenchmarkError, match='overlap: 2'):
        _validate_publication_harness(harness, _publication_system(harness))


def test_publication_rejects_uncontrolled_or_overlapping_cpu_topology() -> None:
    harness = HarnessConfig(
        server_cpus=(2, 3, 4, 5),
        load_cpus=(0, 1, 6, 7),
        management_cpus=(8,),
    )

    offline = _publication_system(harness)
    assert offline['server'] is not None
    offline['server']['topology'][0]['online'] = False
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

    policy = _publication_system(harness)
    assert policy['load_generator'] is not None
    policy['load_generator']['topology'][0]['frequency']['scaling_governor'] = (
        'powersave'
    )
    with pytest.raises(BenchmarkError, match='performance CPU governor'):
        _validate_publication_harness(harness, policy)

    same_llc = _publication_system(harness)
    assert same_llc['load_generator'] is not None
    for entry in same_llc['load_generator']['topology']:
        entry['last_level_cache']['cache_id'] = 0
        entry['last_level_cache']['shared_cpus'] = [2, 3, 4, 5]
    with pytest.raises(BenchmarkError, match='distinct server/load last-level-cache'):
        _validate_publication_harness(harness, same_llc)

    physical_overlap = _publication_system(harness)
    assert physical_overlap['server'] is not None
    assert physical_overlap['load_generator'] is not None
    physical_overlap['load_generator']['topology'][0]['core_id'] = physical_overlap[
        'server'
    ]['topology'][0]['core_id']
    with pytest.raises(BenchmarkError, match='disjoint physical'):
        _validate_publication_harness(harness, physical_overlap)


def test_snapshot_gate_rehashes_only_when_the_fingerprint_changes(
    tmp_path, monkeypatch
) -> None:
    executable = tmp_path / 'server'
    executable.write_text('first')
    executable.chmod(0o755)
    command = {'server': [str(executable)]}
    git: GitIdentity = {
        'git_head': None,
        'git_diff_sha256': None,
        'git_status_sha256': None,
        'git_untracked_sha256': None,
    }
    system = _publication_system(
        HarnessConfig(
            server_cpus=(2, 3, 4, 5), load_cpus=(0, 1, 6, 7), management_cpus=(8,)
        )
    )
    monkeypatch.setattr('bench.bench._git_identity', lambda: git)
    monkeypatch.setattr('bench.bench.capture_system_state', lambda *_args: system)
    monkeypatch.setattr(
        'bench.bench._module_artifact', lambda _module: {'path': '', 'sha256': None}
    )
    monkeypatch.setattr('bench.bench.BENCHMARK_PYTHON_MODULES', ())

    artifacts = artifact_snapshot(command, set())
    gate = RunSnapshotGate(artifacts, git, system, HarnessConfig(), command, set())

    # Untouched artifacts: the cheap fingerprint check is sufficient.
    gate.verify_cell()

    # A rewrite with identical contents changes the fingerprint but not the
    # hash: the full verification runs once and the new fingerprint is kept.
    executable.write_text('first')
    gate.verify_cell()
    gate.verify_cell()

    # A real content change fails exactly as the full check would.
    executable.write_text('second')
    with pytest.raises(BenchmarkError, match='server_executable:server'):
        gate.verify_cell()


def test_run_snapshot_rejects_system_policy_drift(monkeypatch) -> None:
    harness = HarnessConfig(
        server_cpus=(2, 3, 4, 5),
        load_cpus=(0, 1, 6, 7),
        management_cpus=(8,),
    )
    expected = _publication_system(harness)
    changed = _publication_system(harness)
    assert changed['load_generator'] is not None
    changed['load_generator']['topology'][0]['frequency']['scaling_governor'] = (
        'powersave'
    )
    git: GitIdentity = {
        'git_head': None,
        'git_diff_sha256': None,
        'git_status_sha256': None,
        'git_untracked_sha256': None,
    }
    monkeypatch.setattr('bench.bench.verify_artifact_snapshot', lambda *_args: None)
    monkeypatch.setattr('bench.bench._git_identity', lambda: git)
    monkeypatch.setattr('bench.bench.capture_system_state', lambda *_args: changed)

    with pytest.raises(BenchmarkError, match='system state changed'):
        verify_run_snapshot({}, git, expected, harness, {}, set())


def test_run_snapshot_allows_thread_count_churn_with_frozen_affinity(
    monkeypatch,
) -> None:
    harness = HarnessConfig(
        server_cpus=(2, 3, 4, 5),
        load_cpus=(0, 1, 6, 7),
        management_cpus=(8,),
    )
    expected = _publication_system(harness)
    current = _publication_system(harness)
    current['thread_affinity_masks'][0]['thread_count'] = 32
    git: GitIdentity = {
        'git_head': None,
        'git_diff_sha256': None,
        'git_status_sha256': None,
        'git_untracked_sha256': None,
    }
    monkeypatch.setattr('bench.bench.verify_artifact_snapshot', lambda *_args: None)
    monkeypatch.setattr('bench.bench._git_identity', lambda: git)
    monkeypatch.setattr('bench.bench.capture_system_state', lambda *_args: current)

    verify_run_snapshot({}, git, expected, harness, {}, set())


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
            'executable': '/usr/bin/oha',
            'executable_sha256': 'abc',
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
        HarnessConfig(load_cpus=(4, 5, 6, 7), max_load_utilization=0.85),
    )

    assert usage['logical_cpu_utilization'] == pytest.approx(cpu_seconds / 4)
    assert usage['physical_core_utilization'] == pytest.approx(physical_utilization)
    assert usage['physical_core_headroom'] == pytest.approx(headroom)
    assert usage['sufficient_headroom'] is False


def test_publication_ambient_cpu_gate_rejects_background_contention() -> None:
    probe = _quiet_probe(
        system_physical_core_utilization=0.08,
        server_physical_core_utilization=0.12,
        server_llc_physical_core_utilization=0.07,
        load_llc_physical_core_utilization=0.04,
        maximum_cpu_utilization=0.12,
    )

    with pytest.raises(AmbientCpuError, match=r'server-cores=12\.0%'):
        _validate_ambient_cpu(probe, HarnessConfig())

    probe['server_physical_core_utilization'] = 0.09
    _validate_ambient_cpu(probe, HarnessConfig())


def test_publication_interference_gate_rejects_short_activity_window() -> None:
    activity = _quiet_activity(
        elapsed_seconds=10.0,
        total_ticks=1_000,
        active_ticks=50,
        physical_core_utilization=0.05,
        maximum_cpu_utilization=0.05,
        maximum_raw_window_physical_core_utilization=0.20,
        maximum_raw_window_cpu_utilization=0.20,
        maximum_window_physical_core_utilization=0.20,
        maximum_window_cpu_utilization=0.20,
    )

    with pytest.raises(AmbientCpuError, match=r'worst-window-physical-cores=20\.0%'):
        _validate_interference_cpu(activity, HarnessConfig())

    activity['maximum_window_physical_core_utilization'] = 0.09
    activity['maximum_window_cpu_utilization'] = 0.09
    _validate_interference_cpu(activity, HarnessConfig())


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
                'executable': variant,
                'executable_sha256': None,
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
        'bench.bench._capture_ambient_cpu', lambda *_args: _quiet_probe()
    )
    monkeypatch.setattr(
        'bench.bench._interference_monitor',
        lambda *_args: _StubInterferenceMonitor(),
    )
    monkeypatch.setattr(
        'bench.bench.wait_for_worker_pids',
        lambda *_args, **_kwargs: (True, set()),
    )

    system = _publication_system(
        HarnessConfig(
            server_cpus=(0, 1, 2, 3),
            load_cpus=(4, 5, 6, 7),
            management_cpus=(8,),
        )
    )
    harness = HarnessConfig(load_cpus=(4, 5, 6, 7), management_cpus=(8,))
    ladder = _measure_load_headroom(
        Scenario('cell', 1, 'h1'),
        'h2corn',
        harness,
        tmp_path,
        _snapshot_gate(harness, system),
        system,
    )

    assert ladder['warmup_order'] == ['reduced', 'full']
    assert ladder['order'] == ['reduced', 'full', 'full', 'reduced'] * 7
    assert ladder['load_cpus'] == [4, 5, 6, 7]
    assert ladder['reduced_worker_threads'] == 12
    assert ladder['full_worker_threads'] == 16
    assert ladder['plateau_observed'] is expected_plateau
    if full_base_rps > 100.0:
        assert ladder['paired_gain_lower_quartile'] > 0.15
    elif full_base_rps < 100.0:
        assert ladder['paired_gain_upper_quartile'] < -0.15
    else:
        assert ladder['full_vs_reduced_gain'] == 0.0
    assert len(ladder['paired_gain_samples']) == 14


def test_headroom_sign_test_rejects_noise_not_just_a_large_median() -> None:
    wins, comparisons, probability = _one_sided_sign_test_p([
        0.39,
        0.10,
        0.07,
        0.05,
        0.04,
        -0.03,
        -0.33,
        -0.42,
    ])

    assert (wins, comparisons) == (5, 8)
    assert probability == pytest.approx(0.36328125)


def test_headroom_equivalence_rejects_mixed_nonsignificant_scaling() -> None:
    below_margin, comparisons, probability = _equivalence_sign_test_p(
        [-0.20] * 6 + [0.50] * 6,
        0.02,
    )

    assert (below_margin, comparisons) == (6, 12)
    assert probability == pytest.approx(0.61279296875)
    assert probability > 0.05 / 21


def test_headroom_persists_failed_ambient_probe(tmp_path, monkeypatch) -> None:
    harness = HarnessConfig(
        server_cpus=(0, 1, 2, 3),
        load_cpus=(4, 5, 6, 7),
        management_cpus=(8,),
    )
    system = _publication_system(harness)
    probe = _quiet_probe(
        system_physical_core_utilization=0.2,
        maximum_cpu_utilization=0.2,
    )
    monkeypatch.setattr('bench.bench._capture_ambient_cpu', lambda *_args: probe)

    with pytest.raises(AmbientCpuError):
        _measure_load_headroom(
            Scenario('cell', 1, 'h1'),
            'h2corn',
            harness,
            tmp_path,
            _snapshot_gate(harness, system),
            system,
        )

    progress = json.loads(
        (tmp_path / 'raw/benchmark_cell_headroom_progress.json').read_text()
    )
    assert progress['status'] == 'failed'
    assert progress['ambient_cpu_attempts'] == [probe]
    assert 'quiet-window gate failed' in progress['error']


def test_headroom_persists_nonambient_load_failure(tmp_path, monkeypatch) -> None:
    harness = HarnessConfig(
        server_cpus=(0, 1, 2, 3),
        load_cpus=(4, 5, 6, 7),
        management_cpus=(8,),
    )
    system = _publication_system(harness)

    @contextmanager
    def fake_server(*_args, **_kwargs):
        yield {
            'command': [],
            'log': '',
            'worker_pids': [],
            'process_group_id': 77,
        }

    monkeypatch.setattr(
        'bench.bench._capture_ambient_cpu', lambda *_args: _quiet_probe()
    )
    monkeypatch.setattr('bench.bench.running_server', fake_server)
    monkeypatch.setattr(
        'bench.bench.validate_response_contract', lambda *_args: {'status': 200}
    )
    monkeypatch.setattr('bench.bench.time.sleep', lambda _seconds: None)
    monkeypatch.setattr('bench.bench.ProcessGroupResourceSampler', _StubResourceSampler)
    monkeypatch.setattr(
        'bench.bench._interference_monitor', lambda *_args: _StubInterferenceMonitor()
    )
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
            _snapshot_gate(harness, system),
            system,
        )

    progress = json.loads(
        (tmp_path / 'raw/benchmark_cell_headroom_progress.json').read_text()
    )
    assert progress['status'] == 'failed'
    assert progress['error'] == 'BenchmarkError: load failed'


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
