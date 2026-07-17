import argparse
import hashlib
import importlib
import subprocess
import sys
from itertools import pairwise
from pathlib import Path
from typing import Any

import pytest

from bench import provenance, system

compare = importlib.import_module('bench.compare')
mask_compare = importlib.import_module('bench.mask_kernel_compare')
blocked_orders = compare.blocked_orders
paired_comparison = compare.paired_comparison
parse_args = compare.parse_args
parse_cpu_set = compare.parse_cpu_set
parse_named_command = compare.parse_named_command


def test_blocked_orders_are_reproducible_balanced_and_interleaved():
    first = blocked_orders(10, 41)
    assert first == blocked_orders(10, 41)
    assert all(set(order) == {'control', 'candidate'} for order in first)
    assert all(left[0] != right[0] for left, right in pairwise(first))
    control_leads = sum(order[0] == 'control' for order in first)
    assert control_leads == 5


def test_paired_comparison_detects_stable_throughput_improvement():
    result = paired_comparison(
        [100.0, 102.0, 98.0, 101.0, 99.0],
        [110.0, 112.2, 107.8, 111.1, 108.9],
        seed=7,
        higher_is_better=True,
        bootstrap_samples=1_000,
    )

    assert result['paired_delta_median_percent'] == pytest.approx(10.0)
    assert result['paired_delta_ci95_percent'][0] > 0.0
    assert result['improvement_median_percent'] == pytest.approx(10.0)
    assert result['directionally_stable_above_iqr'] is True


def test_paired_comparison_marks_effect_inside_noise_as_inconclusive():
    result = paired_comparison(
        [100.0, 100.0, 100.0, 100.0, 100.0],
        [98.0, 102.0, 99.0, 101.0, 100.0],
        seed=11,
        higher_is_better=False,
        bootstrap_samples=1_000,
    )

    assert result['paired_delta_ci95_percent'][0] <= 0.0
    assert result['paired_delta_ci95_percent'][1] >= 0.0
    assert result['directionally_stable_above_iqr'] is False


def test_named_command_and_cpu_set_parsing():
    command = parse_named_command(
        'baseline=python -m h2corn "bench.bench_app:app" --flag="two words"'
    )
    assert command.name == 'baseline'
    assert command.argv == (
        'python',
        '-m',
        'h2corn',
        'bench.bench_app:app',
        '--flag=two words',
    )
    assert parse_cpu_set('1,3-5,4') == (1, 3, 4, 5)
    with pytest.raises(argparse.ArgumentTypeError):
        parse_named_command('bad name=server')
    with pytest.raises(argparse.ArgumentTypeError):
        parse_cpu_set('3-1')


def test_cli_parsing_applies_reproducible_defaults(tmp_path):
    output = tmp_path / 'evidence.json'
    args = parse_args([
        '--control',
        'old=old-server --port 8000',
        '--candidate',
        'new=new-server --port 8000',
        '--duration',
        '250ms',
        '--server-cpus',
        '2',
        '--load-cpus',
        '4-5',
        '--management-cpus',
        '0',
        '--expected-body',
        'Hello, World!',
        '--output',
        str(output),
    ])

    assert args.control.name == 'old'
    assert args.candidate.name == 'new'
    assert args.duration_seconds == pytest.approx(0.25)
    assert args.trials == 10
    assert args.server_cpus == (2,)
    assert args.load_cpus == (4, 5)
    assert args.management_cpus == (0,)
    assert args.load_warmup_duration == '1s'
    assert args.ambient_cpu_probe_seconds == 1.0
    assert args.max_ambient_cpu_utilization == 0.1
    assert args.max_ambient_single_cpu_utilization == 0.15
    assert args.allow_variant_environment_drift is False
    assert args.ready_url == args.url
    assert args.output == output


def test_relaxed_noisy_host_limits_are_explicitly_diagnostic():
    compare_args = parse_args([
        '--control',
        'old=old-server',
        '--candidate',
        'new=new-server',
        '--expected-body',
        'ok',
        '--max-ambient-cpu-utilization',
        '2',
        '--max-ambient-single-cpu-utilization',
        '1',
    ])
    mask_args = mask_compare.parse_args([
        '--cpu',
        '2',
        '--max-ambient-cpu-utilization',
        '2',
        '--max-ambient-single-cpu-utilization',
        '1',
    ])

    assert compare_args.max_ambient_cpu_utilization == 2.0
    assert compare_args.max_ambient_single_cpu_utilization == 1.0
    assert (
        compare.host_noise_mode(
            (2,),
            compare_args.max_ambient_cpu_utilization,
            compare_args.max_ambient_single_cpu_utilization,
        )
        == 'diagnostic-pinned-noisy'
    )
    assert mask_args.max_ambient_cpu_utilization == 2.0
    assert mask_args.max_ambient_single_cpu_utilization == 1.0


def test_cli_rejects_too_few_trials():
    with pytest.raises(SystemExit):
        parse_args([
            '--control',
            'old=old-server',
            '--candidate',
            'new=new-server',
            '--trials',
            '4',
        ])

    with pytest.raises(SystemExit):
        parse_args([
            '--control',
            'old=old-server',
            '--candidate',
            'new=new-server',
            '--trials',
            '7',
        ])


def test_cli_rejects_partial_or_overlapping_cpu_roles():
    common = [
        '--control',
        'old=old-server',
        '--candidate',
        'new=new-server',
        '--expected-body',
        'Hello, World!',
    ]
    with pytest.raises(SystemExit):
        parse_args([*common, '--server-cpus', '2', '--load-cpus', '4'])
    with pytest.raises(SystemExit):
        parse_args([
            *common,
            '--server-cpus',
            '2',
            '--load-cpus',
            '4',
            '--management-cpus',
            '2',
        ])


def test_cli_rejects_http2_with_disabled_keepalive():
    with pytest.raises(SystemExit):
        parse_args([
            '--control',
            'old=old-server',
            '--candidate',
            'new=new-server',
            '--http2',
            '--disable-keepalive',
        ])


def test_cli_rejects_disabled_keepalive_with_k6():
    with pytest.raises(SystemExit):
        parse_args([
            '--control',
            'old=old-server',
            '--candidate',
            'new=new-server',
            '--load-driver',
            'k6',
            '--disable-keepalive',
        ])


def test_k6_metrics_are_converted_to_canonical_seconds():
    metrics = compare._extract_k6_metrics({
        'metrics': {
            'iterations': {'values': {'rate': 1200.0}},
            'ws_session_duration': {
                'values': {'med': 1.5, 'p(99)': 4.0, 'p(99.9)': 8.5}
            },
        }
    })

    assert metrics == {
        'requests_per_second': 1200.0,
        'latency_p50_seconds': pytest.approx(0.0015),
        'latency_p99_seconds': pytest.approx(0.004),
        'latency_p99_9_seconds': pytest.approx(0.0085),
    }


def test_current_k6_flat_summary_is_converted_to_canonical_seconds():
    metrics = compare._extract_k6_metrics({
        'metrics': {
            'iterations': {'count': 200, 'rate': 123.0},
            'ws_session_duration': {
                'med': 1.25,
                'p(99)': 3.5,
                'p(99.9)': 7.75,
            },
        }
    })

    assert metrics == {
        'requests_per_second': 123.0,
        'latency_p50_seconds': pytest.approx(0.00125),
        'latency_p99_seconds': pytest.approx(0.0035),
        'latency_p99_9_seconds': pytest.approx(0.00775),
    }


def test_http_response_contract_is_exact_before_and_after(monkeypatch):
    body = b'expected payload'
    args = argparse.Namespace(
        expected_body_sha256=hashlib.sha256(body).hexdigest(),
        expected_body_size=len(body),
        expected_status=201,
        url='http://127.0.0.1:8000/value',
        method='POST',
        body='request',
        header=['Content-Type: text/plain'],
        unix_socket=None,
        insecure=False,
        http2=False,
    )
    monkeypatch.setattr(
        compare,
        '_fetch_http_response',
        lambda *_args, **_kwargs: (201, body, '1.1'),
    )

    assert compare._expected_response(args) == {
        'status': 201,
        'body_size': len(body),
        'body_sha256': hashlib.sha256(body).hexdigest(),
        'protocol': '1.1',
    }

    monkeypatch.setattr(
        compare,
        '_fetch_http_response',
        lambda *_args, **_kwargs: (201, body + b'!', '1.1'),
    )
    with pytest.raises(compare.BenchmarkError, match='response contract failed'):
        compare._expected_response(args)


def test_http2_response_contract_uses_protocol_aware_probe(monkeypatch):
    body = b'h2 payload'
    args = argparse.Namespace(
        expected_body_sha256=hashlib.sha256(body).hexdigest(),
        expected_body_size=len(body),
        expected_status=200,
        url='https://127.0.0.1:8443/value',
        method='GET',
        body=None,
        header=[],
        unix_socket=None,
        insecure=True,
        http2=True,
    )
    calls = []
    monkeypatch.setattr(
        compare,
        '_fetch_h2_response',
        lambda *positional, **keyword: (
            calls.append((positional, keyword)) or (200, body, '2')
        ),
    )

    assert compare._expected_response(args)['protocol'] == '2'
    assert calls and calls[0][0] == (args.url,)

    monkeypatch.setattr(
        compare,
        '_fetch_h2_response',
        lambda *_args, **_kwargs: (200, body, '1.1'),
    )
    with pytest.raises(compare.BenchmarkError, match='expected_protocol=2'):
        compare._expected_response(args)


def test_http2_probe_streams_request_body_across_flow_control_windows():
    class FakeH2Connection:
        max_outbound_frame_size = 16_384

        def __init__(self):
            self.window = 65_535
            self.frames = []

        def local_flow_control_window(self, _stream_id):
            return self.window

        def send_data(self, stream_id, data, *, end_stream):
            self.window -= len(data)
            self.frames.append((stream_id, len(data), end_stream))

    connection = FakeH2Connection()
    payload = b'x' * 65_536

    offset = compare._queue_h2_request_body(
        connection,
        1,
        payload,
        offset=0,
    )

    assert offset == 65_535
    assert [frame[1] for frame in connection.frames] == [
        16_384,
        16_384,
        16_384,
        16_383,
    ]
    assert all(not frame[2] for frame in connection.frames)

    connection.window = 1
    offset = compare._queue_h2_request_body(
        connection,
        1,
        payload,
        offset=offset,
    )

    assert offset == len(payload)
    assert connection.frames[-1] == (1, 1, True)


def test_worker_convergence_requires_exact_distinct_pid_count(monkeypatch):
    class Process:
        returncode = None

        @staticmethod
        def poll():
            return None

    responses = iter([
        (200, b'12', '1.1'),
        (200, b'12', '1.1'),
        (200, b'10', '1.1'),
        (200, b'11', '1.1'),
    ])
    monkeypatch.setattr(
        compare, '_fetch_http_response', lambda *_args, **_kwargs: next(responses)
    )
    args = argparse.Namespace(
        startup_timeout=1.0,
        expected_workers=3,
        ready_url='http://127.0.0.1:8000/',
        ready_unix_socket=None,
        insecure=False,
    )

    assert compare._wait_for_workers(Process(), args) == [10, 11, 12]


def test_process_group_is_owned_before_monitor_start(monkeypatch):
    process = subprocess.Popen(
        [sys.executable, '-c', 'import time; time.sleep(60)'],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )

    def fail_start(_monitor):
        raise RuntimeError('monitor start failed')

    monkeypatch.setattr(system.ProcessGroupResourceSampler, 'start', fail_start)
    try:
        with (
            pytest.raises(RuntimeError, match='monitor start failed'),
            compare._owned_process_group(process, 0.01),
        ):
            pass
        assert process.poll() is not None
    finally:
        system.terminate_process_group(process)


def test_load_generator_resources_gate_aggregate_cpu_headroom(monkeypatch):
    args = argparse.Namespace(load_cpus=(8, 9, 24, 25), max_load_utilization=0.85)
    monkeypatch.setattr('bench.compare.physical_core_capacity', lambda _cpus: 2)
    resources = compare._load_generator_resources(
        args,
        {
            'elapsed_seconds': 2.0,
            'cpu_seconds': 3.6,
            'average_cpu_cores': 1.8,
            'peak_rss_bytes': 100,
            'peak_process_count': 2,
            'sample_count': 20,
            'sampling_interval_seconds': 0.05,
        },
    )

    assert resources['logical_cpu_utilization'] == pytest.approx(0.45)
    assert resources['physical_core_utilization'] == pytest.approx(0.9)
    assert resources['sufficient_headroom'] is False


def _ambient_cpu_probe(maximum_cpu_utilization=0.01):
    return {
        'started_at': 'start',
        'completed_at': 'end',
        'elapsed_seconds': 1.0,
        'system_physical_core_utilization': 0.01,
        'server_physical_core_utilization': 0.01,
        'server_llc_physical_core_utilization': 0.01,
        'load_llc_physical_core_utilization': 0.01,
        'maximum_cpu_utilization': maximum_cpu_utilization,
        'per_cpu': [],
    }


def _cpu_activity(maximum_window_cpu_utilization=0.01):
    return {
        'started_at': 'start',
        'completed_at': 'end',
        'elapsed_seconds': 1.0,
        'cpus': [1],
        'physical_core_count': 1,
        'total_ticks': 100,
        'active_ticks': 1,
        'physical_core_utilization': 0.01,
        'maximum_cpu_utilization': 0.01,
        'per_cpu': [],
        'interval_seconds': 1.0,
        'windows': [],
        'maximum_raw_window_physical_core_utilization': 0.01,
        'maximum_raw_window_cpu_utilization': maximum_window_cpu_utilization,
        'maximum_window_physical_core_utilization': 0.01,
        'maximum_window_cpu_utilization': maximum_window_cpu_utilization,
    }


def _noise_gate_args():
    return argparse.Namespace(
        server_cpus=(2,),
        load_cpus=(8, 24),
        management_cpus=(0,),
        ambient_cpu_probe_seconds=1.0,
        max_ambient_cpu_utilization=0.1,
        max_ambient_single_cpu_utilization=0.15,
    )


def test_pinned_load_retains_preload_and_during_load_cpu_evidence(monkeypatch):
    probes = iter([_ambient_cpu_probe(), _ambient_cpu_probe()])
    activity = _cpu_activity()
    events = []

    class Monitor:
        def start(self):
            events.append('monitor-start')

        def stop(self):
            events.append('monitor-stop')
            return activity

    load = {'raw': {}, 'resources': {'sufficient_headroom': True}}
    monkeypatch.setattr(
        compare,
        '_capture_ambient_cpu',
        lambda *_args: events.append('probe') or next(probes),
    )
    monkeypatch.setattr(
        compare,
        '_interference_monitor',
        lambda *_args: events.append('monitor-create') or Monitor(),
    )
    monkeypatch.setattr(
        compare,
        '_run_load',
        lambda _args: events.append('load') or (load, {'requests_per_second': 1.0}),
    )
    evidence = {
        'mode': 'pinned-noise-gated',
        'ambient_cpu_attempts': [],
        'ambient_cpu_before': None,
        'interference_cpu_during': None,
    }

    result = compare._run_noise_gated_load(
        _noise_gate_args(),
        {},
        evidence,
        lambda: events.append('checkpoint'),
    )

    assert result == (load, {'requests_per_second': 1.0})
    assert evidence['ambient_cpu_attempts'] == [
        _ambient_cpu_probe(),
        _ambient_cpu_probe(),
    ]
    assert evidence['ambient_cpu_before'] == _ambient_cpu_probe()
    assert evidence['interference_cpu_during'] == activity
    assert events == [
        'probe',
        'checkpoint',
        'probe',
        'monitor-create',
        'monitor-start',
        'load',
        'monitor-stop',
        'checkpoint',
    ]


def test_pinned_load_persists_busy_preload_probe_before_rejecting(monkeypatch):
    busy = _ambient_cpu_probe(maximum_cpu_utilization=0.2)
    monkeypatch.setattr(compare, '_capture_ambient_cpu', lambda *_args: busy)
    evidence = {
        'mode': 'pinned-noise-gated',
        'ambient_cpu_attempts': [],
        'ambient_cpu_before': None,
        'interference_cpu_during': None,
    }
    checkpoints = []

    with pytest.raises(compare.BenchmarkError, match='quiet-window gate failed'):
        compare._run_noise_gated_load(
            _noise_gate_args(), {}, evidence, lambda: checkpoints.append(True)
        )

    assert evidence['ambient_cpu_attempts'] == [busy]
    assert checkpoints == [True]


def test_interference_gate_rejects_fixed_window_cpu_spike():
    with pytest.raises(compare.BenchmarkError, match='interference gate failed'):
        compare._validate_interference_cpu(_cpu_activity(0.2), _noise_gate_args())


def test_ambient_probe_covers_online_host_and_role_cache_domains(monkeypatch):
    calls = []
    server = {
        'physical_core_count': 1,
        'topology': [
            {
                'thread_siblings': [2, 18],
                'last_level_cache': {'shared_cpus': [0, 1, 2, 16, 17, 18]},
            }
        ],
    }
    load = {
        'physical_core_count': 1,
        'topology': [
            {
                'thread_siblings': [8, 24],
                'last_level_cache': {'shared_cpus': [8, 9, 24, 25]},
            }
        ],
    }
    host_state: Any = {
        'online_cpus': [0, 1, 2, 8, 16, 17, 18, 24],
        'online_physical_core_count': 4,
        'server': server,
        'load_generator': load,
    }
    monkeypatch.setattr(
        system,
        'physical_core_capacity',
        lambda cpus: calls.append(('capacity', cpus)) or 2,
    )
    expected = _ambient_cpu_probe()
    monkeypatch.setattr(
        system,
        'capture_ambient_cpu_probe',
        lambda *positional, **keyword: (
            calls.append(('probe', positional, keyword)) or expected
        ),
    )

    assert system.capture_role_ambient_cpu(host_state, 1.0) is expected
    assert calls == [
        ('capacity', (0, 1, 2, 16, 17, 18)),
        ('capacity', (8, 24)),
        (
            'probe',
            (
                (0, 1, 2, 8, 16, 17, 18, 24),
                (2, 18),
                (0, 1, 2, 16, 17, 18),
                (8, 24),
            ),
            {
                'online_physical_core_count': 4,
                'server_physical_core_count': 1,
                'server_llc_physical_core_count': 2,
                'load_llc_physical_core_count': 2,
                'duration_seconds': 1.0,
            },
        ),
    ]


def test_interference_monitor_covers_every_unassigned_online_cpu(monkeypatch):
    captured = []
    monkeypatch.setattr(
        compare,
        'physical_core_capacity',
        lambda cpus: captured.append(('capacity', cpus)) or 2,
    )
    monkeypatch.setattr(
        compare,
        'CpuActivityMonitor',
        lambda cpus, cores: captured.append(('monitor', cpus, cores)) or object(),
    )
    system = {
        'online_cpus': [0, 1, 2, 8, 16, 18, 24, 31],
        'cgroup_allowed_cpus': [0, 1, 2, 8, 16, 18, 24],
    }

    monitor = compare._interference_monitor(_noise_gate_args(), system)

    assert monitor is not None
    assert captured == [
        ('capacity', (1, 16, 18, 31)),
        ('monitor', (1, 16, 18, 31), 2),
    ]


def test_unpinned_summary_is_explicitly_diagnostic(monkeypatch):
    monkeypatch.setattr(
        compare,
        '_run_load',
        lambda _args: ({'raw': {}}, {'requests_per_second': 1.0}),
    )
    evidence = {
        'mode': 'diagnostic-unpinned',
        'ambient_cpu_attempts': [],
        'ambient_cpu_before': None,
        'interference_cpu_during': None,
    }
    assert compare._run_noise_gated_load(object(), {}, evidence) == (
        {'raw': {}},
        {'requests_per_second': 1.0},
    )
    trials = [
        {
            'runs': {
                'control': {'metrics': {'requests_per_second': 100.0}},
                'candidate': {'metrics': {'requests_per_second': 110.0}},
            }
        }
        for _ in range(6)
    ]
    summary = compare.summarize_trials(trials, 7, 1_000, 'diagnostic-unpinned')

    assert summary['requests_per_second']['verdict'] == 'DIAGNOSTIC_UNPINNED'
    assert summary['requests_per_second']['directionally_stable_above_iqr'] is True

    confounded = compare.summarize_trials(
        trials,
        7,
        1_000,
        'pinned-noise-gated',
        'confounded-opt-out',
    )
    assert confounded['requests_per_second']['verdict'] == (
        'DIAGNOSTIC_VARIANT_ENVIRONMENT_DRIFT'
    )

    noisy = compare.summarize_trials(
        trials,
        7,
        1_000,
        'diagnostic-pinned-noisy',
    )
    assert noisy['requests_per_second']['verdict'] == 'DIAGNOSTIC_PINNED_NOISY'


def test_comparison_identity_freezes_additional_inputs(tmp_path):
    identity_input = tmp_path / 'scenario.toml'
    identity_input.write_text('value = 1\n')
    args = parse_args([
        '--control',
        'old=true',
        '--candidate',
        'new=false',
        '--expected-body',
        'Hello, World!',
        '--trials',
        '6',
        '--identity-input',
        str(identity_input),
    ])
    frozen = compare.comparison_identity(args)
    gate = compare.ComparisonIdentityGate(args, frozen)

    # Untouched inputs pass on the cheap fingerprint alone.
    gate.verify('test')

    identity_input.write_text('value = 2\n')
    with pytest.raises(
        compare.BenchmarkError, match='frozen benchmark identity changed'
    ):
        gate.verify('test')
    with pytest.raises(
        compare.BenchmarkError, match='frozen benchmark identity changed'
    ):
        compare._verify_comparison_identity(args, frozen, phase='test')


def _cpu_topology(cpu, core, siblings, *, online=True):
    return {
        'cpu': cpu,
        'online': online,
        'physical_package_id': 0,
        'die_id': 0,
        'core_id': core,
        'thread_siblings': siblings,
        'frequency': {
            'scaling_governor': 'performance',
            'scaling_driver': 'amd-pstate-epp',
            'energy_performance_preference': 'performance',
        },
    }


def _cpu_set(cpus, topology):
    return {
        'cpus': cpus,
        'logical_cpu_count': len(cpus),
        'physical_core_count': len({entry['core_id'] for entry in topology}),
        'topology': topology,
    }


def _pinned_system_state():
    server = _cpu_set([2], [_cpu_topology(2, 2, [2, 18])])
    load = _cpu_set(
        [8, 24],
        [_cpu_topology(8, 8, [8, 24]), _cpu_topology(24, 8, [8, 24])],
    )
    management = _cpu_set([0], [_cpu_topology(0, 0, [0, 16])])
    return {
        'online_cpus': [0, 2, 8, 16, 18, 24],
        'allowed_cpus': [0],
        'cgroup_allowed_cpus': [0, 2, 8, 16, 18, 24],
        'thread_affinity_masks': [{'cpus': [0], 'thread_count': 7}],
        'server': server,
        'load_generator': load,
        'management': management,
    }


def test_cpu_role_validation_proves_thread_and_physical_isolation():
    args = argparse.Namespace(server_cpus=(2,), load_cpus=(8, 24), management_cpus=(0,))
    state = _pinned_system_state()
    compare._validate_benchmark_system(args, state)

    state['thread_affinity_masks'] = [{'cpus': [0, 1], 'thread_count': 1}]
    with pytest.raises(compare.BenchmarkError, match='every benchmark-driver thread'):
        compare._validate_benchmark_system(args, state)


def test_cpu_role_validation_rejects_physical_overlap_and_incomplete_load_smt():
    args = argparse.Namespace(server_cpus=(2,), load_cpus=(8, 24), management_cpus=(0,))
    state = _pinned_system_state()
    state['load_generator']['topology'][0]['core_id'] = 2
    with pytest.raises(compare.BenchmarkError, match='physical cores'):
        compare._validate_benchmark_system(args, state)

    state = _pinned_system_state()
    args.load_cpus = (8,)
    state['load_generator'] = _cpu_set([8], [_cpu_topology(8, 8, [8, 24])])
    with pytest.raises(compare.BenchmarkError, match='every usable SMT sibling'):
        compare._validate_benchmark_system(args, state)


def test_mask_host_validation_requires_performance_policy():
    state = _pinned_system_state()
    mask_compare._validate_pinned_host(state, 2, 0)

    state['server']['topology'][0]['frequency']['scaling_governor'] = 'powersave'
    with pytest.raises(RuntimeError, match='performance governor'):
        mask_compare._validate_pinned_host(state, 2, 0)


def test_fresh_measured_server_warmup_uses_exact_load_path(monkeypatch):
    response = {
        'status': 200,
        'body_size': 13,
        'body_sha256': '0' * 64,
        'protocol': '1.1',
    }
    load = {
        'command': ['oha'],
        'elapsed_seconds': 1.0,
        'exit_code': 0,
        'resources': {'sufficient_headroom': True},
        'raw': {'summary': {'successRate': 1}},
    }
    calls = []
    monkeypatch.setattr(
        compare,
        '_run_load',
        lambda _args, **kwargs: calls.append(kwargs) or (load, {'rps': 1.0}),
    )
    monkeypatch.setattr(compare, '_expected_response', lambda _args: response)
    monkeypatch.setattr(compare, '_wait_for_workers', lambda *_args: [10, 11])

    class Process:
        returncode = None

        @staticmethod
        def poll():
            return None

    evidence = compare._warm_fresh_server(
        Process(),
        argparse.Namespace(
            load_warmup_duration='750ms', load_warmup_duration_seconds=0.75
        ),
        [10, 11],
    )

    assert calls == [{'duration': '750ms', 'duration_seconds': 0.75}]
    assert evidence == {
        'duration': '750ms',
        'duration_seconds': 0.75,
        'response': response,
        'worker_pids': [10, 11],
        'load': load,
    }


def test_k6_rejects_unix_socket_loads(tmp_path):
    with pytest.raises(SystemExit):
        parse_args([
            '--control',
            'old=old-server',
            '--candidate',
            'new=new-server',
            '--load-driver',
            'k6',
            '--unix-socket',
            str(tmp_path / 'server.sock'),
        ])


def test_k6_derives_http_readiness_from_standalone_websocket_url():
    args = parse_args([
        '--control',
        'old=old-server',
        '--candidate',
        'new=new-server',
        '--load-driver',
        'k6',
        '--url',
        'wss://benchmark.test:8443/ws?ignored=yes',
    ])

    assert args.ready_url == 'https://benchmark.test:8443/'


def test_variant_provenance_hashes_resolved_executable():
    command = provenance.NamedCommand('true', ('true',))
    artifacts = provenance.variant_artifacts(command)

    assert artifacts['executable'] is not None
    assert Path(artifacts['executable']).is_file()
    assert artifacts['executable_sha256'] is not None
    assert len(artifacts['executable_sha256']) == 64
    assert artifacts['extension'] is None


def _python_variant_artifacts(
    *, dependency_version='1.0', dependency_sha='a' * 64, package='control'
) -> provenance.VariantArtifacts:
    return {
        'executable': f'/benchmark/{package}/bin/h2corn',
        'executable_sha256': package * 8,
        'extension': f'/benchmark/{package}/h2corn/_lib.so',
        'extension_sha256': package * 8,
        'python_executable': f'/benchmark/{package}/bin/python',
        'python_executable_sha256': 'b' * 64,
        'python_version': '3.13.5 (main, Jul 1 2026) [GCC 15.1.0]',
        'python_implementation': 'cpython',
        'python_cache_tag': 'cpython-313',
        'python_abi_flags': '',
        'python_gil_enabled': True,
        'python_package': f'/benchmark/{package}/h2corn',
        'python_package_sha256': package * 8,
        'dependencies': {
            'h11': {
                'distribution': 'h11',
                'version': dependency_version,
                'path': f'/benchmark/{package}/h11',
                'sha256': dependency_sha,
            }
        },
        'command_inputs': {},
    }


def test_variant_environment_ignores_expected_paths_and_h2corn_artifacts():
    control = _python_variant_artifacts(package='control')
    candidate = _python_variant_artifacts(package='candidate')

    evidence = provenance.variant_environment_evidence(
        control, candidate, allow_drift=False
    )

    assert evidence == {'mode': 'equivalent', 'differences': []}


def test_variant_environment_drift_requires_explicit_diagnostic_opt_out():
    control = _python_variant_artifacts()
    candidate = _python_variant_artifacts(
        dependency_version='2.0', dependency_sha='c' * 64, package='candidate'
    )

    with pytest.raises(compare.BenchmarkError, match='equivalent Python runtime'):
        provenance.variant_environment_evidence(control, candidate, allow_drift=False)

    evidence = provenance.variant_environment_evidence(
        control, candidate, allow_drift=True
    )
    assert evidence['mode'] == 'confounded-opt-out'
    assert {difference['field'] for difference in evidence['differences']} == {
        'dependencies.h11.version',
        'dependencies.h11.sha256',
    }


def test_subprocess_environment_fixes_python_hash_seed(monkeypatch):
    monkeypatch.setenv('PYTHONHASHSEED', 'random')
    monkeypatch.setenv('NO_COLOR', '1')

    environment = provenance.subprocess_environment()

    assert environment['PYTHONHASHSEED'] == '0'
    assert 'NO_COLOR' not in environment


def test_mask_comparison_discards_warmups_and_balances_retained_leads(
    monkeypatch, tmp_path
):
    host = mask_compare.MaskBenchmarkHost(
        mode='diagnostic-unmanaged',
        measurement_cpu=2,
        management_cpu=None,
        system={},
        measurement_physical_cpus=(2, 18),
        measurement_llc_cpus=(0, 1, 2, 16, 17, 18),
        interference_cpus=(),
        interference_physical_core_count=0,
    )
    monkeypatch.setattr(mask_compare, '_verify_host_identity', lambda _host: {})

    def fake_measure(_binary, kernel, _cpu, _workload, _chunk_size):
        value = 10.0 if kernel == 'legacy' else 9.0
        return dict.fromkeys(mask_compare.LENGTH_WEIGHTS, value)

    monkeypatch.setattr(mask_compare, 'measure', fake_measure)
    result = mask_compare.compare_kernels(
        tmp_path / 'unused',
        control='legacy',
        candidate='production',
        workload='cartesian',
        chunk_size=0,
        host=host,
        trials=6,
        warmups=2,
        seed=17,
        ambient_cpu_probe_seconds=1.0,
        maximum_ambient_cpu_utilization=0.1,
        maximum_ambient_single_cpu_utilization=0.15,
    )

    assert [block['retained'] for block in result['blocks']] == [
        False,
        False,
        True,
        True,
        True,
        True,
        True,
        True,
    ]
    retained = result['blocks'][2:]
    assert sum(block['lead_order'][0] == 'legacy' for block in retained) == 3
    assert len(result['weighted']['control_samples']) == 6
    assert result['weighted']['improvement_median_percent'] == pytest.approx(10.0)


def test_mask_comparison_defaults_to_the_replaced_eager_key_kernel():
    args = mask_compare.parse_args(['--cpu', '2'])

    assert args.control_kernel == 'eager-key'


def test_mask_measurement_cpu_is_required():
    with pytest.raises(SystemExit):
        mask_compare.parse_args([])


def test_mask_summary_retains_the_exact_noise_gate_configuration():
    paired = paired_comparison(
        [10.0, 10.0],
        [9.0, 9.0],
        seed=1,
        higher_is_better=False,
        bootstrap_samples=100,
    )
    report = {
        'schema_version': mask_compare.SCHEMA_VERSION,
        'evidence_mode': 'pinned-noise-gated',
        'cpu': 2,
        'management_cpu': 0,
        'trials': 6,
        'warmups': 2,
        'seed': 17,
        'weights': {'1': 1.0},
        'ambient_cpu_probe_seconds': 1.25,
        'maximum_ambient_cpu_utilization': 0.08,
        'maximum_ambient_single_cpu_utilization': 0.12,
        'host': {},
        'provenance': {},
        'comparison': {
            'control': 'eager-key',
            'candidate': 'production',
            'workload': 'phase0',
            'chunk_size': 0,
            'lead_orders': [],
            'blocks': [],
            'weighted': paired,
            'cells': {'1': paired},
        },
    }

    summary = mask_compare._summary(report)

    assert mask_compare.SCHEMA_VERSION == 3
    assert summary['ambient_cpu_probe_seconds'] == 1.25
    assert summary['maximum_ambient_cpu_utilization'] == 0.08
    assert summary['maximum_ambient_single_cpu_utilization'] == 0.12
    assert summary['comparison']['control'] == 'eager-key'


def test_relaxed_mask_noise_limits_are_diagnostic_but_still_collect_probes(
    monkeypatch, tmp_path
):
    assert mask_compare.evidence_mode(0, 0.1, 0.15) == 'pinned-noise-gated'
    assert mask_compare.evidence_mode(0, 0.11, 0.15) == 'diagnostic-pinned-noisy'
    assert mask_compare.evidence_mode(0, 0.1, 0.16) == 'diagnostic-pinned-noisy'
    assert mask_compare.evidence_mode(None, 0.1, 0.15) == 'diagnostic-unmanaged'

    host = mask_compare.MaskBenchmarkHost(
        mode='diagnostic-pinned-noisy',
        measurement_cpu=2,
        management_cpu=0,
        system={},
        measurement_physical_cpus=(2, 18),
        measurement_llc_cpus=(0, 1, 2, 16, 17, 18),
        interference_cpus=(1, 16, 17, 18),
        interference_physical_core_count=2,
    )
    probe = {'retained': True}
    validated = []
    measured = []
    monkeypatch.setattr(mask_compare, '_verify_host_identity', lambda _host: {})
    monkeypatch.setattr(
        mask_compare,
        '_capture_ambient_cpu',
        lambda _host, _seconds: probe,
    )
    monkeypatch.setattr(
        mask_compare,
        'validate_ambient_cpu',
        lambda value, *, max_aggregate, max_single: validated.append((
            value,
            max_aggregate,
            max_single,
        )),
    )

    def fake_measure(_binary, kernel, *_args, **_kwargs):
        measured.append(kernel)
        return {
            'kernel': kernel,
            'cells': dict.fromkeys(mask_compare.LENGTH_WEIGHTS, 10.0),
            'interference_cpu_during': {'retained': True},
        }

    monkeypatch.setattr(mask_compare, '_measure_with_host_evidence', fake_measure)
    result = mask_compare.compare_kernels(
        tmp_path / 'unused',
        control='eager-key',
        candidate='production',
        workload='cartesian',
        chunk_size=0,
        host=host,
        trials=6,
        warmups=1,
        seed=17,
        ambient_cpu_probe_seconds=1.0,
        maximum_ambient_cpu_utilization=2.0,
        maximum_ambient_single_cpu_utilization=1.0,
    )

    assert all(block['ambient_cpu_before'] is probe for block in result['blocks'])
    assert validated == [(probe, 2.0, 1.0)] * 7
    assert len(measured) == 14


def test_shebang_probe_preserves_virtual_environment_python_symlink(tmp_path):
    interpreter = tmp_path / 'python3'
    interpreter.symlink_to(sys.executable)
    command_path = tmp_path / 'server'
    command_path.write_text(f'#!{interpreter}\n')
    command_path.chmod(0o755)
    command = provenance.NamedCommand('server', (str(command_path),))

    executable = provenance.resolved_executable(command)
    assert executable is not None
    assert provenance.command_python(command, executable) == interpreter
