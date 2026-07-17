import argparse
import hashlib
import importlib
import subprocess
import sys
from itertools import pairwise

import pytest

from bench import system

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
    assert args.ready_url == args.url
    assert args.output == output


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


def test_comparison_identity_contains_only_measurement_settings():
    args = parse_args([
        '--control',
        'old=true',
        '--candidate',
        'new=false',
        '--expected-body',
        'Hello, World!',
        '--trials',
        '6',
        '--server-cpus',
        '2',
        '--load-cpus',
        '4-5',
        '--management-cpus',
        '0',
    ])
    identity = compare.comparison_identity(args)

    assert identity['variants']['control'] == {'name': 'old', 'argv': ['true']}
    assert identity['scenario']['load_driver'] == 'oha'
    assert identity['trials'] == 6
    assert identity['cpu_roles'] == {
        'server': [2],
        'load': [4, 5],
        'management': [0],
    }
    assert set(identity) == {
        'variants',
        'scenario',
        'duration',
        'concurrency',
        'trials',
        'warmups',
        'seed',
        'cpu_roles',
        'load_warmup_duration',
        'startup_timeout',
        'load_grace',
        'settle',
        'rss_sample_interval',
        'maximum_load_utilization',
        'bootstrap_samples',
    }


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


def test_subprocess_environment_fixes_python_hash_seed(monkeypatch):
    monkeypatch.setenv('PYTHONHASHSEED', 'random')
    monkeypatch.setenv('NO_COLOR', '1')

    environment = compare.subprocess_environment()

    assert environment['PYTHONHASHSEED'] == '0'
    assert 'NO_COLOR' not in environment


def test_mask_comparison_discards_warmups_and_balances_retained_leads(
    monkeypatch, tmp_path
):
    host = mask_compare.MaskBenchmarkHost(
        measurement_cpu=2,
        management_cpu=None,
        system={},
    )

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


def test_mask_summary_keeps_report_metadata_and_drops_raw_blocks():
    paired = paired_comparison(
        [10.0, 10.0],
        [9.0, 9.0],
        seed=1,
        higher_is_better=False,
        bootstrap_samples=100,
    )
    report = {
        'cpu': 2,
        'management_cpu': 0,
        'trials': 6,
        'warmups': 2,
        'seed': 17,
        'weights': {'1': 1.0},
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

    assert summary['comparison']['control'] == 'eager-key'
