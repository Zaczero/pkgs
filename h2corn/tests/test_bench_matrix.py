import argparse
import hashlib
import importlib
import json

import pytest

compare = importlib.import_module('bench.compare')
matrix = importlib.import_module('bench.matrix')


def _scenarios():
    return matrix.load_manifest(matrix.DEFAULT_MANIFEST)[1]


def test_manifest_boundary_returns_only_validated_typed_data(tmp_path):
    path = tmp_path / 'matrix.toml'
    path.write_text(
        """
schema_version = 1

[defaults]
duration = "2s"
concurrency = 10
workers = [1]
loop_threads = [1]

[[families]]
id = "h2-upload"
description = "typed boundary"
transport = "tcp"
protocol = "h2"
workload = "stream-upload"
path = "/streaming-post-fast"
load_driver = "oha"
method = "POST"
body_size = 65536
default = true
"""
    )

    manifest, scenarios = matrix.load_manifest(path)

    assert manifest == {
        'schema_version': 1,
        'defaults': {
            'duration': '2s',
            'concurrency': 10,
            'workers': [1],
            'loop_threads': [1],
        },
        'families': [
            {
                'id': 'h2-upload',
                'description': 'typed boundary',
                'transport': 'tcp',
                'protocol': 'h2',
                'workload': 'stream-upload',
                'path': '/streaming-post-fast',
                'load_driver': 'oha',
                'method': 'POST',
                'body_size': 65536,
                'default': True,
            }
        ],
    }
    assert len(scenarios) == 1
    assert scenarios[0].body_size == 65536


@pytest.mark.parametrize(
    ('field', 'value', 'message'),
    [
        ('workers', '["1"]', 'workers must be a list of integers'),
        ('concurrency', 'true', 'concurrency must be an integer'),
    ],
)
def test_manifest_boundary_rejects_coercible_family_types(
    tmp_path, field, value, message
):
    path = tmp_path / 'matrix.toml'
    path.write_text(
        f"""
schema_version = 1

[defaults]
duration = "2s"
concurrency = 10
workers = [1]
loop_threads = [1]

[[families]]
id = "unary"
description = "invalid defaults"
transport = "tcp"
protocol = "h1"
workload = "unary"
path = "/"
load_driver = "oha"
{field} = {value}
"""
    )

    with pytest.raises(TypeError, match=message):
        matrix.load_manifest(path)


def test_manifest_boundary_rejects_unknown_family_keys(tmp_path):
    path = tmp_path / 'matrix.toml'
    path.write_text(
        """
schema_version = 1

[defaults]
duration = "2s"
concurrency = 10
workers = [1]
loop_threads = [1]

[[families]]
id = "unary"
description = "unknown key"
transport = "tcp"
protocol = "h1"
workload = "unary"
path = "/"
load_driver = "oha"
concurreny = 10
"""
    )

    with pytest.raises(ValueError, match=r'unsupported keys.*concurreny'):
        matrix.load_manifest(path)


def test_manifest_covers_supported_benchmark_capabilities():
    scenarios = _scenarios()

    assert scenarios
    assert len({scenario.id for scenario in scenarios}) == len(scenarios)
    capabilities = {
        (
            scenario.transport,
            scenario.protocol,
            scenario.workload,
            scenario.workers,
            scenario.loop_threads,
        )
        for scenario in scenarios
    }
    assert ('tcp', 'h1', 'unary', 1, 1) in capabilities
    assert ('tcp', 'h2', 'stream-upload', 1, 1) in capabilities
    assert ('tls', 'h2', 'unary', 1, 1) in capabilities
    assert ('uds', 'h1', 'unary', 1, 1) in capabilities
    assert ('tcp', 'h1', 'websocket', 1, 1) in capabilities
    assert any(workers > 1 for *_, workers, _loop_threads in capabilities)
    assert any(loop_threads > 1 for *_, loop_threads in capabilities)

    upload = next(
        scenario
        for scenario in scenarios
        if scenario.id == 'h2-tcp-stream-upload-w1-l1'
    )
    assert upload.path == '/streaming-post-fast'
    assert upload.body_size == 64 * 1024
    assert matrix._expected_response_body(upload) == b'65536'


def test_default_and_glob_selection_are_intentional():
    scenarios = _scenarios()

    assert [
        scenario.id for scenario in matrix.select_scenarios(scenarios, (), full=False)
    ] == ['h1-tcp-unary-w1-l1']
    selected = matrix.select_scenarios(scenarios, ('h2-tcp-*-w1-l1',), full=False)
    assert {scenario.family for scenario in selected} == {
        'h2-tcp-pathsend',
        'h2-tcp-stream-download',
        'h2-tcp-stream-upload',
        'h2-tcp-unary',
    }


def test_matrix_accepts_full_core_noise_as_an_explicit_retained_limit():
    args = matrix.parse_args([
        '--control',
        'old=old-server',
        '--candidate',
        'new=new-server',
        '--server-cpus',
        '2',
        '--load-cpus',
        '4-5',
        '--management-cpus',
        '0',
        '--max-ambient-cpu-utilization',
        '2',
        '--max-ambient-single-cpu-utilization',
        '1',
    ])

    assert args.max_ambient_cpu_utilization == 2.0
    assert args.max_ambient_single_cpu_utilization == 1.0
    assert (
        compare.host_noise_mode(
            args.server_cpus,
            args.max_ambient_cpu_utilization,
            args.max_ambient_single_cpu_utilization,
        )
        == 'diagnostic-pinned-noisy'
    )


def test_compare_argv_carries_topology_affinity_and_protocol(tmp_path):
    scenario = next(item for item in _scenarios() if item.id == 'h2-tcp-unary-w4-l1')
    args = argparse.Namespace(
        duration=None,
        concurrency=None,
        trials=8,
        warmups=2,
        seed=41,
        server_cpus='2',
        load_cpus='4-5',
        management_cpus='0',
        load_warmup_duration='750ms',
        max_load_utilization=0.8,
        ambient_cpu_probe_seconds=0.75,
        max_ambient_cpu_utilization=0.09,
        max_ambient_single_cpu_utilization=0.14,
        manifest=matrix.DEFAULT_MANIFEST,
    )
    argv = matrix.build_compare_argv(
        scenario,
        compare.NamedCommand('old', ('old-python', '-m', 'h2corn', 'app:app')),
        compare.NamedCommand('new', ('new-python', '-m', 'h2corn', 'app:app')),
        port=18081,
        socket_path=tmp_path / 'server.sock',
        cert=None,
        key=None,
        output=tmp_path / 'result.json',
        args=args,
    )
    parsed = compare.parse_args(argv)

    assert parsed.http2 is True
    assert parsed.server_cpus == (2,)
    assert parsed.load_cpus == (4, 5)
    assert parsed.management_cpus == (0,)
    assert parsed.load_warmup_duration == '750ms'
    assert parsed.expected_workers == 4
    assert parsed.expected_body_size == len(b'Hello, World!')
    assert parsed.expected_body_sha256 == hashlib.sha256(b'Hello, World!').hexdigest()
    assert parsed.max_load_utilization == 0.8
    assert parsed.ambient_cpu_probe_seconds == 0.75
    assert parsed.max_ambient_cpu_utilization == 0.09
    assert parsed.max_ambient_single_cpu_utilization == 0.14
    assert parsed.control.argv[-6:] == (
        '--bind',
        '127.0.0.1:18081',
        '--workers',
        '4',
        '--loop-threads',
        '1',
    )


def test_websocket_matrix_uses_http_readiness_url(tmp_path):
    scenario = next(
        item for item in _scenarios() if item.id == 'h1-tcp-websocket-w1-l1'
    )
    args = argparse.Namespace(
        duration=None,
        concurrency=None,
        trials=8,
        warmups=2,
        seed=41,
        server_cpus=None,
        load_cpus=None,
        management_cpus=None,
        load_warmup_duration='1s',
        max_load_utilization=0.85,
        ambient_cpu_probe_seconds=1.0,
        max_ambient_cpu_utilization=0.1,
        max_ambient_single_cpu_utilization=0.15,
        manifest=matrix.DEFAULT_MANIFEST,
    )
    argv = matrix.build_compare_argv(
        scenario,
        compare.NamedCommand('old', ('old-python', '-m', 'h2corn', 'app:app')),
        compare.NamedCommand('new', ('new-python', '-m', 'h2corn', 'app:app')),
        port=18082,
        socket_path=tmp_path / 'server.sock',
        cert=None,
        key=None,
        output=tmp_path / 'result.json',
        args=args,
    )
    parsed = compare.parse_args(argv)

    assert parsed.url == 'ws://127.0.0.1:18082/ws'
    assert parsed.ready_url == 'http://127.0.0.1:18082/'
    assert parsed.load_driver == 'k6'


def test_gil_capability_is_explicit_not_tied_to_harness(monkeypatch):
    scenario = next(item for item in _scenarios() if item.id == 'h1-tcp-unary-w1-l4')
    monkeypatch.setattr(matrix.shutil, 'which', lambda _name: '/bin/tool')

    assert matrix.unsupported_reason(scenario, gil_enabled=False) is None
    assert 'GIL-enabled' in matrix.unsupported_reason(scenario, gil_enabled=True)


def test_resume_requires_exact_schema_and_comparison_identity(tmp_path):
    path = tmp_path / 'result.json'
    identity = {
        'variants': {'control': 'old', 'candidate': 'new'},
        'duration': '1s',
        'host_noise_mode': 'diagnostic-unpinned',
    }
    path.write_text(
        json.dumps({
            'schema_version': compare.COMPARISON_SCHEMA_VERSION,
            'status': 'complete',
            'comparison_identity': identity,
        })
    )
    assert matrix._is_complete(path, identity) is True
    assert matrix._is_complete(path, {**identity, 'duration': '2s'}) is False
    assert (
        matrix._is_complete(path, {**identity, 'host_noise_mode': 'pinned-noise-gated'})
        is False
    )
    path.write_text(
        json.dumps({
            'schema_version': 4,
            'status': 'complete',
            'comparison_identity': identity,
        })
    )
    assert matrix._is_complete(path, identity) is False


def test_matrix_identity_freezes_cpu_system_state(monkeypatch, tmp_path):
    args = matrix.parse_args([
        '--control',
        'old=old-server',
        '--candidate',
        'new=new-server',
        '--server-cpus',
        '2-5',
        '--load-cpus',
        '8-9,24-25',
        '--management-cpus',
        '0',
        '--output-dir',
        str(tmp_path),
    ])
    state = {'governor': 'performance', 'boost_enabled': True}
    monkeypatch.setattr(matrix, 'capture_system_state', lambda *_args: state)
    monkeypatch.setattr(matrix, 'validate_cpu_roles', lambda *_args: None)
    monkeypatch.setattr(matrix, 'variant_artifacts', lambda command: command.name)
    monkeypatch.setattr(
        matrix,
        'variant_environment_evidence',
        lambda *_args, **_kwargs: {'mode': 'equivalent', 'differences': []},
    )
    monkeypatch.setattr(matrix, 'tool_identity', lambda *_args: {})
    monkeypatch.setattr(matrix, 'git_metadata', dict)

    identity = matrix._matrix_identity(args, _scenarios()[:1], None)

    assert identity['benchmark_system'] is state


def test_tls_identity_is_reused_for_resumable_matrix(tmp_path, monkeypatch):
    tls = tmp_path / 'tls'
    tls.mkdir()
    cert = tls / 'server.pem'
    key = tls / 'server.key'
    cert.write_text('cert')
    key.write_text('key')
    monkeypatch.setattr(
        matrix,
        '_issue_tls_certificate',
        lambda _directory: pytest.fail('existing TLS identity should be reused'),
    )

    assert matrix._tls_certificate(tls) == (cert, key)


def test_dry_run_records_every_selected_scenario(tmp_path, monkeypatch):
    monkeypatch.setattr(matrix, 'unsupported_reason', lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        matrix,
        '_issue_tls_certificate',
        lambda directory: (directory / 'cert.pem', directory / 'key.pem'),
    )
    # The frozen identity hashes the repository state; pin it so concurrent
    # working-tree edits cannot abort the per-scenario identity re-check.
    monkeypatch.setattr(matrix, 'git_metadata', dict)
    monkeypatch.setattr(compare, 'git_metadata', dict)
    output_dir = tmp_path / 'matrix'

    assert (
        matrix.main([
            '--control',
            'old=old-python -m h2corn bench.bench_app:app',
            '--candidate',
            'new=new-python -m h2corn bench.bench_app:app',
            '--full',
            '--dry-run',
            '--runtime-gil',
            'disabled',
            '--output-dir',
            str(output_dir),
        ])
        == 0
    )

    record = json.loads((output_dir / 'matrix.json').read_text())
    identity = record['matrix_identity']
    assert record['status'] == 'dry-run'
    assert identity['settings']['host_noise_mode'] == 'diagnostic-unpinned'
    assert identity['selection']
    assert set(identity['selection']) == {scenario.id for scenario in _scenarios()}
    assert set(record['scenarios']) == set(identity['selection'])
    assert {entry['status'] for entry in record['scenarios'].values()} == {'planned'}
    assert {entry['host_noise_mode'] for entry in record['scenarios'].values()} == {
        'diagnostic-unpinned'
    }
