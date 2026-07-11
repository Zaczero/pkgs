import argparse
import importlib
import json

compare = importlib.import_module('bench.compare')
matrix = importlib.import_module('bench.matrix')


def _scenarios():
    return matrix.load_manifest(matrix.DEFAULT_MANIFEST)[1]


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


def test_compare_argv_carries_topology_affinity_and_protocol(tmp_path):
    scenario = next(item for item in _scenarios() if item.id == 'h2-tcp-unary-w4-l1')
    args = argparse.Namespace(
        duration=None,
        concurrency=None,
        trials=7,
        warmups=2,
        seed=41,
        server_cpus='2',
        load_cpus='4-5',
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
    assert parsed.control.argv[-6:] == (
        '--bind',
        '127.0.0.1:18081',
        '--workers',
        '4',
        '--loop-threads',
        '1',
    )


def test_gil_capability_is_explicit_not_tied_to_harness(monkeypatch):
    scenario = next(item for item in _scenarios() if item.id == 'h1-tcp-unary-w1-l4')
    monkeypatch.setattr(matrix.shutil, 'which', lambda _name: '/bin/tool')

    assert matrix.unsupported_reason(scenario, gil_enabled=False) is None
    assert 'GIL-enabled' in matrix.unsupported_reason(scenario, gil_enabled=True)


def test_resume_requires_exact_schema_and_comparison_identity(tmp_path):
    path = tmp_path / 'result.json'
    identity = {'variants': {'control': 'old', 'candidate': 'new'}, 'duration': '1s'}
    path.write_text(
        json.dumps({
            'schema_version': 2,
            'status': 'complete',
            'comparison_identity': identity,
        })
    )

    assert matrix._is_complete(path, identity) is True
    assert matrix._is_complete(path, {**identity, 'duration': '2s'}) is False
    path.write_text(
        json.dumps({
            'schema_version': 1,
            'status': 'complete',
            'comparison_identity': identity,
        })
    )
    assert matrix._is_complete(path, identity) is False


def test_dry_run_records_every_selected_scenario(tmp_path, monkeypatch):
    monkeypatch.setattr(matrix, 'unsupported_reason', lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        matrix,
        '_issue_tls_certificate',
        lambda directory: (directory / 'cert.pem', directory / 'key.pem'),
    )
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
    assert record['status'] == 'dry-run'
    assert record['selection']
    assert set(record['selection']) == {scenario.id for scenario in _scenarios()}
    assert set(record['scenarios']) == set(record['selection'])
    assert {entry['status'] for entry in record['scenarios'].values()} == {'planned'}
