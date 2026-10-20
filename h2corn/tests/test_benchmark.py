"""Benchmark evidence through real listeners and operating-system accounting."""

import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bench._core import (
    BenchmarkError,
    MeasuredMetrics,
    Scenario,
    ServerRun,
    _oha_metrics,
    allocated_listener,
    check_response,
    request_response,
    running_server,
)
from bench.bench import (
    benchmark_scenarios,
    paired_contrasts,
    server_command,
    unsupported_reason,
    write_hypercorn_config,
)
from bench.evidence import verify_workers

pytestmark = pytest.mark.skipif(
    sys.platform != 'linux',
    reason='the benchmark harness requires Linux inherited listeners and PSS accounting',
)


@pytest.mark.parametrize(
    ('name', 'protocol'),
    [
        ('h2corn', 'h1'),
        ('h2corn', 'h1_uds'),
        ('h2corn', 'h2'),
        ('hypercorn', 'h1'),
        ('hypercorn', 'h1_uds'),
        ('hypercorn', 'h2'),
        ('uvicorn', 'h1'),
        ('uvicorn', 'h1_uds'),
        ('gunicorn', 'h1'),
        ('gunicorn', 'h1_uds'),
    ],
)
@pytest.mark.parametrize('workers', [1, 4])
def test_benchmark_inherited_listener_and_protocol_readiness(
    name, protocol, workers, tmp_path
):
    scenario = Scenario('Listener readiness', workers, protocol)
    config = tmp_path / 'hypercorn.toml'
    identity_directory = tmp_path / 'worker-identities'
    identity_directory.mkdir()
    write_hypercorn_config(config)
    with allocated_listener(scenario) as (live, listener):
        command = server_command(
            name,
            live,
            hypercorn_config=config,
            listener_fd=listener.fileno(),
        )
        with running_server(
            command,
            workers=workers,
            socket_path=live.socket_path,
            scenario=live,
            listener_fd=listener.fileno(),
            identity_directory=identity_directory,
        ) as server:
            check_response(live)
            receipt = json.loads(request_response(live, '/__bench/identity', timeout=5))
            assert receipt['pid'] in server.worker_pids
            assert receipt['executable'] == sys.executable
            assert receipt['event_loop'] == 'asyncio.unix_events'
            assert len(server.worker_pids) == workers
            for pid in server.worker_pids:
                startup = json.loads((identity_directory / f'{pid}.json').read_text())
                assert startup['pid'] == pid
            assert server.memory_bytes() > 0
        assert listener.fileno() >= 0


@pytest.mark.parametrize('name', ['uvicorn', 'gunicorn'])
def test_benchmark_unsupported_cleartext_http2_is_not_launched(name, tmp_path):
    scenario = Scenario('Cleartext HTTP/2', 1, 'h2')
    assert unsupported_reason(name, scenario) is not None
    with pytest.raises(BenchmarkError, match='no prior-knowledge cleartext HTTP/2'):
        server_command(name, scenario, hypercorn_config=tmp_path / 'hypercorn.toml')


def test_benchmark_pss_counts_a_single_process_once():
    pid = os.getpid()
    single = ServerRun([], pid, ())
    duplicate = ServerRun([], pid, (pid, pid))
    before = single.memory_bytes()
    measured = duplicate.memory_bytes()
    after = single.memory_bytes()
    assert 0 < measured < min(before, after) * 1.5


def test_benchmark_missing_pss_is_unavailable():
    server = ServerRun([], 2**30, ())
    with pytest.raises(BenchmarkError, match='PSS unavailable'):
        server.memory_bytes()


def test_benchmark_rate_comparison_resamples_paired_blocks():
    def sample(rate: float) -> MeasuredMetrics:
        return {
            'rps': rate,
            'completed': 100,
            'peak_memory_bytes': 1000,
            'latency_percentiles': {},
        }

    contrasts = paired_contrasts({
        'h2corn': [sample(rate) for rate in (20, 200, 40, 400)],
        'baseline': [sample(rate) for rate in (10, 100, 20, 200)],
    })
    assert contrasts['baseline']['block_ratios'] == [2, 2, 2, 2]
    assert contrasts['baseline']['interval_95'] == [2, 2]


def test_benchmark_multiplexing_is_one_connection_with_ten_streams():
    scenario = next(
        case for case in benchmark_scenarios() if 'multiplexed' in case.name
    )
    assert scenario.concurrency == 1
    assert scenario.http2_parallelism == 10


def test_benchmark_rate_counts_completed_responses_not_deadline_attempts():
    metrics = _oha_metrics({
        'summary': {'requestsPerSec': 20, 'total': 2},
        'statusCodeDistribution': {'200': 30},
        'errorDistribution': {'aborted due to deadline': 10},
    })
    assert metrics['completed'] == 30
    assert metrics['rps'] == 15


def test_benchmark_worker_receipt_rejects_a_different_native_image():
    identity = {
        'python': {'path': str(Path(sys.executable).resolve())},
        'packages': {
            'zttp': {'files': {'native': {'path': '/codec.so', 'sha256': 'abc'}}}
        },
    }
    trial = {
        'workers': {
            42: {
                'executable': sys.executable,
                'event_loop': 'asyncio.unix_events',
                'modules': {'zttp._zttp': {'path': '/codec.so', 'sha256': 'def'}},
            }
        }
    }
    with pytest.raises(RuntimeError, match='differs from installed inventory'):
        verify_workers(trial, identity)


def test_grpc_benchmark_outcomes_reject_an_unfinished_call():
    from bench.grpc import validate_outcomes

    raw = {
        'attempted': 3,
        'started': 3,
        'admission_failures': 0,
        'verified_ok': 2,
        'expected_cancelled': 0,
        'deadline_exceeded': 0,
        'other_non_ok': 0,
        'invalid_response': 0,
        'unfinished': 1,
    }
    with pytest.raises(BenchmarkError, match='did not all complete'):
        validate_outcomes(raw, {'shape': 'unary', 'requests': 0})


def test_grpc_benchmark_outcomes_reject_missing_stream_suffix():
    from bench.grpc import validate_outcomes

    raw = {
        'attempted': 2,
        'started': 2,
        'admission_failures': 0,
        'verified_ok': 2,
        'expected_cancelled': 0,
        'deadline_exceeded': 0,
        'other_non_ok': 0,
        'invalid_response': 0,
        'unfinished': 0,
        'verified_messages': 7,
        'verified_payload_bytes': 80,
    }
    with pytest.raises(BenchmarkError, match='useful message/byte totals'):
        validate_outcomes(
            raw,
            {
                'shape': 'duplex',
                'requests': 2,
                'messages': 4,
                'size': 10,
            },
        )


@pytest.mark.asyncio
async def test_grpc_benchmark_duplex_reads_ahead_while_output_is_held():
    import asyncio

    pytest.importorskip('connectrpc')
    from bench.grpc_app import Work
    from examples.grpc.gen.h2corn.interop.v1.notebook_pb import Edit

    consumed_three = asyncio.Event()
    work = Work({'size': 4, 'messages': 3})

    async def requests():
        for sequence in range(3):
            if sequence == 2:
                consumed_three.set()
            yield Edit(note_id='distinct-id', base_revision=sequence, text='xxxx')

    responses = work.duplex(requests())
    first = await anext(responses)
    await asyncio.wait_for(consumed_three.wait(), timeout=3)
    assert first.revision == 1
    assert work.consumed == 3
    assert [response.revision async for response in responses] == [2, 3]
    assert work.active == 0
    assert work.readers == 0


@pytest.mark.asyncio
async def test_grpc_benchmark_duplex_closure_joins_its_input_reader():
    import asyncio

    pytest.importorskip('connectrpc')
    from bench.grpc_app import Work
    from examples.grpc.gen.h2corn.interop.v1.notebook_pb import Edit

    released = asyncio.Event()
    work = Work({'size': 1, 'messages': 2})

    async def requests():
        try:
            yield Edit(note_id='close-id', text='x')
            await asyncio.Event().wait()
        finally:
            released.set()

    responses = work.duplex(requests())
    assert (await anext(responses)).revision == 1
    await responses.aclose()
    assert released.is_set()
    assert work.active == 0
    assert work.readers == 0


def test_grpc_benchmark_process_scope_includes_live_child():
    import subprocess

    with subprocess.Popen(
        [sys.executable, '-c', 'import sys; sys.stdin.read()'],
        stdin=subprocess.PIPE,
    ) as child:
        try:
            server = ServerRun(
                [], os.getpid(), (os.getpid(),), include_descendants=True
            )
            assert child.pid in server.process_pids()
            assert server.memory_bytes() > 0
        finally:
            child.communicate()


def test_grpc_benchmark_wheel_verification_rejects_changed_runtime_bytes(
    tmp_path, monkeypatch
):
    import zipfile

    from bench.grpc import verify_wheel

    metadata = tmp_path / 'benchmark_fixture-1.0.dist-info'
    metadata.mkdir()
    (metadata / 'METADATA').write_text(
        'Metadata-Version: 2.1\nName: benchmark-fixture\nVersion: 1.0\n'
    )
    module = tmp_path / 'benchmark_fixture.py'
    module.write_text('answer = 42\n')
    monkeypatch.syspath_prepend(str(tmp_path))
    wheel = tmp_path / 'benchmark_fixture-1.0-py3-none-any.whl'
    with zipfile.ZipFile(wheel, 'w') as archive:
        archive.writestr('benchmark_fixture.py', 'answer = 42\n')
    assert verify_wheel(wheel, 'benchmark-fixture')['members']
    module.write_text('answer = 43\n')
    with pytest.raises(BenchmarkError, match='installed bytes differ'):
        verify_wheel(wheel, 'benchmark-fixture')


def test_grpc_publication_preserves_http_artifacts(tmp_path, monkeypatch):
    from bench import bench

    results = tmp_path / 'results'
    http = results / 'raw/http.json'
    http.parent.mkdir(parents=True)
    http.write_text('existing HTTP evidence')
    run = tmp_path / 'run'
    (run / 'raw').mkdir(parents=True)
    (run / 'plots').mkdir()
    (run / 'profile.json').write_text(
        json.dumps({
            'profile': 'grpc',
            'benchmark_family': 'grpc',
        })
    )
    (run / 'raw/rpc.json').write_text('new RPC evidence')
    (run / 'identity.json').write_text('{}')
    monkeypatch.setattr(bench, 'RESULTS_DIRECTORY', results)
    bench.publish([run])
    assert http.read_text() == 'existing HTTP evidence'
    assert (results / 'grpc/raw/rpc-grpc.json').read_text() == 'new RPC evidence'
    assert (results / 'grpc/evidence/grpc/identity.json').is_file()


def test_grpc_publication_requires_qualification_receipts(tmp_path):
    from bench.grpc import assert_rpc_publishable

    (tmp_path / 'identity.json').write_text('{}')
    with pytest.raises(BenchmarkError, match='retained qualification'):
        assert_rpc_publishable(tmp_path)


def test_grpc_table_distinguishes_subset_omission_from_unsupported():
    from bench.report import render_accessible_results

    table = render_accessible_results({
        'type': 'grpc',
        'rate_unit': 'messages/s',
        'reference_label': 'Connect / h2corn',
        'aggregate': {},
        'requested_servers': ['grpclib'],
        'not_selected': {'grpclib': 'Representative subset'},
    })
    assert 'messages/s' in table
    assert '| grpclib | not selected |' in table


def test_grpc_stream_table_reports_messages_complete_calls_and_trial_counts():
    from bench.report import render_accessible_results

    table = render_accessible_results({
        'type': 'grpc',
        'benchmark_family': 'grpc',
        'rate_unit': 'messages/s',
        'aggregate': {
            'grpcio': {
                'rps': 128,
                'rpc_rate': 2,
                'completed': 42.5,
                'peak_memory_bytes': 3_000_000,
                'latency_percentiles': {'p50': 0.001, 'p99': 0.005},
            },
        },
        'requested_servers': ['grpcio'],
    })
    assert 'Median messages/s' in table
    assert 'Median complete RPC/s' in table
    assert 'Median completed RPCs/trial' in table
    assert '| grpcio | 128 | 2.00 | 42.5 | 1.000 | 5.000 | 3.0 | - |' in table


@pytest.mark.parametrize(
    'arguments',
    [
        ['--qualify-only'],
        ['--family', 'grpc', '--publish', '--rpc-cases', 'U32'],
        [
            '--family',
            'grpc',
            '--qualify-only',
            '--rpc-cases',
            'U32',
            '--connect-development-revision',
            'abcdef',
        ],
    ],
)
def test_grpc_cli_rejects_incomplete_experiments_before_starting_work(arguments):
    import subprocess

    result = subprocess.run(
        [sys.executable, 'bench/bench.py', *arguments],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == 2
    assert 'require' in result.stderr
