"""Benchmark evidence through real listeners and operating-system accounting."""

import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bench._core import (
    BenchmarkError,
    DeadlineError,
    MeasuredMetrics,
    NoCompletionsError,
    ResponseError,
    Scenario,
    ServerRun,
    _oha_metrics,
    _validate_oha,
    allocated_listener,
    check_response,
    request_response,
    running_server,
)
from bench.bench import (
    TrialError,
    benchmark_scenarios,
    measure_cell,
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
            assert (
                Path(receipt['executable']).resolve() == Path(sys.executable).resolve()
            )
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


def test_benchmark_deadline_cleanup_reaps_a_detached_child(tmp_path):
    import json
    import select
    import subprocess

    from bench._core import terminate_process_tree

    child = (
        'import json, os, subprocess, sys, time; '
        'process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"], '
        'start_new_session=True); '
        'print(json.dumps({"pid": process.pid}), flush=True); '
        'time.sleep(30)'
    )
    process = subprocess.Popen(
        [sys.executable, '-c', child],
        stdout=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    assert process.stdout is not None
    child_pid = json.loads(process.stdout.readline())['pid']
    child_pidfd = os.pidfd_open(child_pid)
    try:
        terminated = terminate_process_tree(process, timeout=0.1)
        poller = select.poll()
        poller.register(child_pidfd, select.POLLIN | select.POLLHUP)

        assert process.returncode is not None
        assert child_pid in terminated
        assert poller.poll(0)
    finally:
        os.close(child_pidfd)
        process.stdout.close()


def test_benchmark_missing_pss_is_unavailable():
    server = ServerRun([], 2**30, ())
    with pytest.raises(BenchmarkError, match='PSS unavailable'):
        server.memory_bytes()


def test_benchmark_nested_deadline_restores_outer_timer():
    import signal
    import time

    from bench._core import deadline_bound

    with deadline_bound(time.monotonic() + 30):
        outer = signal.getitimer(signal.ITIMER_REAL)[0]
        assert outer > 0
        with deadline_bound(time.monotonic() + 20):
            assert signal.getitimer(signal.ITIMER_REAL)[0] > 0
        assert signal.getitimer(signal.ITIMER_REAL)[0] > 0


def test_benchmark_deadline_reaps_load_child_and_retains_interruption(tmp_path):
    import time

    from bench._core import _run, deadline_bound

    evidence = tmp_path / 'child.process.json'
    with (
        pytest.raises(DeadlineError, match='benchmark deadline expired'),
        deadline_bound(time.monotonic() + 0.2),
    ):
        _run(
            [sys.executable, '-c', 'import time; time.sleep(30)'],
            60,
            evidence_path=evidence,
        )

    receipt = json.loads(evidence.read_text())
    assert receipt['status'] == 'interrupted'
    assert isinstance(receipt['returncode'], int)


def test_benchmark_rate_comparison_uses_order_interval_on_paired_blocks():
    def sample(rate: float) -> MeasuredMetrics:
        return {
            'rps': rate,
            'completed': 100,
            'peak_memory_bytes': 1000,
            'elapsed_seconds': 1.0,
            'latency_percentiles': {},
        }

    contrasts = paired_contrasts({
        'h2corn': [sample(rate) for rate in (20, 200, 40, 400, 60, 600)],
        'baseline': [sample(rate) for rate in (10, 100, 20, 200, 30, 300)],
    })
    assert contrasts['baseline']['block_ratios'] == [2, 2, 2, 2, 2, 2]
    assert contrasts['baseline']['interval'] == [2, 2]
    assert contrasts['baseline']['achieved_coverage'] == 62 / 64
    assert contrasts['baseline']['decision'] == 'slower'


def test_benchmark_paired_contrast_rejects_mismatched_block_identity():
    def sample(rate: float) -> MeasuredMetrics:
        return {
            'rps': rate,
            'completed': 100,
            'peak_memory_bytes': 1000,
            'elapsed_seconds': 1.0,
            'latency_percentiles': {},
        }

    with pytest.raises(ValueError, match='matching distinct block'):
        paired_contrasts(
            {
                'h2corn': [sample(rate) for rate in (20, 200, 40, 400, 60, 600)],
                'baseline': [sample(rate) for rate in (10, 100, 20, 200, 30)],
            },
            {
                'h2corn': [1, 2, 3, 4, 5, 6],
                'baseline': [1, 2, 3, 4, 5],
            },
        )


def test_calibration_without_early_completions_extends_in_the_same_server(
    monkeypatch, tmp_path
):
    from bench import bench

    windows = []

    def load(scenario, *, duration, **kwargs):
        windows.append(duration)
        if len(windows) == 1:
            raise NoCompletionsError('no completed request in this short window')
        return {'rps': 100.0, 'completed': 100, 'latency_percentiles': {}}

    monkeypatch.setattr(bench, 'run_load', load)
    measured = measure_cell(
        'uvicorn',
        Scenario('Calibration without early completions', 1, 'h1'),
        '0.5s',
        '0s',
        hypercorn_config=tmp_path / 'unused.toml',
        evidence_directory=tmp_path / 'calibration',
        calibration_windows=(0.5, 1.0, 2.0, 4.0),
    )

    assert windows == ['0.5s', '1s', '2s']
    assert 'status' in measured
    assert measured['status'] == 'stable'
    assert measured['windows'][0]['completed'] == 0
    assert measured['selected_duration_seconds'] == 2.0


@pytest.mark.parametrize(
    ('failure', 'expected_windows'),
    [(NoCompletionsError, 4), (ResponseError, 1)],
)
def test_calibration_distinguishes_no_completions_from_invalid_responses(
    monkeypatch, tmp_path, failure, expected_windows
):
    from bench import bench

    windows = []

    def load(scenario, *, duration, **kwargs):
        windows.append(duration)
        raise failure('load did not produce valid completed work')

    monkeypatch.setattr(bench, 'run_load', load)
    with pytest.raises(TrialError, match='load did not produce') as caught:
        measure_cell(
            'uvicorn',
            Scenario('Calibration failure', 1, 'h1'),
            '0.5s',
            '0s',
            hypercorn_config=tmp_path / 'unused.toml',
            evidence_directory=tmp_path / 'calibration',
            calibration_windows=(0.5, 1.0, 2.0, 4.0),
        )

    assert type(caught.value.__cause__) is failure
    assert len(windows) == expected_windows


def test_benchmark_multiplexing_is_one_connection_with_ten_streams():
    scenario = next(
        case for case in benchmark_scenarios() if 'multiplexed' in case.name
    )
    assert scenario.concurrency == 1
    assert scenario.http2_parallelism == 10


@pytest.mark.parametrize(
    ('body', 'declared_length', 'status', 'response_body'),
    [
        (b'x' * 1024, 1024, 200, b'1024'),
        (b'x' * 512, 1024, 400, b'upload body length mismatch'),
    ],
)
@pytest.mark.asyncio
async def test_benchmark_upload_endpoint_checks_each_body_length(
    body, declared_length, status, response_body
):
    from starlette.requests import Request

    from bench.bench_app import streaming_post_fast

    async def receive():
        return {
            'type': 'http.request',
            'body': body,
            'more_body': False,
        }

    request = Request(
        {
            'type': 'http',
            'headers': [(b'content-length', str(declared_length).encode())],
        },
        receive,
    )
    response = await streaming_post_fast(request)

    assert response.status_code == status
    assert response.body == response_body


@pytest.mark.asyncio
async def test_raw_benchmark_upload_endpoint_rejects_a_truncated_body():
    from h2corn import HTTPRequest, Scope, SendMessage

    from bench.raw_app import app

    messages: list[HTTPRequest] = [
        {'type': 'http.request', 'body': b'xxxx', 'more_body': False},
    ]
    sent: list[SendMessage] = []

    async def receive() -> HTTPRequest:
        return messages.pop()

    async def send(message: SendMessage) -> None:
        sent.append(message)

    scope: Scope = {
        'type': 'http',
        'asgi': {'version': '3.0', 'spec_version': '2.5'},
        'http_version': '2',
        'method': 'POST',
        'scheme': 'http',
        'path': '/streaming-post-fast',
        'raw_path': b'/streaming-post-fast',
        'query_string': b'',
        'headers': [(b'content-length', b'8')],
        'server': ('127.0.0.1', 8000),
        'extensions': {'http.response.pathsend': {}},
    }
    await app(scope, receive, send)

    start = sent[0]
    assert start['type'] == 'http.response.start'
    assert start['status'] == 400
    body = sent[1]
    assert body['type'] == 'http.response.body'
    assert body.get('body') == b'upload body length mismatch'


def test_benchmark_rate_counts_completed_responses_not_deadline_attempts():
    metrics = _oha_metrics({
        'summary': {'requestsPerSec': 20, 'total': 2},
        'statusCodeDistribution': {'200': 30},
        'errorDistribution': {'aborted due to deadline': 10},
    })
    assert metrics['completed'] == 30
    assert metrics['rps'] == 15


def test_window_cut_before_any_completion_is_not_a_wrong_response():
    with pytest.raises(NoCompletionsError):
        _validate_oha(
            {
                'statusCodeDistribution': {},
                'errorDistribution': {'aborted due to deadline': 1000},
            },
            max_deadline_aborts=2000,
        )
    with pytest.raises(ResponseError) as caught:
        _validate_oha(
            {'statusCodeDistribution': {'200': 5, '503': 1}, 'errorDistribution': {}},
            max_deadline_aborts=2000,
        )
    assert not isinstance(caught.value, NoCompletionsError)


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
    (tmp_path / 'profile.json').write_text(
        json.dumps({
            'profile': 'grpc',
            'benchmark_family': 'grpc',
            'status': 'completed',
        })
    )
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
                'rps_samples': [120, 124, 128, 132, 136, 140],
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
    assert '| grpcio | 128 | 6 | 2.00 | 42.5 | 1.000 | 5.000 | 3.0 | - |' in table


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
        ['--family', 'grpc', '--connect-source', '/missing/connect'],
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
