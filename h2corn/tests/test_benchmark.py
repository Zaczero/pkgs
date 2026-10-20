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
    write_hypercorn_config,
)
from bench.evidence import verify_workers


@pytest.mark.parametrize('name', ['h2corn', 'uvicorn', 'hypercorn', 'gunicorn'])
@pytest.mark.parametrize('protocol', ['h1', 'h1_uds', 'h2'])
def test_benchmark_inherited_listener_and_protocol_readiness(name, protocol, tmp_path):
    scenario = Scenario('Listener readiness', 1, protocol)
    config = tmp_path / 'hypercorn.toml'
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
            workers=1,
            socket_path=live.socket_path,
            scenario=live,
            listener_fd=listener.fileno(),
        ) as server:
            check_response(live)
            receipt = json.loads(request_response(live, '/__bench/identity', timeout=5))
            assert receipt['pid'] in server.worker_pids
            assert receipt['executable'] == sys.executable
            assert receipt['event_loop'] == 'asyncio.unix_events'
            assert server.memory_bytes() > 0
        assert listener.fileno() >= 0


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
