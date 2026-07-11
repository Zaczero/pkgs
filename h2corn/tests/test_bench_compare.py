import argparse
import importlib
import sys
from itertools import pairwise
from pathlib import Path

import pytest

compare = importlib.import_module('bench.compare')
mask_compare = importlib.import_module('bench.mask_kernel_compare')
blocked_orders = compare.blocked_orders
paired_comparison = compare.paired_comparison
parse_args = compare.parse_args
parse_cpu_set = compare.parse_cpu_set
parse_named_command = compare.parse_named_command


def test_blocked_orders_are_reproducible_balanced_and_interleaved():
    first = blocked_orders(9, 41)
    assert first == blocked_orders(9, 41)
    assert all(set(order) == {'control', 'candidate'} for order in first)
    assert all(left[0] != right[0] for left, right in pairwise(first))
    control_leads = sum(order[0] == 'control' for order in first)
    assert control_leads in {4, 5}


def test_perf_stat_parser_accepts_raw_unmultiplexed_count():
    assert mask_compare.parse_perf_stat(
        '6523560;;cycles;1459640;100.00;;\n', 'cycles'
    ) == (6_523_560, 100.0)


@pytest.mark.parametrize(
    'output',
    [
        '<not counted>;;cycles;0;0.00;;\n',
        '6523560;;cycles;1459640;87.50;;\n',
    ],
)
def test_perf_stat_parser_rejects_unavailable_or_multiplexed_counts(output):
    with pytest.raises(RuntimeError):
        mask_compare.parse_perf_stat(output, 'cycles')


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
    assert result['significant'] is True


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
    assert result['significant'] is False


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
        '--trials',
        '5',
        '--server-cpus',
        '2',
        '--load-cpus',
        '4-5',
        '--output',
        str(output),
    ])

    assert args.control.name == 'old'
    assert args.candidate.name == 'new'
    assert args.duration_seconds == pytest.approx(0.25)
    assert args.server_cpus == (2,)
    assert args.load_cpus == (4, 5)
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


def test_variant_provenance_hashes_resolved_executable():
    command = compare.NamedCommand('true', ('true',))
    artifacts = compare._variant_artifacts(command)

    assert Path(artifacts['executable']).is_file()
    assert len(artifacts['executable_sha256']) == 64
    assert artifacts['extension'] is None


def test_shebang_probe_preserves_virtual_environment_python_symlink(tmp_path):
    interpreter = tmp_path / 'python3'
    interpreter.symlink_to(sys.executable)
    command_path = tmp_path / 'server'
    command_path.write_text(f'#!{interpreter}\n')
    command_path.chmod(0o755)
    command = compare.NamedCommand('server', (str(command_path),))

    executable = compare._resolved_executable(command)
    assert executable is not None
    assert compare._command_python(command, executable) == interpreter
