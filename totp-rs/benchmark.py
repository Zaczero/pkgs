from __future__ import annotations

import argparse
import os
import tempfile
from dataclasses import dataclass
from importlib.metadata import version

import pyotp
import pyperf
from prettytable import PrettyTable
from totp_rs import totp_generate, totp_verify


@dataclass(frozen=True)
class ResultRow:
    name: str
    totp_rs_ns: float | None
    pyotp_ns: float | None


def _mean_ns(bench: pyperf.Benchmark) -> float:
    # Use the mean of the benchmark values. pyperf stores seconds.
    return bench.mean() * 1e9


def _format_ns(value_ns: float | None) -> str:
    if value_ns is None:
        return 'n/a'
    if value_ns >= 1e6:
        return f'{value_ns / 1e6:8.3f} ms'
    if value_ns >= 1e3:
        return f'{value_ns / 1e3:8.3f} µs'
    return f'{value_ns:8.1f} ns'


def _speedup(totp_rs_ns: float | None, pyotp_ns: float | None) -> str:
    if totp_rs_ns is None or pyotp_ns is None:
        return 'n/a'
    if totp_rs_ns <= 0:
        return 'n/a'
    return f'{pyotp_ns / totp_rs_ns:6.2f}x'


def _render_tables(rows: list[ResultRow]) -> str:
    totp_rs_version = version('totp-rs')
    pyotp_version = version('pyotp')

    timings = PrettyTable()
    timings.field_names = [
        'Benchmark',
        f'totp-rs {totp_rs_version}',
        f'pyotp {pyotp_version}',
    ]
    timings.align['Benchmark'] = 'l'
    timings.align[f'totp-rs {totp_rs_version}'] = 'r'
    timings.align[f'pyotp {pyotp_version}'] = 'r'
    for row in rows:
        timings.add_row([
            row.name,
            _format_ns(row.totp_rs_ns),
            _format_ns(row.pyotp_ns),
        ])

    speedups = PrettyTable()
    speedups.field_names = ['Benchmark', 'Speedup vs pyotp']
    speedups.align['Benchmark'] = 'l'
    speedups.align['Speedup vs pyotp'] = 'r'
    for row in rows:
        speedups.add_row([row.name, _speedup(row.totp_rs_ns, row.pyotp_ns)])

    return f'{timings}\n\n{speedups}'


def _load_suite(path: str) -> pyperf.BenchmarkSuite:
    with open(path, encoding='utf-8') as f:
        return pyperf.BenchmarkSuite.load(f)


def main() -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--table', action='store_true')
    args, pyperf_args = parser.parse_known_args()

    # RFC 6238 test seed in base32; 20 bytes. Keep the same secret across libraries.
    secret_b32 = 'GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ'

    # A deterministic timestamp which looks "real" (not RFC vector times).
    now = 1_700_000_000
    prev = now - 30

    # Setup pyotp once; benchmarks should not include construction time.
    pyotp_totp = pyotp.TOTP(secret_b32, digits=6, interval=30)
    expected_code = pyotp_totp.at(now)
    expected_prev_code = pyotp_totp.at(prev)
    wrong_code = '000000' if expected_code != '000000' else '111111'

    output_path = None
    tmp_path = None
    if '-o' not in pyperf_args and '--output' not in pyperf_args:
        fd, tmp_path = tempfile.mkstemp(prefix='totp_rs_bench_', suffix='.json')
        os.close(fd)
        # pyperf refuses to overwrite an existing file, so we only use the
        # generated name and remove the file immediately.
        os.unlink(tmp_path)
        output_path = tmp_path
        pyperf_args = ['-o', output_path, *pyperf_args]

    runner = pyperf.Runner()
    runner.parse_args(pyperf_args)
    runner.metadata['totp_params'] = 'sha1 digits=6 step_seconds=30 window=1'
    runner.metadata['timestamp'] = str(now)

    runner.bench_func(
        'totp-rs generate',
        lambda: totp_generate(
            secret_b32, digits=6, algorithm='sha1', time=now, step_seconds=30, t0=0
        ),
    )
    runner.bench_func(
        'pyotp generate',
        lambda: pyotp_totp.at(now),
    )

    runner.bench_func(
        'totp-rs verify ok',
        lambda: totp_verify(
            secret_b32,
            expected_code,
            digits=6,
            algorithm='sha1',
            time=now,
            step_seconds=30,
            t0=0,
            window=1,
        ),
    )
    runner.bench_func(
        'pyotp verify ok',
        lambda: pyotp_totp.verify(expected_code, for_time=now, valid_window=1),
    )

    runner.bench_func(
        'totp-rs verify prev ok',
        lambda: totp_verify(
            secret_b32,
            expected_prev_code,
            digits=6,
            algorithm='sha1',
            time=now,
            step_seconds=30,
            t0=0,
            window=1,
        ),
    )
    runner.bench_func(
        'pyotp verify prev ok',
        lambda: pyotp_totp.verify(expected_prev_code, for_time=now, valid_window=1),
    )

    runner.bench_func(
        'totp-rs verify bad',
        lambda: totp_verify(
            secret_b32,
            wrong_code,
            digits=6,
            algorithm='sha1',
            time=now,
            step_seconds=30,
            t0=0,
            window=1,
        ),
    )
    runner.bench_func(
        'pyotp verify bad',
        lambda: pyotp_totp.verify(wrong_code, for_time=now, valid_window=1),
    )

    # In pyperf worker processes, the manager collects JSON results via a pipe.
    # Don't attempt to load/format results in the worker process.
    if runner.args.worker:
        return 0

    output_path = output_path or runner.args.output
    suite = _load_suite(output_path)
    by_name = {b.get_name(): b for b in suite}

    rows = [
        ResultRow(
            name='generate',
            totp_rs_ns=_mean_ns(by_name['totp-rs generate']),
            pyotp_ns=_mean_ns(by_name['pyotp generate']),
        ),
        ResultRow(
            name='verify ok',
            totp_rs_ns=_mean_ns(by_name['totp-rs verify ok']),
            pyotp_ns=_mean_ns(by_name['pyotp verify ok']),
        ),
        ResultRow(
            name='verify prev ok',
            totp_rs_ns=_mean_ns(by_name['totp-rs verify prev ok']),
            pyotp_ns=_mean_ns(by_name['pyotp verify prev ok']),
        ),
        ResultRow(
            name='verify bad',
            totp_rs_ns=_mean_ns(by_name['totp-rs verify bad']),
            pyotp_ns=_mean_ns(by_name['pyotp verify bad']),
        ),
    ]

    if args.table:
        text = _render_tables(rows)
        print(text)
        print()
        print('```text')
        print(text)
        print('```')

    if tmp_path is not None:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
