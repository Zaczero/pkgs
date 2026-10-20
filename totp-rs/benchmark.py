import argparse
import base64
import hashlib
import json
import subprocess
import sys
from functools import partial
from importlib.metadata import version
from pathlib import Path

import pyperf
from totp_rs import _lib, totp_generate, totp_time_window, totp_verify

from benchmark_cases import ALGORITHMS, case_definitions


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--impl', required=True, choices=['pyotp', 'totp-rs'])
    args, pyperf_args = parser.parse_known_args()

    now = 1_700_000_000
    prev = now - 30
    runner = pyperf.Runner(program_args=(sys.argv[0], '--impl', args.impl))
    runner.metadata['implementation'] = f'{args.impl} {version(args.impl)}'
    runner.metadata['implementation_name'] = args.impl
    runner.metadata['implementation_version'] = version(args.impl)
    runner.metadata['benchmark_sha256'] = hashlib.sha256(
        Path(__file__).read_bytes()
    ).hexdigest()
    runner.metadata['native_extension_sha256'] = hashlib.sha256(
        Path(_lib.__file__).read_bytes()
    ).hexdigest()
    runner.metadata['native_package_version'] = version('totp-rs')
    runner.metadata['pyotp_version'] = version('pyotp')
    runner.metadata['pyperf_version'] = pyperf.__version__
    runner.metadata['source_revision'] = subprocess.check_output(
        ['git', 'rev-parse', 'HEAD'], text=True
    ).strip()
    runner.metadata['report_sha256'] = hashlib.sha256(
        Path(__file__).with_name('tools').joinpath('benchmark_report.py').read_bytes()
    ).hexdigest()
    runner.metadata['case_definitions'] = json.dumps(
        case_definitions(args.impl), separators=(',', ':'), sort_keys=True
    )
    runner.metadata['comparison_workload'] = (
        'Fixed Unix timestamp 1,700,000,000; SHA-1/SHA-256/SHA-512; '
        'Base32 secrets; six-digit codes; 30-second period; verification '
        'window ±1. Shared cases compare the same generation and verification '
        'operations.'
    )
    runner.metadata['workload'] = runner.metadata['comparison_workload']
    if args.impl == 'totp-rs':
        runner.metadata['workload'] += (
            ' Native-only cases measure raw-key and time-counter generation '
            'and time-counter extraction.'
        )
    runner.parse_args(pyperf_args)
    if args.impl == 'pyotp':
        import pyotp

    definitions = case_definitions(args.impl)
    for algorithm, raw_secret in ALGORITHMS:
        secret = base64.b32encode(raw_secret).decode('ascii')
        expected = totp_generate(secret, algorithm=algorithm, time=now)
        expected_prev = totp_generate(secret, algorithm=algorithm, time=prev)
        if args.impl == 'pyotp':
            digest = getattr(hashlib, algorithm)
            provider = pyotp.TOTP(secret, digits=6, interval=30, digest=digest)
            generate = partial(provider.at, now)
            verify = partial(provider.verify, for_time=now, valid_window=1)
        else:
            generate = partial(totp_generate, secret, algorithm=algorithm, time=now)
            verify = partial(
                totp_verify, secret, algorithm=algorithm, time=now, window=1
            )
        assert generate() == expected
        assert verify(expected)
        assert verify(expected_prev)
        assert not verify('999999')
        comparison_calls = {
            'generate': generate,
            'verify-current': partial(verify, expected),
            'verify-previous': partial(verify, expected_prev),
            'verify-invalid': partial(verify, '999999'),
        }
        for case in definitions:
            if case['group'] != 'comparison' or case['algorithm'] != algorithm:
                continue
            runner.bench_func(case['name'], comparison_calls[case['operation']])
        if args.impl == 'totp-rs':
            counter = totp_time_window(now)
            generate_raw = partial(
                totp_generate, raw_secret, algorithm=algorithm, time=now
            )
            generate_counter = partial(
                totp_generate, raw_secret, algorithm=algorithm, time_window=counter
            )
            assert generate_raw() == expected
            assert generate_counter() == expected
            native_calls = {
                'generate-raw-key': generate_raw,
                'generate-counter': generate_counter,
            }
            for case in definitions:
                if case['group'] != 'native-only' or case['algorithm'] != algorithm:
                    continue
                runner.bench_func(case['name'], native_calls[case['operation']])
    if args.impl == 'totp-rs':
        runner.bench_func('Extract time counter', totp_time_window, now)


if __name__ == '__main__':
    main()
