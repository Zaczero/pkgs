import argparse
import base64
import hashlib
import sys
from functools import partial
from importlib.metadata import version
from pathlib import Path

import pyperf
from totp_rs import _lib, totp_generate, totp_time_window, totp_verify


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--impl', required=True, choices=['pyotp', 'totp-rs'])
    args, pyperf_args = parser.parse_known_args()

    now = 1_700_000_000
    prev = now - 30
    runner = pyperf.Runner(program_args=(sys.argv[0], '--impl', args.impl))
    runner.metadata['implementation'] = f'{args.impl} {version(args.impl)}'
    runner.metadata['benchmark_sha256'] = hashlib.sha256(
        Path(__file__).read_bytes()
    ).hexdigest()
    runner.metadata['native_extension_sha256'] = hashlib.sha256(
        Path(_lib.__file__).read_bytes()
    ).hexdigest()
    runner.metadata['workload'] = (
        'SHA-1/SHA-256/SHA-512, Base32 keys, timestamp input; '
        '6 digits, 30-second period, verification window ±1'
    )
    if args.impl == 'totp-rs':
        runner.metadata['workload'] += '; raw-key and explicit-counter generation'
    runner.parse_args(pyperf_args)
    if args.impl == 'pyotp':
        import pyotp

    algorithms = (
        ('sha1', b'12345678901234567890', hashlib.sha1),
        ('sha256', b'12345678901234567890123456789012', hashlib.sha256),
        (
            'sha512',
            b'1234567890123456789012345678901234567890123456789012345678901234',
            hashlib.sha512,
        ),
    )
    for algorithm, raw_secret, digest in algorithms:
        secret = base64.b32encode(raw_secret).decode('ascii')
        expected = totp_generate(secret, algorithm=algorithm, time=now)
        expected_prev = totp_generate(secret, algorithm=algorithm, time=prev)
        if args.impl == 'pyotp':
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
        prefix = '' if algorithm == 'sha1' else f'{algorithm} '
        for name, call in (
            ('generate', generate),
            ('verify ok', partial(verify, expected)),
            ('verify prev ok', partial(verify, expected_prev)),
            ('verify bad', partial(verify, '999999')),
        ):
            runner.bench_func(prefix + name, call)
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
            runner.bench_func(prefix + 'generate raw key', generate_raw)
            runner.bench_func(prefix + 'generate from counter', generate_counter)
    if args.impl == 'totp-rs':
        runner.bench_func('time window', totp_time_window, now)


if __name__ == '__main__':
    main()
