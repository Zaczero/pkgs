import argparse
import sys
from functools import partial

import pyperf
from totp_rs import totp_generate, totp_verify


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--impl', required=True, choices=['pyotp', 'totp-rs'])
    args, pyperf_args = parser.parse_known_args()

    # RFC 6238 test seed in base32; 20 bytes. Keep the same secret across libraries.
    secret_b32 = 'GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ'

    # A deterministic timestamp which looks "real" (not RFC vector times).
    now = 1_700_000_000
    prev = now - 30

    runner = pyperf.Runner(program_args=(sys.argv[0], '--impl', args.impl))
    runner.parse_args(pyperf_args)

    expected_code = totp_generate(secret_b32, time=now)
    expected_prev_code = totp_generate(secret_b32, time=prev)
    wrong_code = '000000' if expected_code != '000000' else '111111'

    if args.impl == 'pyotp':
        import pyotp

        pyotp_totp = pyotp.TOTP(secret_b32, digits=6, interval=30)

        runner.bench_func(
            'generate',
            partial(
                pyotp_totp.at,
                now,
            ),
        )
        runner.bench_func(
            'verify ok',
            partial(
                pyotp_totp.verify,
                expected_code,
                for_time=now,
                valid_window=1,
            ),
        )
        runner.bench_func(
            'verify prev ok',
            partial(
                pyotp_totp.verify,
                expected_prev_code,
                for_time=now,
                valid_window=1,
            ),
        )
        runner.bench_func(
            'verify bad',
            partial(
                pyotp_totp.verify,
                wrong_code,
                for_time=now,
                valid_window=1,
            ),
        )

    elif args.impl == 'totp-rs':
        runner.bench_func(
            'generate',
            partial(
                totp_generate,
                secret_b32,
                time=now,
            ),
        )
        runner.bench_func(
            'verify ok',
            partial(
                totp_verify,
                secret_b32,
                expected_code,
                time=now,
            ),
        )
        runner.bench_func(
            'verify prev ok',
            partial(
                totp_verify,
                secret_b32,
                expected_prev_code,
                time=now,
            ),
        )
        runner.bench_func(
            'verify bad',
            partial(
                totp_verify,
                secret_b32,
                wrong_code,
                time=now,
            ),
        )


if __name__ == '__main__':
    main()
