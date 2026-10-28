"""Train TOTP generation, login verification, and clock-drift handling."""

from __future__ import annotations

import base64
import hmac
import random
import sys
from pathlib import Path

import pgo_process

GENERATE_FLAGS = ()
PACKAGE = Path(__file__).resolve().parents[1]
OPERATIONS = 108_000
stage_generator = pgo_process.stage_generator
verify_runtime = pgo_process.verify_runtime
known_profile_diagnostic = pgo_process.known_profile_diagnostic


def collect(output: Path) -> Path:
    from totp_rs import _lib

    return pgo_process.collect(
        output,
        Path(__file__),
        Path(_lib.__file__),
        units=OPERATIONS,
        functions=['totp_generate', 'totp_verify', 'verify_with_mac', 'Secret6decode'],
    )


def test(output: Path) -> None:
    pgo_process.run_tests(output, PACKAGE)


def train() -> None:
    from totp_rs import _lib, totp_generate, totp_time_window, totp_verify

    rng = random.Random(820175)
    units = 0
    # Each algorithm has its own compression and key-scheduling regime. Equal
    # call counts keep the default SHA-1 path from making the others cold.
    for algorithm in ('sha1', 'sha256', 'sha512'):
        for size in (20, 64, 320):
            raw = rng.randbytes(size)
            encoded = base64.b32encode(raw).decode()
            formatted = ' '.join(
                encoded[index : index + 8].lower()
                for index in range(0, len(encoded), 8)
            )
            for secret in (raw, encoded, formatted):
                for digits in (6, 8):
                    counter = rng.randrange(1_000_000, 2_000_000)
                    codes = []
                    for offset in (-1, 0, 1):
                        digest = hmac.digest(
                            raw, (counter + offset).to_bytes(8, 'big'), algorithm
                        )
                        start = digest[-1] & 15
                        value = int.from_bytes(digest[start : start + 4], 'big')
                        codes.append(f'{(value & 0x7FFFFFFF) % 10**digits:0{digits}}')
                    wrong = '0' * digits
                    while wrong in codes:
                        wrong = f'{int(wrong) + 1:0{digits}}'
                    for _ in range(200):
                        if totp_time_window(counter * 30) != counter:
                            raise RuntimeError(
                                'TOTP training counter conversion failed'
                            )
                        if (
                            len(
                                totp_generate(
                                    secret, digits=digits, algorithm=algorithm
                                )
                            )
                            != digits
                        ):
                            raise RuntimeError('TOTP training clock generation failed')
                        if (
                            totp_generate(
                                secret,
                                digits=digits,
                                algorithm=algorithm,
                                time_window=counter,
                            )
                            != codes[1]
                        ):
                            raise RuntimeError('TOTP training generation failed')
                        for code, accepted in (
                            (codes[1], True),
                            (codes[1], True),
                            (codes[0], True),
                            (codes[2], True),
                            (wrong, False),
                            (int(codes[1]), True),
                            ('invalid', False),
                        ):
                            if (
                                totp_verify(
                                    secret,
                                    code,
                                    digits=digits,
                                    algorithm=algorithm,
                                    time=counter * 30,
                                    window=1,
                                )
                                != accepted
                            ):
                                raise RuntimeError('TOTP training verification failed')
                        units += 10
    pgo_process.completed(units, Path(_lib.__file__))


if __name__ == '__main__':
    if sys.argv[1:] != ['train']:
        raise SystemExit('expected train')
    train()
