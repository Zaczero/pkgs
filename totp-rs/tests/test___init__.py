import math
from collections.abc import Callable
from functools import partial
from typing import Literal

import pytest
from totp_rs import totp_generate, totp_time_window, totp_verify

Algorithm = Literal['sha1', 'sha256', 'sha512']


SECRET_SHA1 = b'12345678901234567890'
SECRET_SHA256 = b'12345678901234567890123456789012'
SECRET_SHA512 = b'1234567890123456789012345678901234567890123456789012345678901234'

SECRET_SHA1_B32 = 'GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ'
SECRET_SHA256_B32 = 'GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQGEZA===='
SECRET_SHA512_B32 = (
    'GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ'
    'GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQGEZDGNA='
)

_TIME_OPERATIONS = [
    pytest.param(totp_time_window, id='counter'),
    pytest.param(partial(totp_generate, SECRET_SHA1), id='generate'),
    pytest.param(partial(totp_verify, SECRET_SHA1, '123456'), id='verify'),
]


@pytest.mark.parametrize(
    ('secret', 'algorithm', 'time', 'expected'),
    [
        (SECRET_SHA1, 'sha1', 59, '94287082'),
        (SECRET_SHA256, 'sha256', 59, '46119246'),
        (SECRET_SHA512, 'sha512', 59, '90693936'),
        (SECRET_SHA1, 'sha1', 1111111109, '07081804'),
        (SECRET_SHA256, 'sha256', 1111111109, '68084774'),
        (SECRET_SHA512, 'sha512', 1111111109, '25091201'),
        (SECRET_SHA1, 'sha1', 1111111111, '14050471'),
        (SECRET_SHA256, 'sha256', 1111111111, '67062674'),
        (SECRET_SHA512, 'sha512', 1111111111, '99943326'),
        (SECRET_SHA1, 'sha1', 1234567890, '89005924'),
        (SECRET_SHA256, 'sha256', 1234567890, '91819424'),
        (SECRET_SHA512, 'sha512', 1234567890, '93441116'),
        (SECRET_SHA1, 'sha1', 2000000000, '69279037'),
        (SECRET_SHA256, 'sha256', 2000000000, '90698825'),
        (SECRET_SHA512, 'sha512', 2000000000, '38618901'),
        (SECRET_SHA1, 'sha1', 20000000000, '65353130'),
        (SECRET_SHA256, 'sha256', 20000000000, '77737706'),
        (SECRET_SHA512, 'sha512', 20000000000, '47863826'),
    ],
)
def test_rfc6238_vectors(
    secret: bytes, algorithm: Algorithm, time: int, expected: str
) -> None:
    assert (
        totp_generate(
            secret,
            digits=8,
            algorithm=algorithm,
            time=time,
            step_seconds=30,
            t0=0,
        )
        == expected
    )
    assert totp_verify(
        secret,
        expected,
        digits=8,
        algorithm=algorithm,
        time=time,
        step_seconds=30,
        t0=0,
        window=0,
    )
    assert totp_verify(
        secret,
        f' {expected[:4]} {expected[4:6]}-{expected[6:]}\n',
        digits=8,
        algorithm=algorithm,
        time=time,
        step_seconds=30,
        t0=0,
        window=0,
    )
    assert not totp_verify(
        secret,
        expected[:-1] + ('0' if expected[-1] != '0' else '1'),
        digits=8,
        algorithm=algorithm,
        time=time,
        step_seconds=30,
        t0=0,
        window=0,
    )


def test_time_window_and_time_window_override() -> None:
    assert totp_time_window(0, step_seconds=30, t0=0) == 0
    assert totp_time_window(29, step_seconds=30, t0=0) == 0
    assert totp_time_window(30, step_seconds=30, t0=0) == 1
    assert totp_time_window(59, step_seconds=30, t0=0) == 1
    assert totp_time_window(60, step_seconds=30, t0=0) == 2

    w = totp_time_window(59, step_seconds=30, t0=0)
    assert (
        totp_generate(SECRET_SHA1, digits=8, algorithm='sha1', time_window=w)
        == '94287082'
    )
    assert (
        totp_generate(
            SECRET_SHA1,
            digits=8,
            algorithm='sha1',
            time_window=w,
            step_seconds=0,
        )
        == '94287082'
    )


def test_negative_t0_hybrid() -> None:
    assert totp_time_window(0, step_seconds=30, t0=-30) == 1

    code = totp_generate(
        SECRET_SHA1,
        digits=8,
        algorithm='sha1',
        time=-1,
        t0=-60,
        step_seconds=30,
    )
    assert totp_verify(
        SECRET_SHA1,
        code,
        digits=8,
        algorithm='sha1',
        time=-1,
        t0=-60,
        step_seconds=30,
        window=0,
    )

    w = totp_time_window(-1, step_seconds=30, t0=-60)
    assert totp_verify(
        SECRET_SHA1,
        code,
        digits=8,
        algorithm='sha1',
        time_window=w,
        window=0,
    )


def test_verify_window_allows_drift() -> None:
    # Arrange: RFC vector at t=59s is counter=1.
    code = 94287082

    # Act / Assert: verifying at t=61s is counter=2, so window=1 should accept (delta=-1).
    assert totp_verify(
        SECRET_SHA1,
        code,
        digits=8,
        algorithm='sha1',
        time=61,
        step_seconds=30,
        t0=0,
        window=1,
    )
    assert not totp_verify(
        SECRET_SHA1,
        code,
        digits=8,
        algorithm='sha1',
        time=61,
        step_seconds=30,
        t0=0,
        window=0,
    )


def test_time_accepts_float_seconds() -> None:
    assert totp_time_window(59.9, step_seconds=30, t0=0) == totp_time_window(
        59, step_seconds=30, t0=0
    )


@pytest.mark.parametrize('operation', _TIME_OPERATIONS)
@pytest.mark.parametrize('period', [0, -1, -30])
def test_time_derivation_rejects_nonpositive_periods(
    operation: Callable[..., object], period: int
) -> None:
    with pytest.raises(ValueError, match='step_seconds'):
        operation(time=59, step_seconds=period)


@pytest.mark.parametrize('operation', _TIME_OPERATIONS)
@pytest.mark.parametrize(
    'timestamp',
    [
        pytest.param(float('nan'), id='nan'),
        pytest.param(float('inf'), id='positive-infinity'),
        pytest.param(float('-inf'), id='negative-infinity'),
        pytest.param(float(1 << 63), id='upper-bound'),
        pytest.param(
            math.nextafter(float(-(1 << 63)), -math.inf), id='below-lower-bound'
        ),
    ],
)
def test_time_derivation_rejects_nonfinite_or_out_of_range_seconds(
    operation: Callable[..., object], timestamp: float
) -> None:
    with pytest.raises(ValueError, match='time'):
        operation(time=timestamp)


@pytest.mark.parametrize(
    ('timestamp', 'expected'),
    [
        pytest.param(-0.1, -1, id='negative-fraction'),
        pytest.param(float(-(1 << 63)), -(1 << 63), id='lower-bound'),
        pytest.param(
            math.nextafter(float(1 << 63), 0.0),
            9_223_372_036_854_774_784,
            id='largest-representable-in-range',
        ),
    ],
)
def test_time_seconds_floor_without_saturating(timestamp: float, expected: int) -> None:
    assert totp_time_window(timestamp, step_seconds=1) == expected


@pytest.mark.parametrize(
    ('secret_bytes', 'secret_b32', 'algorithm'),
    [
        (SECRET_SHA1, SECRET_SHA1_B32, 'sha1'),
        (SECRET_SHA256, SECRET_SHA256_B32, 'sha256'),
        (SECRET_SHA512, SECRET_SHA512_B32, 'sha512'),
    ],
)
def test_secret_accepts_base32_str(
    secret_bytes: bytes, secret_b32: str, algorithm: Algorithm
) -> None:
    expected = totp_generate(
        secret_bytes,
        digits=8,
        algorithm=algorithm,
        time=59,
        step_seconds=30,
        t0=0,
    )
    assert (
        totp_generate(
            secret_b32,
            digits=8,
            algorithm=algorithm,
            time=59,
            step_seconds=30,
            t0=0,
        )
        == expected
    )
    assert (
        totp_generate(
            secret_b32.rstrip('='),
            digits=8,
            algorithm=algorithm,
            time=59,
            step_seconds=30,
            t0=0,
        )
        == expected
    )
    assert (
        totp_generate(
            secret_b32.lower().rstrip('='),
            digits=8,
            algorithm=algorithm,
            time=59,
            step_seconds=30,
            t0=0,
        )
        == expected
    )


def test_invalid_inputs() -> None:
    with pytest.raises(ValueError, match='Invalid step_seconds: must be positive'):
        totp_time_window(0, step_seconds=0)

    for digits in (0, 10):
        with pytest.raises(
            ValueError,
            match=f'Invalid digits: expected 1..=9, got {digits}',
        ):
            totp_generate(SECRET_SHA1, digits=digits)

    with pytest.raises(
        ValueError,
        match='Invalid algorithm: expected sha1, sha256, or sha512',
    ):
        totp_generate(SECRET_SHA1, algorithm='md5')  # pyright: ignore[reportArgumentType]

    with pytest.raises(
        ValueError,
        match='Invalid arguments: time and time_window cannot both be set',
    ):
        totp_generate(SECRET_SHA1, time=0, time_window=0)

    with pytest.raises(
        ValueError,
        match='Invalid arguments: time and time_window cannot both be set',
    ):
        totp_verify(
            SECRET_SHA1,
            '94287082',
            digits=8,
            time=59,
            time_window=1,
        )

    for code in ('not-a-code', '12345', '1234567'):
        assert not totp_verify(SECRET_SHA1, code, digits=6)

    assert not totp_verify(SECRET_SHA1, '123456', digits=1)
    assert not totp_verify(SECRET_SHA1, 10_000_000_000, digits=9)
    assert not totp_verify(SECRET_SHA1, -1, digits=6)


def test_invalid_code_returns_false_before_invalid_secret_is_parsed() -> None:
    assert not totp_verify('0', 'not-a-code')

    with pytest.raises(
        ValueError,
        match='Invalid secret: invalid Base32 character at index 0',
    ):
        totp_verify('0', '123456')
