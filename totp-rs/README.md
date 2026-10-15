# totp-rs

Generate and verify time-based one-time passwords (TOTP) in Python using the
[RFC 6238](https://datatracker.ietf.org/doc/html/rfc6238) counter model.
Requires Python 3.10 or newer.

## Installation

```sh
pip install totp-rs
```

## Usage

```py
from totp_rs import totp_generate, totp_verify

secret = b'12345678901234567890'

code = totp_generate(
    secret,
    digits=8,
    algorithm='sha1',
    time=59,
    step_seconds=30,
    t0=0,
)
assert code == '94287082'

assert totp_verify(
    secret,
    code,
    digits=8,
    algorithm='sha1',
    time=59,
    step_seconds=30,
    t0=0,
    window=0,
)
```

This deterministic example uses the RFC 6238 SHA-1 test vector. For live
generation, omit `time` to use the current system time.

## Inputs and errors

- A secret is raw `bytes` or Base32 text. Base32 text is case-insensitive;
  ASCII whitespace and `=` are ignored.
- `algorithm` is exactly `sha1`, `sha256`, or `sha512`. `digits` must be from
  1 through 9.
- `time` is a Unix timestamp in seconds; finite values are floored to whole
  seconds. For a positive step, the counter is
  `floor((floor(time) - t0) / step_seconds)`. Negative nonzero steps are also
  accepted and use Euclidean integer division. `time_window` supplies the
  signed counter directly. They cannot be passed together. If both are
  omitted, generation and verification use the current system time.
- `step_seconds` is a nonzero signed 64-bit integer when deriving a counter;
  the default 30 seconds is the common RFC 6238 setting. Negative steps use
  Euclidean integer division. `t0` is the signed 64-bit counter origin in
  seconds. `time_window` supplies a signed 64-bit counter directly, in which
  case `step_seconds` and `t0` are unused.
- `totp_verify` checks the current counter first, then each previous and next
  counter in increasing drift distance. Its `window` is an unsigned-byte count;
  `window=1` accepts T-1, T, or T+1.
- A string code is parsed by keeping its ASCII decimal digits and ignoring
  other bytes. It must contain exactly the configured number of digits.
  Integer codes must be between zero and `10**digits - 1`. A malformed code
  returns `False`; an invalid secret raises `ValueError` only after the code
  has a valid shape.
- Invalid secrets, algorithms, digit counts, conflicting time arguments, a
  zero step when deriving a counter, and derived out-of-range counters raise
  `ValueError`. Typed numeric options must fit their declared native integer
  widths; incompatible types or oversized integers raise `TypeError` or
  `OverflowError`.
- Nonfinite and out-of-range floating timestamps are converted to signed
  64-bit seconds using Rust's saturating float-to-integer conversion: NaN
  becomes zero, and positive or negative infinity becomes the corresponding
  integer endpoint.

The code-byte comparison is constant-time. The full verification call is not:
validation and the drift search can return early. The API stores no replay
state and does not rate-limit attempts. Applications must track successful
counters and enforce their own attempt policy.

## Mutually exclusive time inputs

Use either a timestamp or a counter:

```py
from totp_rs import totp_generate

try:
    totp_generate(b'12345678901234567890', time=59, time_window=1)
except ValueError:
    pass
else:
    raise AssertionError('time and time_window cannot both be supplied')
```

## API reference and support

- The [`totp_time_window`, `totp_generate`, and `totp_verify` contracts](https://github.com/Zaczero/pkgs/blob/main/totp-rs/python/totp_rs/_lib.pyi)
  include their accepted inputs and errors.
- Report issues in the [repository issue tracker](https://github.com/Zaczero/pkgs/issues).
