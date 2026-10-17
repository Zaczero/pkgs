# totp-rs

Generate and verify time-based one-time passwords in Python, backed by Rust.
Supports [RFC 6238](https://datatracker.ietf.org/doc/html/rfc6238) with SHA-1,
SHA-256, or SHA-512. Python 3.10+.

## Installation

```sh
pip install totp-rs
```

## Usage

```py
from totp_rs import totp_generate, totp_verify

# Example secret; provision a unique random secret for each account.
secret = b'12345678901234567890'
code = totp_generate(secret)
print(code)  # Changes with the current time.
valid = totp_verify(secret, code, window=1)
```

Secrets may be raw bytes or Base32 text. `window=1` accepts the previous,
current, or next time step to allow for clock drift. Choose the algorithm,
number of digits, and step length to match the authenticator.

<a id="inputs-and-errors"></a>
<a id="mutually-exclusive-time-inputs"></a>

## Explicit timestamps

Pass `time` for a fixed Unix timestamp, or `time_window` for a counter directly;
the two are mutually exclusive. This example reproduces the RFC's SHA-1 test
vector:

```py
from totp_rs import totp_generate

code = totp_generate(b'12345678901234567890', digits=8, time=59)
print(code)  # 94287082
```

Timestamps must be finite and fit signed 64-bit whole seconds after flooring;
negative timestamps are supported. Deriving a counter requires a positive
step length. An explicit `time_window` uses the supplied counter directly and
ignores the step length and origin.

Verification compares code bytes in constant time, but validation and the drift
search can return early. The library does not store replay state or rate-limit
attempts; enforce those in your application.

## Benchmarks

**30–75× faster than PyOTP** across these generation and verification cases,
with median calls below one microsecond.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="benchmarks/results-dark.svg">
  <img src="benchmarks/results.svg" alt="TOTP generation and verification: totp-rs medians range from 0.42 to 0.61 microseconds, compared with 15.6 to 44.3 microseconds for PyOTP.">
</picture>

<details>
<summary>Detailed results</summary>

| Operation | totp-rs | PyOTP |
| --- | ---: | ---: |
| Generate | 0.42 µs | 15.6 µs |
| Verify current code | 0.43 µs | 32.4 µs |
| Verify previous code | 0.52 µs | 15.7 µs |
| Reject incorrect code | 0.61 µs | 44.3 µs |

</details>

Measured with totp-rs 1.0.0 and PyOTP 2.10.0 on CPython 3.14.8, Linux 7.2.9,
and a Ryzen 9 5950X shared workstation. Both use the same Base32 secret,
timestamp, SHA-1, six digits, 30-second period, and ±1 verification window.
Values are medians of twelve worker-process means, with three readings after
warmup in each process; six-process batches run in A–B–B–A order.
[Intervals and full table](benchmarks/results.md), raw results
([totp-rs](benchmarks/totp-rs.json), [PyOTP](benchmarks/pyotp.json)), and the
[benchmark](benchmark.py) retain the measurement details.

## API reference and support

See the [`totp_time_window`, `totp_generate`, and `totp_verify` reference](https://github.com/Zaczero/pkgs/blob/main/totp-rs/python/totp_rs/_lib.pyi)
for parameters, defaults, and errors.

[Issues and questions](https://github.com/Zaczero/pkgs/issues) ·
[Private security reports](https://github.com/Zaczero/pkgs/security/advisories/new) ·
[Premium support](https://monicz.dev/#get-in-touch)
