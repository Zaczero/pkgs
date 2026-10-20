# totp-rs

Generate and verify time-based one-time passwords in Python, backed by Rust.
Supports [RFC 6238](https://datatracker.ietf.org/doc/html/rfc6238) with SHA-1,
SHA-256, or SHA-512. Python 3.11+.

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

**5–60× faster than PyOTP** at generating and verifying six-digit codes with
SHA-1, SHA-256, and SHA-512.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="benchmarks/results-dark.svg">
  <img src="benchmarks/results.svg" alt="Median time to generate and verify TOTP codes with SHA-1, SHA-256, and SHA-512.">
</picture>

| Algorithm / implementation | Generate current code | Verify current | Verify previous step | Reject incorrect |
| --- | ---: | ---: | ---: | ---: |
| SHA-1 · totp-rs 1.0.0 | 386 ns | 394 ns | 476 ns | 568 ns |
| SHA-1 · PyOTP 2.10.0 | 10.8 µs | 22.7 µs | 11.3 µs | 33.7 µs |
| SHA-256 · totp-rs 1.0.0 | 451 ns | 449 ns | 558 ns | 647 ns |
| SHA-256 · PyOTP 2.10.0 | 10.9 µs | 22.8 µs | 11.5 µs | 33.6 µs |
| SHA-512 · totp-rs 1.0.0 | 1.79 µs | 1.77 µs | 2.48 µs | 3.17 µs |
| SHA-512 · PyOTP 2.10.0 | 11.7 µs | 24.3 µs | 12.3 µs | 36.0 µs |

Median time per call on CPython 3.15.0, a Ryzen 9 5950X, and Linux 7.2.9. Both
use the same Base32 secret, timestamp, 30-second period, and ±1 window.

[Full results](benchmarks/results.md) · Raw PyPerf results:
[totp-rs](benchmarks/totp-rs.json), [PyOTP](benchmarks/pyotp.json) ·
[Benchmark source](benchmark.py)

## API reference and support

See the [`totp_time_window`, `totp_generate`, and `totp_verify` reference](https://github.com/Zaczero/pkgs/blob/main/totp-rs/python/totp_rs/_lib.pyi)
for parameters, defaults, and errors.

[Issues and questions](https://github.com/Zaczero/pkgs/issues) ·
[Private security reports](https://github.com/Zaczero/pkgs/security/advisories/new) ·
[Premium support](https://monicz.dev/#get-in-touch)
