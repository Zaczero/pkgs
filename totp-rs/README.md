# totp-rs

Fast [RFC 6238](https://datatracker.ietf.org/doc/html/rfc6238)-compliant TOTP implementation.

## Install

```bash
pip install totp-rs
```

## Usage

```py
from totp_rs import totp_generate, totp_verify

secret_b32 = "JBSWY3DPEHPK3PXP"

code = totp_generate(secret_b32)

assert totp_verify(secret_b32, code)
```

## Benchmarks

See `benchmark.py`.

Benchmarks use `window=1` (typical drift tolerance).

Linux x86_64, CPython 3.14, `pyperf --rigorous`:

```text
+-----------------+---------------+-------------+
| Benchmark       | totp-rs 1.0.0 | pyotp 2.9.0 |
+-----------------+---------------+-------------+
| generate        |      456.6 ns |   17.191 µs |
| verify ok       |      436.7 ns |   35.315 µs |
| verify prev ok  |      518.5 ns |   17.655 µs |
| verify bad      |      598.9 ns |   53.279 µs |
+-----------------+---------------+-------------+

+-----------------+------------------+
| Benchmark       | Speedup vs pyotp |
+-----------------+------------------+
| generate        |           37.65x |
| verify ok       |           80.87x |
| verify prev ok  |           34.05x |
| verify bad      |           88.96x |
+-----------------+------------------+
```

## Notes

- Implements RFC 6238 TOTP (HOTP + time counter).
- Constant-time code comparison.
- Uses RustCrypto implementations for HMAC/SHA-1/SHA-2.
