# totp-rs

Fast [RFC 6238](https://datatracker.ietf.org/doc/html/rfc6238)-compliant TOTP implementation.

## Installation

```sh
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

Benchmarks use `window=1` (typical drift tolerance).

### Run

```sh
benchmark.py --impl totp-rs -o totp-rs.json --rigorous
benchmark.py --impl pyotp -o pyotp.json --rigorous
```

### Results

Linux x86_64, CPython 3.14.0:

| Benchmark      | pyotp-2.9.0 | totp-rs-1.0.0 |
|----------------|:-------:|:---------------------:|
| generate       | 17.6 us   | 466 ns: 37.85x faster |
| verify ok      | 36.1 us   | 435 ns: 82.91x faster |
| verify prev ok | 18.0 us   | 518 ns: 34.70x faster |
| verify bad     | 53.2 us   | 598 ns: 88.92x faster |
| Geometric mean | (ref)     | 55.78x faster         |

## Notes

- Implements RFC 6238 TOTP (HOTP + time counter).
- Constant-time code comparison.
- Uses RustCrypto implementations for HMAC/SHA-1/SHA-2.
