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

Verification compares code bytes in constant time, but validation and the drift
search can return early. The library does not store replay state or rate-limit
attempts; enforce those in your application.

## API reference and support

See the [`totp_time_window`, `totp_generate`, and `totp_verify` reference](https://github.com/Zaczero/pkgs/blob/main/totp-rs/python/totp_rs/_lib.pyi)
for parameters, defaults, and errors.
[Report issues on GitHub](https://github.com/Zaczero/pkgs/issues).
