# TOTP generation and verification

AMD Ryzen 9 5950X 16-Core Processor · CPython 3.14.8 (64-bit)
SHA-1 · 6 digits · 30-second period · ±1 verification window
Independent worker-process means · setup excluded

Values in seconds; lower is better.

| Case | Implementation | Size | Median | 95% interval | Runs |
| --- | --- | ---: | ---: | --- | ---: |
| generate | totp-rs 1.0.0 | — | 4.184e-07 | 4.12e-07 to 4.273e-07 | 12 |
| verify ok | totp-rs 1.0.0 | — | 4.327e-07 | 4.235e-07 to 4.407e-07 | 12 |
| verify prev ok | totp-rs 1.0.0 | — | 5.246e-07 | 5.141e-07 to 5.31e-07 | 12 |
| verify bad | totp-rs 1.0.0 | — | 6.059e-07 | 5.913e-07 to 6.179e-07 | 12 |
| generate | pyotp 2.10.0 | — | 1.556e-05 | 1.451e-05 to 1.654e-05 | 12 |
| verify ok | pyotp 2.10.0 | — | 3.238e-05 | 2.959e-05 to 3.535e-05 | 12 |
| verify prev ok | pyotp 2.10.0 | — | 1.571e-05 | 1.357e-05 to 1.8e-05 | 12 |
| verify bad | pyotp 2.10.0 | — | 4.431e-05 | 4.184e-05 to 4.602e-05 | 12 |

Intervals use a percentile bootstrap over independent runs. A single run has no interval.
