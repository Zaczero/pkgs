# TOTP generation and verification

AMD Ryzen 9 5950X 16-Core Processor · CPython 3.15.0 (64-bit)

Fixed timestamp · Six digits · 30-second period · verification window ±1.
Median time per call; lower is faster.

## Code generation and verification

| Workload | totp-rs 2.0.0 | pyotp 2.10.0 |
| --- | ---: | ---: |
| Generate current code · SHA-1 | 396 ns | 10.9 µs |
| Verify current code · SHA-1 | 455 ns | 23 µs |
| Verify previous-step code · SHA-1 | 509 ns | 11.3 µs |
| Reject incorrect code · SHA-1 | 592 ns | 34.5 µs |
| Generate current code · SHA-256 | 436 ns | 11 µs |
| Verify current code · SHA-256 | 459 ns | 23.3 µs |
| Verify previous-step code · SHA-256 | 550 ns | 12.4 µs |
| Reject incorrect code · SHA-256 | 651 ns | 37.1 µs |
| Generate current code · SHA-512 | 1.82 µs | 13.2 µs |
| Verify current code · SHA-512 | 1.73 µs | 24.6 µs |
| Verify previous-step code · SHA-512 | 2.47 µs | 11.8 µs |
| Reject incorrect code · SHA-512 | 3.16 µs | 35 µs |

## Raw-key and time-counter calls · totp-rs 2.0.0

| Workload | Median time per call |
| --- | ---: |
| Generate from raw key · SHA-1 | 299 ns |
| Generate from time counter · SHA-1 | 268 ns |
| Generate from raw key · SHA-256 | 336 ns |
| Generate from time counter · SHA-256 | 295 ns |
| Generate from raw key · SHA-512 | 1.55 µs |
| Generate from time counter · SHA-512 | 1.5 µs |
| Extract time counter | 68.4 ns |
