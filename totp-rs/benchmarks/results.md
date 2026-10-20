# TOTP generation and verification

AMD Ryzen 9 5950X 16-Core Processor · CPython 3.15.0 (64-bit)

Fixed timestamp · Six digits · 30-second period · verification window ±1.
Median time per call; lower is faster.

## Code generation and verification

| Workload | totp-rs 1.0.0 | pyotp 2.10.0 |
| --- | ---: | ---: |
| Generate current code · SHA-1 | 386 ns | 10.8 µs |
| Verify current code · SHA-1 | 394 ns | 22.7 µs |
| Verify previous-step code · SHA-1 | 476 ns | 11.3 µs |
| Reject incorrect code · SHA-1 | 568 ns | 33.7 µs |
| Generate current code · SHA-256 | 451 ns | 10.9 µs |
| Verify current code · SHA-256 | 449 ns | 22.8 µs |
| Verify previous-step code · SHA-256 | 558 ns | 11.5 µs |
| Reject incorrect code · SHA-256 | 647 ns | 33.6 µs |
| Generate current code · SHA-512 | 1.79 µs | 11.7 µs |
| Verify current code · SHA-512 | 1.77 µs | 24.3 µs |
| Verify previous-step code · SHA-512 | 2.48 µs | 12.3 µs |
| Reject incorrect code · SHA-512 | 3.17 µs | 36 µs |

## Raw-key and time-counter calls · totp-rs 1.0.0

| Workload | Median time per call |
| --- | ---: |
| Generate from raw key · SHA-1 | 326 ns |
| Generate from time counter · SHA-1 | 284 ns |
| Generate from raw key · SHA-256 | 340 ns |
| Generate from time counter · SHA-256 | 303 ns |
| Generate from raw key · SHA-512 | 1.58 µs |
| Generate from time counter · SHA-512 | 1.53 µs |
| Extract time counter | 70.1 ns |
