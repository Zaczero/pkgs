# TOTP generation and verification

AMD Ryzen 9 5950X 16-Core Processor · Linux-7.2.9-x86_64-with-glibc2.42 · cpython 3.15.0 (64-bit)
Fixed timestamp · SHA-1, SHA-256, and SHA-512 · six digits · 30-second period · verification window ±1
12 independent worker-process means per workload

Intervals are 95% percentile bootstrap intervals with 10,000 resamples across worker-process means.

## Matched PyOTP comparison

| Workload | Implementation | Median time | 95% interval | Worker processes |
| --- | --- | ---: | ---: | ---: |
| Generate current code · SHA-1 | totp-rs 1.0.0 | 386 ns | 382 ns to 393 ns | 12 |
| Generate current code · SHA-1 | pyotp 2.10.0 | 10.8 µs | 10.7 µs to 11 µs | 12 |
| Verify current code · SHA-1 | totp-rs 1.0.0 | 394 ns | 387 ns to 402 ns | 12 |
| Verify current code · SHA-1 | pyotp 2.10.0 | 22.7 µs | 22.5 µs to 22.9 µs | 12 |
| Verify previous-step code · SHA-1 | totp-rs 1.0.0 | 476 ns | 473 ns to 480 ns | 12 |
| Verify previous-step code · SHA-1 | pyotp 2.10.0 | 11.3 µs | 11.3 µs to 11.5 µs | 12 |
| Reject incorrect code · SHA-1 | totp-rs 1.0.0 | 568 ns | 561 ns to 578 ns | 12 |
| Reject incorrect code · SHA-1 | pyotp 2.10.0 | 33.7 µs | 33.5 µs to 34.4 µs | 12 |
| Generate current code · SHA-256 | totp-rs 1.0.0 | 451 ns | 445 ns to 460 ns | 12 |
| Generate current code · SHA-256 | pyotp 2.10.0 | 10.9 µs | 10.7 µs to 11.2 µs | 12 |
| Verify current code · SHA-256 | totp-rs 1.0.0 | 449 ns | 448 ns to 455 ns | 12 |
| Verify current code · SHA-256 | pyotp 2.10.0 | 22.8 µs | 22.1 µs to 22.9 µs | 12 |
| Verify previous-step code · SHA-256 | totp-rs 1.0.0 | 558 ns | 552 ns to 567 ns | 12 |
| Verify previous-step code · SHA-256 | pyotp 2.10.0 | 11.5 µs | 11.3 µs to 11.6 µs | 12 |
| Reject incorrect code · SHA-256 | totp-rs 1.0.0 | 647 ns | 644 ns to 665 ns | 12 |
| Reject incorrect code · SHA-256 | pyotp 2.10.0 | 33.6 µs | 33.4 µs to 34.7 µs | 12 |
| Generate current code · SHA-512 | totp-rs 1.0.0 | 1.79 µs | 1.77 µs to 1.83 µs | 12 |
| Generate current code · SHA-512 | pyotp 2.10.0 | 11.7 µs | 11.7 µs to 12.1 µs | 12 |
| Verify current code · SHA-512 | totp-rs 1.0.0 | 1.77 µs | 1.75 µs to 1.79 µs | 12 |
| Verify current code · SHA-512 | pyotp 2.10.0 | 24.3 µs | 23.9 µs to 25 µs | 12 |
| Verify previous-step code · SHA-512 | totp-rs 1.0.0 | 2.48 µs | 2.45 µs to 2.51 µs | 12 |
| Verify previous-step code · SHA-512 | pyotp 2.10.0 | 12.3 µs | 12.2 µs to 12.5 µs | 12 |
| Reject incorrect code · SHA-512 | totp-rs 1.0.0 | 3.17 µs | 3.15 µs to 3.19 µs | 12 |
| Reject incorrect code · SHA-512 | pyotp 2.10.0 | 36 µs | 35.7 µs to 36.3 µs | 12 |

## Native-only input forms

| Workload | Implementation | Median time | 95% interval | Worker processes |
| --- | --- | ---: | ---: | ---: |
| Generate from raw key · SHA-1 | totp-rs 1.0.0 | 326 ns | 319 ns to 332 ns | 12 |
| Generate from time counter · SHA-1 | totp-rs 1.0.0 | 284 ns | 281 ns to 287 ns | 12 |
| Generate from raw key · SHA-256 | totp-rs 1.0.0 | 340 ns | 334 ns to 354 ns | 12 |
| Generate from time counter · SHA-256 | totp-rs 1.0.0 | 303 ns | 299 ns to 308 ns | 12 |
| Generate from raw key · SHA-512 | totp-rs 1.0.0 | 1.58 µs | 1.55 µs to 1.6 µs | 12 |
| Generate from time counter · SHA-512 | totp-rs 1.0.0 | 1.53 µs | 1.5 µs to 1.55 µs | 12 |
| Extract time counter | totp-rs 1.0.0 | 70.1 ns | 69.7 ns to 70.9 ns | 12 |

Raw-key generation, generation from a precomputed time counter, and time-counter extraction have no PyOTP row and are not included in the speed comparison.
