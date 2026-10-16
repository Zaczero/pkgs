**Conclusion:** Only h2corn produced an eligible result, so no cross-server comparison is available.

**Environment:** Python 3.14.8 | Linux 7.2.9 | x86_64; CPU: AMD Ryzen 9 5950X 16-Core Processor; rtt50 (50.1 ms measured RTT); 10s load after 2s warmup.

| Server (version) | Measure | Value | Observed rate range |
| --- | --- | --- | --- |
| h2corn (1.6.0) | Measured trials | 4 | - |
| h2corn (1.6.0) | Median rate (requests/s) | 1,975 | 1,959-1,977 |
| h2corn (1.6.0) | Maximum sampled PSS (MB) | 50.1 | - |
| h2corn (1.6.0) | Median p50 latency (ms) | 50.281 | - |
| h2corn (1.6.0) | Median p75 latency (ms) | 50.388 | - |
| h2corn (1.6.0) | Median p90 latency (ms) | 50.587 | - |
| h2corn (1.6.0) | Median p95 latency (ms) | 50.810 | - |
| h2corn (1.6.0) | Median p99 latency (ms) | 51.337 | - |
| h2corn (1.6.0) | Median p99.9 latency (ms) | 53.988 | - |

Rates are medians across cold-start trials; the observed rate range is the trial minimum-maximum, not a confidence interval. A dash means no range is reported for that measure. Latency percentiles are medians across trials and have no interval shown. Peak proportional set size (PSS) is the maximum sampled value across the supervisor and workers; MB uses decimal units.

**Excluded servers:** hypercorn: oha reported request errors: {'aborted due to deadline': 100, 'connection error': 7, 'error reading a body from connection': 2, 'http2 error': 91}
