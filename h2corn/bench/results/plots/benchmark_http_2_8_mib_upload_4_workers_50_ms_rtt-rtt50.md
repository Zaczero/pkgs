**Conclusion:** Only h2corn produced an eligible result, so no cross-server comparison is available.

**Environment:** Python 3.14.8 | Linux 7.2.9 | x86_64; CPU: AMD Ryzen 9 5950X 16-Core Processor; rtt50 (50.1 ms measured RTT); 10s load after 2s warmup.

| Server (version) | Measure | Value | Observed rate range |
| --- | --- | --- | --- |
| h2corn (1.6.0) | Measured trials | 4 | - |
| h2corn (1.6.0) | Median rate (requests/s) | 14 | 14-14 |
| h2corn (1.6.0) | Maximum sampled PSS (MB) | 117.0 | - |
| h2corn (1.6.0) | Median p50 latency (ms) | 490.416 | - |
| h2corn (1.6.0) | Median p75 latency (ms) | 558.711 | - |
| h2corn (1.6.0) | Median p90 latency (ms) | 840.707 | - |
| h2corn (1.6.0) | Median p95 latency (ms) | 1,017.800 | - |
| h2corn (1.6.0) | Median p99 latency (ms) | 2,225.521 | - |
| h2corn (1.6.0) | Median p99.9 latency (ms) | 3,409.461 | - |

Rates are medians across cold-start trials; the observed rate range is the trial minimum-maximum, not a confidence interval. A dash means no range is reported for that measure. Latency percentiles are medians across trials and have no interval shown. Peak proportional set size (PSS) is the maximum sampled value across the supervisor and workers; MB uses decimal units.

**Excluded servers:** hypercorn: oha saw non-200 responses: {}
