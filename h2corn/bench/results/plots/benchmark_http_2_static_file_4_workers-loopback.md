**Conclusion:** h2corn leads this throughput comparison; its 95% bootstrap interval is separated from the runner-up.

**Environment:** Python 3.14.8 | Linux 7.2.9 | x86_64; CPU: AMD Ryzen 9 5950X 16-Core Processor; loopback; 10s load after 2s warmup.

| Server (version) | Measure | Value | Observed rate range |
| --- | --- | --- | --- |
| h2corn (1.6.0) | Measured trials | 4 | - |
| h2corn (1.6.0) | Median rate (requests/s) | 26,095 | 22,396-28,122 |
| h2corn (1.6.0) | Maximum sampled PSS (MB) | 381.9 | - |
| h2corn (1.6.0) | Median p50 latency (ms) | 3.586 | - |
| h2corn (1.6.0) | Median p75 latency (ms) | 4.618 | - |
| h2corn (1.6.0) | Median p90 latency (ms) | 5.897 | - |
| h2corn (1.6.0) | Median p95 latency (ms) | 6.769 | - |
| h2corn (1.6.0) | Median p99 latency (ms) | 8.999 | - |
| h2corn (1.6.0) | Median p99.9 latency (ms) | 17.937 | - |
| hypercorn (0.18.0) | Measured trials | 4 | - |
| hypercorn (0.18.0) | Median rate (requests/s) | 2,927 | 2,893-3,004 |
| hypercorn (0.18.0) | Maximum sampled PSS (MB) | 278.3 | - |
| hypercorn (0.18.0) | Median p50 latency (ms) | 30.198 | - |
| hypercorn (0.18.0) | Median p75 latency (ms) | 39.816 | - |
| hypercorn (0.18.0) | Median p90 latency (ms) | 51.633 | - |
| hypercorn (0.18.0) | Median p95 latency (ms) | 56.476 | - |
| hypercorn (0.18.0) | Median p99 latency (ms) | 65.918 | - |
| hypercorn (0.18.0) | Median p99.9 latency (ms) | 86.236 | - |

Rates are medians across cold-start trials; the observed rate range is the trial minimum-maximum, not a confidence interval. A dash means no range is reported for that measure. Latency percentiles are medians across trials and have no interval shown. Peak proportional set size (PSS) is the maximum sampled value across the supervisor and workers; MB uses decimal units.
