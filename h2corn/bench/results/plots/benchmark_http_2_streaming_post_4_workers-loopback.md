**Conclusion:** h2corn leads this throughput comparison; its 95% bootstrap interval is separated from the runner-up.

**Environment:** Python 3.14.8 | Linux 7.2.9 | x86_64; CPU: AMD Ryzen 9 5950X 16-Core Processor; loopback; 10s load after 2s warmup.

| Server (version) | Measure | Value | Observed rate range |
| --- | --- | --- | --- |
| h2corn (1.6.0) | Measured trials | 4 | - |
| h2corn (1.6.0) | Median rate (requests/s) | 43,746 | 43,533-44,345 |
| h2corn (1.6.0) | Maximum sampled PSS (MB) | 181.4 | - |
| h2corn (1.6.0) | Median p50 latency (ms) | 22.261 | - |
| h2corn (1.6.0) | Median p75 latency (ms) | 23.131 | - |
| h2corn (1.6.0) | Median p90 latency (ms) | 24.496 | - |
| h2corn (1.6.0) | Median p95 latency (ms) | 25.528 | - |
| h2corn (1.6.0) | Median p99 latency (ms) | 28.850 | - |
| h2corn (1.6.0) | Median p99.9 latency (ms) | 34.753 | - |
| hypercorn (0.18.0) | Measured trials | 4 | - |
| hypercorn (0.18.0) | Median rate (requests/s) | 4,596 | 4,044-4,788 |
| hypercorn (0.18.0) | Maximum sampled PSS (MB) | 232.2 | - |
| hypercorn (0.18.0) | Median p50 latency (ms) | 175.979 | - |
| hypercorn (0.18.0) | Median p75 latency (ms) | 391.432 | - |
| hypercorn (0.18.0) | Median p90 latency (ms) | 455.020 | - |
| hypercorn (0.18.0) | Median p95 latency (ms) | 484.603 | - |
| hypercorn (0.18.0) | Median p99 latency (ms) | 526.119 | - |
| hypercorn (0.18.0) | Median p99.9 latency (ms) | 564.368 | - |

Rates are medians across cold-start trials; the observed rate range is the trial minimum-maximum, not a confidence interval. A dash means no range is reported for that measure. Latency percentiles are medians across trials and have no interval shown. Peak proportional set size (PSS) is the maximum sampled value across the supervisor and workers; MB uses decimal units.
