**Conclusion:** h2corn leads this throughput comparison; its 95% bootstrap interval is separated from the runner-up.

**Environment:** Python 3.14.8 | Linux 7.2.9 | x86_64; CPU: AMD Ryzen 9 5950X 16-Core Processor; loopback; 10s load after 2s warmup.

| Server (version) | Measure | Value | Observed rate range |
| --- | --- | --- | --- |
| h2corn (1.6.0) | Measured trials | 4 | - |
| h2corn (1.6.0) | Median rate (requests/s) | 97,193 | 86,628-101,128 |
| h2corn (1.6.0) | Maximum sampled PSS (MB) | 52.0 | - |
| h2corn (1.6.0) | Median p50 latency (ms) | 0.982 | - |
| h2corn (1.6.0) | Median p75 latency (ms) | 1.015 | - |
| h2corn (1.6.0) | Median p90 latency (ms) | 1.120 | - |
| h2corn (1.6.0) | Median p95 latency (ms) | 1.247 | - |
| h2corn (1.6.0) | Median p99 latency (ms) | 2.218 | - |
| h2corn (1.6.0) | Median p99.9 latency (ms) | 4.152 | - |
| hypercorn (0.18.0) | Measured trials | 4 | - |
| hypercorn (0.18.0) | Median rate (requests/s) | 2,423 | 2,369-2,499 |
| hypercorn (0.18.0) | Maximum sampled PSS (MB) | 58.1 | - |
| hypercorn (0.18.0) | Median p50 latency (ms) | 40.298 | - |
| hypercorn (0.18.0) | Median p75 latency (ms) | 42.429 | - |
| hypercorn (0.18.0) | Median p90 latency (ms) | 45.140 | - |
| hypercorn (0.18.0) | Median p95 latency (ms) | 50.891 | - |
| hypercorn (0.18.0) | Median p99 latency (ms) | 59.608 | - |
| hypercorn (0.18.0) | Median p99.9 latency (ms) | 66.038 | - |

Rates are medians across cold-start trials; the observed rate range is the trial minimum-maximum, not a confidence interval. A dash means no range is reported for that measure. Latency percentiles are medians across trials and have no interval shown. Peak proportional set size (PSS) is the maximum sampled value across the supervisor and workers; MB uses decimal units.
