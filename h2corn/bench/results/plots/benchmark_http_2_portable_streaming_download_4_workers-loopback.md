**Conclusion:** h2corn leads this throughput comparison; its 95% bootstrap interval is separated from the runner-up.

**Environment:** Python 3.14.8 | Linux 7.2.9 | x86_64; CPU: AMD Ryzen 9 5950X 16-Core Processor; loopback; 10s load after 2s warmup.

| Server (version) | Measure | Value | Observed rate range |
| --- | --- | --- | --- |
| h2corn (1.6.0) | Measured trials | 4 | - |
| h2corn (1.6.0) | Median rate (requests/s) | 45,908 | 44,226-48,103 |
| h2corn (1.6.0) | Maximum sampled PSS (MB) | 126.6 | - |
| h2corn (1.6.0) | Median p50 latency (ms) | 2.010 | - |
| h2corn (1.6.0) | Median p75 latency (ms) | 2.430 | - |
| h2corn (1.6.0) | Median p90 latency (ms) | 2.858 | - |
| h2corn (1.6.0) | Median p95 latency (ms) | 3.145 | - |
| h2corn (1.6.0) | Median p99 latency (ms) | 4.232 | - |
| h2corn (1.6.0) | Median p99.9 latency (ms) | 13.890 | - |
| hypercorn (0.18.0) | Measured trials | 4 | - |
| hypercorn (0.18.0) | Median rate (requests/s) | 4,190 | 4,133-4,302 |
| hypercorn (0.18.0) | Maximum sampled PSS (MB) | 142.7 | - |
| hypercorn (0.18.0) | Median p50 latency (ms) | 22.916 | - |
| hypercorn (0.18.0) | Median p75 latency (ms) | 32.401 | - |
| hypercorn (0.18.0) | Median p90 latency (ms) | 35.346 | - |
| hypercorn (0.18.0) | Median p95 latency (ms) | 36.764 | - |
| hypercorn (0.18.0) | Median p99 latency (ms) | 40.032 | - |
| hypercorn (0.18.0) | Median p99.9 latency (ms) | 54.118 | - |

Rates are medians across cold-start trials; the observed rate range is the trial minimum-maximum, not a confidence interval. A dash means no range is reported for that measure. Latency percentiles are medians across trials and have no interval shown. Peak proportional set size (PSS) is the maximum sampled value across the supervisor and workers; MB uses decimal units.
