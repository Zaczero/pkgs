**Conclusion:** h2corn leads this throughput comparison; its 95% bootstrap interval is separated from the runner-up.

**Environment:** Python 3.14.8 | Linux 7.2.9 | x86_64; CPU: AMD Ryzen 9 5950X 16-Core Processor; loopback; 10s load after 2s warmup.

| Server (version) | Measure | Value | Observed rate range |
| --- | --- | --- | --- |
| h2corn (1.6.0) | Measured trials | 4 | - |
| h2corn (1.6.0) | Median rate (requests/s) | 16,482 | 15,116-17,552 |
| h2corn (1.6.0) | Maximum sampled PSS (MB) | 115.3 | - |
| h2corn (1.6.0) | Median p50 latency (ms) | 57.072 | - |
| h2corn (1.6.0) | Median p75 latency (ms) | 61.447 | - |
| h2corn (1.6.0) | Median p90 latency (ms) | 66.791 | - |
| h2corn (1.6.0) | Median p95 latency (ms) | 70.297 | - |
| h2corn (1.6.0) | Median p99 latency (ms) | 81.047 | - |
| h2corn (1.6.0) | Median p99.9 latency (ms) | 565.602 | - |
| hypercorn (0.18.0) | Measured trials | 4 | - |
| hypercorn (0.18.0) | Median rate (requests/s) | 1,299 | 1,295-1,390 |
| hypercorn (0.18.0) | Maximum sampled PSS (MB) | 148.9 | - |
| hypercorn (0.18.0) | Median p50 latency (ms) | 767.249 | - |
| hypercorn (0.18.0) | Median p75 latency (ms) | 799.149 | - |
| hypercorn (0.18.0) | Median p90 latency (ms) | 889.569 | - |
| hypercorn (0.18.0) | Median p95 latency (ms) | 1,117.605 | - |
| hypercorn (0.18.0) | Median p99 latency (ms) | 1,171.820 | - |
| hypercorn (0.18.0) | Median p99.9 latency (ms) | 1,278.050 | - |

Rates are medians across cold-start trials; the observed rate range is the trial minimum-maximum, not a confidence interval. A dash means no range is reported for that measure. Latency percentiles are medians across trials and have no interval shown. Peak proportional set size (PSS) is the maximum sampled value across the supervisor and workers; MB uses decimal units.
