**Conclusion:** h2corn leads this throughput comparison; its 95% bootstrap interval is separated from the runner-up.

**Environment:** Python 3.14.8 | Linux 7.2.9 | x86_64; CPU: AMD Ryzen 9 5950X 16-Core Processor; loopback; 10s load after 2s warmup.

| Server (version) | Measure | Value | Observed rate range |
| --- | --- | --- | --- |
| h2corn (1.6.0) | Measured trials | 4 | - |
| h2corn (1.6.0) | Median rate (requests/s) | 20,238 | 17,877-21,009 |
| h2corn (1.6.0) | Maximum sampled PSS (MB) | 60.5 | - |
| h2corn (1.6.0) | Median p50 latency (ms) | 4.742 | - |
| h2corn (1.6.0) | Median p75 latency (ms) | 5.181 | - |
| h2corn (1.6.0) | Median p90 latency (ms) | 5.760 | - |
| h2corn (1.6.0) | Median p95 latency (ms) | 6.740 | - |
| h2corn (1.6.0) | Median p99 latency (ms) | 12.782 | - |
| h2corn (1.6.0) | Median p99.9 latency (ms) | 20.502 | - |
| hypercorn (0.18.0) | Measured trials | 4 | - |
| hypercorn (0.18.0) | Median rate (requests/s) | 1,084 | 1,076-1,140 |
| hypercorn (0.18.0) | Maximum sampled PSS (MB) | 61.6 | - |
| hypercorn (0.18.0) | Median p50 latency (ms) | 91.172 | - |
| hypercorn (0.18.0) | Median p75 latency (ms) | 95.541 | - |
| hypercorn (0.18.0) | Median p90 latency (ms) | 102.081 | - |
| hypercorn (0.18.0) | Median p95 latency (ms) | 106.292 | - |
| hypercorn (0.18.0) | Median p99 latency (ms) | 118.129 | - |
| hypercorn (0.18.0) | Median p99.9 latency (ms) | 130.910 | - |

Rates are medians across cold-start trials; the observed rate range is the trial minimum-maximum, not a confidence interval. A dash means no range is reported for that measure. Latency percentiles are medians across trials and have no interval shown. Peak proportional set size (PSS) is the maximum sampled value across the supervisor and workers; MB uses decimal units.
