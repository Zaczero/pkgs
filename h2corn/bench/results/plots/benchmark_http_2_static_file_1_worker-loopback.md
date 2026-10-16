**Conclusion:** h2corn leads this throughput comparison; its 95% bootstrap interval is separated from the runner-up.

**Environment:** Python 3.14.8 | Linux 7.2.9 | x86_64; CPU: AMD Ryzen 9 5950X 16-Core Processor; loopback; 10s load after 2s warmup.

| Server (version) | Measure | Value | Observed rate range |
| --- | --- | --- | --- |
| h2corn (1.6.0) | Measured trials | 4 | - |
| h2corn (1.6.0) | Median rate (requests/s) | 9,629 | 9,382-10,583 |
| h2corn (1.6.0) | Maximum sampled PSS (MB) | 158.3 | - |
| h2corn (1.6.0) | Median p50 latency (ms) | 10.053 | - |
| h2corn (1.6.0) | Median p75 latency (ms) | 11.436 | - |
| h2corn (1.6.0) | Median p90 latency (ms) | 13.010 | - |
| h2corn (1.6.0) | Median p95 latency (ms) | 14.332 | - |
| h2corn (1.6.0) | Median p99 latency (ms) | 17.902 | - |
| h2corn (1.6.0) | Median p99.9 latency (ms) | 26.945 | - |
| hypercorn (0.18.0) | Measured trials | 4 | - |
| hypercorn (0.18.0) | Median rate (requests/s) | 821 | 781-901 |
| hypercorn (0.18.0) | Maximum sampled PSS (MB) | 136.4 | - |
| hypercorn (0.18.0) | Median p50 latency (ms) | 117.795 | - |
| hypercorn (0.18.0) | Median p75 latency (ms) | 124.905 | - |
| hypercorn (0.18.0) | Median p90 latency (ms) | 135.003 | - |
| hypercorn (0.18.0) | Median p95 latency (ms) | 141.318 | - |
| hypercorn (0.18.0) | Median p99 latency (ms) | 160.565 | - |
| hypercorn (0.18.0) | Median p99.9 latency (ms) | 178.167 | - |

Rates are medians across cold-start trials; the observed rate range is the trial minimum-maximum, not a confidence interval. A dash means no range is reported for that measure. Latency percentiles are medians across trials and have no interval shown. Peak proportional set size (PSS) is the maximum sampled value across the supervisor and workers; MB uses decimal units.
