**Conclusion:** h2corn leads this throughput comparison; its 95% bootstrap interval is separated from the runner-up.

**Environment:** Python 3.14.8 | Linux 7.2.9 | x86_64; CPU: AMD Ryzen 9 5950X 16-Core Processor; rtt50 (50.1 ms measured RTT); 10s load after 2s warmup.

| Server (version) | Measure | Value | Observed rate range |
| --- | --- | --- | --- |
| h2corn (1.6.0) | Measured trials | 4 | - |
| h2corn (1.6.0) | Median rate (requests/s) | 1,400 | 1,398-1,401 |
| h2corn (1.6.0) | Maximum sampled PSS (MB) | 111.7 | - |
| h2corn (1.6.0) | Median p50 latency (ms) | 71.119 | - |
| h2corn (1.6.0) | Median p75 latency (ms) | 71.322 | - |
| h2corn (1.6.0) | Median p90 latency (ms) | 71.562 | - |
| h2corn (1.6.0) | Median p95 latency (ms) | 71.703 | - |
| h2corn (1.6.0) | Median p99 latency (ms) | 72.071 | - |
| h2corn (1.6.0) | Median p99.9 latency (ms) | 72.512 | - |
| hypercorn (0.18.0) | Measured trials | 4 | - |
| hypercorn (0.18.0) | Median rate (requests/s) | 1,371 | 1,369-1,373 |
| hypercorn (0.18.0) | Maximum sampled PSS (MB) | 129.9 | - |
| hypercorn (0.18.0) | Median p50 latency (ms) | 72.520 | - |
| hypercorn (0.18.0) | Median p75 latency (ms) | 73.135 | - |
| hypercorn (0.18.0) | Median p90 latency (ms) | 73.924 | - |
| hypercorn (0.18.0) | Median p95 latency (ms) | 74.543 | - |
| hypercorn (0.18.0) | Median p99 latency (ms) | 76.939 | - |
| hypercorn (0.18.0) | Median p99.9 latency (ms) | 83.368 | - |

Rates are medians across cold-start trials; the observed rate range is the trial minimum-maximum, not a confidence interval. A dash means no range is reported for that measure. Latency percentiles are medians across trials and have no interval shown. Peak proportional set size (PSS) is the maximum sampled value across the supervisor and workers; MB uses decimal units.
