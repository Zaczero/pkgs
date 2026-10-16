**Conclusion:** Throughput is close: the 95% bootstrap intervals do not separate a leader.

**Environment:** Python 3.14.8 | Linux 7.2.9 | x86_64; CPU: AMD Ryzen 9 5950X 16-Core Processor; rtt50 (50.1 ms measured RTT); 10s load after 2s warmup.

| Server (version) | Measure | Value | Observed rate range |
| --- | --- | --- | --- |
| hypercorn (0.18.0) | Measured trials | 4 | - |
| hypercorn (0.18.0) | Median rate (requests/s) | 790 | 754-848 |
| hypercorn (0.18.0) | Maximum sampled PSS (MB) | 135.6 | - |
| hypercorn (0.18.0) | Median p50 latency (ms) | 127.930 | - |
| hypercorn (0.18.0) | Median p75 latency (ms) | 134.093 | - |
| hypercorn (0.18.0) | Median p90 latency (ms) | 140.588 | - |
| hypercorn (0.18.0) | Median p95 latency (ms) | 146.108 | - |
| hypercorn (0.18.0) | Median p99 latency (ms) | 175.079 | - |
| hypercorn (0.18.0) | Median p99.9 latency (ms) | 183.406 | - |
| h2corn (1.6.0) | Measured trials | 4 | - |
| h2corn (1.6.0) | Median rate (requests/s) | 780 | 765-796 |
| h2corn (1.6.0) | Maximum sampled PSS (MB) | 116.0 | - |
| h2corn (1.6.0) | Median p50 latency (ms) | 126.014 | - |
| h2corn (1.6.0) | Median p75 latency (ms) | 133.114 | - |
| h2corn (1.6.0) | Median p90 latency (ms) | 157.579 | - |
| h2corn (1.6.0) | Median p95 latency (ms) | 172.608 | - |
| h2corn (1.6.0) | Median p99 latency (ms) | 189.725 | - |
| h2corn (1.6.0) | Median p99.9 latency (ms) | 205.358 | - |

Rates are medians across cold-start trials; the observed rate range is the trial minimum-maximum, not a confidence interval. A dash means no range is reported for that measure. Latency percentiles are medians across trials and have no interval shown. Peak proportional set size (PSS) is the maximum sampled value across the supervisor and workers; MB uses decimal units.
