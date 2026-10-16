**Conclusion:** h2corn leads this throughput comparison; its 95% bootstrap interval is separated from the runner-up.

**Environment:** Python 3.14.8 | Linux 7.2.9 | x86_64; CPU: AMD Ryzen 9 5950X 16-Core Processor; loopback; 10s load after 2s warmup.

| Server (version) | Measure | Value | Observed rate range |
| --- | --- | --- | --- |
| h2corn (1.6.0) | Measured trials | 4 | - |
| h2corn (1.6.0) | Median rate (requests/s) | 43,794 | 42,332-45,542 |
| h2corn (1.6.0) | Maximum sampled PSS (MB) | 121.2 | - |
| h2corn (1.6.0) | Median p50 latency (ms) | 2.203 | - |
| h2corn (1.6.0) | Median p75 latency (ms) | 2.879 | - |
| h2corn (1.6.0) | Median p90 latency (ms) | 3.360 | - |
| h2corn (1.6.0) | Median p95 latency (ms) | 3.689 | - |
| h2corn (1.6.0) | Median p99 latency (ms) | 4.377 | - |
| h2corn (1.6.0) | Median p99.9 latency (ms) | 5.994 | - |
| gunicorn (26.0.0) | Measured trials | 4 | - |
| gunicorn (26.0.0) | Median rate (requests/s) | 13,938 | 13,737-14,418 |
| gunicorn (26.0.0) | Maximum sampled PSS (MB) | 89.6 | - |
| gunicorn (26.0.0) | Median p50 latency (ms) | 6.715 | - |
| gunicorn (26.0.0) | Median p75 latency (ms) | 9.537 | - |
| gunicorn (26.0.0) | Median p90 latency (ms) | 10.811 | - |
| gunicorn (26.0.0) | Median p95 latency (ms) | 11.635 | - |
| gunicorn (26.0.0) | Median p99 latency (ms) | 13.300 | - |
| gunicorn (26.0.0) | Median p99.9 latency (ms) | 16.848 | - |
| hypercorn (0.18.0) | Measured trials | 4 | - |
| hypercorn (0.18.0) | Median rate (requests/s) | 6,051 | 5,997-6,323 |
| hypercorn (0.18.0) | Maximum sampled PSS (MB) | 129.8 | - |
| hypercorn (0.18.0) | Median p50 latency (ms) | 15.758 | - |
| hypercorn (0.18.0) | Median p75 latency (ms) | 22.619 | - |
| hypercorn (0.18.0) | Median p90 latency (ms) | 26.505 | - |
| hypercorn (0.18.0) | Median p95 latency (ms) | 28.466 | - |
| hypercorn (0.18.0) | Median p99 latency (ms) | 31.925 | - |
| hypercorn (0.18.0) | Median p99.9 latency (ms) | 36.835 | - |
| uvicorn (0.52.1) | Measured trials | 4 | - |
| uvicorn (0.52.1) | Median rate (requests/s) | 2,318 | 2,295-2,340 |
| uvicorn (0.52.1) | Maximum sampled PSS (MB) | 115.2 | - |
| uvicorn (0.52.1) | Median p50 latency (ms) | 42.977 | - |
| uvicorn (0.52.1) | Median p75 latency (ms) | 44.930 | - |
| uvicorn (0.52.1) | Median p90 latency (ms) | 46.095 | - |
| uvicorn (0.52.1) | Median p95 latency (ms) | 47.496 | - |
| uvicorn (0.52.1) | Median p99 latency (ms) | 49.480 | - |
| uvicorn (0.52.1) | Median p99.9 latency (ms) | 56.377 | - |

Rates are medians across cold-start trials; the observed rate range is the trial minimum-maximum, not a confidence interval. A dash means no range is reported for that measure. Latency percentiles are medians across trials and have no interval shown. Peak proportional set size (PSS) is the maximum sampled value across the supervisor and workers; MB uses decimal units.
