**Conclusion:** h2corn leads this throughput comparison; its 95% bootstrap interval is separated from the runner-up.

**Environment:** Python 3.14.8 | Linux 7.2.9 | x86_64; CPU: AMD Ryzen 9 5950X 16-Core Processor; loopback; 10s load after 2s warmup.

| Server (version) | Measure | Value | Observed rate range |
| --- | --- | --- | --- |
| h2corn (1.6.0) | Measured trials | 4 | - |
| h2corn (1.6.0) | Median rate (requests/s) | 11,052 | 10,394-11,380 |
| h2corn (1.6.0) | Maximum sampled PSS (MB) | 162.4 | - |
| h2corn (1.6.0) | Median p50 latency (ms) | 8.839 | - |
| h2corn (1.6.0) | Median p75 latency (ms) | 10.068 | - |
| h2corn (1.6.0) | Median p90 latency (ms) | 11.419 | - |
| h2corn (1.6.0) | Median p95 latency (ms) | 12.527 | - |
| h2corn (1.6.0) | Median p99 latency (ms) | 15.548 | - |
| h2corn (1.6.0) | Median p99.9 latency (ms) | 19.924 | - |
| uvicorn (0.52.1) | Measured trials | 4 | - |
| uvicorn (0.52.1) | Median rate (requests/s) | 1,611 | 1,579-1,625 |
| uvicorn (0.52.1) | Maximum sampled PSS (MB) | 180.4 | - |
| uvicorn (0.52.1) | Median p50 latency (ms) | 61.681 | - |
| uvicorn (0.52.1) | Median p75 latency (ms) | 64.356 | - |
| uvicorn (0.52.1) | Median p90 latency (ms) | 68.530 | - |
| uvicorn (0.52.1) | Median p95 latency (ms) | 70.427 | - |
| uvicorn (0.52.1) | Median p99 latency (ms) | 74.622 | - |
| uvicorn (0.52.1) | Median p99.9 latency (ms) | 81.615 | - |
| hypercorn (0.18.0) | Measured trials | 4 | - |
| hypercorn (0.18.0) | Median rate (requests/s) | 1,285 | 1,236-1,289 |
| hypercorn (0.18.0) | Maximum sampled PSS (MB) | 117.3 | - |
| hypercorn (0.18.0) | Median p50 latency (ms) | 77.108 | - |
| hypercorn (0.18.0) | Median p75 latency (ms) | 80.912 | - |
| hypercorn (0.18.0) | Median p90 latency (ms) | 85.105 | - |
| hypercorn (0.18.0) | Median p95 latency (ms) | 88.234 | - |
| hypercorn (0.18.0) | Median p99 latency (ms) | 97.440 | - |
| hypercorn (0.18.0) | Median p99.9 latency (ms) | 116.234 | - |
| gunicorn (26.0.0) | Measured trials | 4 | - |
| gunicorn (26.0.0) | Median rate (requests/s) | 20 | 20-20 |
| gunicorn (26.0.0) | Maximum sampled PSS (MB) | 75.3 | - |
| gunicorn (26.0.0) | Median p50 latency (ms) | 32.301 | - |
| gunicorn (26.0.0) | Median p75 latency (ms) | 33.697 | - |
| gunicorn (26.0.0) | Median p90 latency (ms) | 40.617 | - |
| gunicorn (26.0.0) | Median p95 latency (ms) | 47.176 | - |
| gunicorn (26.0.0) | Median p99 latency (ms) | 50.749 | - |
| gunicorn (26.0.0) | Median p99.9 latency (ms) | 50.749 | - |

Rates are medians across cold-start trials; the observed rate range is the trial minimum-maximum, not a confidence interval. A dash means no range is reported for that measure. Latency percentiles are medians across trials and have no interval shown. Peak proportional set size (PSS) is the maximum sampled value across the supervisor and workers; MB uses decimal units.
