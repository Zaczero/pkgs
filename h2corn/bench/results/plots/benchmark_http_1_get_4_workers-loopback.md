**Conclusion:** h2corn leads this throughput comparison; its 95% bootstrap interval is separated from the runner-up.

**Environment:** Python 3.14.8 | Linux 7.2.9 | x86_64; CPU: AMD Ryzen 9 5950X 16-Core Processor; loopback; 10s load after 2s warmup.

| Server (version) | Measure | Value | Observed rate range |
| --- | --- | --- | --- |
| h2corn (1.6.0) | Measured trials | 4 | - |
| h2corn (1.6.0) | Median rate (requests/s) | 221,204 | 205,617-226,951 |
| h2corn (1.6.0) | Maximum sampled PSS (MB) | 109.6 | - |
| h2corn (1.6.0) | Median p50 latency (ms) | 0.417 | - |
| h2corn (1.6.0) | Median p75 latency (ms) | 0.562 | - |
| h2corn (1.6.0) | Median p90 latency (ms) | 0.706 | - |
| h2corn (1.6.0) | Median p95 latency (ms) | 0.779 | - |
| h2corn (1.6.0) | Median p99 latency (ms) | 0.934 | - |
| h2corn (1.6.0) | Median p99.9 latency (ms) | 1.579 | - |
| gunicorn (26.0.0) | Measured trials | 4 | - |
| gunicorn (26.0.0) | Median rate (requests/s) | 39,040 | 38,472-40,614 |
| gunicorn (26.0.0) | Maximum sampled PSS (MB) | 82.3 | - |
| gunicorn (26.0.0) | Median p50 latency (ms) | 2.633 | - |
| gunicorn (26.0.0) | Median p75 latency (ms) | 3.163 | - |
| gunicorn (26.0.0) | Median p90 latency (ms) | 3.427 | - |
| gunicorn (26.0.0) | Median p95 latency (ms) | 3.673 | - |
| gunicorn (26.0.0) | Median p99 latency (ms) | 4.472 | - |
| gunicorn (26.0.0) | Median p99.9 latency (ms) | 6.119 | - |
| hypercorn (0.18.0) | Measured trials | 4 | - |
| hypercorn (0.18.0) | Median rate (requests/s) | 12,632 | 12,344-12,789 |
| hypercorn (0.18.0) | Maximum sampled PSS (MB) | 122.3 | - |
| hypercorn (0.18.0) | Median p50 latency (ms) | 8.037 | - |
| hypercorn (0.18.0) | Median p75 latency (ms) | 12.087 | - |
| hypercorn (0.18.0) | Median p90 latency (ms) | 13.407 | - |
| hypercorn (0.18.0) | Median p95 latency (ms) | 14.039 | - |
| hypercorn (0.18.0) | Median p99 latency (ms) | 15.319 | - |
| hypercorn (0.18.0) | Median p99.9 latency (ms) | 17.877 | - |
| uvicorn (0.52.1) | Measured trials | 4 | - |
| uvicorn (0.52.1) | Median rate (requests/s) | 2,435 | 2,433-2,437 |
| uvicorn (0.52.1) | Maximum sampled PSS (MB) | 110.0 | - |
| uvicorn (0.52.1) | Median p50 latency (ms) | 41.004 | - |
| uvicorn (0.52.1) | Median p75 latency (ms) | 41.042 | - |
| uvicorn (0.52.1) | Median p90 latency (ms) | 41.997 | - |
| uvicorn (0.52.1) | Median p95 latency (ms) | 42.016 | - |
| uvicorn (0.52.1) | Median p99 latency (ms) | 42.496 | - |
| uvicorn (0.52.1) | Median p99.9 latency (ms) | 44.501 | - |

Rates are medians across cold-start trials; the observed rate range is the trial minimum-maximum, not a confidence interval. A dash means no range is reported for that measure. Latency percentiles are medians across trials and have no interval shown. Peak proportional set size (PSS) is the maximum sampled value across the supervisor and workers; MB uses decimal units.
