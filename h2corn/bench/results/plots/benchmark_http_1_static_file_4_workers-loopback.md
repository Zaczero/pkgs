**Conclusion:** h2corn leads this throughput comparison; its 95% bootstrap interval is separated from the runner-up.

**Environment:** Python 3.14.8 | Linux 7.2.9 | x86_64; CPU: AMD Ryzen 9 5950X 16-Core Processor; loopback; 10s load after 2s warmup.

| Server (version) | Measure | Value | Observed rate range |
| --- | --- | --- | --- |
| h2corn (1.6.0) | Measured trials | 4 | - |
| h2corn (1.6.0) | Median rate (requests/s) | 28,019 | 25,576-30,141 |
| h2corn (1.6.0) | Maximum sampled PSS (MB) | 358.6 | - |
| h2corn (1.6.0) | Median p50 latency (ms) | 3.192 | - |
| h2corn (1.6.0) | Median p75 latency (ms) | 4.621 | - |
| h2corn (1.6.0) | Median p90 latency (ms) | 6.132 | - |
| h2corn (1.6.0) | Median p95 latency (ms) | 7.181 | - |
| h2corn (1.6.0) | Median p99 latency (ms) | 10.242 | - |
| h2corn (1.6.0) | Median p99.9 latency (ms) | 19.619 | - |
| uvicorn (0.52.1) | Measured trials | 4 | - |
| uvicorn (0.52.1) | Median rate (requests/s) | 5,527 | 5,423-5,642 |
| uvicorn (0.52.1) | Maximum sampled PSS (MB) | 239.3 | - |
| uvicorn (0.52.1) | Median p50 latency (ms) | 16.957 | - |
| uvicorn (0.52.1) | Median p75 latency (ms) | 22.619 | - |
| uvicorn (0.52.1) | Median p90 latency (ms) | 26.036 | - |
| uvicorn (0.52.1) | Median p95 latency (ms) | 27.083 | - |
| uvicorn (0.52.1) | Median p99 latency (ms) | 30.611 | - |
| uvicorn (0.52.1) | Median p99.9 latency (ms) | 48.236 | - |
| hypercorn (0.18.0) | Measured trials | 4 | - |
| hypercorn (0.18.0) | Median rate (requests/s) | 4,202 | 4,072-4,432 |
| hypercorn (0.18.0) | Maximum sampled PSS (MB) | 247.5 | - |
| hypercorn (0.18.0) | Median p50 latency (ms) | 19.803 | - |
| hypercorn (0.18.0) | Median p75 latency (ms) | 31.114 | - |
| hypercorn (0.18.0) | Median p90 latency (ms) | 37.852 | - |
| hypercorn (0.18.0) | Median p95 latency (ms) | 39.917 | - |
| hypercorn (0.18.0) | Median p99 latency (ms) | 46.486 | - |
| hypercorn (0.18.0) | Median p99.9 latency (ms) | 93.062 | - |
| gunicorn (26.0.0) | Measured trials | 4 | - |
| gunicorn (26.0.0) | Median rate (requests/s) | 20 | 20-20 |
| gunicorn (26.0.0) | Maximum sampled PSS (MB) | 115.5 | - |
| gunicorn (26.0.0) | Median p50 latency (ms) | 9.157 | - |
| gunicorn (26.0.0) | Median p75 latency (ms) | 20.668 | - |
| gunicorn (26.0.0) | Median p90 latency (ms) | 23.258 | - |
| gunicorn (26.0.0) | Median p95 latency (ms) | 24.762 | - |
| gunicorn (26.0.0) | Median p99 latency (ms) | 32.461 | - |
| gunicorn (26.0.0) | Median p99.9 latency (ms) | 32.461 | - |

Rates are medians across cold-start trials; the observed rate range is the trial minimum-maximum, not a confidence interval. A dash means no range is reported for that measure. Latency percentiles are medians across trials and have no interval shown. Peak proportional set size (PSS) is the maximum sampled value across the supervisor and workers; MB uses decimal units.
