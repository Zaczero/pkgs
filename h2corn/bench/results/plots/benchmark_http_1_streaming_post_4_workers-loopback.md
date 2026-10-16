**Conclusion:** h2corn leads this throughput comparison; its 95% bootstrap interval is separated from the runner-up.

**Environment:** Python 3.14.8 | Linux 7.2.9 | x86_64; CPU: AMD Ryzen 9 5950X 16-Core Processor; loopback; 10s load after 2s warmup.

| Server (version) | Measure | Value | Observed rate range |
| --- | --- | --- | --- |
| h2corn (1.6.0) | Measured trials | 4 | - |
| h2corn (1.6.0) | Median rate (requests/s) | 41,942 | 40,071-43,785 |
| h2corn (1.6.0) | Maximum sampled PSS (MB) | 173.5 | - |
| h2corn (1.6.0) | Median p50 latency (ms) | 22.493 | - |
| h2corn (1.6.0) | Median p75 latency (ms) | 23.485 | - |
| h2corn (1.6.0) | Median p90 latency (ms) | 25.168 | - |
| h2corn (1.6.0) | Median p95 latency (ms) | 27.424 | - |
| h2corn (1.6.0) | Median p99 latency (ms) | 51.061 | - |
| h2corn (1.6.0) | Median p99.9 latency (ms) | 97.222 | - |
| gunicorn (26.0.0) | Measured trials | 4 | - |
| gunicorn (26.0.0) | Median rate (requests/s) | 19,387 | 18,488-20,448 |
| gunicorn (26.0.0) | Maximum sampled PSS (MB) | 121.3 | - |
| gunicorn (26.0.0) | Median p50 latency (ms) | 50.129 | - |
| gunicorn (26.0.0) | Median p75 latency (ms) | 56.421 | - |
| gunicorn (26.0.0) | Median p90 latency (ms) | 62.587 | - |
| gunicorn (26.0.0) | Median p95 latency (ms) | 66.607 | - |
| gunicorn (26.0.0) | Median p99 latency (ms) | 80.337 | - |
| gunicorn (26.0.0) | Median p99.9 latency (ms) | 99.352 | - |
| uvicorn (0.52.1) | Measured trials | 4 | - |
| uvicorn (0.52.1) | Median rate (requests/s) | 12,009 | 11,732-12,369 |
| uvicorn (0.52.1) | Maximum sampled PSS (MB) | 143.7 | - |
| uvicorn (0.52.1) | Median p50 latency (ms) | 79.977 | - |
| uvicorn (0.52.1) | Median p75 latency (ms) | 96.714 | - |
| uvicorn (0.52.1) | Median p90 latency (ms) | 109.194 | - |
| uvicorn (0.52.1) | Median p95 latency (ms) | 114.740 | - |
| uvicorn (0.52.1) | Median p99 latency (ms) | 130.754 | - |
| uvicorn (0.52.1) | Median p99.9 latency (ms) | 150.674 | - |
| hypercorn (0.18.0) | Measured trials | 4 | - |
| hypercorn (0.18.0) | Median rate (requests/s) | 5,822 | 5,495-7,048 |
| hypercorn (0.18.0) | Maximum sampled PSS (MB) | 184.0 | - |
| hypercorn (0.18.0) | Median p50 latency (ms) | 175.866 | - |
| hypercorn (0.18.0) | Median p75 latency (ms) | 232.264 | - |
| hypercorn (0.18.0) | Median p90 latency (ms) | 255.875 | - |
| hypercorn (0.18.0) | Median p95 latency (ms) | 273.476 | - |
| hypercorn (0.18.0) | Median p99 latency (ms) | 305.773 | - |
| hypercorn (0.18.0) | Median p99.9 latency (ms) | 369.669 | - |

Rates are medians across cold-start trials; the observed rate range is the trial minimum-maximum, not a confidence interval. A dash means no range is reported for that measure. Latency percentiles are medians across trials and have no interval shown. Peak proportional set size (PSS) is the maximum sampled value across the supervisor and workers; MB uses decimal units.
