**Conclusion:** h2corn leads this throughput comparison; its 95% bootstrap interval is separated from the runner-up.

**Environment:** Python 3.14.8 | Linux 7.2.9 | x86_64; CPU: AMD Ryzen 9 5950X 16-Core Processor; loopback; 10s load after 2s warmup.

| Server (version) | Measure | Value | Observed rate range |
| --- | --- | --- | --- |
| h2corn (1.6.0) | Measured trials | 4 | - |
| h2corn (1.6.0) | Median rate (requests/s) | 95,389 | 92,923-97,485 |
| h2corn (1.6.0) | Maximum sampled PSS (MB) | 51.1 | - |
| h2corn (1.6.0) | Median p50 latency (ms) | 0.995 | - |
| h2corn (1.6.0) | Median p75 latency (ms) | 1.046 | - |
| h2corn (1.6.0) | Median p90 latency (ms) | 1.182 | - |
| h2corn (1.6.0) | Median p95 latency (ms) | 1.268 | - |
| h2corn (1.6.0) | Median p99 latency (ms) | 1.671 | - |
| h2corn (1.6.0) | Median p99.9 latency (ms) | 2.475 | - |
| gunicorn (26.0.0) | Measured trials | 4 | - |
| gunicorn (26.0.0) | Median rate (requests/s) | 10,280 | 10,143-11,040 |
| gunicorn (26.0.0) | Maximum sampled PSS (MB) | 38.8 | - |
| gunicorn (26.0.0) | Median p50 latency (ms) | 9.458 | - |
| gunicorn (26.0.0) | Median p75 latency (ms) | 9.978 | - |
| gunicorn (26.0.0) | Median p90 latency (ms) | 10.568 | - |
| gunicorn (26.0.0) | Median p95 latency (ms) | 11.084 | - |
| gunicorn (26.0.0) | Median p99 latency (ms) | 12.253 | - |
| gunicorn (26.0.0) | Median p99.9 latency (ms) | 16.118 | - |
| uvicorn (0.52.1) | Measured trials | 4 | - |
| uvicorn (0.52.1) | Median rate (requests/s) | 7,312 | 6,984-7,719 |
| uvicorn (0.52.1) | Maximum sampled PSS (MB) | 49.5 | - |
| uvicorn (0.52.1) | Median p50 latency (ms) | 13.367 | - |
| uvicorn (0.52.1) | Median p75 latency (ms) | 13.822 | - |
| uvicorn (0.52.1) | Median p90 latency (ms) | 14.769 | - |
| uvicorn (0.52.1) | Median p95 latency (ms) | 15.546 | - |
| uvicorn (0.52.1) | Median p99 latency (ms) | 20.721 | - |
| uvicorn (0.52.1) | Median p99.9 latency (ms) | 26.375 | - |
| hypercorn (0.18.0) | Measured trials | 4 | - |
| hypercorn (0.18.0) | Median rate (requests/s) | 3,267 | 3,209-3,330 |
| hypercorn (0.18.0) | Maximum sampled PSS (MB) | 49.7 | - |
| hypercorn (0.18.0) | Median p50 latency (ms) | 30.003 | - |
| hypercorn (0.18.0) | Median p75 latency (ms) | 31.183 | - |
| hypercorn (0.18.0) | Median p90 latency (ms) | 33.176 | - |
| hypercorn (0.18.0) | Median p95 latency (ms) | 34.749 | - |
| hypercorn (0.18.0) | Median p99 latency (ms) | 38.665 | - |
| hypercorn (0.18.0) | Median p99.9 latency (ms) | 48.154 | - |

Rates are medians across cold-start trials; the observed rate range is the trial minimum-maximum, not a confidence interval. A dash means no range is reported for that measure. Latency percentiles are medians across trials and have no interval shown. Peak proportional set size (PSS) is the maximum sampled value across the supervisor and workers; MB uses decimal units.
