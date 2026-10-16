**Conclusion:** h2corn leads this throughput comparison; its 95% bootstrap interval is separated from the runner-up.

**Environment:** Python 3.14.8 | Linux 7.2.9 | x86_64; CPU: AMD Ryzen 9 5950X 16-Core Processor; loopback; 10s load after 2s warmup.

| Server (version) | Measure | Value | Observed rate range |
| --- | --- | --- | --- |
| h2corn (1.6.0) | Measured trials | 4 | - |
| h2corn (1.6.0) | Median rate (requests/s) | 17,933 | 17,537-18,561 |
| h2corn (1.6.0) | Maximum sampled PSS (MB) | 58.3 | - |
| h2corn (1.6.0) | Median p50 latency (ms) | 5.534 | - |
| h2corn (1.6.0) | Median p75 latency (ms) | 6.190 | - |
| h2corn (1.6.0) | Median p90 latency (ms) | 6.917 | - |
| h2corn (1.6.0) | Median p95 latency (ms) | 7.501 | - |
| h2corn (1.6.0) | Median p99 latency (ms) | 10.980 | - |
| h2corn (1.6.0) | Median p99.9 latency (ms) | 14.357 | - |
| gunicorn (26.0.0) | Measured trials | 4 | - |
| gunicorn (26.0.0) | Median rate (requests/s) | 3,609 | 3,531-3,811 |
| gunicorn (26.0.0) | Maximum sampled PSS (MB) | 44.3 | - |
| gunicorn (26.0.0) | Median p50 latency (ms) | 26.605 | - |
| gunicorn (26.0.0) | Median p75 latency (ms) | 28.660 | - |
| gunicorn (26.0.0) | Median p90 latency (ms) | 31.280 | - |
| gunicorn (26.0.0) | Median p95 latency (ms) | 32.918 | - |
| gunicorn (26.0.0) | Median p99 latency (ms) | 39.061 | - |
| gunicorn (26.0.0) | Median p99.9 latency (ms) | 48.944 | - |
| uvicorn (0.52.1) | Measured trials | 4 | - |
| uvicorn (0.52.1) | Median rate (requests/s) | 2,443 | 2,336-2,528 |
| uvicorn (0.52.1) | Maximum sampled PSS (MB) | 59.1 | - |
| uvicorn (0.52.1) | Median p50 latency (ms) | 39.742 | - |
| uvicorn (0.52.1) | Median p75 latency (ms) | 42.056 | - |
| uvicorn (0.52.1) | Median p90 latency (ms) | 45.943 | - |
| uvicorn (0.52.1) | Median p95 latency (ms) | 49.250 | - |
| uvicorn (0.52.1) | Median p99 latency (ms) | 53.823 | - |
| uvicorn (0.52.1) | Median p99.9 latency (ms) | 61.330 | - |
| hypercorn (0.18.0) | Measured trials | 4 | - |
| hypercorn (0.18.0) | Median rate (requests/s) | 1,659 | 1,566-1,710 |
| hypercorn (0.18.0) | Maximum sampled PSS (MB) | 54.2 | - |
| hypercorn (0.18.0) | Median p50 latency (ms) | 59.243 | - |
| hypercorn (0.18.0) | Median p75 latency (ms) | 62.952 | - |
| hypercorn (0.18.0) | Median p90 latency (ms) | 66.624 | - |
| hypercorn (0.18.0) | Median p95 latency (ms) | 69.302 | - |
| hypercorn (0.18.0) | Median p99 latency (ms) | 74.610 | - |
| hypercorn (0.18.0) | Median p99.9 latency (ms) | 78.892 | - |

Rates are medians across cold-start trials; the observed rate range is the trial minimum-maximum, not a confidence interval. A dash means no range is reported for that measure. Latency percentiles are medians across trials and have no interval shown. Peak proportional set size (PSS) is the maximum sampled value across the supervisor and workers; MB uses decimal units.
