**Conclusion:** h2corn leads this throughput comparison; its 95% bootstrap interval is separated from the runner-up.

**Environment:** Python 3.14.8 | Linux 7.2.9 | x86_64; CPU: AMD Ryzen 9 5950X 16-Core Processor; loopback; 10s load after 2s warmup.

| Server (version) | Measure | Value | Observed rate range |
| --- | --- | --- | --- |
| h2corn (1.6.0) | Measured trials | 4 | - |
| h2corn (1.6.0) | Median rate (sessions/s) | 23,540 | 21,428-25,124 |
| h2corn (1.6.0) | Maximum sampled PSS (MB) | 59.5 | - |
| h2corn (1.6.0) | Median p50 session duration (ms) | 4.056 | - |
| h2corn (1.6.0) | Median p90 session duration (ms) | 5.023 | - |
| h2corn (1.6.0) | Median p95 session duration (ms) | 5.392 | - |
| h2corn (1.6.0) | Median p99 session duration (ms) | 6.650 | - |
| gunicorn (26.0.0) | Measured trials | 4 | - |
| gunicorn (26.0.0) | Median rate (sessions/s) | 2,864 | 2,734-2,967 |
| gunicorn (26.0.0) | Maximum sampled PSS (MB) | 363.4 | - |
| gunicorn (26.0.0) | Median p50 session duration (ms) | 18.475 | - |
| gunicorn (26.0.0) | Median p90 session duration (ms) | 23.629 | - |
| gunicorn (26.0.0) | Median p95 session duration (ms) | 25.726 | - |
| gunicorn (26.0.0) | Median p99 session duration (ms) | 479.326 | - |
| hypercorn (0.18.0) | Measured trials | 4 | - |
| hypercorn (0.18.0) | Median rate (sessions/s) | 2,083 | 2,044-2,113 |
| hypercorn (0.18.0) | Maximum sampled PSS (MB) | 58.5 | - |
| hypercorn (0.18.0) | Median p50 session duration (ms) | 46.316 | - |
| hypercorn (0.18.0) | Median p90 session duration (ms) | 52.735 | - |
| hypercorn (0.18.0) | Median p95 session duration (ms) | 57.128 | - |
| hypercorn (0.18.0) | Median p99 session duration (ms) | 77.714 | - |
| uvicorn (0.52.1) | Measured trials | 4 | - |
| uvicorn (0.52.1) | Median rate (sessions/s) | 1,916 | 1,890-1,933 |
| uvicorn (0.52.1) | Maximum sampled PSS (MB) | 121.6 | - |
| uvicorn (0.52.1) | Median p50 session duration (ms) | 48.046 | - |
| uvicorn (0.52.1) | Median p90 session duration (ms) | 56.186 | - |
| uvicorn (0.52.1) | Median p95 session duration (ms) | 65.999 | - |
| uvicorn (0.52.1) | Median p99 session duration (ms) | 134.518 | - |

Rates are medians across cold-start trials; the observed rate range is the trial minimum-maximum, not a confidence interval. A dash means no range is reported for that measure. Latency percentiles are medians across trials and have no interval shown. Peak proportional set size (PSS) is the maximum sampled value across the supervisor and workers; MB uses decimal units.
