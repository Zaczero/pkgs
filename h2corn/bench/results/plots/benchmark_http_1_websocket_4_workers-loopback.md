**Conclusion:** h2corn leads this throughput comparison; its 95% bootstrap interval is separated from the runner-up.

**Environment:** Python 3.14.8 | Linux 7.2.9 | x86_64; CPU: AMD Ryzen 9 5950X 16-Core Processor; loopback; 10s load after 2s warmup.

| Server (version) | Measure | Value | Observed rate range |
| --- | --- | --- | --- |
| h2corn (1.6.0) | Measured trials | 4 | - |
| h2corn (1.6.0) | Median rate (sessions/s) | 37,495 | 27,097-40,136 |
| h2corn (1.6.0) | Maximum sampled PSS (MB) | 134.8 | - |
| h2corn (1.6.0) | Median p50 session duration (ms) | 2.287 | - |
| h2corn (1.6.0) | Median p90 session duration (ms) | 4.153 | - |
| h2corn (1.6.0) | Median p95 session duration (ms) | 4.856 | - |
| h2corn (1.6.0) | Median p99 session duration (ms) | 6.662 | - |
| gunicorn (26.0.0) | Measured trials | 4 | - |
| gunicorn (26.0.0) | Median rate (sessions/s) | 9,646 | 6,594-9,706 |
| gunicorn (26.0.0) | Maximum sampled PSS (MB) | 1,202.5 | - |
| gunicorn (26.0.0) | Median p50 session duration (ms) | 5.686 | - |
| gunicorn (26.0.0) | Median p90 session duration (ms) | 10.239 | - |
| gunicorn (26.0.0) | Median p95 session duration (ms) | 12.746 | - |
| gunicorn (26.0.0) | Median p99 session duration (ms) | 32.573 | - |
| uvicorn (0.52.1) | Measured trials | 4 | - |
| uvicorn (0.52.1) | Median rate (sessions/s) | 6,606 | 5,290-7,143 |
| uvicorn (0.52.1) | Maximum sampled PSS (MB) | 170.9 | - |
| uvicorn (0.52.1) | Median p50 session duration (ms) | 14.033 | - |
| uvicorn (0.52.1) | Median p90 session duration (ms) | 23.211 | - |
| uvicorn (0.52.1) | Median p95 session duration (ms) | 26.832 | - |
| uvicorn (0.52.1) | Median p99 session duration (ms) | 42.559 | - |
| hypercorn (0.18.0) | Measured trials | 4 | - |
| hypercorn (0.18.0) | Median rate (sessions/s) | 6,290 | 5,423-7,015 |
| hypercorn (0.18.0) | Maximum sampled PSS (MB) | 141.9 | - |
| hypercorn (0.18.0) | Median p50 session duration (ms) | 14.372 | - |
| hypercorn (0.18.0) | Median p90 session duration (ms) | 24.341 | - |
| hypercorn (0.18.0) | Median p95 session duration (ms) | 30.260 | - |
| hypercorn (0.18.0) | Median p99 session duration (ms) | 42.160 | - |

Rates are medians across cold-start trials; the observed rate range is the trial minimum-maximum, not a confidence interval. A dash means no range is reported for that measure. Latency percentiles are medians across trials and have no interval shown. Peak proportional set size (PSS) is the maximum sampled value across the supervisor and workers; MB uses decimal units.
