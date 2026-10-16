**Conclusion:** h2corn leads this throughput comparison; its 95% bootstrap interval is separated from the runner-up.

**Environment:** Python 3.14.8 | Linux 7.2.9 | x86_64; CPU: AMD Ryzen 9 5950X 16-Core Processor; loopback; 10s load after 2s warmup.

| Server (version) | Measure | Value | Observed rate range |
| --- | --- | --- | --- |
| h2corn (1.6.0) | Measured trials | 4 | - |
| h2corn (1.6.0) | Median rate (requests/s) | 97,875 | 94,653-98,352 |
| h2corn (1.6.0) | Maximum sampled PSS (MB) | 47.8 | - |
| h2corn (1.6.0) | Median p50 latency (ms) | 0.992 | - |
| h2corn (1.6.0) | Median p75 latency (ms) | 1.014 | - |
| h2corn (1.6.0) | Median p90 latency (ms) | 1.080 | - |
| h2corn (1.6.0) | Median p95 latency (ms) | 1.197 | - |
| h2corn (1.6.0) | Median p99 latency (ms) | 1.648 | - |
| h2corn (1.6.0) | Median p99.9 latency (ms) | 2.184 | - |
| gunicorn (26.0.0) | Measured trials | 4 | - |
| gunicorn (26.0.0) | Median rate (requests/s) | 12,330 | 11,755-12,817 |
| gunicorn (26.0.0) | Maximum sampled PSS (MB) | 38.6 | - |
| gunicorn (26.0.0) | Median p50 latency (ms) | 7.949 | - |
| gunicorn (26.0.0) | Median p75 latency (ms) | 8.251 | - |
| gunicorn (26.0.0) | Median p90 latency (ms) | 8.684 | - |
| gunicorn (26.0.0) | Median p95 latency (ms) | 9.211 | - |
| gunicorn (26.0.0) | Median p99 latency (ms) | 10.525 | - |
| gunicorn (26.0.0) | Median p99.9 latency (ms) | 14.110 | - |
| uvicorn (0.52.1) | Measured trials | 4 | - |
| uvicorn (0.52.1) | Median rate (requests/s) | 8,568 | 8,405-8,835 |
| uvicorn (0.52.1) | Maximum sampled PSS (MB) | 48.1 | - |
| uvicorn (0.52.1) | Median p50 latency (ms) | 11.446 | - |
| uvicorn (0.52.1) | Median p75 latency (ms) | 11.903 | - |
| uvicorn (0.52.1) | Median p90 latency (ms) | 12.646 | - |
| uvicorn (0.52.1) | Median p95 latency (ms) | 13.397 | - |
| uvicorn (0.52.1) | Median p99 latency (ms) | 14.925 | - |
| uvicorn (0.52.1) | Median p99.9 latency (ms) | 22.398 | - |
| hypercorn (0.18.0) | Measured trials | 4 | - |
| hypercorn (0.18.0) | Median rate (requests/s) | 3,527 | 3,252-3,689 |
| hypercorn (0.18.0) | Maximum sampled PSS (MB) | 49.0 | - |
| hypercorn (0.18.0) | Median p50 latency (ms) | 27.966 | - |
| hypercorn (0.18.0) | Median p75 latency (ms) | 28.988 | - |
| hypercorn (0.18.0) | Median p90 latency (ms) | 30.455 | - |
| hypercorn (0.18.0) | Median p95 latency (ms) | 31.674 | - |
| hypercorn (0.18.0) | Median p99 latency (ms) | 34.901 | - |
| hypercorn (0.18.0) | Median p99.9 latency (ms) | 54.946 | - |

Rates are medians across cold-start trials; the observed rate range is the trial minimum-maximum, not a confidence interval. A dash means no range is reported for that measure. Latency percentiles are medians across trials and have no interval shown. Peak proportional set size (PSS) is the maximum sampled value across the supervisor and workers; MB uses decimal units.
