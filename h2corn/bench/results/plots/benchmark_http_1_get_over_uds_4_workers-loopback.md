**Conclusion:** h2corn leads this throughput comparison; its 95% bootstrap interval is separated from the runner-up.

**Environment:** Python 3.14.8 | Linux 7.2.9 | x86_64; CPU: AMD Ryzen 9 5950X 16-Core Processor; loopback; 10s load after 2s warmup.

| Server (version) | Measure | Value | Observed rate range |
| --- | --- | --- | --- |
| h2corn (1.6.0) | Measured trials | 4 | - |
| h2corn (1.6.0) | Median rate (requests/s) | 235,491 | 233,176-237,655 |
| h2corn (1.6.0) | Maximum sampled PSS (MB) | 105.8 | - |
| h2corn (1.6.0) | Median p50 latency (ms) | 0.388 | - |
| h2corn (1.6.0) | Median p75 latency (ms) | 0.524 | - |
| h2corn (1.6.0) | Median p90 latency (ms) | 0.637 | - |
| h2corn (1.6.0) | Median p95 latency (ms) | 0.690 | - |
| h2corn (1.6.0) | Median p99 latency (ms) | 0.825 | - |
| h2corn (1.6.0) | Median p99.9 latency (ms) | 1.579 | - |
| gunicorn (26.0.0) | Measured trials | 4 | - |
| gunicorn (26.0.0) | Median rate (requests/s) | 44,152 | 41,673-46,319 |
| gunicorn (26.0.0) | Maximum sampled PSS (MB) | 81.1 | - |
| gunicorn (26.0.0) | Median p50 latency (ms) | 1.891 | - |
| gunicorn (26.0.0) | Median p75 latency (ms) | 3.403 | - |
| gunicorn (26.0.0) | Median p90 latency (ms) | 3.813 | - |
| gunicorn (26.0.0) | Median p95 latency (ms) | 4.104 | - |
| gunicorn (26.0.0) | Median p99 latency (ms) | 4.820 | - |
| gunicorn (26.0.0) | Median p99.9 latency (ms) | 5.743 | - |
| uvicorn (0.52.1) | Measured trials | 4 | - |
| uvicorn (0.52.1) | Median rate (requests/s) | 32,153 | 32,033-32,981 |
| uvicorn (0.52.1) | Maximum sampled PSS (MB) | 109.3 | - |
| uvicorn (0.52.1) | Median p50 latency (ms) | 2.805 | - |
| uvicorn (0.52.1) | Median p75 latency (ms) | 4.585 | - |
| uvicorn (0.52.1) | Median p90 latency (ms) | 4.805 | - |
| uvicorn (0.52.1) | Median p95 latency (ms) | 5.048 | - |
| uvicorn (0.52.1) | Median p99 latency (ms) | 5.665 | - |
| uvicorn (0.52.1) | Median p99.9 latency (ms) | 6.736 | - |
| hypercorn (0.18.0) | Measured trials | 4 | - |
| hypercorn (0.18.0) | Median rate (requests/s) | 13,373 | 13,182-13,733 |
| hypercorn (0.18.0) | Maximum sampled PSS (MB) | 121.6 | - |
| hypercorn (0.18.0) | Median p50 latency (ms) | 7.793 | - |
| hypercorn (0.18.0) | Median p75 latency (ms) | 11.422 | - |
| hypercorn (0.18.0) | Median p90 latency (ms) | 12.795 | - |
| hypercorn (0.18.0) | Median p95 latency (ms) | 13.432 | - |
| hypercorn (0.18.0) | Median p99 latency (ms) | 14.661 | - |
| hypercorn (0.18.0) | Median p99.9 latency (ms) | 17.646 | - |

Rates are medians across cold-start trials; the observed rate range is the trial minimum-maximum, not a confidence interval. A dash means no range is reported for that measure. Latency percentiles are medians across trials and have no interval shown. Peak proportional set size (PSS) is the maximum sampled value across the supervisor and workers; MB uses decimal units.
