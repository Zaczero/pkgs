**Conclusion:** h2corn leads this throughput comparison; its 95% bootstrap interval is separated from the runner-up.

**Environment:** Python 3.14.8 | Linux 7.2.9 | x86_64; CPU: AMD Ryzen 9 5950X 16-Core Processor; loopback; 10s load after 2s warmup.

| Server (version) | Measure | Value | Observed rate range |
| --- | --- | --- | --- |
| h2corn (1.6.0) | Measured trials | 4 | - |
| h2corn (1.6.0) | Median rate (requests/s) | 17,070 | 15,916-17,847 |
| h2corn (1.6.0) | Maximum sampled PSS (MB) | 107.4 | - |
| h2corn (1.6.0) | Median p50 latency (ms) | 55.982 | - |
| h2corn (1.6.0) | Median p75 latency (ms) | 61.851 | - |
| h2corn (1.6.0) | Median p90 latency (ms) | 69.512 | - |
| h2corn (1.6.0) | Median p95 latency (ms) | 76.167 | - |
| h2corn (1.6.0) | Median p99 latency (ms) | 124.211 | - |
| h2corn (1.6.0) | Median p99.9 latency (ms) | 211.114 | - |
| gunicorn (26.0.0) | Measured trials | 4 | - |
| gunicorn (26.0.0) | Median rate (requests/s) | 4,688 | 4,504-5,112 |
| gunicorn (26.0.0) | Maximum sampled PSS (MB) | 71.6 | - |
| gunicorn (26.0.0) | Median p50 latency (ms) | 191.233 | - |
| gunicorn (26.0.0) | Median p75 latency (ms) | 217.444 | - |
| gunicorn (26.0.0) | Median p90 latency (ms) | 243.519 | - |
| gunicorn (26.0.0) | Median p95 latency (ms) | 257.191 | - |
| gunicorn (26.0.0) | Median p99 latency (ms) | 1,501.186 | - |
| gunicorn (26.0.0) | Median p99.9 latency (ms) | 4,255.288 | - |
| uvicorn (0.52.1) | Measured trials | 4 | - |
| uvicorn (0.52.1) | Median rate (requests/s) | 2,898 | 2,857-2,919 |
| uvicorn (0.52.1) | Maximum sampled PSS (MB) | 144.7 | - |
| uvicorn (0.52.1) | Median p50 latency (ms) | 340.408 | - |
| uvicorn (0.52.1) | Median p75 latency (ms) | 371.501 | - |
| uvicorn (0.52.1) | Median p90 latency (ms) | 407.705 | - |
| uvicorn (0.52.1) | Median p95 latency (ms) | 422.924 | - |
| uvicorn (0.52.1) | Median p99 latency (ms) | 468.926 | - |
| uvicorn (0.52.1) | Median p99.9 latency (ms) | 521.392 | - |
| hypercorn (0.18.0) | Measured trials | 4 | - |
| hypercorn (0.18.0) | Median rate (requests/s) | 1,722 | 1,671-1,788 |
| hypercorn (0.18.0) | Maximum sampled PSS (MB) | 102.4 | - |
| hypercorn (0.18.0) | Median p50 latency (ms) | 576.918 | - |
| hypercorn (0.18.0) | Median p75 latency (ms) | 616.203 | - |
| hypercorn (0.18.0) | Median p90 latency (ms) | 701.616 | - |
| hypercorn (0.18.0) | Median p95 latency (ms) | 815.506 | - |
| hypercorn (0.18.0) | Median p99 latency (ms) | 968.692 | - |
| hypercorn (0.18.0) | Median p99.9 latency (ms) | 990.569 | - |

Rates are medians across cold-start trials; the observed rate range is the trial minimum-maximum, not a confidence interval. A dash means no range is reported for that measure. Latency percentiles are medians across trials and have no interval shown. Peak proportional set size (PSS) is the maximum sampled value across the supervisor and workers; MB uses decimal units.
