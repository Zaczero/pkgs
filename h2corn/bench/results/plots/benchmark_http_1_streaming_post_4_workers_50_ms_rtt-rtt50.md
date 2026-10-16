**Conclusion:** Throughput is close: the 95% bootstrap intervals do not separate a leader.

**Environment:** Python 3.14.8 | Linux 7.2.9 | x86_64; CPU: AMD Ryzen 9 5950X 16-Core Processor; rtt50 (50.1 ms measured RTT); 10s load after 2s warmup.

| Server (version) | Measure | Value | Observed rate range |
| --- | --- | --- | --- |
| h2corn (1.6.0) | Measured trials | 4 | - |
| h2corn (1.6.0) | Median rate (requests/s) | 1,399 | 1,387-1,401 |
| h2corn (1.6.0) | Maximum sampled PSS (MB) | 110.3 | - |
| h2corn (1.6.0) | Median p50 latency (ms) | 71.216 | - |
| h2corn (1.6.0) | Median p75 latency (ms) | 71.521 | - |
| h2corn (1.6.0) | Median p90 latency (ms) | 71.736 | - |
| h2corn (1.6.0) | Median p95 latency (ms) | 71.837 | - |
| h2corn (1.6.0) | Median p99 latency (ms) | 72.705 | - |
| h2corn (1.6.0) | Median p99.9 latency (ms) | 122.375 | - |
| gunicorn (26.0.0) | Measured trials | 4 | - |
| gunicorn (26.0.0) | Median rate (requests/s) | 1,393 | 1,392-1,394 |
| gunicorn (26.0.0) | Maximum sampled PSS (MB) | 83.2 | - |
| gunicorn (26.0.0) | Median p50 latency (ms) | 71.467 | - |
| gunicorn (26.0.0) | Median p75 latency (ms) | 71.738 | - |
| gunicorn (26.0.0) | Median p90 latency (ms) | 72.072 | - |
| gunicorn (26.0.0) | Median p95 latency (ms) | 72.279 | - |
| gunicorn (26.0.0) | Median p99 latency (ms) | 74.257 | - |
| gunicorn (26.0.0) | Median p99.9 latency (ms) | 124.418 | - |
| hypercorn (0.18.0) | Measured trials | 4 | - |
| hypercorn (0.18.0) | Median rate (requests/s) | 1,383 | 1,373-1,385 |
| hypercorn (0.18.0) | Maximum sampled PSS (MB) | 125.1 | - |
| hypercorn (0.18.0) | Median p50 latency (ms) | 71.936 | - |
| hypercorn (0.18.0) | Median p75 latency (ms) | 72.323 | - |
| hypercorn (0.18.0) | Median p90 latency (ms) | 72.881 | - |
| hypercorn (0.18.0) | Median p95 latency (ms) | 73.489 | - |
| hypercorn (0.18.0) | Median p99 latency (ms) | 80.300 | - |
| hypercorn (0.18.0) | Median p99.9 latency (ms) | 129.764 | - |
| uvicorn (0.52.1) | Measured trials | 4 | - |
| uvicorn (0.52.1) | Median rate (requests/s) | 710 | 709-710 |
| uvicorn (0.52.1) | Maximum sampled PSS (MB) | 114.1 | - |
| uvicorn (0.52.1) | Median p50 latency (ms) | 141.014 | - |
| uvicorn (0.52.1) | Median p75 latency (ms) | 141.973 | - |
| uvicorn (0.52.1) | Median p90 latency (ms) | 142.023 | - |
| uvicorn (0.52.1) | Median p95 latency (ms) | 142.469 | - |
| uvicorn (0.52.1) | Median p99 latency (ms) | 152.108 | - |
| uvicorn (0.52.1) | Median p99.9 latency (ms) | 164.882 | - |

Rates are medians across cold-start trials; the observed rate range is the trial minimum-maximum, not a confidence interval. A dash means no range is reported for that measure. Latency percentiles are medians across trials and have no interval shown. Peak proportional set size (PSS) is the maximum sampled value across the supervisor and workers; MB uses decimal units.
