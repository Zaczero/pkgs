**Conclusion:** Throughput is close: the 95% bootstrap intervals do not separate a leader.

**Environment:** Python 3.14.8 | Linux 7.2.9 | x86_64; CPU: AMD Ryzen 9 5950X 16-Core Processor; rtt50 (50.1 ms measured RTT); 10s load after 2s warmup.

| Server (version) | Measure | Value | Observed rate range |
| --- | --- | --- | --- |
| h2corn (1.6.0) | Measured trials | 4 | - |
| h2corn (1.6.0) | Median rate (requests/s) | 774 | 763-830 |
| h2corn (1.6.0) | Maximum sampled PSS (MB) | 116.9 | - |
| h2corn (1.6.0) | Median p50 latency (ms) | 126.336 | - |
| h2corn (1.6.0) | Median p75 latency (ms) | 137.660 | - |
| h2corn (1.6.0) | Median p90 latency (ms) | 163.531 | - |
| h2corn (1.6.0) | Median p95 latency (ms) | 180.098 | - |
| h2corn (1.6.0) | Median p99 latency (ms) | 205.766 | - |
| h2corn (1.6.0) | Median p99.9 latency (ms) | 236.186 | - |
| gunicorn (26.0.0) | Measured trials | 4 | - |
| gunicorn (26.0.0) | Median rate (requests/s) | 764 | 760-769 |
| gunicorn (26.0.0) | Maximum sampled PSS (MB) | 85.3 | - |
| gunicorn (26.0.0) | Median p50 latency (ms) | 130.751 | - |
| gunicorn (26.0.0) | Median p75 latency (ms) | 137.120 | - |
| gunicorn (26.0.0) | Median p90 latency (ms) | 147.974 | - |
| gunicorn (26.0.0) | Median p95 latency (ms) | 155.730 | - |
| gunicorn (26.0.0) | Median p99 latency (ms) | 192.543 | - |
| gunicorn (26.0.0) | Median p99.9 latency (ms) | 211.322 | - |
| hypercorn (0.18.0) | Measured trials | 4 | - |
| hypercorn (0.18.0) | Median rate (requests/s) | 762 | 750-771 |
| hypercorn (0.18.0) | Maximum sampled PSS (MB) | 126.7 | - |
| hypercorn (0.18.0) | Median p50 latency (ms) | 131.493 | - |
| hypercorn (0.18.0) | Median p75 latency (ms) | 135.933 | - |
| hypercorn (0.18.0) | Median p90 latency (ms) | 141.913 | - |
| hypercorn (0.18.0) | Median p95 latency (ms) | 148.864 | - |
| hypercorn (0.18.0) | Median p99 latency (ms) | 205.192 | - |
| hypercorn (0.18.0) | Median p99.9 latency (ms) | 237.015 | - |
| uvicorn (0.52.1) | Measured trials | 4 | - |
| uvicorn (0.52.1) | Median rate (requests/s) | 574 | 550-620 |
| uvicorn (0.52.1) | Maximum sampled PSS (MB) | 115.7 | - |
| uvicorn (0.52.1) | Median p50 latency (ms) | 175.010 | - |
| uvicorn (0.52.1) | Median p75 latency (ms) | 175.838 | - |
| uvicorn (0.52.1) | Median p90 latency (ms) | 177.603 | - |
| uvicorn (0.52.1) | Median p95 latency (ms) | 180.430 | - |
| uvicorn (0.52.1) | Median p99 latency (ms) | 287.923 | - |
| uvicorn (0.52.1) | Median p99.9 latency (ms) | 297.782 | - |

Rates are medians across cold-start trials; the observed rate range is the trial minimum-maximum, not a confidence interval. A dash means no range is reported for that measure. Latency percentiles are medians across trials and have no interval shown. Peak proportional set size (PSS) is the maximum sampled value across the supervisor and workers; MB uses decimal units.
