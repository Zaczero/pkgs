| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 262,530 | 6 | 0.333 | 0.691 | 110.9 | - |
| uvicorn | 34,157 | 6 | 2.367 | 5.046 | 110.6 | slower |
| hypercorn | 14,230 | 6 | 6.381 | 12.604 | 122.9 | slower |
| gunicorn | 46,001 | 6 | 1.946 | 3.945 | 85.1 | slower |
