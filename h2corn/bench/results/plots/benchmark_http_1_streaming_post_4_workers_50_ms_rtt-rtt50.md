| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 1,345 | 6 | 71.043 | 121.939 | 111.1 | - |
| uvicorn | 1,310 | 6 | 71.855 | 127.155 | 118.2 | slower |
| hypercorn | 1,312 | 6 | 71.865 | 128.327 | 130.2 | slower |
| gunicorn | 1,327 | 6 | 71.540 | 124.369 | 84.9 | slower |
