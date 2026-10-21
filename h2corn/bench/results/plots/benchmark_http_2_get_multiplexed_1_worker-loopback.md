| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 80,921 | 6 | 0.114 | 0.257 | 49.6 | - |
| uvicorn | unsupported | - | - | - | - | - |
| hypercorn | 3,247 | 6 | 3.058 | 3.836 | 49.3 | slower |
| gunicorn | unsupported | - | - | - | - | - |
