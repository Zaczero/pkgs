| Server | Requests/s | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- |
| h2corn | 6.84 | 796.607 | 2,288.923 | 117.0 | - |
| uvicorn | unsupported | - | - | - | - |
| hypercorn | 0.94 | 10,317.172 | 10,327.294 | 120.8 | slower |
| gunicorn | unsupported | - | - | - | - |
