| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 197 | 8 | 50.323 | 50.762 | 49.7 | - |
| uvicorn | unsupported | - | - | - | - | - |
| hypercorn | 194 | 8 | 50.539 | 53.172 | 47.0 | slower |
| gunicorn | unsupported | - | - | - | - | - |
