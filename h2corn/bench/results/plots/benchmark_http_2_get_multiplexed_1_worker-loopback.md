| Server | Requests/s | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- |
| h2corn | 86,466 | 0.107 | 0.247 | 50.7 | - |
| uvicorn | unsupported | - | - | - | - |
| hypercorn | 3,296 | 3.001 | 3.709 | 53.0 | slower |
| gunicorn | unsupported | - | - | - | - |
