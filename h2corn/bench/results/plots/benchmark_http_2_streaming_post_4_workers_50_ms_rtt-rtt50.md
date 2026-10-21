| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 1,389 | 8 | 71.139 | 74.793 | 118.0 | - |
| uvicorn | unsupported | - | - | - | - | - |
| hypercorn | 1,364 | 8 | 72.349 | 77.827 | 137.4 | slower |
| gunicorn | unsupported | - | - | - | - | - |
