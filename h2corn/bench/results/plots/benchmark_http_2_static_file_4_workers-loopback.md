| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 34,877 | 8 | 2.734 | 6.523 | 336.7 | - |
| uvicorn | unsupported | - | - | - | - | - |
| hypercorn | 3,197 | 8 | 30.798 | 51.726 | 249.0 | slower |
| gunicorn | unsupported | - | - | - | - | - |
