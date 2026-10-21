| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 109,642 | 6 | 0.847 | 1.387 | 53.2 | - |
| uvicorn | unsupported | - | - | - | - | - |
| hypercorn | 2,478 | 6 | 37.558 | 61.963 | 56.2 | slower |
| gunicorn | unsupported | - | - | - | - | - |
