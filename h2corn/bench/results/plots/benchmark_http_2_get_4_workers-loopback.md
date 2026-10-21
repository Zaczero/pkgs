| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 232,628 | 8 | 0.415 | 0.809 | 112.5 | - |
| uvicorn | unsupported | - | - | - | - | - |
| hypercorn | 10,476 | 8 | 9.538 | 19.340 | 141.8 | slower |
| gunicorn | unsupported | - | - | - | - | - |
