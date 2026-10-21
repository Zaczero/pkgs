| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 1,343 | 6 | 71.061 | 72.086 | 109.3 | - |
| uvicorn | unsupported | - | - | - | - | - |
| hypercorn | 1,300 | 6 | 72.347 | 82.223 | 134.5 | slower |
| gunicorn | unsupported | - | - | - | - | - |
