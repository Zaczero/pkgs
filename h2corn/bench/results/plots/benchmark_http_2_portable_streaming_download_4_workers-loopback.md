| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 45,541 | 8 | 2.032 | 4.926 | 127.4 | - |
| uvicorn | unsupported | - | - | - | - | - |
| hypercorn | 4,334 | 8 | 21.280 | 44.061 | 147.9 | slower |
| gunicorn | unsupported | - | - | - | - | - |
