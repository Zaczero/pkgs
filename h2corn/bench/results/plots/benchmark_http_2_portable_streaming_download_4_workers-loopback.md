| Server | Requests/s | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- |
| h2corn | 45,541 | 2.032 | 4.926 | 127.4 | - |
| uvicorn | unsupported | - | - | - | - |
| hypercorn | 4,334 | 21.280 | 44.061 | 147.9 | slower |
| gunicorn | unsupported | - | - | - | - |
