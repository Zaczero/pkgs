| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 216,980 | 6 | 0.441 | 0.821 | 109.0 | - |
| uvicorn | unsupported | - | - | - | - | - |
| hypercorn | 10,179 | 6 | 9.250 | 20.902 | 130.3 | slower |
| gunicorn | unsupported | - | - | - | - | - |
