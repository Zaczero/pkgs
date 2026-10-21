| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 42,962 | 6 | 22.336 | 30.839 | 175.3 | - |
| uvicorn | unsupported | - | - | - | - | - |
| hypercorn | 4,302 | 6 | 134.596 | 542.575 | 241.0 | slower |
| gunicorn | unsupported | - | - | - | - | - |
