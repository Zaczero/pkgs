| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 10,535 | 6 | 9.213 | 15.746 | 146.9 | - |
| uvicorn | unsupported | - | - | - | - | - |
| hypercorn | 863 | 6 | 112.926 | 134.714 | 115.8 | slower |
| gunicorn | unsupported | - | - | - | - | - |
