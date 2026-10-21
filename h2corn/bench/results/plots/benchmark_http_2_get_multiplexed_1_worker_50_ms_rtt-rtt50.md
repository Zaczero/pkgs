| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 194 | 6 | 50.270 | 50.443 | 48.3 | - |
| uvicorn | unsupported | - | - | - | - | - |
| hypercorn | 191 | 6 | 50.531 | 100.568 | 46.3 | slower |
| gunicorn | unsupported | - | - | - | - | - |
