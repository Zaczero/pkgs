| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 34,921 | 6 | 2.680 | 6.424 | 284.3 | - |
| uvicorn | unsupported | - | - | - | - | - |
| hypercorn | 3,174 | 6 | 28.090 | 55.385 | 228.0 | slower |
| gunicorn | unsupported | - | - | - | - | - |
