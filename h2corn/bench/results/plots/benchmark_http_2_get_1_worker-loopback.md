| Server | Requests/s | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- |
| h2corn | 113,267 | 0.847 | 1.340 | 55.1 | - |
| uvicorn | unsupported | - | - | - | - |
| hypercorn | 2,731 | 35.743 | 51.540 | 59.4 | slower |
| gunicorn | unsupported | - | - | - | - |
