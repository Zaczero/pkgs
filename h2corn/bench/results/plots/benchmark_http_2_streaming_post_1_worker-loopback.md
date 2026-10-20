| Server | Requests/s | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- |
| h2corn | 13,478 | 65.974 | 108.092 | 117.3 | - |
| uvicorn | unsupported | - | - | - | - |
| hypercorn | 1,174 | 803.161 | 1,272.152 | 152.4 | slower |
| gunicorn | unsupported | - | - | - | - |
