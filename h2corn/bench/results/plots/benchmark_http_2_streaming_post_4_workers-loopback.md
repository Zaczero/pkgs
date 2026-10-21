| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 39,710 | 8 | 23.154 | 43.033 | 183.3 | - |
| uvicorn | unsupported | - | - | - | - | - |
| hypercorn | 4,181 | 8 | 187.027 | 514.189 | 252.1 | slower |
| gunicorn | unsupported | - | - | - | - | - |
