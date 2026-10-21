| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 34,264 | 6 | 2.666 | 6.734 | 319.1 | - |
| uvicorn | 5,054 | 6 | 18.211 | 33.592 | 221.1 | slower |
| hypercorn | 4,103 | 6 | 22.147 | 43.110 | 231.6 | slower |
| gunicorn | failed | - | - | - | - | - |
