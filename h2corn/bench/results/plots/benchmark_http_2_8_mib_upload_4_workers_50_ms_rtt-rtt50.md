| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 7.62 | 6 | 803.907 | 2,048.578 | 116.6 | - |
| uvicorn | unsupported | - | - | - | - | - |
| hypercorn | 0.94 | 6 | 10,313.203 | 10,320.987 | 120.0 | slower |
| gunicorn | unsupported | - | - | - | - | - |
