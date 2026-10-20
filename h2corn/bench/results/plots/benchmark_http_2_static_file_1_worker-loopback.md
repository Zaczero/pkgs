| Server | Requests/s | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- |
| h2corn | 10,986 | 8.887 | 15.133 | 142.8 | - |
| uvicorn | unsupported | - | - | - | - |
| hypercorn | 886 | 111.056 | 133.758 | 101.8 | slower |
| gunicorn | unsupported | - | - | - | - |
