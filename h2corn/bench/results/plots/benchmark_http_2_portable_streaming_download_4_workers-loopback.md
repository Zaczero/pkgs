| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 49,665 | 6 | 1.905 | 3.529 | 121.9 | - |
| uvicorn | unsupported | - | - | - | - | - |
| hypercorn | 4,594 | 6 | 19.956 | 40.866 | 144.1 | slower |
| gunicorn | unsupported | - | - | - | - | - |
