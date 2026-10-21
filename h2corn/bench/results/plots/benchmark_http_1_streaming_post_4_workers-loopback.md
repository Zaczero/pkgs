| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 42,387 | 8 | 22.780 | 34.977 | 174.6 | - |
| uvicorn | 11,466 | 8 | 84.488 | 147.219 | 150.5 | slower |
| hypercorn | 5,186 | 8 | 189.579 | 423.767 | 200.1 | slower |
| gunicorn | 17,076 | 8 | 55.633 | 107.114 | 121.4 | slower |
