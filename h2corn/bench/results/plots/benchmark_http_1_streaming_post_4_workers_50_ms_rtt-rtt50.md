| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 1,390 | 8 | 71.074 | 75.971 | 110.9 | - |
| uvicorn | 1,378 | 8 | 71.735 | 75.525 | 118.5 | slower |
| hypercorn | 1,374 | 8 | 71.852 | 76.355 | 130.4 | slower |
| gunicorn | 1,382 | 8 | 71.487 | 72.960 | 82.3 | slower |
