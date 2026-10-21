| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 10,651 | 6 | 8.927 | 16.847 | 110.7 | - |
| uvicorn | 1,406 | 6 | 68.511 | 77.739 | 67.0 | slower |
| hypercorn | 1,094 | 6 | 87.300 | 98.254 | 94.2 | slower |
| gunicorn | failed | - | - | - | - | - |
