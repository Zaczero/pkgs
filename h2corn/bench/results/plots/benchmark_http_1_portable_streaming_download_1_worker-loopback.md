| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 18,061 | 6 | 5.219 | 12.015 | 63.3 | - |
| uvicorn | 2,736 | 6 | 34.844 | 49.960 | 32.0 | slower |
| hypercorn | 1,784 | 6 | 52.916 | 63.993 | 56.8 | slower |
| gunicorn | 4,103 | 6 | 23.256 | 35.771 | 45.9 | slower |
