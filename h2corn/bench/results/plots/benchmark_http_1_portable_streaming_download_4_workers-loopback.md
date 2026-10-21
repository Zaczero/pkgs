| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 48,717 | 8 | 1.986 | 3.932 | 122.5 | - |
| uvicorn | 9,884 | 8 | 9.186 | 19.626 | 123.0 | slower |
| hypercorn | 6,743 | 8 | 13.360 | 26.392 | 135.3 | slower |
| gunicorn | 14,006 | 8 | 7.041 | 12.694 | 92.6 | slower |
