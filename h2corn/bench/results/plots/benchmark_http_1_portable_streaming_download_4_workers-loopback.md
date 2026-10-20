| Server | Requests/s | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- |
| h2corn | 48,717 | 1.986 | 3.932 | 122.5 | - |
| uvicorn | 9,884 | 9.186 | 19.626 | 123.0 | slower |
| hypercorn | 6,743 | 13.360 | 26.392 | 135.3 | slower |
| gunicorn | 14,006 | 7.041 | 12.694 | 92.6 | slower |
