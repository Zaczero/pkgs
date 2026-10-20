| Server | Requests/s | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- |
| h2corn | 253,806 | 0.368 | 0.835 | 107.0 | - |
| uvicorn | 29,218 | 3.861 | 5.736 | 110.6 | slower |
| hypercorn | 13,288 | 7.719 | 13.895 | 122.5 | slower |
| gunicorn | 34,358 | 2.418 | 5.911 | 82.5 | slower |
