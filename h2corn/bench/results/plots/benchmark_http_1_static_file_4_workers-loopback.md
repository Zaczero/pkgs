| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 33,756 | 8 | 2.735 | 7.008 | 349.1 | - |
| uvicorn | 5,258 | 8 | 17.297 | 33.497 | 238.9 | slower |
| hypercorn | 4,288 | 8 | 21.115 | 45.017 | 253.2 | slower |
| gunicorn | 6,751 | 8 | 13.816 | 22.435 | 194.0 | slower |
