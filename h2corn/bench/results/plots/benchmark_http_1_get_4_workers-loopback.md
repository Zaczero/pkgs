| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 255,781 | 6 | 0.363 | 0.808 | 108.7 | - |
| uvicorn | 28,661 | 6 | 3.163 | 6.490 | 110.7 | slower |
| hypercorn | 13,263 | 6 | 7.696 | 13.252 | 123.1 | slower |
| gunicorn | 41,346 | 6 | 1.939 | 4.528 | 85.1 | slower |
