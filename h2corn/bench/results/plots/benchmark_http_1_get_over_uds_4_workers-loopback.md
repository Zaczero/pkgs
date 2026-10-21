| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 262,844 | 8 | 0.346 | 0.749 | 107.6 | - |
| uvicorn | 33,631 | 8 | 2.776 | 6.087 | 110.7 | slower |
| hypercorn | 14,553 | 8 | 7.945 | 12.082 | 123.5 | slower |
| gunicorn | 43,942 | 8 | 2.251 | 4.005 | 82.0 | slower |
