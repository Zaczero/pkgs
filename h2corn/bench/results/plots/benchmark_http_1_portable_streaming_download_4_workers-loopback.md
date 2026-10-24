| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 41,362 | 6 | 2.352 | 5.634 | 136.2 | - |
| uvicorn | 8,830 | 6 | 10.470 | 23.990 | 123.7 | slower |
| hypercorn | 5,900 | 6 | 17.657 | 35.305 | 136.7 | slower |
| gunicorn | 12,869 | 6 | 7.268 | 14.140 | 92.1 | slower |

Collection: direction-resolved.
