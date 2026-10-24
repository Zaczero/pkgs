| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 202,293 | 6 | 0.457 | 1.121 | 119.4 | - |
| uvicorn | 24,064 | 6 | 3.798 | 8.421 | 111.9 | slower |
| hypercorn | 11,179 | 6 | 9.103 | 18.138 | 124.2 | slower |
| gunicorn | 34,237 | 6 | 3.000 | 5.188 | 86.5 | slower |

Collection: direction-resolved.
