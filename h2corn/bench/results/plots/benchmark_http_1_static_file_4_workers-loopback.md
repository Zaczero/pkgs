| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 22,324 | 6 | 4.067 | 13.406 | 384.4 | - |
| uvicorn | 3,676 | 6 | 26.247 | 49.064 | 206.2 | slower |
| hypercorn | 2,959 | 6 | 29.244 | 61.126 | 217.2 | slower |
| gunicorn | failed | - | - | - | - | - |

Collection: direction-resolved.
