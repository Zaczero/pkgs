| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 19,497 | 6 | 4.785 | 12.324 | 68.0 | - |
| uvicorn | unsupported | - | - | - | - | - |
| hypercorn | 998 | 6 | 96.295 | 134.920 | 66.2 | slower |
| gunicorn | unsupported | - | - | - | - | - |

Collection: direction-resolved.
