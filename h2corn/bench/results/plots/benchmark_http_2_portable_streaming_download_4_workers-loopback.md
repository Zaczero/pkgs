| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 44,998 | 6 | 2.123 | 3.867 | 134.4 | - |
| uvicorn | unsupported | - | - | - | - | - |
| hypercorn | 4,044 | 6 | 21.171 | 47.374 | 145.8 | slower |
| gunicorn | unsupported | - | - | - | - | - |

Collection: direction-resolved.
