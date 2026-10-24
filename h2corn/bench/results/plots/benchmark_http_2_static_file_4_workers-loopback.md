| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 14,464 | 6 | 5.505 | 31.033 | 392.2 | - |
| uvicorn | unsupported | - | - | - | - | - |
| hypercorn | 1,893 | 6 | 53.222 | 93.276 | 216.5 | slower |
| gunicorn | unsupported | - | - | - | - | - |

Collection: direction-resolved.
