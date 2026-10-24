| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 83,847 | 6 | 1.108 | 1.740 | 61.0 | - |
| uvicorn | unsupported | - | - | - | - | - |
| hypercorn | 2,240 | 6 | 40.758 | 75.234 | 56.8 | slower |
| gunicorn | unsupported | - | - | - | - | - |

Collection: direction-resolved.
