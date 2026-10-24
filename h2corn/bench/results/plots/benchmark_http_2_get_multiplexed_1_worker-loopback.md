| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 66,073 | 6 | 0.131 | 0.341 | 58.7 | - |
| uvicorn | unsupported | - | - | - | - | - |
| hypercorn | 2,873 | 6 | 3.312 | 6.083 | 49.7 | slower |
| gunicorn | unsupported | - | - | - | - | - |

Collection: direction-resolved.
