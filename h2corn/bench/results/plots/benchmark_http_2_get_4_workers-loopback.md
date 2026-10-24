| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 187,215 | 6 | 0.499 | 1.295 | 119.7 | - |
| uvicorn | unsupported | - | - | - | - | - |
| hypercorn | 8,621 | 6 | 8.576 | 26.046 | 130.6 | slower |
| gunicorn | unsupported | - | - | - | - | - |

Collection: direction-resolved.
