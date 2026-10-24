| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 7,532 | 6 | 12.138 | 30.739 | 283.1 | - |
| uvicorn | unsupported | - | - | - | - | - |
| hypercorn | 604 | 6 | 146.983 | 287.197 | 107.3 | inconclusive (calibration-limited) |
| gunicorn | unsupported | - | - | - | - | - |

Collection: calibration-limited.
