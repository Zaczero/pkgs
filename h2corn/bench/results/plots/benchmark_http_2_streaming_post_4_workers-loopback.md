| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 39,175 | 6 | 23.602 | 42.464 | 184.9 | - |
| uvicorn | unsupported | - | - | - | - | - |
| hypercorn | 3,440 | 6 | 254.765 | 516.871 | 239.6 | inconclusive (calibration-limited) |
| gunicorn | unsupported | - | - | - | - | - |

Collection: calibration-limited.
