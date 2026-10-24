| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 9,356 | 6 | 77.189 | 762.721 | 120.2 | - |
| uvicorn | unsupported | - | - | - | - | - |
| hypercorn | 748 | 6 | 955.204 | 1,691.458 | 149.7 | inconclusive (calibration-limited) |
| gunicorn | unsupported | - | - | - | - | - |

Collection: calibration-limited.
