| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 14,021 | 6 | 66.273 | 279.944 | 115.6 | - |
| uvicorn | 2,344 | 6 | 384.438 | 548.202 | 72.1 | inconclusive (calibration-limited) |
| hypercorn | 1,466 | 6 | 584.880 | 985.080 | 114.6 | inconclusive (calibration-limited) |
| gunicorn | 4,041 | 6 | 175.390 | 2,653.769 | 69.8 | inconclusive (calibration-limited) |

Collection: calibration-limited.
