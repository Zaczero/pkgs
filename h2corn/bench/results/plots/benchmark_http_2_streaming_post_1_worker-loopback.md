| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 15,656 | 6 | 52.810 | 604.421 | 112.6 | - |
| uvicorn | unsupported | - | - | - | - | - |
| hypercorn | 1,077 | 6 | 694.243 | 1,405.321 | 139.2 | inconclusive (calibration-limited) |
| gunicorn | unsupported | - | - | - | - | - |
