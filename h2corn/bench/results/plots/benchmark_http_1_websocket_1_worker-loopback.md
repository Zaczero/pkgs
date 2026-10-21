| Server | Sessions/s | Blocks | p50 session (ms) | p99 session (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 23,786 | 6 | 4.060 | 6.276 | 61.3 | - |
| uvicorn | 2,149 | 6 | 43.333 | 100.922 | 58.8 | inconclusive (calibration-limited) |
| hypercorn | 2,136 | 6 | 45.273 | 75.159 | 60.3 | inconclusive (calibration-limited) |
| gunicorn | 3,273 | 6 | 17.983 | 410.084 | 366.4 | inconclusive (calibration-limited) |
