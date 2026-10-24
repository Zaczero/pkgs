| Server | Sessions/s | Blocks | p50 session (ms) | p99 session (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 20,168 | 6 | 4.594 | 10.518 | 70.6 | - |
| uvicorn | 1,682 | 6 | 53.942 | 163.818 | 59.6 | inconclusive (calibration-limited) |
| hypercorn | 1,781 | 6 | 54.634 | 100.598 | 60.8 | inconclusive (calibration-limited) |
| gunicorn | 2,805 | 6 | 21.252 | 416.293 | 337.2 | inconclusive (calibration-limited) |

Collection: calibration-limited.
