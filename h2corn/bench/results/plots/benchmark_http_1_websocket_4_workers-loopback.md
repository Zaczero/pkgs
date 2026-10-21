| Server | Sessions/s | Blocks | p50 session (ms) | p99 session (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 43,133 | 6 | 2.007 | 5.430 | 130.0 | - |
| uvicorn | 7,486 | 6 | 12.493 | 31.680 | 164.7 | inconclusive (calibration-limited) |
| hypercorn | 7,512 | 6 | 12.715 | 27.902 | 141.9 | inconclusive (calibration-limited) |
| gunicorn | 11,452 | 6 | 5.384 | 74.994 | 1,240.3 | inconclusive (calibration-limited) |
