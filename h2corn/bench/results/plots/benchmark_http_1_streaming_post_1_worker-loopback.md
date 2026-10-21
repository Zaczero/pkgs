| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 18,673 | 6 | 50.374 | 207.292 | 113.5 | - |
| uvicorn | 3,024 | 6 | 306.446 | 390.310 | 56.6 | inconclusive (calibration-limited) |
| hypercorn | 1,747 | 6 | 508.513 | 732.149 | 113.6 | inconclusive (calibration-limited) |
| gunicorn | 5,067 | 6 | 175.870 | 1,455.940 | 69.8 | inconclusive (calibration-limited) |
