| Server | Sessions/s | Blocks | p50 session (ms) | p99 session (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 22,923 | 8 | 4.176 | 7.062 | 62.2 | - |
| uvicorn | 1,929 | 8 | 47.580 | 153.846 | 60.9 | slower |
| hypercorn | 1,953 | 8 | 49.351 | 88.172 | 61.5 | slower |
| gunicorn | 3,157 | 8 | 19.659 | 392.009 | 277.4 | slower |
