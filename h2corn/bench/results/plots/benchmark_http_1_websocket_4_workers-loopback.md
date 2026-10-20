| Server | Sessions/s | p50 session (ms) | p99 session (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- |
| h2corn | 40,322 | 2.089 | 6.491 | 133.1 | - |
| uvicorn | 7,308 | 12.784 | 31.363 | 166.5 | slower |
| hypercorn | 7,321 | 13.091 | 29.173 | 143.2 | slower |
| gunicorn | 12,679 | 5.448 | 29.818 | 932.9 | slower |
