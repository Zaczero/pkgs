| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 43,656 | 6 | 22.167 | 28.806 | 173.3 | - |
| uvicorn | 12,451 | 6 | 76.263 | 114.058 | 148.3 | slower |
| hypercorn | 7,278 | 6 | 143.906 | 259.122 | 190.4 | slower |
| gunicorn | 20,431 | 6 | 47.212 | 67.748 | 122.3 | slower |
