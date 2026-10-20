| Server | Requests/s | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- |
| h2corn | 17,155 | 53.880 | 110.687 | 117.9 | - |
| uvicorn | 2,671 | 349.055 | 583.435 | 69.8 | slower |
| hypercorn | 1,695 | 551.208 | 931.417 | 107.3 | slower |
| gunicorn | 4,215 | 214.232 | 1,717.600 | 67.0 | slower |
