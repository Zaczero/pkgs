| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 106,942 | 6 | 0.910 | 1.413 | 51.8 | - |
| uvicorn | 7,721 | 6 | 12.606 | 15.550 | 26.6 | slower |
| hypercorn | 3,412 | 6 | 28.058 | 41.550 | 50.4 | slower |
| gunicorn | 10,994 | 6 | 8.795 | 10.531 | 40.1 | slower |
