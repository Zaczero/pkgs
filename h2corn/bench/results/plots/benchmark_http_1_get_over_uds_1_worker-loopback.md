| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 107,674 | 6 | 0.906 | 1.275 | 51.7 | - |
| uvicorn | 9,288 | 6 | 10.658 | 11.840 | 26.7 | slower |
| hypercorn | 3,780 | 6 | 25.372 | 35.896 | 50.9 | slower |
| gunicorn | 13,240 | 6 | 7.441 | 8.960 | 40.2 | slower |
