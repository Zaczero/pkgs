| Server | Requests/s | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- |
| h2corn | 18,616 | 5.345 | 10.701 | 61.4 | - |
| uvicorn | 2,828 | 34.584 | 46.737 | 33.1 | slower |
| hypercorn | 1,826 | 53.533 | 67.572 | 58.8 | slower |
| gunicorn | 3,849 | 25.287 | 32.622 | 44.3 | slower |
