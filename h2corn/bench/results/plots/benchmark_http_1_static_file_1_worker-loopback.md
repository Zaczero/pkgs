| Server | Requests/s | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- |
| h2corn | 11,604 | 8.409 | 14.568 | 144.8 | - |
| uvicorn | 1,429 | 69.443 | 78.584 | 67.7 | slower |
| hypercorn | 1,144 | 86.384 | 99.527 | 95.7 | slower |
| gunicorn | 1,866 | 53.139 | 62.378 | 77.4 | slower |
