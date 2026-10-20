| Server | Requests/s | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- |
| h2corn | 100,075 | 0.917 | 1.742 | 51.0 | - |
| uvicorn | 7,775 | 12.564 | 17.191 | 26.9 | slower |
| hypercorn | 3,408 | 28.505 | 36.102 | 50.7 | slower |
| gunicorn | 9,065 | 10.706 | 14.866 | 37.2 | slower |
