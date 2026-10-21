| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 106,292 | 8 | 0.911 | 1.344 | 51.4 | - |
| uvicorn | 9,208 | 8 | 10.636 | 13.610 | 27.0 | slower |
| hypercorn | 3,832 | 8 | 25.761 | 30.191 | 50.7 | slower |
| gunicorn | 11,943 | 8 | 8.227 | 10.586 | 37.2 | slower |
