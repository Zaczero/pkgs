| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 80,855 | 6 | 1.183 | 1.972 | 60.7 | - |
| uvicorn | 7,939 | 6 | 12.246 | 17.073 | 27.2 | slower |
| hypercorn | 3,102 | 6 | 30.358 | 49.371 | 51.0 | slower |
| gunicorn | 11,220 | 6 | 8.652 | 11.052 | 40.8 | slower |

Collection: direction-resolved.
