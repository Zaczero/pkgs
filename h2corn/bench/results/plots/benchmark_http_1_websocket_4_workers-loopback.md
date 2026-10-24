| Server | Sessions/s | Blocks | p50 session (ms) | p99 session (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 31,469 | 6 | 2.700 | 9.689 | 145.8 | - |
| uvicorn | 6,260 | 6 | 14.652 | 45.078 | 165.7 | slower |
| hypercorn | 6,079 | 6 | 15.447 | 36.233 | 142.4 | slower |
| gunicorn | 9,800 | 6 | 6.535 | 136.879 | 1,123.0 | slower |

Collection: direction-resolved.
