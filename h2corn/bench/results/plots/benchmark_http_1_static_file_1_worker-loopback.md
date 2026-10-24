| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 9,049 | 6 | 10.718 | 20.868 | 161.2 | - |
| uvicorn | 1,132 | 6 | 86.850 | 97.205 | 67.5 | slower |
| hypercorn | 845 | 6 | 112.922 | 139.157 | 92.3 | slower |
| gunicorn | failed | - | - | - | - | - |

Collection: direction-resolved.
