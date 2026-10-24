| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 223,459 | 6 | 0.422 | 0.927 | 119.4 | - |
| uvicorn | 29,455 | 6 | 3.209 | 5.207 | 111.9 | slower |
| hypercorn | 12,539 | 6 | 7.107 | 16.699 | 123.8 | slower |
| gunicorn | 39,412 | 6 | 2.503 | 4.572 | 86.8 | slower |

Collection: direction-resolved.
