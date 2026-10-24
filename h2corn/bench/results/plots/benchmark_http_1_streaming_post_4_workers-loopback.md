| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 40,491 | 6 | 23.144 | 40.891 | 186.8 | - |
| uvicorn | 9,746 | 6 | 96.480 | 150.120 | 154.9 | slower |
| hypercorn | 3,942 | 6 | 196.910 | 592.993 | 198.4 | slower |
| gunicorn | 16,740 | 6 | 56.052 | 93.489 | 124.2 | slower |

Collection: direction-resolved.
