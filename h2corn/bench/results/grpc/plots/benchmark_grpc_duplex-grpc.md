| Server | Median messages/s | Blocks | Median complete RPC/s | Median completed RPCs/trial | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Connect / h2corn | 59,968 | 6 | 937 | 941.5 | 8.541 | 11.455 | 62.9 | - |
| Connect / Hypercorn | 14,083 | 6 | 220 | 224.5 | 36.904 | 63.880 | 36.7 | slower |
| grpc.aio | 25,991 | 6 | 406 | 410.5 | 18.379 | 37.188 | 56.1 | slower |
| grpclib | 17,833 | 6 | 279 | 281 | 29.054 | 34.233 | 29.7 | slower |

Collection: direction-resolved.
