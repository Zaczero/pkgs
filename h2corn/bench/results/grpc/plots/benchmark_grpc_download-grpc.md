| Server | Median messages/s | Blocks | Median complete RPC/s | Median completed RPCs/trial | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Connect / h2corn | 4,040 | 12 | 252 | 258 | 27.129 | 83.387 | 60.9 | - |
| Connect / Hypercorn | 2,277 | 12 | 142 | 148 | 50.091 | 126.641 | 57.0 | slower |
| grpc.aio | 6,368 | 12 | 398 | 401 | 18.290 | 53.170 | 55.0 | inconclusive |
| grpclib | not selected | - | - | - | - | - | - | - |

Collection: maximum-look.
