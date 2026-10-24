| Server | Median RPC/s | Blocks | Median completed RPCs/trial | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Connect / h2corn | 4,260 | 6 | 4,260 | 0.219 | 0.510 | 60.5 | - |
| Connect / Hypercorn | 1,720 | 6 | 1,721 | 0.543 | 1.063 | 36.4 | slower |
| grpc.aio | 2,936 | 6 | 2,936 | 0.317 | 0.576 | 53.4 | slower |
| grpclib | not selected | - | - | - | - | - | - |

Collection: direction-resolved.
