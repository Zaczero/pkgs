| Server | Median RPC/s | Blocks | Median completed RPCs/trial | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Connect / h2corn | 15,457 | 6 | 15,507 | 3.911 | 8.592 | 68.5 | - |
| Connect / Hypercorn | 2,544 | 6 | 2,590.5 | 24.400 | 36.148 | 39.7 | slower |
| grpc.aio | 5,143 | 6 | 5,204.5 | 12.342 | 15.626 | 58.8 | slower |
| grpclib | not selected | - | - | - | - | - | - |

Collection: direction-resolved.
