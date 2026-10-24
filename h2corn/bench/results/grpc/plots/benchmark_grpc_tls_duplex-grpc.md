| Server | Median messages/s | Blocks | Median complete RPC/s | Median completed RPCs/trial | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Connect / h2corn | 64,624 | 6 | 1,010 | 1,013 | 7.810 | 10.684 | 68.0 | - |
| Connect / Hypercorn | 13,923 | 6 | 218 | 221.5 | 37.804 | 49.979 | 39.2 | slower |
| grpc.aio | 28,784 | 6 | 450 | 454.5 | 17.746 | 22.422 | 57.7 | slower |
| grpclib | not selected | - | - | - | - | - | - | - |

Collection: direction-resolved.
