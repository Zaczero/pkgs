| Server | Median RPC/s | Blocks | Median completed RPCs/trial | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Connect / h2corn | 1,044 | 12 | 1,046.5 | 7.581 | 10.845 | 69.2 | - |
| Connect / Hypercorn | 213 | 12 | 217 | 37.059 | 43.673 | 38.6 | slower |
| grpc.aio | 1,012 | 12 | 1,015 | 7.887 | 11.398 | 159.9 | inconclusive |
| grpclib | not selected | - | - | - | - | - | - |

Collection: maximum-look.
