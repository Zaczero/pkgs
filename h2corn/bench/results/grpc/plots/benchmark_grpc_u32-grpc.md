| Server | Median RPC/s | Blocks | Median completed RPCs/trial | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Connect / h2corn | 16,123 | 6 | 32,297 | 3.777 | 7.618 | 65.4 | - |
| Connect / Hypercorn | 2,966 | 6 | 5,952 | 21.333 | 34.223 | 38.1 | slower |
| grpc.aio | 5,695 | 6 | 11,452.5 | 11.287 | 13.565 | 57.1 | slower |
| grpclib | 4,506 | 6 | 9,080.5 | 13.838 | 19.819 | 30.7 | slower |

Collection: direction-resolved.
