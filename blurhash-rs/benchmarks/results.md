# BlurHash image benchmarks

AMD Ryzen 9 5950X 16-Core Processor · CPython 3.15.0 (64-bit) · Pillow 13.0.0 · NumPy 2.5.4

Median time per image; lower is faster.

## Encode Pillow images

| Workload | blurhash-rs 1.1.0 | blurhash 1.1.5 | blurhash-python 1.2.2 |
| --- | ---: | ---: | ---: |
| 32 x 32 square · RGB · 4 x 3 components | 4.12 µs | 5.07 ms | 524 µs |
| 64 x 64 square · RGB · 4 x 3 components | 7.73 µs | 20 ms | 2.12 ms |
| 128 x 128 square · RGB · 4 x 3 components | 22.1 µs | 78.6 ms | 8.37 ms |
| 96 x 54 landscape · RGB · 4 x 3 components | 8.68 µs | 25.3 ms | 2.74 ms |
| 54 x 96 portrait · RGB · 3 x 4 components | 9.19 µs | 24.9 ms | 2.68 ms |
| 32 x 32 square · RGB · 1 x 1 component | 3.95 µs | 1.31 ms | 160 µs |
| 32 x 32 square · RGB · 9 x 9 components | 6.18 µs | 28.3 ms | 3.05 ms |
| 64 x 32 landscape · RGBA input · 4 x 3 components | 8.38 µs | 10.1 ms | 1.07 ms |
| 32 x 32 square · grayscale input · 4 x 3 components | 6.14 µs | 5.12 ms | 551 µs |

## Decode RGB Pillow images

| Workload | blurhash-rs 1.1.0 | blurhash 1.1.5 | blurhash-python 1.2.2 |
| --- | ---: | ---: | ---: |
| 32 x 32 square · RGB · 4 x 3 components | 10.5 µs | 5.38 ms | 216 µs |
| 64 x 64 square · RGB · 4 x 3 components | 31.8 µs | 21.6 ms | 841 µs |
| 128 x 128 square · RGB · 4 x 3 components | 115 µs | 84 ms | 3.33 ms |
| 96 x 54 landscape · RGB · 4 x 3 components | 38 µs | 26.7 ms | 1.07 ms |
| 54 x 96 portrait · RGB · 3 x 4 components | 38.7 µs | 27.2 ms | 1.06 ms |
| 32 x 32 square · RGB · 1 x 1 component | 10.8 µs | 1.21 ms | 63.4 µs |
| 32 x 32 square · RGB · 9 x 9 components | 13 µs | 30.3 ms | 1.5 ms |
