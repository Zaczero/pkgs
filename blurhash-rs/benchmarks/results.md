# BlurHash image benchmarks

AMD Ryzen 9 5950X 16-Core Processor · Linux-7.2.9-x86_64-with-glibc2.42 · cpython 3.15.0 (64-bit)
Pillow 13.0.0 · NumPy 2.5.4
Each value is a median of 12 independent worker-process means; 95% percentile bootstrap intervals use 10,000 resamples across those workers.

## Encode a Pillow image

| Workload | Implementation | Median time | 95% interval | Worker processes |
| --- | --- | ---: | ---: | ---: |
| Encode · 32 x 32 square · RGB · 4 x 3 components | blurhash-rs 1.1.0 | 4.12 µs | 4.02 µs to 4.28 µs | 12 |
| Encode · 32 x 32 square · RGB · 4 x 3 components | blurhash 1.1.5 | 5.07 ms | 4.89 ms to 5.27 ms | 12 |
| Encode · 32 x 32 square · RGB · 4 x 3 components | blurhash-python 1.2.2 | 524 µs | 517 µs to 532 µs | 12 |
| Encode · 64 x 64 square · RGB · 4 x 3 components | blurhash-rs 1.1.0 | 7.73 µs | 7.49 µs to 7.96 µs | 12 |
| Encode · 64 x 64 square · RGB · 4 x 3 components | blurhash 1.1.5 | 20 ms | 19.4 ms to 20.6 ms | 12 |
| Encode · 64 x 64 square · RGB · 4 x 3 components | blurhash-python 1.2.2 | 2.12 ms | 2.09 ms to 2.17 ms | 12 |
| Encode · 128 x 128 square · RGB · 4 x 3 components | blurhash-rs 1.1.0 | 22.1 µs | 21.6 µs to 22.4 µs | 12 |
| Encode · 128 x 128 square · RGB · 4 x 3 components | blurhash 1.1.5 | 78.6 ms | 75.7 ms to 80.1 ms | 12 |
| Encode · 128 x 128 square · RGB · 4 x 3 components | blurhash-python 1.2.2 | 8.37 ms | 8.07 ms to 8.82 ms | 12 |
| Encode · 96 x 54 landscape · RGB · 4 x 3 components | blurhash-rs 1.1.0 | 8.68 µs | 8.41 µs to 8.96 µs | 12 |
| Encode · 96 x 54 landscape · RGB · 4 x 3 components | blurhash 1.1.5 | 25.3 ms | 24.6 ms to 25.9 ms | 12 |
| Encode · 96 x 54 landscape · RGB · 4 x 3 components | blurhash-python 1.2.2 | 2.74 ms | 2.64 ms to 3.05 ms | 12 |
| Encode · 54 x 96 portrait · RGB · 3 x 4 components | blurhash-rs 1.1.0 | 9.19 µs | 8.85 µs to 9.34 µs | 12 |
| Encode · 54 x 96 portrait · RGB · 3 x 4 components | blurhash 1.1.5 | 24.9 ms | 24.5 ms to 25.9 ms | 12 |
| Encode · 54 x 96 portrait · RGB · 3 x 4 components | blurhash-python 1.2.2 | 2.68 ms | 2.62 ms to 2.74 ms | 12 |
| Encode · 32 x 32 square · RGB · 1 x 1 component | blurhash-rs 1.1.0 | 3.95 µs | 3.82 µs to 4 µs | 12 |
| Encode · 32 x 32 square · RGB · 1 x 1 component | blurhash 1.1.5 | 1.31 ms | 1.28 ms to 1.35 ms | 12 |
| Encode · 32 x 32 square · RGB · 1 x 1 component | blurhash-python 1.2.2 | 160 µs | 157 µs to 163 µs | 12 |
| Encode · 32 x 32 square · RGB · 9 x 9 components | blurhash-rs 1.1.0 | 6.18 µs | 6.11 µs to 6.48 µs | 12 |
| Encode · 32 x 32 square · RGB · 9 x 9 components | blurhash 1.1.5 | 28.3 ms | 27.9 ms to 28.6 ms | 12 |
| Encode · 32 x 32 square · RGB · 9 x 9 components | blurhash-python 1.2.2 | 3.05 ms | 2.95 ms to 3.1 ms | 12 |
| Encode · 64 x 32 landscape · RGBA input · 4 x 3 components | blurhash-rs 1.1.0 | 8.38 µs | 8.29 µs to 8.57 µs | 12 |
| Encode · 64 x 32 landscape · RGBA input · 4 x 3 components | blurhash 1.1.5 | 10.1 ms | 9.93 ms to 10.3 ms | 12 |
| Encode · 64 x 32 landscape · RGBA input · 4 x 3 components | blurhash-python 1.2.2 | 1.07 ms | 1.03 ms to 1.09 ms | 12 |
| Encode · 32 x 32 square · grayscale input · 4 x 3 components | blurhash-rs 1.1.0 | 6.14 µs | 6.05 µs to 6.2 µs | 12 |
| Encode · 32 x 32 square · grayscale input · 4 x 3 components | blurhash 1.1.5 | 5.12 ms | 5.01 ms to 5.18 ms | 12 |
| Encode · 32 x 32 square · grayscale input · 4 x 3 components | blurhash-python 1.2.2 | 551 µs | 538 µs to 559 µs | 12 |

## Decode to a Pillow image

| Workload | Implementation | Median time | 95% interval | Worker processes |
| --- | --- | ---: | ---: | ---: |
| Decode · 32 x 32 square · RGB · 4 x 3 components | blurhash-rs 1.1.0 | 10.5 µs | 10.2 µs to 10.7 µs | 12 |
| Decode · 32 x 32 square · RGB · 4 x 3 components | blurhash 1.1.5 | 5.38 ms | 5.23 ms to 5.58 ms | 12 |
| Decode · 32 x 32 square · RGB · 4 x 3 components | blurhash-python 1.2.2 | 216 µs | 210 µs to 220 µs | 12 |
| Decode · 64 x 64 square · RGB · 4 x 3 components | blurhash-rs 1.1.0 | 31.8 µs | 31.2 µs to 32.5 µs | 12 |
| Decode · 64 x 64 square · RGB · 4 x 3 components | blurhash 1.1.5 | 21.6 ms | 20.7 ms to 22.4 ms | 12 |
| Decode · 64 x 64 square · RGB · 4 x 3 components | blurhash-python 1.2.2 | 841 µs | 825 µs to 859 µs | 12 |
| Decode · 128 x 128 square · RGB · 4 x 3 components | blurhash-rs 1.1.0 | 115 µs | 112 µs to 116 µs | 12 |
| Decode · 128 x 128 square · RGB · 4 x 3 components | blurhash 1.1.5 | 84 ms | 82.9 ms to 90.2 ms | 12 |
| Decode · 128 x 128 square · RGB · 4 x 3 components | blurhash-python 1.2.2 | 3.33 ms | 3.29 ms to 3.49 ms | 12 |
| Decode · 96 x 54 landscape · RGB · 4 x 3 components | blurhash-rs 1.1.0 | 38 µs | 37.5 µs to 39.2 µs | 12 |
| Decode · 96 x 54 landscape · RGB · 4 x 3 components | blurhash 1.1.5 | 26.7 ms | 26.3 ms to 28 ms | 12 |
| Decode · 96 x 54 landscape · RGB · 4 x 3 components | blurhash-python 1.2.2 | 1.07 ms | 1.04 ms to 1.11 ms | 12 |
| Decode · 54 x 96 portrait · RGB · 3 x 4 components | blurhash-rs 1.1.0 | 38.7 µs | 38.1 µs to 39.1 µs | 12 |
| Decode · 54 x 96 portrait · RGB · 3 x 4 components | blurhash 1.1.5 | 27.2 ms | 27 ms to 27.7 ms | 12 |
| Decode · 54 x 96 portrait · RGB · 3 x 4 components | blurhash-python 1.2.2 | 1.06 ms | 1.03 ms to 1.08 ms | 12 |
| Decode · 32 x 32 square · RGB · 1 x 1 component | blurhash-rs 1.1.0 | 10.8 µs | 10.5 µs to 12.4 µs | 12 |
| Decode · 32 x 32 square · RGB · 1 x 1 component | blurhash 1.1.5 | 1.21 ms | 1.18 ms to 1.25 ms | 12 |
| Decode · 32 x 32 square · RGB · 1 x 1 component | blurhash-python 1.2.2 | 63.4 µs | 44.3 µs to 92.8 µs | 12 |
| Decode · 32 x 32 square · RGB · 9 x 9 components | blurhash-rs 1.1.0 | 13 µs | 12.7 µs to 13.9 µs | 12 |
| Decode · 32 x 32 square · RGB · 9 x 9 components | blurhash 1.1.5 | 30.3 ms | 29.8 ms to 31.1 ms | 12 |
| Decode · 32 x 32 square · RGB · 9 x 9 components | blurhash-python 1.2.2 | 1.5 ms | 1.42 ms to 2.87 ms | 12 |

All three implementations use the same image dimensions and component counts. Pillow-to-NumPy conversion, the RGB image copy required by blurhash-python, and Pillow output construction are included where their interfaces require them.
