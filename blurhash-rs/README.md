# blurhash-rs

High-performance [BlurHash](https://blurha.sh) encoder/decoder for Python.

## Installation

```sh
pip install blurhash-rs
```

## Usage

```py
from PIL import Image
from blurhash_rs import blurhash_encode, blurhash_decode

with Image.open('image.jpg') as img:
  # Reduce resolution for faster encoding
  img.thumbnail((100, 100))

  blurhash = blurhash_encode(img, x_components=4, y_components=3)
  preview = blurhash_decode(blurhash, 32, 32)
  preview.save("preview.png")
```

## Benchmarks

### Run

```sh
benchmark.py --impl blurhash-rs -o blurhash-rs.json --rigorous
benchmark.py --impl blurhash-python -o blurhash-python.json --rigorous
benchmark.py --impl blurhash -o blurhash.json --rigorous
```

### Results

Linux x86_64, CPython 3.14.0:

#### blurhash-python vs blurhash-rs

| Benchmark            | blurhash-python-1.2.2 | blurhash-rs-1.0.0       |
|----------------------|:---------------------:|:-----------------------:|
| encode 32x32 (4x3)   | 486 us                | 5.57 us: 87.29x faster  |
| encode 64x64 (4x3)   | 1.91 ms               | 10.8 us: 177.73x faster |
| encode 128x128 (4x3) | 7.59 ms               | 30.6 us: 247.76x faster |
| decode 32x32 (4x3)   | 160 us                | 12.0 us: 13.31x faster  |
| decode 64x64 (4x3)   | 626 us                | 34.6 us: 18.08x faster  |
| decode 128x128 (4x3) | 2.45 ms               | 122 us: 20.08x faster   |
| Geometric mean       | (ref)                 | 51.46x faster           |

#### blurhash vs blurhash-rs

| Benchmark            | blurhash-1.1.5 | blurhash-rs-1.0.0        |
|----------------------|:--------------:|:------------------------:|
| encode 32x32 (4x3)   | 6.00 ms        | 5.57 us: 1078.28x faster |
| encode 64x64 (4x3)   | 24.2 ms        | 10.8 us: 2249.49x faster |
| encode 128x128 (4x3) | 96.4 ms        | 30.6 us: 3146.95x faster |
| decode 32x32 (4x3)   | 6.91 ms        | 12.0 us: 573.46x faster  |
| decode 64x64 (4x3)   | 27.6 ms        | 34.6 us: 797.99x faster  |
| decode 128x128 (4x3) | 112 ms         | 122 us: 912.88x faster   |
| Geometric mean       | (ref)          | 1213.21x faster          |
