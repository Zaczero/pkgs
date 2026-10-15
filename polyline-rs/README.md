# polyline-rs

[![PyPI - Python Version](https://shields.monicz.dev/pypi/pyversions/polyline-rs)](https://pypi.org/p/polyline-rs)

Encode and decode Google Encoded Polyline strings from Python with an explicit choice of `(latitude, longitude)` or `(longitude, latitude)` coordinate order. Requires Python 3.10 or newer.

[Encoded Polyline Algorithm Format](https://developers.google.com/maps/documentation/utilities/polylinealgorithm)

## Installation

```sh
pip install polyline-rs
```

## Basic usage

```py
from polyline_rs import decode_latlon, decode_lonlat, encode_latlon, encode_lonlat

line = encode_latlon([(38.5, -120.2), (40.7, -120.95), (43.252, -126.453)], 5)
assert line == "_p~iF~ps|U_ulLnnqC_mqNvxq`@"

coords = decode_latlon(line, 5)
assert [tuple(round(value, 5) for value in row) for row in coords] == [
    (38.5, -120.2),
    (40.7, -120.95),
    (43.252, -126.453),
]

coords2 = decode_lonlat(line, 5)
assert [tuple(round(value, 5) for value in row) for row in coords2] == [
    (-120.2, 38.5),
    (-120.95, 40.7),
    (-126.453, 43.252),
]
```

## Coordinate and precision limits

Each coordinate is multiplied by `10**precision` and rounded half away from
zero. The rounded integer coordinate and each difference from the preceding
coordinate must fit signed 32-bit integers; an unrepresentable coordinate or
delta raises `ValueError` instead of producing a changed location. A precision
is accepted when its decimal scale and inverse are finite and positive and the
decoded signed 32-bit coordinate range remains finite. No separate arbitrary
precision cap is imposed.

Rows need at least two float-convertible values. Additional values are ignored.
The decoder strips surrounding whitespace and raises `ValueError` for invalid
characters, unterminated values, incomplete coordinate pairs, or coordinate
overflow. Wrong input types raise `TypeError`; Python iterator exceptions
propagate. Integers outside the native argument range raise `OverflowError`.

## API reference and support

The four function signatures and error contracts are documented in the
[package typing stubs](https://github.com/Zaczero/pkgs/blob/main/polyline-rs/python/polyline_rs/_lib.pyi).
Report issues in the [repository issue tracker](https://github.com/Zaczero/pkgs/issues).
