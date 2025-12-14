# polyline-rs

[![PyPI - Python Version](https://shields.monicz.dev/pypi/pyversions/polyline-rs)](https://pypi.org/project/polyline-rs)

Fast Google Encoded Polyline encoding & decoding in Rust with Python bindings. Library with out-of-the-box support for both (lat, lon) and (lon, lat) coordinates.

[Encoded Polyline Algorithm Format](https://developers.google.com/maps/documentation/utilities/polylinealgorithm)

## Installation

```sh
pip install polyline-rs
```

## Basic usage

```py
from polyline_rs import encode_latlon, encode_lonlat, decode_latlon, decode_lonlat

line = encode_latlon([(38.5, -120.2), (40.7, -120.95), (43.252, -126.453)], 5)
assert line == "_p~iF~ps|U_ulLnnqC_mqNvxq`@"

coords = decode_latlon(line, 5)
assert coords == [(38.5, -120.2), (40.7, -120.95), (43.252, -126.453)]

coords2 = decode_lonlat(line, 5)
assert coords2 == [(-120.2, 38.5), (-120.95, 40.7), (-126.453, 43.252)]
```
