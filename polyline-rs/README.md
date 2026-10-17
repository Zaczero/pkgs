# polyline-rs

[![PyPI - Python Version](https://shields.monicz.dev/pypi/pyversions/polyline-rs)](https://pypi.org/p/polyline-rs)

Encode and decode Google Encoded Polyline strings from Python with an explicit choice of `(latitude, longitude)` or `(longitude, latitude)` coordinate order. Requires Python 3.11 or newer.

[Encoded Polyline Algorithm Format](https://developers.google.com/maps/documentation/utilities/polylinealgorithm)

## Installation

```sh
pip install polyline-rs
```

## Basic usage

```py
from polyline_rs import decode_latlon, decode_lonlat, encode_latlon

line = encode_latlon([(38.5, -120.2), (40.7, -120.95), (43.252, -126.453)], 5)
assert line == "_p~iF~ps|U_ulLnnqC_mqNvxq`@"

coords = decode_latlon(line, 5)
lonlat = decode_lonlat(line, 5)  # Same points, with longitude first.
```

## Coordinate and precision limits

Use the same precision when encoding and decoding; `5` means five decimal
places. Coordinates are rounded half away from zero. Scaled coordinates and
their successive differences must fit signed 32-bit integers; overflow and
malformed encodings raise `ValueError`.

The `latlon` functions use latitude first; `lonlat` uses longitude first.
Rows accept at least two numbers and ignore additional values.

## API reference and support

The four function signatures and error contracts are documented in the
[package typing stubs](https://github.com/Zaczero/pkgs/blob/main/polyline-rs/python/polyline_rs/_lib.pyi).

[Issues and questions](https://github.com/Zaczero/pkgs/issues) ·
[Private security reports](https://github.com/Zaczero/pkgs/security/advisories/new) ·
[Premium support](https://monicz.dev/#get-in-touch)
