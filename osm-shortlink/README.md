# osm-shortlink

[![PyPI - Python Version](https://shields.monicz.dev/pypi/pyversions/osm-shortlink)](https://pypi.org/p/osm-shortlink)

Encode and decode OpenStreetMap shortlink codes from Python 3.10 or newer. The
API uses explicit `(longitude, latitude, zoom)` arguments.

## Installation

```sh
pip install osm-shortlink
```

## Basic usage

```py
from osm_shortlink import shortlink_decode, shortlink_encode

code = shortlink_encode(0.055, 51.511, 9)
assert code == '0EEQjE--'

lon, lat, zoom = shortlink_decode(code)
assert (lon, lat, zoom) == (0.054931640625, 51.510772705078125, 9)
```

The decoded coordinate is the grid-aligned location represented by the code,
not a bit-for-bit copy of the input. In this example it identifies the cell
containing longitude `0.055` and latitude `51.511`.

## Coordinate and code behavior

Longitude and latitude must be finite. Longitude wraps around the world;
latitude reflects across either pole and shifts longitude by 180 degrees. The
north pole maps to the highest latitude cell.
Zoom values from 0 through 22 are supported. A Python integer outside the
unsigned-byte range can fail during argument conversion; an extracted zoom
above 22 raises `ValueError` before coordinate finiteness is checked.

The decoder ignores characters outside its alphabet and accepts the legacy
`@` and `=` spellings. It is a shortlink-code decoder, not a complete URL
parser. Too-short and too-long encoded values raise `ValueError`.

```py
from osm_shortlink import shortlink_encode

try:
    shortlink_encode(0.0, 0.0, 23)
except ValueError:
    pass
else:
    raise AssertionError('zoom 23 must be rejected')
```

## Format specification

The [OpenStreetMap Shortlink format](https://wiki.openstreetmap.org/wiki/Shortlink)
defines the token and its coordinate quantization.

## API reference and support

The two function signatures and behavior are documented in the
[package typing stubs](https://github.com/Zaczero/pkgs/blob/main/osm-shortlink/python/osm_shortlink/_lib.pyi).
Report issues in the [repository issue tracker](https://github.com/Zaczero/pkgs/issues).
