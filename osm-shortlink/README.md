# osm-shortlink

[![PyPI - Python Version](https://shields.monicz.dev/pypi/pyversions/osm-shortlink)](https://pypi.org/p/osm-shortlink)

Encode and decode OpenStreetMap shortlink codes from Python 3.11 or newer. The
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
Zoom values from 0 through 22 are supported.

The decoder ignores characters outside its alphabet and accepts the legacy
`@` and `=` spellings. It is a shortlink-code decoder, not a complete URL
parser. Too-short and too-long encoded values raise `ValueError`.

## Format specification

The [OpenStreetMap Shortlink format](https://wiki.openstreetmap.org/wiki/Shortlink)
defines the token and its coordinate quantization.

## API reference and support

The two function signatures and behavior are documented in the
[package typing stubs](https://github.com/Zaczero/pkgs/blob/main/osm-shortlink/python/osm_shortlink/_lib.pyi).

[Issues and questions](https://github.com/Zaczero/pkgs/issues) ·
[Private security reports](https://github.com/Zaczero/pkgs/security/advisories/new) ·
[Premium support](https://monicz.dev/#get-in-touch)
