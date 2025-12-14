# osm-shortlink

[![PyPI - Python Version](https://shields.monicz.dev/pypi/pyversions/osm-shortlink)](https://pypi.org/project/osm-shortlink)

Fast and correct OpenStreetMap shortlink encoder and decoder implementation in Rust with Python bindings. Shortlinks allow you to represent a location on the map with a short code.

## Installation

```sh
pip install osm-shortlink
```

## Basic usage

```py
from osm_shortlink import shortlink_encode
shortlink_encode(0.054, 51.510, 9)  # -> '0EEQhq--'
shortlink_encode(19.579, 51.876, 19)  # -> '0OP4tR~rx'
shortlink_encode(0, 0, 23)  # ValueError: Invalid zoom: must be between 0 and 22, got 23

from osm_shortlink import shortlink_decode
shortlink_decode('0EEQhq--')  # -> (0.054, 51.510, 9)
shortlink_decode('0OP4tR~rx')  # -> (19.579, 51.876, 19)
shortlink_decode('X')  # ValueError: Invalid shortlink: too short
```

## Format specification

<https://wiki.openstreetmap.org/wiki/Shortlink>
