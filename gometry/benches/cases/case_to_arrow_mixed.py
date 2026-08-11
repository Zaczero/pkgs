"""Print-only A/B case: twenty mixed GeoArrow exports over 30,000 rows."""

from __future__ import annotations

import time

import gometry as gm


ROWS = 30_000
EXPORTS = 20


def mixed_rows() -> gm.GeometryArray:
    return gm.GeometryArray([
        gm.Point(float(index), float(index % 97), crs=4326)
        if index % 3 == 0
        else gm.LineString([(float(index), 0.0), (float(index) + 0.5, 1.0)], crs=4326)
        if index % 3 == 1
        else gm.box(float(index), 0.0, float(index) + 0.25, 0.25, crs=4326)
        for index in range(ROWS)
    ])


geometries = mixed_rows()
warm = geometries.to_arrow()
assert len(warm) == ROWS

started = time.perf_counter()
for _ in range(EXPORTS):
    exported = geometries.to_arrow()
elapsed = time.perf_counter() - started

assert len(exported) == ROWS
restored = gm.from_arrow(exported)
assert restored.to_wkt()[0] == 'POINT (0 0)'
assert (
    restored.to_wkt()[-1]
    == 'POLYGON ((29999 0, 29999.25 0, 29999.25 0.25, 29999 0.25, 29999 0))'
)
print(elapsed)
