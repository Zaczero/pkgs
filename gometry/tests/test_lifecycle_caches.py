"""Round-5 lane R5-L3: import contract, CRS.info, coords, pandas scatter, facade caches.

Timing claims for CRS.info warm passes, Coordinates O(1) indexing, and pandas
setitem scaling live under ``benches/cases/``; this file keeps only
deterministic identity / isolation / invalidation properties.
"""

from __future__ import annotations

import subprocess
import sys

import gometry as gm
import pytest


def test_import_loads_numpy_keeps_optional_adapters_import_cheap() -> None:
    """Fresh process: public adapters are visible without optional imports."""
    code = """
import sys
import gometry as gm
assert "numpy" in sys.modules
assert not hasattr(gm, "TYPE_CHECKING")
assert gm.Cell is __import__("gometry._types", fromlist=["Cell"]).Cell
from gometry._lib import H3Cell
# Protocol structural check still works after identity re-export.
_ = H3Cell
names = {
    "explore",
    "from_geopandas",
    "from_geoparquet",
    "from_pandas",
    "from_polars",
}
assert names <= set(dir(gm))
assert names <= set(gm.__all__)
for name in ("pandas", "polars", "geopandas", "lonboard", "pyarrow", "arro3"):
    assert name not in sys.modules, name
# NumPy surfaces work immediately (core dependency already loaded).
xs = gm.Point(1.0, 2.0).coords.x
assert list(xs) == [1.0]
print("ok")
"""
    result = subprocess.run(
        [sys.executable, '-c', code],
        check=True,
        capture_output=True,
        text=True,
    )
    assert result.stdout.strip() == 'ok'


def test_held_crs_info_is_receiver_local() -> None:
    """Equivalent CRS receivers expose independent, refreshable info values.

    Generation invalidation after ``crs_clear_cache`` / ``crs_configure`` is
    also exercised in ``tests/test_crs_metadata_info.py``.
    """
    gm.crs_clear_cache()
    first = gm.CRS(4326)
    second = gm.CRS(4326)
    first_info = first.info
    expected = second.info
    assert first_info == expected
    assert first_info['name'] == 'WGS 84'

    first_info['name'] = 'POISON'
    assert first.info == expected
    assert second.info == expected

    gm.crs_clear_cache()
    assert first.info == expected
    assert second.info == expected


def test_coordinates_random_access_and_lazy_iter() -> None:
    """Random access crosses line boundaries and each iterator starts anew."""
    coords = gm.line_strings([
        [(13.0, 17.0), (19.0, 23.0)],
        [(29.0, 31.0), (37.0, 41.0)],
        [(43.0, 47.0), (53.0, 59.0)],
    ]).coords

    assert len(coords) == 6
    assert coords[0] == (13.0, 17.0)
    assert coords[3] == (37.0, 41.0)
    assert coords[-1] == (53.0, 59.0)
    iterator = iter(coords)
    assert next(iterator) == (13.0, 17.0)
    assert next(iterator) == (19.0, 23.0)
    assert next(iter(coords)) == (13.0, 17.0)


def test_crs_authorities_cached_and_isolated() -> None:
    gm.crs_clear_cache()
    cold = gm.crs_authorities()
    warm = gm.crs_authorities()
    assert warm == cold
    assert 'EPSG' in warm
    warm[0] = 'POISON'
    assert gm.crs_authorities() == cold
    gm.crs_clear_cache()
    assert gm.crs_authorities() == cold


def test_optional_exports_are_eager_facade_bindings() -> None:
    names = (
        'explore',
        'from_geopandas',
        'from_geoparquet',
        'from_pandas',
        'from_polars',
    )
    assert set(names) <= set(dir(gm))
    assert set(names) <= set(gm.__all__)
    assert set(names) <= vars(gm).keys()


def test_pandas_setitem_identity() -> None:
    """Fancy-index assignment scatters into the column; CRS preserved.

    Selection-size scaling lives in ``benches/cases/case_pandas_setitem_scale.py``.
    """
    pytest.importorskip('pandas')
    import numpy as np
    from gometry._pandas import GeometryExtensionArray

    arr = gm.points([0.0, 1.0, 2.0, 3.0], [0.0, 1.0, 2.0, 3.0])
    ext = GeometryExtensionArray(arr)

    pt = gm.Point(9.0, 9.0)
    ext[2] = pt
    assert ext[2] == pt
    assert ext._geoms.crs == arr.crs

    positions = np.array([0, 1, 3], dtype=np.intp)
    values = [gm.Point(10.0, 10.0), gm.Point(11.0, 11.0), gm.Point(12.0, 12.0)]
    ext[positions] = values
    for position, expected in zip(positions, values, strict=True):
        assert ext[int(position)] == expected
    assert ext._geoms.crs == arr.crs
