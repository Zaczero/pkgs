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


def test_import_loads_numpy_keeps_optional_adapters_lazy() -> None:
    """Fresh process: import gometry loads NumPy; optional adapters stay out."""
    code = """
import sys
import gometry as gm
assert "numpy" in sys.modules
assert not hasattr(gm, "TYPE_CHECKING")
assert gm.Cell is __import__("gometry._types", fromlist=["Cell"]).Cell
from gometry._lib import H3Cell
# Protocol structural check still works after identity re-export.
_ = H3Cell
assert "from_pandas" not in dir(gm)
assert "from_pandas" not in gm.__all__
for name in ("pandas", "polars", "geopandas", "lonboard", "pyarrow"):
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
    """1k held CRS: repeated .info is receiver-cached; free path isolated.

    Generation-aware invalidation after ``crs_clear_cache`` /
    ``crs_configure`` is pinned in ``tests/test_crs_metadata_info.py``.
    Wall-clock for the warm 1k pass lives in
    ``benches/cases/case_held_crs_info_scale.py``.
    """
    gm.crs_clear_cache()
    held: list[gm.CRS] = []
    for code in range(2000, 5000):
        try:
            held.append(gm.CRS(code))
        except Exception:  # noqa: S112 — skip unconstructible EPSG codes while filling the set
            continue
        if len(held) >= 1000:
            break
    assert len(held) == 1000

    # Fill receiver caches; repeated reads stay content-stable (not re-resolved
    # to a different snapshot without a generation bump).
    first_pass = [crs.info for crs in held]
    second_pass = [crs.info for crs in held]
    assert second_pass == first_pass
    assert all(
        isinstance(info.get('name'), str) and info['name'] for info in first_pass
    )

    # Free path still works and is isolation-safe.
    cold = gm.crs_info(4326)
    warm = gm.crs_info(4326)
    assert warm == cold
    warm['name'] = 'POISON'
    assert gm.crs_info(4326)['name'] == cold['name']

    # After clear, held receivers rebuild (generation-aware).
    name_before = held[0].info['name']
    gm.crs_clear_cache()
    rebuilt = held[0].info
    assert rebuilt['name'] == name_before
    assert isinstance(rebuilt['name'], str) and rebuilt['name']


def test_coordinates_random_access_and_lazy_iter() -> None:
    """coords[0]/[-1] and first-item iter are correct across run counts.

    Storage-shaped O(1) indexing / flat scaling vs run count is measured in
    ``benches/cases/case_coordinates_access_scale.py``.
    """
    for n_runs in (1_000, 20_000, 100_000):
        arr = gm.line_strings([
            [(float(i), 0.0), (float(i), 1.0)] for i in range(n_runs)
        ])
        coords = arr.coords
        assert coords[0] == (0.0, 0.0)
        assert coords[-1] == (float(n_runs - 1), 1.0)
        # Random middle vertex of the last run (open line: 2 verts per run).
        assert coords[2 * (n_runs - 1)] == (float(n_runs - 1), 0.0)

    line = gm.LineString([(float(i), 0.0) for i in range(640_000)])
    coords = line.coords
    first = next(iter(coords))
    assert first == (0.0, 0.0)
    # Fresh iterators restart; identity of first vertex is stable.
    assert next(iter(coords)) == (0.0, 0.0)
    assert coords[0] == (0.0, 0.0)
    assert coords[-1] == (639_999.0, 0.0)


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


def test_lazy_adapter_getattr_is_cached_out_of_dir() -> None:
    # Lazy adapters must not pollute runtime discovery (dir / __all__).
    assert 'from_pandas' not in dir(gm)
    assert 'from_pandas' not in gm.__all__
    # Resolved adapters are not written into module globals; identity is stable
    # because the adapter module is cached by the import system. Repeated access
    # still goes through __getattr__ (may import pandas once on first use).
    first = gm.from_pandas
    second = gm.from_pandas
    assert first is second
    assert 'from_pandas' not in dir(gm)


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
