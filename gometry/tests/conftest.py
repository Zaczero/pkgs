"""Shared test helpers."""

from __future__ import annotations

import importlib.util
import itertools
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import gometry as gm
import numpy as np
import numpy.typing as npt

if TYPE_CHECKING:
    from types import ModuleType
GOMETRY_ROOT = Path(__file__).resolve().parents[1]
TOOLS = GOMETRY_ROOT / 'tools'
TOOL_IMPORT_DIRS = (
    TOOLS,
    TOOLS / 'gates',
    TOOLS / 'stubs',
    TOOLS / 'docs',
)


def _resolve_tool_path(name: str) -> Path:
    """Locate ``{name}.py`` under ``tools/`` or its gate/stub/docs subdirs."""
    for directory in TOOL_IMPORT_DIRS:
        candidate = directory / f'{name}.py'
        if candidate.is_file():
            return candidate
    msg = f'tool script not found: {name}.py (searched {", ".join(str(d) for d in TOOL_IMPORT_DIRS)})'
    raise FileNotFoundError(msg)


def load_tool(name: str, path: Path | None = None, *import_paths: Path) -> ModuleType:
    """Import a repo script (``tools/``, ``bench/``) by file path.

    Registers the module in ``sys.modules`` (some scripts resolve their own
    helpers through it) and temporarily prepends ``import_paths`` for scripts
    with sibling imports.
    """
    path = path or _resolve_tool_path(name)
    previous = list(sys.path)
    sys.path[:0] = [
        *[str(entry) for entry in import_paths],
        *[str(entry) for entry in TOOL_IMPORT_DIRS],
    ]
    try:
        spec = importlib.util.spec_from_file_location(name, path)
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    finally:
        sys.path[:] = previous
    return module


def ndarray_lane(
    array: Any, dtype: npt.DTypeLike, *, shape: tuple[int, ...] | None = None
) -> np.ndarray:
    assert isinstance(array, np.ndarray)
    assert array.dtype == np.dtype(dtype)
    assert not array.flags.writeable
    if shape is not None:
        assert array.shape == shape
    return array


def ndarray_values(array: Any, dtype: npt.DTypeLike) -> Any:
    array = ndarray_lane(array, dtype)
    return array.tolist()


def ids(array: Any) -> list[int] | list[list[int]]:
    import gometry as gm

    if isinstance(array, gm.Groups):
        raw = ndarray_lane(array.values, np.int64)
        offsets = ndarray_lane(array.offsets, np.int64)
        assert offsets[0] == 0
        assert offsets[-1] == len(raw)
        assert np.all(offsets[:-1] <= offsets[1:])
        rows = [ndarray_lane(row, np.int64).tolist() for row in array]
        assert rows == [
            raw[start:end].tolist()
            for start, end in itertools.pairwise(offsets.tolist())
        ]
        return rows
    return ndarray_values(array, np.int64)


def floats(array: Any) -> Any:
    return ndarray_values(array, np.float64)


def bools(array: Any) -> list[bool]:
    return ndarray_values(array, np.bool_)


def ints(array: Any) -> list[int]:
    return ndarray_values(array, np.int64)


def uints(array: Any) -> list[int]:
    return ndarray_values(array, np.uint64)


def pair_rows(pairs: Any) -> list[tuple[int, int]]:
    assert isinstance(pairs, tuple)
    assert len(pairs) == 2
    left, right = pairs
    left = ndarray_lane(left, np.int64)
    right = ndarray_lane(right, np.int64)
    return list(zip(left.tolist(), right.tolist(), strict=True))


def canon(value: object) -> object:
    """Normalize results to comparable plain data (geometries to WKT)."""
    if isinstance(value, gm.Geometry):
        return (value).to_wkt(drop_epoch=True)
    if isinstance(value, gm.ValidationReport):
        return (value.valid, value.reason)
    if isinstance(value, gm.GeometryArray):
        return [(g).to_wkt(drop_epoch=True) for g in value]
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, (list, tuple)):
        return [canon(item) for item in value]
    return value


def storage_twins(axes: str = 'XYZM') -> tuple[gm.GeometryArray, gm.GeometryArray]:
    if axes == 'XY':
        packed = gm.points([0.0, 1.0, 2.0], [10.0, 11.0, 12.0], crs=3857)
        wkts = ['POINT (0 10)', 'POINT (1 11)', 'POINT (2 12)']
    else:
        packed = gm.points(
            [0.0, 1.0, 2.0],
            [10.0, 11.0, 12.0],
            z=[5.0, 6.0, 7.0],
            m=[50.0, 60.0, 70.0],
            crs=3857,
        )
        wkts = ['POINT ZM (0 10 5 50)', 'POINT ZM (1 11 6 60)', 'POINT ZM (2 12 7 70)']
    mixed = gm.from_wkt(wkts, crs=3857)
    assert isinstance(mixed, gm.GeometryArray)
    return (packed, mixed)


def line_storage_twins() -> tuple[gm.GeometryArray, gm.GeometryArray]:
    packed = gm.GeometryArray([
        gm.LineString([(0.1, 0.1), (0.9, 0.9)], crs=3857),
        gm.LineString([(1.1, 1.1), (2.1, 2.1)], crs=3857),
    ])
    mixed = gm.from_wkt(
        ['LINESTRING (0.1 0.1, 0.9 0.9)', 'LINESTRING (1.1 1.1, 2.1 2.1)'], crs=3857
    )
    return (packed, mixed)


def polygon_storage_twins() -> tuple[gm.GeometryArray, gm.GeometryArray]:
    packed = gm.GeometryArray([
        gm.box(0.1, 0.1, 0.9, 0.9, crs=3857),
        gm.from_wkt('POLYGON ((1 1, 2 1, 2 2, 1 2, 1 1))', crs=3857),
    ])
    mixed = gm.from_wkt(
        [
            'POLYGON ((0.1 0.1, 0.9 0.1, 0.9 0.9, 0.1 0.9, 0.1 0.1))',
            'POLYGON ((1 1, 2 1, 2 2, 1 2, 1 1))',
        ],
        crs=3857,
    )
    return (packed, mixed)


def polygon_z_storage_twins() -> tuple[gm.GeometryArray, gm.GeometryArray]:
    packed = gm.GeometryArray([
        gm.from_wkt('POLYGON Z ((0 0 1, 1 0 1, 1 1 1, 0 1 1, 0 0 1))', crs=3857),
        gm.from_wkt('POLYGON Z ((2 0 3, 3 0 3, 3 1 3, 2 1 3, 2 0 3))', crs=3857),
    ])
    mixed = gm.from_wkt(
        [
            'POLYGON Z ((0 0 1, 1 0 1, 1 1 1, 0 1 1, 0 0 1))',
            'POLYGON Z ((2 0 3, 3 0 3, 3 1 3, 2 1 3, 2 0 3))',
        ],
        crs=3857,
    )
    return (packed, mixed)


def geo_line_storage_twins() -> tuple[gm.GeometryArray, gm.GeometryArray]:
    packed = gm.GeometryArray([
        gm.LineString([(0.0, 0.0), (0.0, 10.0)], crs=4326),
        gm.LineString([(0.0, 0.0), (10.0, 0.0)], crs=4326),
    ])
    mixed = gm.from_wkt(['LINESTRING (0 0, 0 10)', 'LINESTRING (0 0, 10 0)'], crs=4326)
    return (packed, mixed)
