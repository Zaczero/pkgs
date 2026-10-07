"""Shared test helpers imported explicitly by the test suite."""

from __future__ import annotations

import importlib.util
import itertools
import math
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import gometry as gm
import numpy as np
import numpy.typing as npt

WEST_RADIAN_SOUTH_GRAD_CRS = """GEOGCRS["West-radian south-grad WGS84",
 DATUM["World Geodetic System 1984",ELLIPSOID["WGS 84",6378137,298.257223563]],
 PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.017453292519943295]],
 CS[ellipsoidal,2],
 AXIS["Longitude",west,ORDER[1],ANGLEUNIT["radian",1]],
 AXIS["Latitude",south,ORDER[2],ANGLEUNIT["grad",0.015707963267948967]]]"""
EAST_DEGREE_SOUTH_LATITUDE_CRS = """GEOGCRS["WGS 84 south latitude",
 DATUM["World Geodetic System 1984",ELLIPSOID["WGS 84",6378137,298.257223563]],
 PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.017453292519943295]],
 CS[ellipsoidal,2],
 AXIS["Longitude",east,ORDER[1],ANGLEUNIT["degree",0.017453292519943295]],
 AXIS["Latitude",south,ORDER[2],ANGLEUNIT["degree",0.017453292519943295]]]"""

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


def chart_zone_area(
    west: float,
    east: float,
    south: float,
    north: float,
    *,
    semi_major: float,
    flattening: float,
) -> float:
    """Closed-form ellipsoidal area between chart parallels."""
    es = flattening * (2.0 - flattening)
    sine_north = math.sin(math.radians(north))
    sine_south = math.sin(math.radians(south))
    if south >= 0.0 and north >= 0.0:
        cosine_midpoint = math.sin(
            math.radians(((90.0 - north) + (90.0 - south)) / 2.0)
        )
    elif south <= 0.0 and north <= 0.0:
        cosine_midpoint = math.sin(
            math.radians(((90.0 + north) + (90.0 + south)) / 2.0)
        )
    else:
        cosine_midpoint = math.cos(math.radians((north + south) / 2.0))

    sine_difference = (
        2.0 * math.sin(math.radians((north - south) / 2.0)) * cosine_midpoint
    )
    if es == 0.0:
        authalic_difference = 2.0 * sine_difference
    else:
        rational_difference = (
            sine_difference
            * (1.0 + es * sine_north * sine_south)
            / (
                (1.0 - es * sine_north * sine_north)
                * (1.0 - es * sine_south * sine_south)
            )
        )
        eccentricity = math.sqrt(abs(es))
        if es > 0.0:
            inverse_difference = math.atanh(
                eccentricity * sine_difference / (1.0 - es * sine_north * sine_south)
            )
        else:
            inverse_difference = math.atan2(
                eccentricity * sine_difference,
                1.0 - es * sine_north * sine_south,
            )
        authalic_difference = (1.0 - es) * (
            rational_difference + inverse_difference / eccentricity
        )

    return semi_major**2 / 2.0 * math.radians(east - west) * authalic_difference


def chart_parallel_length(
    latitude: float, longitude_delta: float, *, semi_major: float, flattening: float
) -> float:
    """Closed-form ellipsoidal length of one straight chart parallel."""
    eccentricity_squared = flattening * (2.0 - flattening)
    sine = math.sin(math.radians(latitude))
    prime_vertical = semi_major / math.sqrt(1.0 - eccentricity_squared * sine * sine)
    return (
        prime_vertical
        * math.cos(math.radians(latitude))
        * math.radians(longitude_delta)
    )


def chart_rectangle_perimeter(
    geodesic: Any,
    west: float,
    east: float,
    south: float,
    north: float,
) -> float:
    """Perimeter oracle for a ring of two parallels and two meridians."""
    lower = chart_parallel_length(
        south,
        east - west,
        semi_major=geodesic.a,
        flattening=geodesic.f,
    )
    upper = chart_parallel_length(
        north,
        east - west,
        semi_major=geodesic.a,
        flattening=geodesic.f,
    )
    meridian = geodesic.inv(west, south, west, north)[2]
    return lower + upper + 2.0 * meridian


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


def with_validity(array: Any, validity: bytes) -> Any:
    """Rebuild a dense GeoArrow export with a geometry-level validity bitmap,
    keeping its storage buffers (missing slots hold placeholder rows).
    """
    import pyarrow as pa

    storage = array.storage
    bitmap = pa.py_buffer(validity)
    if pa.types.is_struct(storage.type):
        children = [storage.field(index) for index in range(storage.type.num_fields)]
        rebuilt = pa.Array.from_buffers(
            storage.type, len(storage), [bitmap], children=children
        )
    elif pa.types.is_list(storage.type):
        rebuilt = pa.Array.from_buffers(
            storage.type,
            len(storage),
            [bitmap, storage.buffers()[1]],
            children=[storage.values],
        )
    else:
        buffers = storage.buffers()
        rebuilt = pa.Array.from_buffers(
            storage.type, len(storage), [bitmap, buffers[1], buffers[2]]
        )
    return pa.ExtensionArray.from_storage(array.type, rebuilt)


class _CapsuleOnly:
    """Arrow C provider without a ``.type`` attribute."""

    __slots__ = ('_capsules',)

    def __init__(self, schema_capsule: object, array_capsule: object) -> None:
        self._capsules = (schema_capsule, array_capsule)

    def __arrow_c_array__(self, requested_schema=None):
        return self._capsules


def _capsule_only_from_pyarrow(arrow: Any) -> _CapsuleOnly:
    schema, array = arrow.__arrow_c_array__()
    return _CapsuleOnly(schema, array)


def _capsule_only_from_gometry(arr: gm.GeometryArray) -> _CapsuleOnly:
    schema, array = arr.__arrow_c_array__()
    return _CapsuleOnly(schema, array)


class _StreamOnly:
    """Force the Arrow C stream path without PyArrow dispatch."""

    __slots__ = ('_obj',)

    def __init__(self, obj: object) -> None:
        self._obj = obj

    def __arrow_c_stream__(self, requested_schema=None):
        return self._obj.__arrow_c_stream__(requested_schema)


def _nullable_packed_linestring_arrow() -> gm.GeometryArray:
    """Build nullable packed lines through the public GeoArrow import path."""
    import pyarrow as pa

    xy = pa.struct([('x', pa.float64()), ('y', pa.float64())])
    points = pa.array([(0.0, 0.0), (1.0, 1.0), (2.0, 2.0), (3.0, 3.0)], type=xy)
    offsets = pa.array([0, 2, 2, 4], type=pa.int32())
    storage = pa.ListArray.from_arrays(
        offsets, points, mask=pa.array([False, True, False])
    )
    field = pa.field(
        'geometry',
        storage.type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.linestring',
            b'ARROW:extension:metadata': b'{}',
        },
    )
    batch = pa.RecordBatch.from_arrays([storage], schema=pa.schema([field]))
    arr = gm.from_arrow(batch)
    callable_, args = arr.drop_missing().__reduce_ex__(5)
    assert callable_.__name__ == '_unpickle_packed_array_v1'
    assert len(args[4]) == 1
    return arr


def _nullable_packed_polygon_arrow() -> gm.GeometryArray:
    """Build nullable packed polygons through the public GeoArrow import path."""
    import pyarrow as pa

    xy = pa.struct([('x', pa.float64()), ('y', pa.float64())])
    nan = float('nan')
    points = pa.array(
        [
            (0.0, 0.0),
            (1.0, 0.0),
            (1.0, 1.0),
            (0.0, 0.0),
            (nan, nan),
            (nan, nan),
            (nan, nan),
            (nan, nan),
            (2.0, 2.0),
            (3.0, 2.0),
            (3.0, 3.0),
            (2.0, 2.0),
        ],
        type=xy,
    )
    ring_offsets = pa.array([0, 4, 8, 12], type=pa.int32())
    rings = pa.ListArray.from_arrays(ring_offsets, points)
    poly_offsets = pa.array([0, 1, 2, 3], type=pa.int32())
    storage = pa.ListArray.from_arrays(
        poly_offsets, rings, mask=pa.array([False, True, False])
    )
    field = pa.field(
        'geometry',
        storage.type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.polygon',
            b'ARROW:extension:metadata': b'{}',
        },
    )
    batch = pa.RecordBatch.from_arrays([storage], schema=pa.schema([field]))
    arr = gm.from_arrow(batch)
    callable_, args = arr.drop_missing().__reduce_ex__(5)
    assert callable_.__name__ == '_unpickle_packed_array_v1'
    assert len(args[4]) == 2
    return arr
