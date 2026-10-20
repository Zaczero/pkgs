"""Sparse, owned scaling cases for the release benchmark manifest.

The builders deliberately expose one dimension at a time.  Their fixtures are
small deterministic derivations of the public benchmark fixtures, and each
builder records the useful output/work unit rather than treating an input
cardinality as a proxy for uncounted output.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np

_PYTHON = Path(__file__).resolve().parent
if str(_PYTHON) not in sys.path:
    sys.path.insert(0, str(_PYTHON))

from _bench_oracles import (
    OracleContext,
    OracleMismatch,
    register_public_scaling_builders,
)
from _bench_public_cases import (
    PublicCase,
    _case,
    _compare_areal_result,
    exact_mask,
    normalized_index_pairs,
    normalized_tile_set,
)
from _bench_public_fixtures import (
    prepared_polygon_and_probes,
    service_areas_1024,
)
from _bench_real_world_layers import BRAZIL_BOUNDS, brazil_bbox_polygon
from _bench_registry import SCALING_OPERATIONS, ScalingOperation

if TYPE_CHECKING:
    from collections.abc import Callable


def _candidate_case(operation: ScalingOperation) -> PublicCase:
    import gometry as gm
    import shapely

    point = operation.point
    spacing = 2.0
    index_count = 10_000
    origin_x, origin_y = 500_000.0, 5_200_000.0
    boxes_gm = gm.GeometryArray([
        gm.box(
            origin_x + index * spacing,
            origin_y,
            origin_x + index * spacing + 1.0,
            origin_y + 1.0,
            crs=32634,
        )
        for index in range(index_count)
    ])
    boxes_sh = np.asarray(
        [
            shapely.box(
                origin_x + index * spacing,
                origin_y,
                origin_x + index * spacing + 1.0,
                origin_y + 1.0,
            )
            for index in range(index_count)
        ],
        dtype=object,
    )
    index = gm.SpatialIndex(boxes_gm)
    tree = shapely.STRtree(boxes_sh)

    if operation.dimension == 'indexed-polygons':
        point = int(point)
        index = gm.SpatialIndex(boxes_gm[:point])
        tree = shapely.STRtree(boxes_sh[:point])
        queries_gm = gm.GeometryArray([
            gm.box(
                origin_x + (index % point) * spacing + 0.1,
                origin_y + 0.1,
                origin_x + (index % point) * spacing + 0.2,
                origin_y + 0.2,
                crs=32634,
            )
            for index in range(1_000)
        ])
        queries_sh = np.asarray(
            [
                shapely.box(
                    origin_x + (index % point) * spacing + 0.1,
                    origin_y + 0.1,
                    origin_x + (index % point) * spacing + 0.2,
                    origin_y + 0.2,
                )
                for index in range(1_000)
            ],
            dtype=object,
        )
    elif operation.dimension == 'queries-with-a-match':
        matching = int(point * 1_000)
        queries_gm = gm.GeometryArray([
            gm.box(
                origin_x + index * spacing + 0.1,
                origin_y + 0.1,
                origin_x + index * spacing + 0.2,
                origin_y + 0.2,
                crs=32634,
            )
            if index < matching
            else gm.box(
                1_000_000.0,
                1_000_000.0,
                1_000_001.0,
                1_000_001.0,
                crs=32634,
            )
            for index in range(1_000)
        ])
        queries_sh = np.asarray(
            [
                shapely.box(
                    origin_x + index * spacing + 0.1,
                    origin_y + 0.1,
                    origin_x + index * spacing + 0.2,
                    origin_y + 0.2,
                )
                if index < matching
                else shapely.box(
                    1_000_000.0,
                    1_000_000.0,
                    1_000_001.0,
                    1_000_001.0,
                )
                for index in range(1_000)
            ],
            dtype=object,
        )
    elif operation.dimension == 'candidate-pairs-per-query':
        point = int(point)
        query_gm = gm.box(
            origin_x - 0.5,
            origin_y - 0.5,
            origin_x + point * spacing - 0.5,
            origin_y + 1.5,
            crs=32634,
        )
        query_sh = shapely.box(
            origin_x - 0.5,
            origin_y - 0.5,
            origin_x + point * spacing - 0.5,
            origin_y + 1.5,
        )
        queries_gm = gm.GeometryArray([query_gm])
        queries_sh = np.asarray([query_sh], dtype=object)
    else:  # pragma: no cover - registry validation owns the dimension set
        raise ValueError(f'unknown index scaling dimension: {operation.dimension}')

    expected = len(tree.query(queries_sh)[0])

    def gometry_call() -> object:
        return index.candidates(queries_gm).to_pairs()

    def competitor_call() -> object:
        pairs = tree.query(queries_sh)
        return pairs[0], pairs[1]

    def precondition() -> None:
        result = gometry_call()
        if len(result[0]) != expected:  # type: ignore[index]
            raise OracleMismatch(
                'index scaling output count changed',
                details=f'got {len(result[0])}, want {expected}',  # type: ignore[index]
            )

    def output_validator(value: object) -> None:
        if value is None or len(value[0]) != expected:  # type: ignore[index]
            raise ValueError(f'expected {expected} candidate pairs')

    return _case(
        gometry_call,
        competitor_call,
        normalized_index_pairs,
        kind='normalized_index_pairs',
        preconditions=(precondition,),
        useful_work={'count': max(expected, 1), 'unit': operation.useful_work_unit},
        output_validator=output_validator,
    )


def _overlay_case(operation: ScalingOperation) -> PublicCase:
    import gometry as gm
    import shapely

    point = int(operation.point)
    data = service_areas_1024()
    gm_values = data['gm'][:point]
    sh_values = data['sh'][:point]

    def gometry_call() -> object:
        return gm.union_all(gm_values)

    def competitor_call() -> object:
        return shapely.union_all(sh_values)

    def output_validator(value: object) -> None:
        if value is None or bool(value.is_empty):  # type: ignore[union-attr]
            raise ValueError('overlay output is empty')

    return _case(
        gometry_call,
        competitor_call,
        _compare_areal_result,
        kind='areal_result',
        useful_work={'count': point, 'unit': operation.useful_work_unit},
        output_validator=output_validator,
    )


def _disjoint_union_case(operation: ScalingOperation) -> PublicCase:
    import gometry as gm
    import shapely

    count = int(operation.point)
    side = int(np.ceil(np.sqrt(count)))
    corners = [(3.0 * (index % side), 3.0 * (index // side)) for index in range(count)]
    gm_values = gm.GeometryArray([gm.box(x, y, x + 1.0, y + 1.0) for x, y in corners])
    sh_values = np.asarray(
        [shapely.box(x, y, x + 1.0, y + 1.0) for x, y in corners], dtype=object
    )

    def output_validator(value: object) -> None:
        parts = (
            len(value)
            if isinstance(value, gm.Geometry)
            else int(shapely.get_num_geometries(value))
        )
        if parts != count:
            raise ValueError(f'expected {count} disjoint parts, got {parts}')

    return _case(
        lambda: gm.union_all(gm_values),
        lambda: shapely.union_all(sh_values),
        _compare_areal_result,
        kind='areal_result',
        useful_work={'count': count, 'unit': operation.useful_work_unit},
        output_validator=output_validator,
    )


def _holes_case(operation: ScalingOperation) -> PublicCase:
    import gometry as gm
    import shapely

    count = int(operation.point)
    side = int(np.ceil(np.sqrt(count)))
    extent = 10.0 * side + 10.0
    shell = [(0.0, 0.0), (extent, 0.0), (extent, extent), (0.0, extent), (0.0, 0.0)]
    holes = []
    for index in range(count):
        x, y = 10.0 * (index % side) + 5.0, 10.0 * (index // side) + 5.0
        holes.append([(x, y), (x, y + 2.0), (x + 2.0, y + 2.0), (x + 2.0, y), (x, y)])
    cut = (1.0, 1.0, 0.5 * extent, 0.75 * extent)
    gm_shell, gm_cut = gm.Polygon(shell, holes), gm.box(*cut)
    sh_shell, sh_cut = shapely.Polygon(shell, holes), shapely.box(*cut)

    def output_validator(value: object) -> None:
        if value is None or bool(value.is_empty):  # type: ignore[union-attr]
            raise ValueError('intersection output is empty')

    return _case(
        lambda: gm.intersection(gm_shell, gm_cut),
        lambda: shapely.intersection(sh_shell, sh_cut),
        _compare_areal_result,
        kind='areal_result',
        useful_work={'count': count, 'unit': operation.useful_work_unit},
        output_validator=output_validator,
    )


def _grid_case(operation: ScalingOperation) -> PublicCase:
    import gometry as gm
    import mercantile

    point = int(operation.point)
    zoom = {990: 8, 3_960: 9, 15_340: 10}[point]
    bbox = brazil_bbox_polygon()
    w, s, e, n = BRAZIL_BOUNDS

    def gometry_call() -> object:
        return gm.tile_cover(bbox, zoom=zoom, cell_rule='bbox', max_cells=None)

    def competitor_call() -> object:
        return tuple(mercantile.tiles(w, s, e, n, [zoom], truncate=False))

    def output_validator(value: object) -> None:
        if value is None or len(value) != point:  # type: ignore[arg-type]
            raise ValueError(f'expected {point} output tiles')

    def compare(left: object, right: object | None, context: OracleContext) -> None:
        if len(left) != point or right is None or len(right) != point:  # type: ignore[arg-type]
            raise OracleMismatch('grid scaling output count mismatch')
        normalized_tile_set(left, right, context)

    return _case(
        gometry_call,
        competitor_call,
        compare,
        kind='normalized_tile_set',
        useful_work={'count': point, 'unit': operation.useful_work_unit},
        output_validator=output_validator,
    )


def _point_in_polygon_case(operation: ScalingOperation) -> PublicCase:
    import gometry as gm
    import shapely

    point = int(operation.point)
    data = prepared_polygon_and_probes()
    xs = data['xs'][:point]
    ys = data['ys'][:point]
    polygon = data['gm_poly']
    sh_poly = data['sh_poly']

    def gometry_call() -> object:
        return gm.contains(polygon, x=xs, y=ys)

    def competitor_call() -> object:
        return shapely.contains_xy(sh_poly, xs, ys)

    def output_validator(value: object) -> None:
        array = np.asarray(value)
        if array.shape != (point,) or array.dtype != np.bool_:
            raise ValueError(f'expected boolean output of shape {(point,)}')

    return _case(
        gometry_call,
        competitor_call,
        exact_mask,
        kind='exact_mask',
        useful_work={'count': point, 'unit': operation.useful_work_unit},
        output_validator=output_validator,
    )


def _builder_for(operation: ScalingOperation) -> Callable[[], PublicCase]:
    if operation.curve_id.startswith('spatial-index-'):
        return lambda: _candidate_case(operation)
    if operation.curve_id == 'overlay-input-count':
        return lambda: _overlay_case(operation)
    if operation.curve_id == 'overlay-disjoint-input-count':
        return lambda: _disjoint_union_case(operation)
    if operation.curve_id == 'overlay-hole-count':
        return lambda: _holes_case(operation)
    if operation.curve_id == 'grid-cover-output-count':
        return lambda: _grid_case(operation)
    if operation.curve_id == 'prepared-point-in-polygon':
        return lambda: _point_in_polygon_case(operation)
    raise ValueError(f'no scaling builder for {operation.curve_id}')


register_public_scaling_builders({
    row: _builder_for(operation)
    for operation in SCALING_OPERATIONS
    for row in (operation.gometry,)
})
