"""GeoParquet covering columns and conservative spatial read pruning."""

from __future__ import annotations

import itertools
import math
from collections.abc import Mapping
from typing import Any

from gometry._geoparquet_metadata import _metadata_error
from gometry._lib import CRS

_XY_FIELDS = ('xmin', 'ymin', 'xmax', 'ymax')
_XYZ_FIELDS = ('xmin', 'ymin', 'zmin', 'xmax', 'ymax', 'zmax')


def add_covering(pa: Any, table: Any, array: Any, column_meta: dict[str, Any]) -> Any:
    name = 'bbox'
    suffix = 0
    existing_names = set(table.column_names)
    while name in existing_names:
        suffix += 1
        name = f'bbox_{suffix}'
    bounds = array.bounds
    covering = pa.StructArray.from_arrays(
        [pa.array(bounds[:, index]) for index in range(4)],
        names=_XY_FIELDS,
        mask=pa.array(array.is_missing),
    )
    column_meta['covering'] = {'bbox': {axis: [name, axis] for axis in _XY_FIELDS}}
    return table.append_column(name, covering)


def covering_column(
    pa: Any, schema: Any, geometry: str, metadata: Mapping[str, Any]
) -> str | None:
    covering = metadata.get('covering')
    if covering is None:
        return None
    if not isinstance(covering, Mapping):
        raise _metadata_error(f'column {geometry!r} covering must be an object')
    bbox = covering.get('bbox')
    if bbox is None:
        return None
    if not isinstance(bbox, Mapping) or set(bbox) != set(_XY_FIELDS):
        raise _metadata_error(
            f'column {geometry!r} covering bbox must name its four XY fields'
        )
    names = []
    for axis in _XY_FIELDS:
        path = bbox[axis]
        if (
            not isinstance(path, list)
            or len(path) != 2
            or path[1] != axis
            or not isinstance(path[0], str)
        ):
            raise _metadata_error(
                f'column {geometry!r} covering {axis} must be [column, {axis!r}]'
            )
        names.append(path[0])
    if len(set(names)) != 1:
        raise _metadata_error('bbox covering fields must belong to the same column')
    name = names[0]
    index = schema.get_field_index(name)
    if index < 0:
        raise _metadata_error(f'bbox covering column {name!r} must occur exactly once')
    field = schema.field(index)
    if not pa.types.is_struct(field.type) or tuple(field.type.names) not in {
        _XY_FIELDS,
        _XYZ_FIELDS,
    }:
        raise _metadata_error(
            f'bbox covering column {name!r} must be an XY or XYZ struct'
        )
    types = [child.type for child in field.type]
    if len(set(types)) != 1 or not (
        pa.types.is_float32(types[0]) or pa.types.is_float64(types[0])
    ):
        raise _metadata_error(
            f'bbox covering column {name!r} must use one floating-point type'
        )
    if field.nullable != schema.field(geometry).nullable:
        raise _metadata_error(
            'bbox covering and geometry must have matching nullability'
        )
    return name


def query_rectangle(bbox: Any, crs: Any, epoch: Any) -> tuple[Any, tuple[float, ...]]:
    from gometry import box

    if isinstance(bbox, (str, bytes)):
        raise TypeError('bbox must contain four finite numbers')
    values = tuple(itertools.islice(bbox, 5))
    if len(values) != 4:
        raise ValueError('bbox must contain four finite numbers')
    values = tuple(float(value) for value in values)
    return box(*values, crs=crs, epoch=epoch), values


def _pruning_latitudes(
    bounds: tuple[float, ...], geographic: bool
) -> tuple[float, float]:
    _, ymin, _, ymax = bounds
    if geographic:
        # Latitude::from_admitted folds the one-exterior-ULP pole neighbor.
        # Expand candidates only; exact refinement retains the query boundary.
        return math.nextafter(ymin, -math.inf), math.nextafter(ymax, math.inf)
    return ymin, ymax


def covering_filter(name: str, bounds: tuple[float, ...], crs: Any) -> Any:
    import pyarrow.compute as pc

    geographic = crs is not None and CRS(crs).is_geographic
    xmin, _, xmax, _ = bounds
    ymin, ymax = _pruning_latitudes(bounds, geographic)
    expression = (pc.field((name, 'ymin')) <= ymax) & (pc.field((name, 'ymax')) >= ymin)
    # Geographic source edges, seam aliases and pole identity do not share
    # ordinary stored-coordinate X envelopes. Latitude bounds remain sound.
    if not geographic:
        expression = (
            expression
            & (pc.field((name, 'xmin')) <= xmax)
            & (pc.field((name, 'xmax')) >= xmin)
        )
    return expression


def candidate_groups(
    parquet_file: Any, geometry: str, bounds: tuple[float, ...], crs: Any
) -> set[int]:
    column_index = next(
        index
        for index in range(len(parquet_file.schema))
        if parquet_file.schema.column(index).path == geometry
    )
    geographic = crs is not None and CRS(crs).is_geographic
    xmin, _, xmax, _ = bounds
    ymin, ymax = _pruning_latitudes(bounds, geographic)
    candidates = set()
    for index in range(parquet_file.num_row_groups):
        stats = (
            parquet_file.metadata.row_group(index).column(column_index).geo_statistics
        )
        if stats is None:
            candidates.add(index)
            continue
        if geographic:
            geometry_types = stats.geospatial_types
            if not geometry_types or any(
                code % 1000 not in {1, 2, 4, 5} for code in geometry_types
            ):
                # A ring can bound a polar cap beyond its vertex latitude range.
                # Native GEOMETRY statistics summarize coordinates, not that face.
                candidates.add(index)
                continue
        if (
            stats.ymin is not None
            and stats.ymax is not None
            and (stats.ymin > ymax or stats.ymax < ymin)
        ):
            continue
        if (
            not geographic
            and stats.xmin is not None
            and stats.xmax is not None
            and (stats.xmin <= stats.xmax)
            and (stats.xmin > xmax or stats.xmax < xmin)
        ):
            continue
        candidates.add(index)
    return candidates
