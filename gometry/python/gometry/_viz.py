"""Optional lonboard visualization glue using the Arrow capsule interface."""

from __future__ import annotations

import itertools
import math
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from gometry._lib import Geometry, GeometryArray, GeometryError
from gometry._optional import missing_optional_dependency
from gometry._types import mapping_as_dict

# Runtime annotation resolution supplies the installed optional namespaces.
if TYPE_CHECKING:
    import pyarrow as pa
    from lonboard import Map

    from gometry._types import Features

_LONBOARD_INSTALL = "install the 'gometry[viz]' extra"
_lonboard_available: bool | None = None
_VIZ_DEFAULTS: dict[str, dict[str, Any]] = {
    'scatterplot_kwargs': {
        'get_fill_color': [28, 119, 195],
        'radius_min_pixels': 4,
        'radius_max_pixels': 18,
        'opacity': 0.9,
        'pickable': True,
        'auto_highlight': True,
    },
    'path_kwargs': {
        'get_color': [24, 102, 172],
        'width_min_pixels': 2,
        'width_max_pixels': 8,
        'opacity': 0.9,
        'pickable': True,
        'auto_highlight': True,
    },
    'polygon_kwargs': {
        'get_fill_color': [70, 145, 209, 80],
        'get_line_color': [24, 102, 172, 220],
        'line_width_min_pixels': 1,
        'opacity': 0.5,
        'pickable': True,
        'auto_highlight': True,
    },
    'map_kwargs': {'height': 520},
}


def _lonboard_importable() -> bool:
    global _lonboard_available
    if _lonboard_available is None:
        try:
            import lonboard  # ruff: ignore[unused-import]
        except ModuleNotFoundError as error:
            if error.name != 'lonboard' and not (error.name or '').startswith(
                'lonboard.'
            ):
                raise
            _lonboard_available = False
        else:
            _lonboard_available = True
    return _lonboard_available


def _require_lonboard() -> Any:
    try:
        import lonboard
    except ModuleNotFoundError as error:
        raise missing_optional_dependency(
            error,
            'lonboard',
            f'interactive map visualization requires lonboard; {_LONBOARD_INSTALL}',
        ) from error
    return lonboard


def _is_map_suitable(arr: GeometryArray[Geometry | None]) -> bool:
    if len(arr) == 0:
        return False
    bounds = arr.total_bounds
    if bounds is None or not all(math.isfinite(v) for v in bounds):
        return False
    return arr.crs is not None


def _array_for_map(
    arr: GeometryArray[Geometry | None],
) -> GeometryArray[Geometry | None]:
    crs = arr.crs
    assert crs is not None
    if crs.to_epsg() == 4326:
        return arr
    return arr.to_crs(4326)


def _attribute_table(
    attributes: Any, row_count: int, geometry_name: str = 'geometry'
) -> Any | None:
    if attributes is None:
        return None
    import numpy as np
    from arro3.core import Array, ChunkedArray, Table

    from gometry._lib import _ArrowColumn

    if isinstance(attributes, Mapping) or callable(getattr(attributes, 'keys', None)):
        # Shared keys()+seen copier (N4): keys()-only ducks + repeated-key reject.
        columns = mapping_as_dict(attributes)
        if geometry_name in columns:
            raise GeometryError(
                f'attributes must not contain the geometry column {geometry_name!r}'
            )
        if not columns:
            return None
        arrays = []
        names = []
        # Iterate mapping items (bounded by its own keys); bound non-Arrow
        # columns to row_count+1 so infinite iterators terminate.
        for name, values in columns.items():
            if not isinstance(name, str):
                raise TypeError('attribute column names must be strings')
            array: Array | ChunkedArray
            if isinstance(values, _ArrowColumn):
                if values._indices() is not None:
                    values = values._materialize()
                array = ChunkedArray([
                    Array.from_arrow(chunk) for chunk in values._chunks()
                ])
            elif hasattr(values, '__arrow_c_array__') or hasattr(
                values, '__arrow_c_stream__'
            ):
                array = Array.from_arrow(values)
            else:
                column = (
                    np.asarray(values)
                    if isinstance(values, np.ndarray)
                    else np.asarray(list(itertools.islice(values, row_count + 1)))
                )
                if column.ndim != 1:
                    raise TypeError('attribute columns must be one-dimensional')
                array = (
                    _object_attribute_array(column)
                    if column.dtype.kind == 'O'
                    else Array.from_numpy(column)
                )
            names.append(name)
            arrays.append(array)
        table = Table.from_arrays(arrays, names=names)
    elif hasattr(attributes, '__arrow_c_stream__'):
        table = Table.from_arrow(attributes)
    else:
        raise TypeError(
            'attributes must be an Arrow table, mapping of columns, or None'
        )
    if geometry_name in table.column_names:
        raise GeometryError(
            f'attributes must not contain the geometry column {geometry_name!r}'
        )
    if table.num_rows != row_count:
        raise GeometryError(
            f'attributes length {table.num_rows} does not match geometry length {row_count}'
        )
    return table


def _object_attribute_array(values: Any) -> Any:
    import numpy as np
    from arro3.core import Array, DataType

    kind = None
    minimum = 0
    maximum = 0
    for value in values:
        if value is None:
            continue
        if isinstance(value, np.generic):
            value = value.item()
        current = type(value)
        if current not in (bool, int, float, str, bytes) or (
            kind is not None and current is not kind
        ):
            raise TypeError('attribute values must have a compatible Arrow scalar type')
        kind = current
        if current is int:
            minimum = min(minimum, value)
            maximum = max(maximum, value)
    if kind is None:
        data_type = DataType.null()
    elif kind is int:
        if minimum < -(1 << 63) or maximum > (1 << 64) - 1:
            raise OverflowError('integer attribute is outside the Arrow integer range')
        if minimum < 0 and maximum > (1 << 63) - 1:
            raise OverflowError('integer attributes do not fit one Arrow integer type')
        data_type = DataType.uint64() if maximum > (1 << 63) - 1 else DataType.int64()
    else:
        data_type = {
            bool: DataType.bool,
            float: DataType.float64,
            str: DataType.string,
            bytes: DataType.binary,
        }[kind]()
    return Array(values, type=data_type)


def _lonboard_data(
    arr: GeometryArray[Geometry | None],
    attributes: Any | None,
    geometry_name: str = 'geometry',
) -> Any:
    if attributes is None:
        return arr
    from arro3.core import Array

    # Import the native GeoArrow capsule directly.  `GeometryArray.to_arrow()`
    # intentionally materializes a PyArrow object and therefore belongs to the
    # `arrow` extra; visualization needs only lonboard's own arro3 dependency.
    return attributes.add_column(0, geometry_name, Array.from_arrow(arr))


def _viz_kwargs(kwargs: Mapping[str, Any]) -> dict[str, Any]:
    """Merge gometry's readable defaults with caller-supplied layer options."""
    configured = dict(kwargs)
    for name, defaults in _VIZ_DEFAULTS.items():
        values = configured.get(name)
        configured[name] = dict(defaults) | ({} if values is None else dict(values))
    return configured


def _lonboard_viz(
    arr: GeometryArray[Geometry | None],
    attributes: Any | None,
    *,
    geometry_name: str = 'geometry',
    **kwargs: Any,
) -> Any:
    """Hand lonboard one Arrow-compatible geometry object."""
    lonboard = _require_lonboard()
    configured = _viz_kwargs(kwargs)
    return lonboard.viz(_lonboard_data(arr, attributes, geometry_name), **configured)


def explore(
    values: Geometry | GeometryArray[Geometry | None] | Features,
    *,
    attributes: pa.Table | Mapping[str, Any] | None = None,
    **kwargs: Any,
) -> Map:
    """Build an interactive lonboard map from geometries or aligned features.

    Geometries are handed to lonboard through the Arrow capsule interface.
    Reprojection, attribute assembly, and widget construction may allocate.

    Parameters
    ----------
    values : Geometry, GeometryArray or Features
        Input geometries or aligned feature data. A scalar geometry is wrapped
        as a one-row array.
    attributes : Arrow table or mapping, optional
        Aligned feature columns carried into lonboard. Feature collections
        already own their attributes and do not accept this argument.
    kwargs : mapping, optional
        Keyword arguments forwarded to ``lonboard.viz``. The default map keeps
        points, lines, and polygons visually distinct and makes features
        inspectable on hover. Pass ``scatterplot_kwargs``, ``path_kwargs``,
        ``polygon_kwargs``, or ``map_kwargs`` to tailor individual settings.

    Returns
    -------
    lonboard.Map
        Interactive map widget for further customization.

    Raises
    ------
    ModuleNotFoundError
        If lonboard is not installed.
    TypeError
        If ``values`` is not a ``Geometry`` or ``GeometryArray``.
    GeometryError
        If the input is empty, CRS-free, or lacks finite bounds.
    """
    from gometry._lib import GeometryError, _FeaturePropertyColumn
    from gometry._types import Features

    geometry_name = 'geometry'
    if isinstance(values, Features):
        if attributes is not None:
            raise TypeError('Features already contain attributes')
        from gometry._feature_arrow import _property_columns

        arr = values.geometries
        geometry_name = values._geometry_column or 'geometry'
        columns = _property_columns(values)
        if values._id_column is not None:
            if values._id_column in columns or values._id_column == geometry_name:
                raise GeometryError('feature ID column conflicts with another column')
            columns[values._id_column] = (
                values.ids._snapshot(detach=False)
                if isinstance(values.ids, _FeaturePropertyColumn)
                else values.ids
            )
        attributes = columns
    elif isinstance(values, GeometryArray):
        arr = values
    elif isinstance(values, Geometry):
        arr = GeometryArray([values])
    else:
        raise TypeError('explore() expects a Geometry, GeometryArray or Features')

    if not _is_map_suitable(arr):
        raise GeometryError(
            'explore() requires a non-empty array with a CRS and finite bounds'
        )

    display_arr = _array_for_map(arr)
    attribute_table = _attribute_table(attributes, len(display_arr), geometry_name)
    return _lonboard_viz(
        display_arr, attribute_table, geometry_name=geometry_name, **kwargs
    )


def array_repr_html(arr: GeometryArray[Geometry | None]) -> str | None:
    """Return standalone lonboard HTML, or ``None`` to fall back to SVG."""
    if not _lonboard_importable() or not _is_map_suitable(arr):
        return None

    display_arr = _array_for_map(arr)
    # Widget-state serialization runs in lonboard's worker pool after this call
    # returns. Materialize the Arrow object so workers do not outlive the
    # direct C-capsule handoff owned by ``display_arr``.
    try:
        data: pa.Array | GeometryArray[Geometry | None]
        data = display_arr.to_arrow()
    except ModuleNotFoundError:
        # Without PyArrow, lonboard serializes the direct capsule through arro3.
        data = display_arr
    return _require_lonboard().viz(data, **_viz_kwargs({})).to_html(title='gometry map')
