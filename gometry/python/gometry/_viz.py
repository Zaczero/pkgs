"""Optional lonboard visualization glue — zero-copy GeoArrow handoff."""

from __future__ import annotations

import itertools
import math
from collections.abc import Mapping
from typing import Any

from gometry._lib import Geometry, GeometryArray
from gometry._optional import missing_optional_dependency
from gometry._types import PyArrowTable, mapping_as_dict

TYPE_CHECKING = False
if TYPE_CHECKING:
    from lonboard import Map
else:
    # Runtime: keep the `Map` annotation name resolvable WITHOUT importing
    # lonboard, so `import gometry` and `gm.explore` stay lazy (explore imports
    # lonboard only when actually called). Static checkers see the real
    # `lonboard.Map` under TYPE_CHECKING above.
    Map = Any

_LONBOARD_INSTALL = "install the 'gometry[viz]' extra"
_lonboard_available: bool | None = None


def _lonboard_importable() -> bool:
    global _lonboard_available
    if _lonboard_available is None:
        try:
            import lonboard  # noqa: F401
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


def _is_map_suitable(arr: GeometryArray) -> bool:
    if len(arr) == 0:
        return False
    bounds = arr.total_bounds
    if bounds is None or not all(math.isfinite(v) for v in bounds):
        return False
    return arr.crs is not None


def _array_for_map(arr: GeometryArray) -> GeometryArray:
    crs = arr.crs
    assert crs is not None
    if crs.to_epsg() == 4326:
        return arr
    return arr.to_crs(4326)


def _attribute_table(attributes: Any, row_count: int) -> Any | None:
    if attributes is None:
        return None
    import numpy as np
    from arro3.core import Array, Table

    if isinstance(attributes, Mapping) or callable(getattr(attributes, 'keys', None)):
        # Shared keys()+seen copier (N4): keys()-only ducks + repeated-key reject.
        columns = mapping_as_dict(attributes)
        if 'geometry' in columns:
            raise ValueError(
                "attributes must not contain the reserved 'geometry' column"
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
            if hasattr(values, '__arrow_c_array__') or hasattr(
                values, '__arrow_c_stream__'
            ):
                array = Array.from_arrow(values)
            else:
                column = np.asarray(list(itertools.islice(values, row_count + 1)))
                if column.ndim != 1:
                    raise TypeError('attribute columns must be one-dimensional')
                array = Array.from_numpy(column)
            names.append(name)
            arrays.append(array)
        table = Table.from_arrays(arrays, names=names)
    elif hasattr(attributes, '__arrow_c_stream__'):
        table = Table.from_arrow(attributes)
    else:
        raise TypeError(
            'attributes must be an Arrow table, mapping of columns, or None'
        )
    if 'geometry' in table.column_names:
        raise ValueError("attributes must not contain the reserved 'geometry' column")
    if table.num_rows != row_count:
        raise ValueError(
            f'attributes length {table.num_rows} does not match geometry length {row_count}'
        )
    return table


def _feature_collection(arr: GeometryArray, attributes: Any | None) -> Any:
    from gometry import to_feature_collection

    properties = None
    if attributes is not None:
        columns = [column.to_pylist() for column in attributes.columns]
        properties = [
            dict(zip(attributes.column_names, row, strict=True))
            for row in zip(*columns, strict=True)
        ]
    return to_feature_collection(arr, properties=properties)


def _lonboard_data(arr: GeometryArray, attributes: Any | None) -> Any:
    if attributes is None:
        return arr
    from arro3.core import Array

    # Import the native GeoArrow capsule directly.  `GeometryArray.to_arrow()`
    # intentionally materializes a PyArrow object and therefore belongs to the
    # `arrow` extra; visualization needs only lonboard's own arro3 dependency.
    return attributes.add_column(0, 'geometry', Array.from_arrow(arr))


def _lonboard_viz(arr: GeometryArray, attributes: Any | None, **kwargs: Any) -> Any:
    """Hand lonboard a GeoArrow capsule first; fall back to GeoJSON if needed."""
    lonboard = _require_lonboard()

    try:
        return lonboard.viz(_lonboard_data(arr, attributes), **kwargs)
    except (ImportError, TypeError):
        return lonboard.viz(_feature_collection(arr, attributes), **kwargs)


def explore(
    values: Geometry | GeometryArray,
    *,
    attributes: PyArrowTable | Mapping[str, Any] | None = None,
    **kwargs: Any,
) -> Map:
    """Build an interactive lonboard map from gometry geometries.

    Geometries are handed to lonboard via zero-copy GeoArrow
    (``__arrow_c_array__``) when possible; a GeoJSON feature-collection
    fallback is used only if the capsule handoff fails.

    Parameters
    ----------
    values : Geometry or GeometryArray
        Input geometries. A scalar geometry is wrapped as a one-row array.
    attributes : Arrow table or mapping, optional
        Aligned feature columns carried into lonboard and GeoJSON tooltips.
    kwargs : mapping, optional
        Keyword arguments forwarded to ``lonboard.viz`` (``scatterplot_kwargs``,
        ``path_kwargs``, ``polygon_kwargs``, and ``map_kwargs``).

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
    from gometry._lib import GeometryError

    if isinstance(values, GeometryArray):
        arr = values
    elif isinstance(values, Geometry):
        arr = GeometryArray([values])
    else:
        raise TypeError('explore() expects a Geometry or GeometryArray')

    if not _is_map_suitable(arr):
        raise GeometryError(
            'explore() requires a non-empty array with a CRS and finite bounds'
        )

    display_arr = _array_for_map(arr)
    attribute_table = _attribute_table(attributes, len(display_arr))
    return _lonboard_viz(display_arr, attribute_table, **kwargs)


def array_repr_html(arr: GeometryArray) -> str | None:
    """Return lonboard embed HTML for Jupyter, or ``None`` to fall back to SVG."""
    if not _lonboard_importable() or not _is_map_suitable(arr):
        return None

    try:
        from ipywidgets.embed import embed_snippet

        display_arr = _array_for_map(arr)
        # Widget-state serialization runs in lonboard's worker pool after this
        # call returns.  Materialize the Arrow object so those workers do not
        # outlive the direct C-capsule handoff owned by ``display_arr``.
        try:
            data = display_arr.to_arrow()
        except ModuleNotFoundError:
            # Without PyArrow, lonboard serializes the direct capsule through
            # arro3 and does not cross the crashing PyArrow worker boundary.
            data = display_arr
        map_obj = _require_lonboard().viz(data)
        return embed_snippet(views=[map_obj], drop_defaults=True)
    except Exception:
        return None
