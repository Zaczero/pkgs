"""Own immutable Arrow buffers without constructing Python feature rows."""

from __future__ import annotations

from typing import Any

from gometry._lib import _snapshot_arrow_array


def snapshot_arrow_array(pa: Any, array: Any) -> Any:
    """Copy an Arrow array or chunked array into immutable owned buffers."""
    if isinstance(array, pa.ChunkedArray):
        return pa.chunked_array(
            [pa.array(_snapshot_arrow_array(chunk)) for chunk in array.chunks],
            type=array.type,
        )
    if isinstance(array, pa.Array):
        return pa.array(_snapshot_arrow_array(array))
    raise TypeError('expected an Arrow array or chunked array')


def snapshot_arrow_table(pa: Any, table: Any) -> Any:
    """Copy a table's columns while preserving its schema and metadata."""
    if not isinstance(table, pa.Table):
        raise TypeError('expected an Arrow table')
    if table.num_columns == 0:
        return table.slice(0)
    return pa.Table.from_arrays(
        [snapshot_arrow_array(pa, column) for column in table.columns],
        schema=table.schema,
    )
