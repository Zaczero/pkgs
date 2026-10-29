"""Arrow export for aligned feature snapshots."""

from __future__ import annotations

import json
import sys
from typing import TYPE_CHECKING, Any, overload

from gometry._arrow import _pyarrow
from gometry._lib import (
    Geometry,
    GeometryArray,
    _arrow_stream_reader,
    _ArrowColumn,
    _ArrowSchema,
    _ArrowTable,
    _FeaturePropertyColumn,
    _is_arrow_geometry,
    _snapshot_arrow_c,
)
from gometry._lib import from_arrow as _geometry_from_arrow

if TYPE_CHECKING:
    from collections.abc import Generator, Mapping, Sequence
    from types import EllipsisType

    import pyarrow as pa

    from gometry._types import ArrowEncoding, CrsInput, Features, _EpochInput


def _declared_geometry(pa: Any, field: Any) -> bool:
    data_type = field.type
    extension_name = (
        data_type.extension_name
        if isinstance(data_type, pa.BaseExtensionType)
        else (field.metadata or {}).get(b'ARROW:extension:name', b'').decode('utf-8')
    )
    return extension_name.startswith('geoarrow.')


def _table_roles(
    names: list[str],
    metadata: Mapping[bytes, bytes],
    candidates: list[str],
    geoarrow_columns: list[str],
    geometry: str | None,
    id_column: Any,
) -> tuple[str, str | None, set[str]]:
    if len(set(names)) != len(names):
        raise ValueError('Arrow feature tables require unique column names')
    geo = None
    if b'geo' in metadata:
        try:
            geo = json.loads(metadata[b'geo'])
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError('invalid geometry schema metadata') from error
        if not isinstance(geo, dict):
            raise ValueError('invalid geometry schema metadata')
    if geometry is not None and not isinstance(geometry, str):
        raise TypeError('geometry must be a column name or None')
    if geometry is None:
        primary = geo.get('primary_column') if geo is not None else None
        if primary is not None:
            if not isinstance(primary, str) or primary not in candidates:
                raise ValueError('declared primary geometry column is invalid')
            geometry = primary
        elif len(geoarrow_columns) == 1:
            geometry = geoarrow_columns[0]
        elif not geoarrow_columns and len(candidates) == 1:
            geometry = candidates[0]
        elif not candidates and names == ['geometry']:
            geometry = names[0]
        elif not candidates:
            raise TypeError('Arrow table has no geometry-like column')
        else:
            raise ValueError(
                'select a geometry column when none or several are present'
            )
    elif geometry not in candidates and geometry not in geoarrow_columns:
        raise ValueError(f'geometry column {geometry!r} is missing or unsupported')
    if id_column is ...:
        encoded_id = metadata.get(b'gometry:feature_id')
        if encoded_id is not None:
            try:
                id_column = encoded_id.decode('utf-8')
            except UnicodeDecodeError as error:
                raise ValueError('invalid feature ID column metadata') from error
        else:
            id_column = None
    if id_column is not None and not isinstance(id_column, str):
        raise TypeError('id_column must be a column name or None')
    if id_column is not None and (id_column not in names or id_column == geometry):
        raise ValueError(f'feature ID column {id_column!r} is missing or is geometry')
    declared = set(geo.get('columns', {})) if geo is not None else set()
    return geometry, id_column, declared


def _native_geometry(
    column: _ArrowColumn, *, crs: Any = None, epoch: Any = None
) -> Any:
    parts = [
        _geometry_from_arrow(chunk, crs=crs, epoch=epoch) for chunk in column._chunks()
    ]
    return parts[0] if len(parts) == 1 else parts[0].concat(parts[1], *parts[2:])


def _native_table(
    table: _ArrowTable, *, geometry: str | None, id_column: Any, crs: Any, epoch: Any
) -> Features:
    from gometry._types import (
        Features,
        _snapshot_feature_columns,
        _snapshot_feature_ids,
    )

    schema = table.schema
    names = schema.names
    candidates = schema.geometry_columns
    geoarrow_columns = schema.geoarrow_columns
    geometry, id_column, declared = _table_roles(
        names,
        schema.metadata,
        candidates,
        geoarrow_columns,
        geometry,
        id_column,
    )
    declared.update(schema.geoarrow_columns)
    geometries = _native_geometry(
        table.column(names.index(geometry)), crs=crs, epoch=epoch
    )
    columns = {}
    for index, name in enumerate(names):
        if name in (geometry, id_column):
            continue
        column = table.column(index)
        columns[name] = _native_geometry(column) if name in declared else column
    ids = None if id_column is None else table.column(names.index(id_column))
    return Features._from_property_rows(
        geometries,
        _snapshot_feature_columns(columns, len(table)),
        _snapshot_feature_ids(ids, len(table)),
        id_column,
        schema,
        geometry,
    )


@overload
def from_arrow(
    data: pa.Array | pa.ChunkedArray,
    *,
    geometry: str | None = None,
    id_column: str | EllipsisType | None = ...,
    crs: CrsInput | None = None,
    epoch: _EpochInput | None = None,
) -> GeometryArray[Geometry | None]: ...


@overload
def from_arrow(
    data: pa.Table | pa.RecordBatch | pa.RecordBatchReader,
    *,
    geometry: str | None = None,
    id_column: str | EllipsisType | None = ...,
    crs: CrsInput | None = None,
    epoch: _EpochInput | None = None,
) -> Features: ...


@overload
def from_arrow(
    data: object,
    *,
    geometry: str | None = None,
    id_column: str | EllipsisType | None = ...,
    crs: CrsInput | None = None,
    epoch: _EpochInput | None = None,
) -> GeometryArray[Geometry | None] | Features: ...


def from_arrow(
    data: Any,
    *,
    geometry: str | None = None,
    id_column: Any = ...,
    crs: Any = None,
    epoch: Any = None,
) -> Any:
    """Import a geometry array or a geometry-bearing Arrow feature table.

    Parameters
    ----------
    data : Arrow array, table, record batch or stream
        GeoArrow or WKB geometry data, or a table with a geometry column and
        aligned attributes. Arrow C data and stream providers are accepted.
    geometry : str or None, default None
        Geometry column name for a table. When omitted, a declared primary
        geometry or an unambiguous geometry column is selected.
    id_column : str, Ellipsis or None, default ...
        Feature ID column. The default uses recorded ID metadata when present;
        ``None`` treats all non-geometry columns as properties.
    crs : CrsInput or None, default None
        Coordinate reference system to apply to the imported geometry.
    epoch : float or None, default None
        Coordinate epoch to apply to the imported geometry.

    Returns
    -------
    GeometryArray or Features
        Arrays become geometry arrays; tables and record-batch streams become
        immutable feature collections.

    Raises
    ------
    CRSError
        If coordinate reference system or epoch metadata is invalid.
    GeometryError
        If geometry data violates a geometry invariant.
    ParseError
        If Arrow buffers, geometry metadata, or encoded geometry are malformed.
    TypeError
        If the input or its schema is unsupported.
    OSError
        If an Arrow stream callback fails.
    MemoryError
        If an Arrow stream callback reports allocation failure.
    """
    if isinstance(data, (Geometry, GeometryArray)):
        raise TypeError('from_arrow expects Arrow data; geometry is already decoded')
    pa = sys.modules.get('pyarrow')
    if pa is None or not isinstance(
        data, (pa.Table, pa.RecordBatch, pa.RecordBatchReader)
    ):
        if pa is None or not isinstance(data, (pa.Array, pa.ChunkedArray)):
            native = _snapshot_arrow_c(data)
            if isinstance(native, _ArrowTable):
                return _native_table(
                    native, geometry=geometry, id_column=id_column, crs=crs, epoch=epoch
                )
            if isinstance(native, _ArrowColumn):
                if geometry is not None or id_column is not ...:
                    raise TypeError(
                        'geometry and id_column require an Arrow feature table'
                    )
                return _native_geometry(native, crs=crs, epoch=epoch)
        if geometry is not None or id_column is not ...:
            raise TypeError('geometry and id_column require an Arrow feature table')
        return _geometry_from_arrow(data, crs=crs, epoch=epoch)
    if isinstance(data, pa.RecordBatchReader):
        data = data.read_all()
    if isinstance(data, pa.RecordBatch):
        data = pa.Table.from_batches([data])
    schema = data.schema
    names = schema.names
    metadata = schema.metadata or {}
    candidates = [
        name
        for index, name in enumerate(names)
        if _is_arrow_geometry(data.column(index), schema.field(index))
    ]
    geometry, id_column, declared = _table_roles(
        names,
        metadata,
        candidates,
        [
            name
            for index, name in enumerate(names)
            if _declared_geometry(pa, schema.field(index))
        ],
        geometry,
        id_column,
    )

    geometry_index = names.index(geometry)
    geometries = _geometry_from_arrow(
        data.select([geometry_index]), crs=crs, epoch=epoch
    )
    columns: dict[str, Any] = {}
    for index, name in enumerate(names):
        if name in (geometry, id_column):
            continue
        field = schema.field(index)
        if name in declared or _declared_geometry(pa, field):
            columns[name] = _geometry_from_arrow(data.select([index]))
        else:
            columns[name] = data.column(index)
    ids = None if id_column is None else data.column(names.index(id_column))
    from gometry._types import (
        Features,
        _snapshot_feature_columns,
        _snapshot_feature_ids,
    )

    return Features._from_property_rows(
        geometries,
        _snapshot_feature_columns(columns, len(geometries)),
        _snapshot_feature_ids(ids, len(geometries)),
        id_column,
        schema,
        geometry,
    )


def from_arrow_batches(
    reader: Any, *, geometry: str | None = None, id_column: Any = ...
) -> Generator[Features, None, None]:
    """Yield feature snapshots from a caller-owned Arrow record-batch stream.

    Parameters
    ----------
    reader : Arrow record-batch reader or stream
        The caller-owned reader or Arrow C stream provider.
    geometry : str or None, default None
        Geometry column name, or ``None`` to select from metadata or an
        unambiguous geometry column.
    id_column : str, Ellipsis or None, default ...
        Feature ID column. The default uses recorded ID metadata when present;
        ``None`` treats non-geometry columns as properties.

    Returns
    -------
    generator of Features
        Immutable snapshots in stream order. Close the generator when stopping
        early to release its stream resources.
    """
    pa = sys.modules.get('pyarrow')
    if pa is not None and isinstance(reader, pa.RecordBatchReader):

        def pyarrow_batches() -> Generator[Features, None, None]:
            for batch in reader:
                yield from_arrow(batch, geometry=geometry, id_column=id_column)

        return pyarrow_batches()
    if not callable(getattr(reader, '__arrow_c_stream__', None)):
        raise TypeError('expected an Arrow record-batch stream')

    def batches() -> Generator[Features, None, None]:
        stream = _arrow_stream_reader(reader)
        try:
            for batch in stream:
                if not isinstance(batch, _ArrowTable):
                    raise TypeError('feature batches require an Arrow table stream')
                yield _native_table(
                    batch, geometry=geometry, id_column=id_column, crs=None, epoch=None
                )
        finally:
            stream.close()

    return batches()


def _row_columns(rows: Sequence[Mapping[str, Any] | None]) -> dict[str, list[Any]]:
    names: dict[str, None] = {}
    for row in rows:
        if row is not None:
            names.update(dict.fromkeys(row))
    return {
        name: [None if row is None else row.get(name) for row in rows] for name in names
    }


def _id_array(pa: Any, ids: Sequence[str | int | float | None]) -> Any:
    if isinstance(ids, _FeaturePropertyColumn):
        values = ids._snapshot(detach=False)
        if isinstance(values, _ArrowColumn):
            return _pyarrow_column(pa, values)
        return (
            values
            if isinstance(values, (pa.Array, pa.ChunkedArray))
            else pa.array(values)
        )
    kind: type[int | float | str] | None = None
    minimum = 0
    maximum = 0
    for value in ids:
        if value is None:
            continue
        current = (
            int
            if isinstance(value, int)
            else float
            if isinstance(value, float)
            else str
        )
        if kind is not None and current is not kind:
            raise TypeError('feature IDs have incompatible types for one Arrow column')
        kind = current
        if isinstance(value, int):
            minimum = min(minimum, value)
            maximum = max(maximum, value)
    if kind is None:
        return pa.nulls(len(ids))
    if kind is int:
        if minimum >= 0 and maximum > (1 << 63) - 1:
            return pa.array(ids, type=pa.uint64())
        return pa.array(ids, type=pa.int64())
    if kind is float:
        return pa.array(ids, type=pa.float64())
    return pa.array(ids, type=pa.string())


def _pyarrow_column(pa: Any, column: _ArrowColumn) -> Any:
    values = pa.chunked_array([pa.array(chunk) for chunk in column._chunks()])
    indices = column._indices()
    return (
        values if indices is None else values.take(pa.array(indices, type=pa.int64()))
    )


def _property_columns(features: Features) -> dict[str, Any]:
    mixed = features._properties._mixed_snapshot(detach=False)
    if mixed is not None:
        base_columns, rows, groups, projection = mixed
        columns = dict(base_columns) if base_columns is not None else _row_columns(rows)
        for group in groups:
            columns.update(group)
    else:
        snapshot = features._properties._column_snapshot(detach=False)
        if snapshot is not None:
            return snapshot
        columns = _row_columns(features._geojson_properties())
        projection = features._properties._projection_snapshot()
    if projection is not None:
        return {
            name: columns[name] if name in columns else [None] * len(features)
            for name in projection
        }
    return columns


def to_arrow(
    features: Features,
    *,
    geometry: str | None,
    id_column: str | None,
    encoding: ArrowEncoding,
) -> Any:
    """Export feature data, normalizing sparse properties to Arrow nulls."""
    return _feature_table(
        features,
        geometry=geometry,
        id_column=id_column,
        encoding=encoding,
        columns=_property_columns(features),
    )


def _feature_table(
    features: Features,
    *,
    geometry: str | None,
    id_column: str | None,
    encoding: ArrowEncoding,
    columns: dict[str, Any],
    geometry_columns: Mapping[str, Any] | None = None,
) -> Any:
    pa = _pyarrow()
    geometry_name = geometry
    if geometry_name is None:
        geometry_name = (
            'geometry'
            if features._geometry_column is None
            else features._geometry_column
        )
    if not isinstance(geometry_name, str):
        raise TypeError('geometry must be a column name or None')
    if id_column is not None and not isinstance(id_column, str):
        raise TypeError('id_column must be a column name or None')
    role_name = features._id_column if id_column is None else id_column
    if features._id_column is None and id_column is not None:
        raise ValueError('features have no ID column to rename')

    if geometry_name in columns:
        raise ValueError(f'geometry column {geometry_name!r} conflicts with a property')
    if role_name is not None and (role_name == geometry_name or role_name in columns):
        raise ValueError(f'ID column {role_name!r} conflicts with another column')

    source_geometry = features._geometry_column
    entries = [
        (
            geometry_name,
            features.geometries.to_arrow(encoding=encoding)
            if geometry_columns is None
            else geometry_columns[geometry_name],
            source_geometry,
        )
    ]
    for name, values in columns.items():
        if isinstance(values, GeometryArray):
            array = (
                values.to_arrow(encoding=encoding)
                if geometry_columns is None
                else geometry_columns[name]
            )
        elif isinstance(values, _ArrowColumn):
            array = _pyarrow_column(pa, values)
        elif isinstance(values, (pa.Array, pa.ChunkedArray)):
            array = values
        else:
            try:
                array = pa.array(values)
            except (TypeError, ValueError, OverflowError, pa.ArrowException) as error:
                raise TypeError(
                    f'cannot export property column {name!r}: {error}'
                ) from error
        entries.append((name, array, name))
    if role_name is not None:
        entries.append((role_name, _id_array(pa, features.ids), features._id_column))

    schema = features._arrow_schema
    if isinstance(schema, _ArrowSchema):
        schema = pa.schema(schema)
    metadata = dict(schema.metadata or {}) if schema is not None else {}
    metadata.pop(b'gometry:feature_id', None)
    if role_name is not None:
        metadata[b'gometry:feature_id'] = role_name.encode('utf-8')
    if b'geo' in metadata and geometry_columns is None:
        from gometry._geoparquet import _build_geo_metadata

        geo = json.loads(metadata[b'geo'])
        old_columns = geo.get('columns', {})
        geometries = {
            geometry_name: (features.geometries, source_geometry),
            **{
                name: (values, name)
                for name, values in columns.items()
                if isinstance(values, GeometryArray)
            },
        }
        arrays_by_name = {name: array for name, array, _ in entries}
        updated = {}
        for name, (values, old_name) in geometries.items():
            extension = arrays_by_name[name].type.extension_name
            parquet_encoding = (
                'WKB'
                if extension == 'geoarrow.wkb'
                else extension.removeprefix('geoarrow.')
            )
            column_meta = _build_geo_metadata(
                values, parquet_encoding, native=geo.get('version', '').startswith('2.')
            )['columns']['geometry']
            extras = dict(old_columns.get(old_name, {}))
            for key in (
                'encoding',
                'geometry_types',
                'crs',
                'gometry:crs_definition',
                'epoch',
                'bbox',
                'covering',
                'orientation',
                'edges',
            ):
                extras.pop(key, None)
            updated[name] = {**extras, **column_meta}
        geo['primary_column'] = geometry_name
        geo['columns'] = updated
        metadata[b'geo'] = json.dumps(geo, separators=(',', ':')).encode('utf-8')
    if schema is not None and features._properties._projection_snapshot() is None:
        positions = {name: index for index, name in enumerate(schema.names)}
        entries.sort(key=lambda entry: positions.get(entry[2], len(positions)))
    fields = []
    arrays = []
    for name, array, source_name in entries:
        if schema is not None and source_name in schema.names:
            original = schema.field(source_name)
            field_metadata = dict(original.metadata or {})
            if original.type != array.type:
                field_metadata.pop(b'ARROW:extension:name', None)
                field_metadata.pop(b'ARROW:extension:metadata', None)
            field = pa.field(
                name,
                array.type,
                nullable=original.nullable or array.null_count > 0,
                metadata=field_metadata or None,
            )
        else:
            field = pa.field(name, array.type)
        fields.append(field)
        arrays.append(array)
    return pa.Table.from_arrays(
        arrays, schema=pa.schema(fields, metadata=metadata or None)
    )
