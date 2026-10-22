"""Native Parquet logical-type admission through PyArrow's schema API."""

from __future__ import annotations

import json
from typing import Any

from gometry._arrow import (
    _crs_projjson,
    _extension_type_from_storage,
    _metadata_frame,
)
from gometry._geoparquet_metadata import (
    _embedded_frame_conflicts,
    _metadata_error,
    _validate_column_metadata,
)
from gometry._lib import CRS, GeometryError


def logical_columns(parquet_schema: Any, schema: Any) -> dict[str, dict[str, Any]]:
    """Read root geometry annotations, without decoding the Parquet format."""
    result = {}
    root_names = set(schema.names)
    for index in range(len(parquet_schema)):
        column = parquet_schema.column(index)
        logical = json.loads(column.logical_type.to_json())
        if logical['Type'] not in {'Geometry', 'Geography'}:
            continue
        if column.path not in root_names:
            raise _metadata_error('native geometry columns must be at the schema root')
        result[column.path] = logical
    return result


def _logical_crs(logical: dict[str, Any], schema: Any) -> str | None:
    value = logical.get('crs', 'OGC:CRS84')
    if value == 'srid:0':
        return None
    if value.startswith('srid:'):
        raise _metadata_error(
            f'Parquet CRS {value!r} requires an external SRID registry; '
            're-encode with an authority code or inline PROJJSON'
        )
    if value.startswith('projjson:'):
        key = value.removeprefix('projjson:').encode()
        definition = (schema.metadata or {}).get(key)
        if definition is None:
            raise _metadata_error(f'Parquet CRS metadata key {key!r} is missing')
        try:
            value = definition.decode('utf-8')
        except UnicodeDecodeError as error:
            raise _metadata_error('Parquet CRS definition is not UTF-8') from error
    try:
        return CRS(value).definition
    except GeometryError as error:
        raise _metadata_error(
            f'invalid native Parquet CRS {value!r}: {error}'
        ) from error


def _extension_frame(
    field: Any, crs: str | None, schema: Any
) -> tuple[str | None, float | None]:
    arrow_type = field.type
    extension_name = getattr(arrow_type, 'extension_name', None)
    raw_name = (field.metadata or {}).get(b'ARROW:extension:name')
    if raw_name is not None:
        try:
            raw_name = raw_name.decode('utf-8')
        except UnicodeDecodeError as error:
            raise _metadata_error('Arrow extension name is not UTF-8') from error
    for name in (extension_name, raw_name):
        if name is not None and name != 'geoarrow.wkb':
            raise _metadata_error(
                f'native Parquet GEOMETRY conflicts with Arrow extension {name!r}; '
                'expected geoarrow.wkb'
            )
    declarations = []
    if hasattr(arrow_type, '__arrow_ext_serialize__'):
        declarations.append(arrow_type.__arrow_ext_serialize__())
    raw_metadata = (field.metadata or {}).get(b'ARROW:extension:metadata')
    if raw_metadata is not None:
        declarations.append(raw_metadata)
    frame: tuple[str | None, float | None] = (None, None)
    for serialized in declarations:
        declared = _extension_metadata_frame(serialized, crs, schema)
        if _embedded_frame_conflicts(
            *declared,
            frame[0] if frame[0] is not None else declared[0],
            frame[1] if frame[1] is not None else declared[1],
        ):
            raise _metadata_error('native Parquet Arrow frame declarations conflict')
        frame = (
            declared[0] if declared[0] is not None else frame[0],
            declared[1] if declared[1] is not None else frame[1],
        )
    return frame


def _extension_metadata_frame(
    serialized: bytes, crs: str | None, schema: Any
) -> tuple[str | None, float | None]:
    try:
        metadata = json.loads(serialized or b'{}')
    except (ValueError, UnicodeDecodeError) as error:
        raise _metadata_error(
            'Arrow extension metadata is not valid UTF-8 JSON'
        ) from error
    if (
        isinstance(metadata, dict)
        and 'crs' in metadata
        and metadata['crs'] is None
        and crs is not None
    ):
        raise _metadata_error('Arrow unknown CRS conflicts with native Parquet CRS')
    if isinstance(metadata, dict) and (
        (
            metadata.get('crs') == 'srid:0'
            and metadata.get('crs_type') in (None, 'authority_code')
        )
        or (metadata.get('crs_type') == 'srid' and metadata.get('crs') == '0')
    ):
        if crs is not None:
            raise _metadata_error('Arrow unknown CRS conflicts with native Parquet CRS')
        metadata.pop('crs')
        metadata.pop('crs_type', None)
        serialized = json.dumps(metadata).encode()
    elif (
        isinstance(metadata, dict)
        and isinstance(reference := metadata.get('crs'), str)
        and reference.startswith('projjson:')
    ):
        referenced_crs = _logical_crs({'crs': reference}, schema)
        assert referenced_crs is not None
        metadata['crs'] = _crs_projjson(referenced_crs)
        metadata['crs_type'] = 'projjson'
        serialized = json.dumps(metadata).encode()
    if isinstance(metadata, dict) and metadata.get('edges', 'planar') != 'planar':
        raise _metadata_error(
            'native GEOMETRY conflicts with Arrow edge metadata; gometry uses '
            'straight source-chart edges. Explicitly convert nonplanar curves '
            'to linear edges before reading'
        )
    try:
        return _metadata_frame(serialized or b'{}')
    except GeometryError as error:
        raise _metadata_error(f'invalid native Parquet Arrow frame: {error}') from error


def admit_native_columns(
    pa: Any,
    schema: Any,
    logical: dict[str, dict[str, Any]],
    metadata: Any,
    geometry: str | None,
    *,
    stored_schema: Any | None,
) -> tuple[Any, Any]:
    """Reconcile native, stored Arrow and optional GeoParquet declarations."""
    if metadata is None:
        if geometry is None:
            if len(logical) != 1:
                raise _metadata_error(
                    'native Parquet has multiple geometry columns and no primary; '
                    'select one with geometry='
                )
            geometry = next(iter(logical))
        if geometry not in logical:
            raise _metadata_error(
                f'{geometry!r} is not a native Parquet geometry column'
            )
        metadata = {
            'version': '2.0.0',
            'primary_column': geometry,
            'columns': {},
        }
        for name, declaration in logical.items():
            crs = _logical_crs(declaration, schema)
            metadata['columns'][name] = {
                'encoding': 'WKB',
                'geometry_types': [],
                'crs': None if crs is None else _crs_projjson(crs),
            }
    elif metadata['version'].startswith('2.'):
        declared = metadata.get('columns')
        if not isinstance(declared, dict) or set(declared) != set(logical):
            raise _metadata_error(
                'GeoParquet 2.0 geometry declarations must match native logical columns'
            )

    fields = list(schema)
    for name, declaration in logical.items():
        if declaration['Type'] == 'Geography':
            raise _metadata_error(
                f'column {name!r} uses GEOGRAPHY with nonplanar edges; gometry '
                'uses straight source-chart edges. Explicitly convert the curves '
                'and re-encode as GEOMETRY before reading'
            )
        crs = _logical_crs(declaration, schema)
        if name not in metadata.get('columns', {}):
            raise _metadata_error(
                f'native geometry column {name!r} is absent from geo metadata'
            )
        column_meta, encoding, declared_crs, epoch = _validate_column_metadata(
            metadata, name
        )
        if encoding != 'WKB':
            raise _metadata_error('native Parquet GEOMETRY requires WKB encoding')
        if (crs is None) != (declared_crs is None) or _embedded_frame_conflicts(
            crs, None, declared_crs, None
        ):
            raise _metadata_error(
                f'column {name!r} geo CRS conflicts with native Parquet CRS'
            )
        index = schema.get_field_index(name)
        if index < 0:
            raise _metadata_error(
                f'native geometry column {name!r} must occur exactly once'
            )
        field = schema.field(index)
        declarations = [field]
        if stored_schema is not None:
            stored_index = stored_schema.get_field_index(name)
            if stored_index < 0:
                raise _metadata_error(
                    f'native geometry column {name!r} must occur exactly once in stored Arrow schema'
                )
            declarations.append(stored_schema.field(stored_index))
        embedded_crs, embedded_epoch = None, None
        for declaration_field in declarations:
            next_crs, next_epoch = _extension_frame(declaration_field, crs, schema)
            if _embedded_frame_conflicts(
                next_crs,
                next_epoch,
                embedded_crs if embedded_crs is not None else next_crs,
                embedded_epoch if embedded_epoch is not None else next_epoch,
            ):
                raise _metadata_error(
                    f'column {name!r} stored Arrow frame conflicts with native Parquet Arrow frame'
                )
            embedded_crs = next_crs if next_crs is not None else embedded_crs
            embedded_epoch = next_epoch if next_epoch is not None else embedded_epoch
        if _embedded_frame_conflicts(
            embedded_crs,
            embedded_epoch,
            declared_crs,
            epoch,
            declared_epoch_present='epoch' in column_meta,
        ):
            raise _metadata_error(
                f'column {name!r} Arrow frame conflicts with native Parquet frame'
            )
        if 'epoch' not in column_meta and embedded_epoch is not None:
            column_meta = dict(column_meta)
            column_meta['epoch'] = embedded_epoch
            metadata['columns'][name] = column_meta
            epoch = embedded_epoch
        storage = getattr(field.type, 'storage_type', field.type)
        extension = _extension_type_from_storage(
            pa, 'geoarrow.wkb', storage, declared_crs, epoch
        )
        field_metadata = {
            key: value
            for key, value in (field.metadata or {}).items()
            if not key.startswith(b'ARROW:extension:')
        }
        fields[index] = pa.field(
            name,
            extension,
            nullable=field.nullable,
            metadata=field_metadata or None,
        )
    return pa.schema(fields, metadata=schema.metadata), metadata
