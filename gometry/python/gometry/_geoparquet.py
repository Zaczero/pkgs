"""GeoParquet feature-table read/write over WKB and native GeoArrow layouts."""

from __future__ import annotations

import itertools
import json
import math
import os  # noqa: TC003 - required by runtime get_type_hints
import re
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Literal

from gometry import GeometryArray, from_arrow
from gometry._lib import Geometry, GeometryError, ParseError
from gometry._optional import missing_optional_dependency
from gometry._types import CrsInput, PyArrowTable, mapping_as_dict

TYPE_CHECKING = False

GEOARROW_ENCODINGS = frozenset({
    'point',
    'linestring',
    'polygon',
    'multipoint',
    'multilinestring',
    'multipolygon',
})
_ENCODING_BASE_KIND = {
    'point': 'Point',
    'linestring': 'LineString',
    'polygon': 'Polygon',
    'multipoint': 'MultiPoint',
    'multilinestring': 'MultiLineString',
    'multipolygon': 'MultiPolygon',
}
_GEOMETRY_TYPES = frozenset({
    kind + suffix
    for kind in (
        'Point',
        'LineString',
        'Polygon',
        'MultiPoint',
        'MultiLineString',
        'MultiPolygon',
        'GeometryCollection',
    )
    for suffix in ('', ' Z')
})
_SEMVER_IDENTIFIER = r'(?:0|[1-9][0-9]*|[0-9A-Za-z-]*[A-Za-z-][0-9A-Za-z-]*)'
_VERSION_PATTERN = re.compile(
    rf'^1\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)'
    rf'(?:-{_SEMVER_IDENTIFIER}(?:\.{_SEMVER_IDENTIFIER})*)?'
    r'(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$'
)
_READ_TABLE_OPTIONS = frozenset({
    'arrow_extensions_enabled',
    'binary_type',
    'buffer_size',
    'coerce_int96_timestamp_unit',
    'decryption_properties',
    'ignore_prefixes',
    'list_type',
    'memory_map',
    'page_checksum_verification',
    'partitioning',
    'pre_buffer',
    'read_dictionary',
    'thrift_container_size_limit',
    'thrift_string_size_limit',
    'use_pandas_metadata',
    'use_threads',
})
_ROW_GROUP_OPTIONS = frozenset({'use_pandas_metadata', 'use_threads'})
_PARQUET_FILE_OPTIONS = frozenset({
    'arrow_extensions_enabled',
    'binary_type',
    'buffer_size',
    'coerce_int96_timestamp_unit',
    'decryption_properties',
    'list_type',
    'memory_map',
    'page_checksum_verification',
    'pre_buffer',
    'read_dictionary',
    'thrift_container_size_limit',
    'thrift_string_size_limit',
})
_PARQUET_INSTALL = "install the 'gometry[arrow]' extra"


def _pyarrow() -> Any:
    try:
        import pyarrow as pa
    except ModuleNotFoundError as error:
        raise missing_optional_dependency(
            error, 'pyarrow', f'geoparquet interop requires pyarrow; {_PARQUET_INSTALL}'
        ) from error
    return pa


def _pyarrow_parquet() -> Any:
    try:
        import pyarrow.parquet as pq
    except ModuleNotFoundError as error:
        raise missing_optional_dependency(
            error, 'pyarrow', f'geoparquet interop requires pyarrow; {_PARQUET_INSTALL}'
        ) from error
    return pq


def _geometry_type_label(kind: str, axes: str) -> str:
    if axes == 'XYZ':
        return f'{kind} Z'
    if axes == 'XYM':
        return f'{kind} M'
    if axes == 'XYZM':
        return f'{kind} ZM'
    return kind


def _axes_token(has_z: bool, has_m: bool) -> str:
    """Coordinate-axes token for a (has_z, has_m) storage pair."""
    if has_z and has_m:
        return 'XYZM'
    if has_z:
        return 'XYZ'
    if has_m:
        return 'XYM'
    return 'XY'


def _physical_storage_axes(encoding: str, source: Any) -> tuple[bool, bool]:
    """Ordinate layout of structural (native GeoArrow) storage only.

    Native GeoArrow coordinate structs expose axes via field names
    (``_storage_axes``). WKB encodes Z/M *inside* the binary blob — binary
    storage makes no XY/XYZ claim, so this returns ``(False, False)`` for WKB
    without inventing a 2D inventory. Callers must not reconcile declared
    ``geometry_types`` against that non-claim for empty/all-null WKB columns.
    """
    if encoding in GEOARROW_ENCODINGS:
        from gometry._arrow import _storage_axes

        storage_type = getattr(source.type, 'storage_type', source.type)
        return _storage_axes(storage_type)
    # WKB / unknown: no structural ordinate fields.
    return False, False


def _geometry_types(arr: GeometryArray) -> list[str]:
    """Sorted unique GeoParquet geometry_types via one native batch call."""
    return arr._geoparquet_geometry_types()


def _remove_id_from_member_of_ensembles(json_dict: dict[str, Any]) -> None:
    for key, value in json_dict.items():
        if isinstance(value, dict):
            _remove_id_from_member_of_ensembles(value)
        elif key == 'members' and isinstance(value, list):
            for member in value:
                if isinstance(member, dict):
                    member.pop('id', None)


def _build_geo_metadata(arr: GeometryArray, encoding: str) -> dict[str, Any]:
    from gometry._arrow import _crs_projjson

    column_meta: dict[str, Any] = {
        'encoding': encoding,
        'geometry_types': _geometry_types(arr),
    }
    if arr.crs is not None:
        crs_dict = _crs_projjson(arr.crs)
        _remove_id_from_member_of_ensembles(crs_dict)
        column_meta['crs'] = crs_dict
    else:
        column_meta['crs'] = None

    if arr.epoch is not None:
        column_meta['epoch'] = arr.epoch

    bounds = arr.total_bounds
    if bounds is not None:
        column_meta['bbox'] = list(bounds)

    return {
        'version': '1.1.0',
        'primary_column': 'geometry',
        'columns': {'geometry': column_meta},
    }


def _as_array(
    values: Geometry | GeometryArray,
    *,
    crs: CrsInput | None = None,
) -> GeometryArray:
    if isinstance(values, Geometry):
        return GeometryArray([values], crs=crs)
    if crs is not None:
        return values.set_crs(crs)
    return values


def _normalize_attribute_columns(
    attributes: Mapping[str, Any] | Any, row_count: int
) -> dict[str, Any]:
    """Normalize a Mapping of columns for table construction.

    Builds a plain ``dict`` via the shared keys()+seen copier (N4) so non-dict
    Mappings (e.g. ``UserDict``), keys()-only ducks, and repeated-key streams
    behave like the Rust mapping boundary. Non-Arrow columns are bounded to
    ``row_count + 1`` so infinite iterators terminate; the caller's row-count
    check then rejects mismatches. Arrow arrays / capsules / streams are
    preserved as-is.
    """
    import pyarrow as pa

    columns = mapping_as_dict(attributes)
    normalized: dict[str, Any] = {}
    for name, values in columns.items():
        if (
            isinstance(values, (pa.Array, pa.ChunkedArray))
            or hasattr(values, '__arrow_c_array__')
            or hasattr(values, '__arrow_c_stream__')
        ):
            normalized[name] = values
        elif hasattr(values, '__len__'):
            # Sized array-likes (numpy arrays, pandas extension arrays, lists) are
            # proportional data — hand them to PyArrow's native conversions. The
            # caller's row-count check still rejects a length mismatch.
            normalized[name] = values
        else:
            # Only UNSIZED iterators need the non-termination bound (N4/D10).
            normalized[name] = list(itertools.islice(values, row_count + 1))
    return normalized


def _attributes_table(pa: Any, attributes: Any, row_count: int) -> Any | None:
    if attributes is None:
        return None
    if isinstance(attributes, pa.Table):
        table = attributes
    elif isinstance(attributes, Mapping) or callable(
        getattr(attributes, 'keys', None)
    ):
        # Plain dict via keys()+seen (N4); UserDict / keys()-only ducks ok;
        # non-Arrow columns bound so infinite iterators terminate at row_count+1.
        table = pa.table(_normalize_attribute_columns(attributes, row_count))
    else:
        raise TypeError(
            'attributes must be a pyarrow Table, mapping of columns, or None'
        )
    if 'geometry' in table.column_names:
        raise GeometryError("attributes must not contain the reserved 'geometry' column")
    if table.num_rows != row_count:
        raise GeometryError(
            f'attributes length {table.num_rows} does not match geometry length {row_count}'
        )
    return table


def to_geoparquet(
    values: Geometry | GeometryArray,
    path: str | os.PathLike[str],
    *,
    attributes: PyArrowTable | Mapping[str, Any] | None = None,
    crs: CrsInput | None = None,
    encoding: Literal['wkb', 'native'] = 'wkb',
    **kwargs: Any,
) -> None:
    """Write a geometry feature table to GeoParquet.

    Parameters
    ----------
    values : Geometry or GeometryArray
        Geometry rows. A scalar is stored as a single-row table.
    path : path-like
        Output Parquet path.
    attributes : pyarrow.Table or mapping, optional
        Aligned non-geometry columns to preserve beside the geometry column.
    crs : str or int, optional
        CRS to attach to metadata-free geometries or validate on framed input.
    encoding : {'wkb', 'native'}, default 'wkb'
        Portable WKB or a separated native layout for homogeneous arrays.
    kwargs : mapping, optional
        Options forwarded to ``pyarrow.parquet.write_table``.

    Returns
    -------
    None

    Raises
    ------
    ModuleNotFoundError
        If pyarrow is not installed.
    GeometryError
        If attributes are misaligned or native encoding is not possible.
    """
    pa = _pyarrow()
    arr = _as_array(values, crs=crs)
    if arr.any_has_m:
        raise GeometryError(
            'geoparquet 1.x does not support M ordinates; use force_2d() or set_m(None)'
        )
    if encoding not in {'wkb', 'native'}:
        raise GeometryError(
            f"unknown GeoParquet encoding {encoding!r}; expected 'wkb' or 'native'"
        )

    arrow = arr.to_arrow(encoding='wkb' if encoding == 'wkb' else 'auto')
    extension_name = arrow.type.extension_name
    parquet_encoding = (
        'WKB'
        if extension_name == 'geoarrow.wkb'
        else extension_name.removeprefix('geoarrow.')
    )
    if encoding == 'native' and parquet_encoding == 'WKB':
        raise GeometryError(
            'native GeoParquet encoding requires one homogeneous geometry type'
        )

    attributes_table = _attributes_table(pa, attributes, len(arr))
    # Native layouts keep the GeoArrow extension type so kind identity
    # (multipoint vs linestring, polygon vs multilinestring) survives Parquet
    # and cannot be silently relabeled by forging geo metadata. WKB stays plain
    # binary storage: kind lives in the WKB payload, and some Parquet readers
    # rewrite geoarrow.wkb extension CRS metadata (EPSG:4326 → OGC:CRS84).
    geometry_column = arrow if parquet_encoding != 'WKB' else arrow.storage
    if attributes_table is None:
        table = pa.table({'geometry': geometry_column})
    else:
        table = attributes_table.add_column(0, 'geometry', geometry_column)
    metadata = dict(table.schema.metadata or {})
    metadata[b'geo'] = json.dumps(
        _build_geo_metadata(arr, parquet_encoding), separators=(',', ':')
    ).encode('utf-8')
    table = table.replace_schema_metadata(metadata)
    _pyarrow_parquet().write_table(table, path, **kwargs)


def _metadata_error(detail: str) -> ParseError:
    error = ParseError(f'malformed GeoParquet geo metadata: {detail}')
    error.format = 'geoparquet'
    return error


def _geometry_type_base(label: str) -> str:
    """Strip ordinate suffixes from a GeoParquet geometry_types label."""
    for suffix in (' ZM', ' Z', ' M'):
        if label.endswith(suffix):
            return label[: -len(suffix)]
    return label


def _geometry_type_axes(label: str) -> tuple[bool, bool]:
    """Return ``(has_z, has_m)`` for a GeoParquet geometry_types label."""
    if label.endswith(' ZM'):
        return True, True
    if label.endswith(' Z'):
        return True, False
    if label.endswith(' M'):
        return False, True
    return False, False


def _require_encoding_geometry_types(
    column_name: str,
    encoding: str,
    geometry_types: Sequence[str],
) -> None:
    """Native encoding declares a single kind; geometry_types must agree."""
    if encoding not in _ENCODING_BASE_KIND:
        return
    expected = _ENCODING_BASE_KIND[encoding]
    for label in geometry_types:
        if _geometry_type_base(label) != expected:
            raise _metadata_error(
                f'column {column_name!r} encoding {encoding!r} is incompatible '
                f'with geometry_types entry {label!r}'
            )


def _require_native_geometry_types_axes(
    column_name: str,
    encoding: str,
    geometry_types: Sequence[str],
    storage_has_z: bool,
    storage_has_m: bool,
) -> None:
    """Reject native declarations whose axes exceed structural storage (F6).

    Native GeoArrow storage has fixed ordinate fields. A ``Point Z`` (or M/ZM)
    inventory entry over fixed XY struct storage is physically impossible.
    WKB is not checked here — axes live in the blob (N6).
    """
    if encoding not in GEOARROW_ENCODINGS:
        return
    for label in geometry_types:
        has_z, has_m = _geometry_type_axes(label)
        if (has_z and not storage_has_z) or (has_m and not storage_has_m):
            raise _metadata_error(
                f'column {column_name!r} geometry_types entry {label!r} exceeds '
                f'structural storage axes (has_z={storage_has_z}, has_m={storage_has_m})'
            )


def _crs_allows_antimeridian_bbox(crs: str | None) -> bool:
    """True when *crs* is geographic (or absent → default CRS84).

    GeoParquet 1.x defaults missing CRS to OGC:CRS84 and adopts RFC 7946
    antimeridian bboxes (west > east is legal for longitude).
    """
    if crs is None:
        return True
    try:
        from gometry import CRS

        return bool(CRS(crs).is_geographic)
    except (TypeError, ValueError, GeometryError):
        # Unknown / non-constructible CRS strings: only the well-known
        # geographic identifiers keep the wrap allowance.
        normalized = crs.strip().upper()
        return normalized in {
            'OGC:CRS84',
            'EPSG:4326',
            'EPSG:4258',
            'EPSG:4269',
            'CRS84',
            'WGS84',
            'WGS 84',
        }


def _validate_bbox(
    column_name: str,
    bbox: object,
    *,
    allow_antimeridian: bool = False,
) -> None:
    """Reject non-finite, wrong-length, or unordered GeoParquet bbox values.

    When *allow_antimeridian* is true (geographic / default CRS84), X may wrap
    (west > east). Y and Z must still be ordered min <= max (D14.2).
    """
    if (
        not isinstance(bbox, list)
        or len(bbox) not in {4, 6}
        or any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            for value in bbox
        )
    ):
        raise _metadata_error(
            f'column {column_name!r} bbox must contain four or six finite numbers'
        )
    values = [float(value) for value in bbox]
    if len(values) == 4:
        xmin, ymin, xmax, ymax = values
        y_ok = ymin <= ymax
        x_ok = allow_antimeridian or xmin <= xmax
        ordered = x_ok and y_ok
    else:
        xmin, ymin, zmin, xmax, ymax, zmax = values
        y_ok = ymin <= ymax
        z_ok = zmin <= zmax
        x_ok = allow_antimeridian or xmin <= xmax
        ordered = x_ok and y_ok and z_ok
    if not ordered:
        raise _metadata_error(
            f'column {column_name!r} bbox min ordinates must not exceed max ordinates'
        )


def _parse_geo_metadata(schema: Any) -> Mapping[str, Any]:
    schema_metadata = schema.metadata
    if schema_metadata is None or b'geo' not in schema_metadata:
        raise _metadata_error('missing geo metadata in GeoParquet file')
    try:
        metadata = json.loads(schema_metadata[b'geo'].decode('utf-8'))
    except (TypeError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise _metadata_error('geo field is not valid UTF-8 JSON') from error
    if not isinstance(metadata, Mapping):
        raise _metadata_error('geo field must contain a JSON object')

    version = metadata.get('version')
    if not isinstance(version, str):
        raise _metadata_error('version is required and must be a string')
    if _VERSION_PATTERN.fullmatch(version) is None:
        raise _metadata_error(f'unsupported GeoParquet version: {version!r}; expected 1.x')
    return metadata


def _validate_column_metadata(
    metadata: Mapping[str, Any],
    column_name: str,
) -> tuple[Mapping[str, Any], str, str | None, float | None]:
    columns = metadata.get('columns')
    if not isinstance(columns, Mapping):
        raise _metadata_error('columns must be an object')
    column_metadata = columns.get(column_name)
    if not isinstance(column_metadata, Mapping):
        raise _metadata_error(f'columns has no object entry for {column_name!r}')

    encoding = column_metadata.get('encoding')
    if not isinstance(encoding, str):
        raise _metadata_error(f'column {column_name!r} encoding is required')
    if encoding not in {'WKB', *GEOARROW_ENCODINGS, 'geometrycollection'}:
        raise _metadata_error(f'unsupported GeoParquet geometry encoding: {encoding!r}')

    geometry_types = column_metadata.get('geometry_types')
    if (
        not isinstance(geometry_types, list)
        or any(not isinstance(value, str) for value in geometry_types)
        or len(set(geometry_types)) != len(geometry_types)
        or any(value not in _GEOMETRY_TYPES for value in geometry_types)
    ):
        raise _metadata_error(
            f'column {column_name!r} geometry_types must be a unique array of supported type names'
        )
    _require_encoding_geometry_types(column_name, encoding, geometry_types)

    orientation = column_metadata.get('orientation')
    if orientation is not None and orientation != 'counterclockwise':
        raise _metadata_error(
            f'column {column_name!r} orientation must be counterclockwise'
        )

    from gometry._lib import _parse_geoparquet_column_frame

    # Resolve CRS before bbox validation: absent CRS defaults to CRS84, and
    # geographic frames allow RFC 7946 antimeridian wrap (west > east).
    # Native parser owns the CRS/epoch/edges frame boundary. Only re-serialize
    # the frame keys — and reject non-finite numbers before dump (Python's
    # json.dumps emits NaN/Infinity by default, which serde rejects).
    frame_payload = {
        key: column_metadata[key]
        for key in ('crs', 'epoch', 'edges')
        if key in column_metadata
    }
    try:
        frame_bytes = json.dumps(
            frame_payload, separators=(',', ':'), allow_nan=False
        ).encode('utf-8')
    except (TypeError, ValueError) as error:
        # ValueError: Out of range float values are not JSON compliant
        raise _metadata_error(
            f"invalid GeoParquet column '{column_name}': epoch must be finite"
            if 'epoch' in frame_payload
            else f"invalid GeoParquet column '{column_name}': {error}"
        ) from error
    crs, epoch = _parse_geoparquet_column_frame(frame_bytes, column_name)
    bbox = column_metadata.get('bbox')
    if bbox is not None:
        _validate_bbox(
            column_name,
            bbox,
            allow_antimeridian=_crs_allows_antimeridian_bbox(crs),
        )
    return column_metadata, encoding, crs, epoch


def _unwrap_dictionary_type(pa: Any, arrow_type: Any) -> Any:
    """Return the value type under zero or more dictionary encodings."""
    current = arrow_type
    while pa.types.is_dictionary(current):
        current = current.value_type
    return current


_ARROW_EXTENSION_NAME_KEY = b'ARROW:extension:name'


def _raw_field_extension_name(field: Any | None) -> str | None:
    """Strictly decode reserved ``ARROW:extension:name`` field metadata."""
    if field is None:
        return None
    metadata = getattr(field, 'metadata', None)
    if not metadata:
        return None
    raw = metadata.get(_ARROW_EXTENSION_NAME_KEY)
    if raw is None:
        raw = metadata.get('ARROW:extension:name')
    if raw is None:
        return None
    if isinstance(raw, bytes):
        try:
            return raw.decode('utf-8')
        except UnicodeDecodeError as error:
            raise _metadata_error(
                'Arrow extension name metadata is not UTF-8'
            ) from error
    if isinstance(raw, str):
        return raw
    raise _metadata_error('Arrow extension name metadata must be text')


def _reconcile_embedded_extension_name(
    arrow_type: Any,
    field: Any | None = None,
) -> str | None:
    """Runtime ExtensionType name and/or raw Field.metadata, reconciled.

    A present field-extension name disagrees with a present runtime extension
    name when they differ — that is always an error (order-independent).
    Either source alone is authoritative for GeoParquet encoding checks.
    """
    type_name = getattr(arrow_type, 'extension_name', None)
    if not isinstance(type_name, str) or not type_name:
        type_name = None
    field_name = _raw_field_extension_name(field)
    if type_name is not None and field_name is not None and type_name != field_name:
        raise _metadata_error(
            f'Arrow extension type {type_name!r} conflicts with '
            f'field metadata extension {field_name!r}'
        )
    return type_name if type_name is not None else field_name


def _is_wkb_physical_type(
    pa: Any, arrow_type: Any, field: Any | None = None
) -> bool:
    """True when *arrow_type* is physical WKB storage (binary family or geoarrow.wkb).

    Dictionary-encoded binary is accepted: PyArrow's ``read_dictionary`` and
    producers may surface WKB as ``dictionary<binary>``. Value type must be the
    binary family (or geoarrow.wkb); indices are not geometry.
    """
    arrow_type = _unwrap_dictionary_type(pa, arrow_type)
    extension_name = _reconcile_embedded_extension_name(arrow_type, field)
    if extension_name is not None:
        return extension_name == 'geoarrow.wkb'
    return (
        pa.types.is_binary(arrow_type)
        or pa.types.is_large_binary(arrow_type)
        or pa.types.is_binary_view(arrow_type)
    )


def _dictionary_decode_column(pa: Any, column: Any) -> Any:
    """Materialize dictionary-encoded chunks to their value type (binary WKB)."""
    if not pa.types.is_dictionary(column.type):
        return column
    chunks = getattr(column, 'chunks', None)
    if chunks is None:
        return column.dictionary_decode()
    decoded = [chunk.dictionary_decode() for chunk in chunks]
    return pa.chunked_array(decoded)


def _require_wkb_physical_column(
    pa: Any, column: Any, column_name: str, field: Any | None = None
) -> None:
    """Reject WKB-declared columns whose physical storage is not binary."""
    if not _is_wkb_physical_type(pa, column.type, field):
        raise _metadata_error(
            f"column {column_name!r} encoding 'WKB' requires Binary, "
            f'LargeBinary, BinaryView, or geoarrow.wkb storage'
            f' (dictionary-encoded binary is allowed)'
        )


def _native_geoarrow_column(
    pa: Any,
    column: Any,
    encoding: str,
    crs: str | None,
    epoch: float | None,
    field: Any | None = None,
) -> Any:
    from gometry._arrow import (
        _extension_type_from_storage,
        _metadata_frame,
        _storage_axes,
    )
    extension_name = f'geoarrow.{encoding}'
    embedded_type = column.type
    # GeoParquet 1.1 does NOT require an embedded Arrow ExtensionType: the
    # declared `encoding` plus exact list depth + coordinate-struct shape pin
    # the kind (resolving multipoint/linestring and polygon/multilinestring).
    # When an extension *is* present — runtime ExtensionType OR raw Field
    # metadata — it must agree with the declaration (P20 / D10), order-
    # independent of whether pyarrow has registered the extension type.
    embedded_extension = _reconcile_embedded_extension_name(embedded_type, field)
    if embedded_extension is not None and embedded_extension != extension_name:
        raise _metadata_error(
            f'native GeoParquet encoding {encoding!r} conflicts with '
            f'embedded Arrow extension {embedded_extension!r}'
        )
    if embedded_extension is None:
        storage_for_depth = embedded_type
        depth = _geoarrow_list_depth(pa, storage_for_depth)
        expected_depth = _NATIVE_ENCODING_DEPTH.get(encoding)
        if depth is None or expected_depth is None or depth != expected_depth:
            raise _metadata_error(
                f'native GeoParquet encoding {encoding!r} does not match '
                f'storage layout (expected list depth {expected_depth}, '
                f'got {depth!r})'
            )
    if hasattr(embedded_type, '__arrow_ext_serialize__'):
        embedded_crs, embedded_epoch = _metadata_frame(
            embedded_type.__arrow_ext_serialize__()
        )
        if embedded_crs != crs or embedded_epoch != epoch:
            raise _metadata_error(
                'geoparquet metadata conflicts with Arrow extension metadata'
            )
    try:
        storage_type = getattr(column.type, 'storage_type', column.type)
        _storage_axes(storage_type)
        extension_type = _extension_type_from_storage(
            pa, extension_name, storage_type, crs, epoch
        )
        chunks = []
        for chunk in column.chunks:
            storage = getattr(chunk, 'storage', chunk)
            chunks.append(pa.ExtensionArray.from_storage(extension_type, storage))
    except (AttributeError, TypeError, ValueError, pa.ArrowInvalid) as error:
        raise _metadata_error(
            f'invalid native GeoParquet {encoding!r} storage layout'
        ) from error
    return pa.chunked_array(chunks, type=extension_type)


def _decode_geometry_column(
    pa: Any,
    column: Any,
    column_metadata: Mapping[str, Any],
    column_name: str,
    encoding: str,
    crs: str | None,
    epoch: float | None,
    field: Any | None = None,
) -> GeometryArray:
    if encoding == 'geometrycollection':
        raise _metadata_error(
            'native GeoArrow geometrycollection read is unsupported; re-encode as WKB'
        )
    if encoding == 'WKB':
        _require_wkb_physical_column(pa, column, column_name, field)
        # Unwrap dictionary<binary> before native import (read_dictionary /
        # dictionary-encoded writers). Generic GeoArrow dict is out of scope.
        source = _dictionary_decode_column(pa, column)
    elif encoding in GEOARROW_ENCODINGS:
        source = _native_geoarrow_column(pa, column, encoding, crs, epoch, field)
    else:
        source = column
    try:
        # Arrow column/chunked inputs always decode as GeometryArray.
        result: GeometryArray = from_arrow(source, crs=crs, epoch=epoch)
    except (AttributeError, TypeError, pa.ArrowInvalid) as error:
        raise _metadata_error(
            f'invalid native GeoParquet {encoding!r} storage layout'
        ) from error

    storage_has_z, storage_has_m = _physical_storage_axes(encoding, source)

    # GeoParquet 1.x has no M ordinate; reject schema-wide from decoded content
    # OR structural native storage (empty/all-null native XYM must not slip).
    if result.any_has_m or storage_has_m:
        raise _metadata_error(
            'geoparquet 1.x does not support M ordinates; use force_2d() or set_m(None)'
        )

    declared = column_metadata['geometry_types']
    # F6: native storage axes are structural — any declared type whose axes
    # exceed the physical fields is impossible (e.g. "Point Z" over XY struct).
    # Must run before the actual ⊆ declared subset test, which a matching base
    # kind (plain "Point") would otherwise mask. WKB skips (N6).
    _require_native_geometry_types_axes(
        column_name, encoding, declared, storage_has_z, storage_has_m
    )
    actual = _geometry_types(result)
    # Empty / all-null columns yield no present-row type labels. For *native*
    # GeoArrow only, axes are structural on the storage type — synthesize
    # inventory from physical storage so declared-vs-physical mismatches (D19)
    # still reject. WKB encodes 3D inside the blob; binary storage makes no
    # XY claim, so leave actual empty and trust the column-level declaration
    # (GeoParquet 1.1 geometry_types inventory; filtered all-null row groups).
    if declared and not actual and encoding in GEOARROW_ENCODINGS:
        axes = _axes_token(storage_has_z, storage_has_m)
        actual = sorted({
            _geometry_type_label(_geometry_type_base(label), axes)
            for label in declared
        })
    # File-level geometry_types is an inventory; a filtered row group may hold a
    # subset. Require actual ⊆ declared when the declaration is nonempty.
    if declared and not set(actual).issubset(declared):
        raise _metadata_error(
            f'column {column_name!r} geometry_types {declared!r} do not cover {actual!r}'
        )
    if encoding in _ENCODING_BASE_KIND:
        expected_kind = _ENCODING_BASE_KIND[encoding]
        # ``actual`` is already the native, sorted unique inventory.  Reading
        # ``geometry_type`` here re-walks every decoded row solely to recreate
        # the same base-kind set.
        actual_kinds = {_geometry_type_base(label) for label in actual}
        if actual_kinds and actual_kinds != {expected_kind}:
            raise _metadata_error(
                f'column {column_name!r} encoding {encoding!r} produced '
                f'geometry kinds {sorted(actual_kinds)!r}'
            )
    # Producer assertion: exteriors CCW / holes CW (RFC 7946 / OGC).
    # Batch-orient once and reject any row that would change.
    if (
        column_metadata.get('orientation') == 'counterclockwise'
        and result.orient_polygons(ccw=True) != result
    ):
        raise _metadata_error(
            f"column {column_name!r} asserts orientation 'counterclockwise' "
            f'but polygonal rings violate that winding'
        )
    return result


_NATIVE_ENCODING_DEPTH = {
    'point': 0,
    'multipoint': 1,
    'linestring': 1,
    'multilinestring': 2,
    'polygon': 2,
    'multipolygon': 3,
}


def _geoarrow_list_depth(pa: Any, arrow_type: Any) -> int | None:
    """List nesting above a coordinate struct, or None when not geoarrow-like."""
    storage = getattr(arrow_type, 'storage_type', arrow_type)
    depth = 0
    current = storage
    while (
        pa.types.is_list(current)
        or pa.types.is_large_list(current)
        or pa.types.is_list_view(current)
    ):
        depth += 1
        current = current.value_type
    if not pa.types.is_struct(current):
        return None
    try:
        names = {current[index].name for index in range(current.num_fields)}
    except (AttributeError, TypeError, ValueError):
        return None
    if 'x' not in names or 'y' not in names:
        return None
    return depth


def _physical_matches_encoding(
    pa: Any, arrow_type: Any, encoding: str, field: Any | None = None
) -> bool:
    """True when *arrow_type* / *field* is the physical storage for *encoding*."""
    if encoding in {'WKB', 'geometrycollection'}:
        return _is_wkb_physical_type(pa, arrow_type, field)
    if encoding not in GEOARROW_ENCODINGS:
        return False
    # Runtime ExtensionType and raw Field.metadata are both binding (P20).
    embedded = _reconcile_embedded_extension_name(arrow_type, field)
    if embedded is not None:
        return embedded == f'geoarrow.{encoding}'
    # No extension present (R09): declared encoding + exact list depth +
    # coordinate-struct shape. Depth is shared across kind pairs, but the
    # *declared* encoding selects which kind the depth maps to.
    depth = _geoarrow_list_depth(pa, arrow_type)
    return depth is not None and depth == _NATIVE_ENCODING_DEPTH[encoding]


def _schema_name_indices(schema_names: Sequence[str]) -> dict[str, list[int]]:
    """Map each schema name to its occurrence indices in one O(schema) pass."""
    indices: dict[str, list[int]] = {}
    for position, name in enumerate(schema_names):
        indices.setdefault(name, []).append(position)
    return indices


def _validate_declared_geometry_columns(
    pa: Any,
    schema: Any,
    metadata: Mapping[str, Any],
) -> frozenset[str]:
    """Validate every geo.columns entry exists once and matches physical encoding.

    Declared geometry columns are only excluded from attributes after this check,
    so a ghost name or a non-geometry physical type cannot silently suppress a
    real attribute column.

    Complexity is O(schema + metadata columns): one name→indices map replaces
    per-declared-column ``count`` / ``index`` scans over ``schema.names``.
    """
    columns = metadata.get('columns')
    if not isinstance(columns, Mapping):
        raise _metadata_error('columns must be an object')

    schema_names = list(schema.names)
    name_indices = _schema_name_indices(schema_names)
    validated: list[str] = []
    for name, column_meta in columns.items():
        if not isinstance(name, str):
            raise _metadata_error('columns keys must be strings')
        occurrences = name_indices.get(name)
        if not occurrences:
            raise _metadata_error(
                f'geometry column {name!r} declared in geo metadata is not '
                f'present in the schema'
            )
        if len(occurrences) != 1:
            raise _metadata_error(
                f'geometry column {name!r} must appear exactly once in the schema'
            )
        if not isinstance(column_meta, Mapping):
            raise _metadata_error(f'columns has no object entry for {name!r}')
        encoding = column_meta.get('encoding')
        if not isinstance(encoding, str):
            raise _metadata_error(f'column {name!r} encoding is required')
        if encoding not in {'WKB', *GEOARROW_ENCODINGS, 'geometrycollection'}:
            raise _metadata_error(
                f'unsupported GeoParquet geometry encoding: {encoding!r}'
            )
        field = schema.field(occurrences[0])
        if not _physical_matches_encoding(pa, field.type, encoding, field):
            raise _metadata_error(
                f'column {name!r} encoding {encoding!r} does not match '
                f'physical storage'
            )
        validated.append(name)
    return frozenset(validated)


def _iter_to_list(values: Iterable[object]) -> list[object]:
    """Materialize an iterable without pre-sizing from untrusted ``__len__``.

    CPython ``list(it)`` uses ``__length_hint__``/``__len__`` to reserve; a
    lying ``sys.maxsize`` hint raises ``MemoryError`` before the first yield
    (m08). Append-one-by-one ignores the hint.
    """
    # Deliberate: never ``list(values)`` (that pre-sizes from ``__len__``).
    out: list[object] = []
    for item in values:
        out.append(item)  # noqa: PERF402
    return out


def _materialize_row_groups(row_groups: Iterable[int] | None) -> list[int] | None:
    """Materialize a one-shot row-group iterable before validate + use."""
    if row_groups is None:
        return None
    if isinstance(row_groups, (str, bytes)):
        raise TypeError('row_groups must be a sequence of non-negative integers')
    return _iter_to_list(row_groups)  # type: ignore[return-value]


def _validate_attribute_columns(
    schema: Any,
    metadata: Mapping[str, Any],
    columns: Iterable[str] | None,
) -> list[str]:
    pa = _pyarrow()
    geometry_columns = _validate_declared_geometry_columns(pa, schema, metadata)
    schema_names = list(schema.names)
    if columns is None:
        return [name for name in schema_names if name not in geometry_columns]
    if isinstance(columns, (str, bytes)):
        raise TypeError('columns must be a sequence of attribute column names or None')
    # Online validation against the finite schema: unknown or duplicate names
    # reject immediately so infinite iterators (e.g. itertools.repeat) terminate.
    schema_name_set = set(schema_names)
    seen: set[str] = set()
    result: list[str] = []
    for name in columns:
        if not isinstance(name, str):
            raise TypeError(
                'columns must be a sequence of attribute column names or None'
            )
        if name in seen:
            raise GeometryError('columns must not contain duplicate names')
        if name in geometry_columns:
            raise GeometryError(f'columns must not include geometry column {name!r}')
        if name not in schema_name_set:
            raise GeometryError(
                f'attribute column {name!r} not found in GeoParquet file'
            )
        seen.add(name)
        result.append(name)
    return result


def _attribute_result(table: Any) -> Any:
    metadata = dict(table.schema.metadata or {})
    metadata.pop(b'geo', None)
    return table.replace_schema_metadata(metadata or None)


def from_geoparquet(
    path: str | os.PathLike[str],
    *,
    geometry: str | None = None,
    columns: Iterable[str] | None = None,
    filters: Any = None,
    row_groups: Iterable[int] | None = None,
    filesystem: Any = None,
    **kwargs: Any,
) -> tuple[GeometryArray, PyArrowTable]:
    """Read a GeoParquet geometry column and its aligned attributes.

    Parameters
    ----------
    path : path-like
        GeoParquet file to read.
    geometry : str, optional
        Geometry column to decode; the primary column is used by default.
    columns : iterable of str, optional
        Attribute columns to preserve; all non-geometry columns by default.
        One-shot iterables (e.g. generators) are materialized once.
    filters : object, optional
        Predicate filters forwarded to ``pyarrow.parquet.read_table``.
    row_groups : iterable of int, optional
        Row groups to read from a single file. Cannot be combined with filters.
        One-shot iterables (e.g. generators) are materialized once.
    filesystem : object, optional
        PyArrow filesystem used to open the source.
    kwargs : mapping, optional
        Curated options forwarded to ``pyarrow.parquet.read_table``.

    Returns
    -------
    tuple of GeometryArray and pyarrow.Table
        Decoded geometry rows and aligned non-geometry attribute columns.

    Raises
    ------
    ModuleNotFoundError
        If pyarrow is not installed.
    ParseError
        If GeoParquet metadata is missing, malformed, or unsupported.
    """
    pa = _pyarrow()
    pq = _pyarrow_parquet()
    unknown_options = sorted(set(kwargs) - _READ_TABLE_OPTIONS)
    if unknown_options:
        raise TypeError(f'unsupported GeoParquet read options: {unknown_options!r}')

    # Materialize row_groups early (needed to choose ParquetFile vs schema-only
    # open). columns= is validated online against the finite schema later so
    # infinite iterators terminate on unknown/duplicate names.
    selected_row_groups = _materialize_row_groups(row_groups)

    parquet_file = None
    if selected_row_groups is not None:
        if any(
            isinstance(group, bool) or not isinstance(group, int) or group < 0
            for group in selected_row_groups
        ):
            raise TypeError('row_groups must be a sequence of non-negative integers')
        if filters is not None:
            raise GeometryError('row_groups cannot be combined with filters')
        row_group_unknown = sorted(
            set(kwargs) - _ROW_GROUP_OPTIONS - _PARQUET_FILE_OPTIONS
        )
        if row_group_unknown:
            raise TypeError(
                f'row_groups do not support read options: {row_group_unknown!r}'
            )
        parquet_file_options = {
            key: value for key, value in kwargs.items() if key in _PARQUET_FILE_OPTIONS
        }
        parquet_file = pq.ParquetFile(
            path, filesystem=filesystem, **parquet_file_options
        )
        schema = parquet_file.schema_arrow
    else:
        schema_options = {
            key: kwargs[key]
            for key in ('memory_map', 'decryption_properties')
            if key in kwargs
        }
        schema = pq.read_schema(path, filesystem=filesystem, **schema_options)

    geo_metadata = _parse_geo_metadata(schema)
    primary = geo_metadata.get('primary_column')
    if not isinstance(primary, str):
        raise _metadata_error('primary_column is required and must be a string')
    geometry_metadata = geo_metadata.get('columns')
    if not isinstance(geometry_metadata, Mapping) or primary not in geometry_metadata:
        raise _metadata_error('primary_column must name an entry in columns')
    if primary not in schema.names:
        raise _metadata_error(
            f'primary geometry column {primary!r} not found in GeoParquet file'
        )
    selected_geometry = primary if geometry is None else geometry
    if not isinstance(selected_geometry, str):
        raise TypeError('geometry must be a column name or None')
    if selected_geometry not in schema.names:
        raise _metadata_error(
            f'geometry column {selected_geometry!r} not found in GeoParquet file'
        )
    column_metadata, encoding, crs, epoch = _validate_column_metadata(
        geo_metadata, selected_geometry
    )
    # Schema is known: validate columns online (bounded by finite attribute set).
    # Selected geometry is already a member of the declared geometry columns.
    attribute_columns = _validate_attribute_columns(schema, geo_metadata, columns)

    projected_columns = [selected_geometry, *attribute_columns]
    if parquet_file is not None:
        row_group_read_options = {
            key: value for key, value in kwargs.items() if key in _ROW_GROUP_OPTIONS
        }
        table = parquet_file.read_row_groups(
            selected_row_groups or [],
            columns=projected_columns,
            **row_group_read_options,
        )
    else:
        table = pq.read_table(
            path,
            columns=projected_columns,
            filters=filters,
            filesystem=filesystem,
            **kwargs,
        )
    geometry_array = _decode_geometry_column(
        pa,
        table[selected_geometry],
        column_metadata,
        selected_geometry,
        encoding,
        crs,
        epoch,
        field=table.schema.field(selected_geometry),
    )
    attributes = _attribute_result(table.select(attribute_columns))
    return geometry_array, attributes
