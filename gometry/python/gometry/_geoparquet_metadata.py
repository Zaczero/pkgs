"""GeoParquet metadata, frame reconciliation and declared layout constraints."""

from __future__ import annotations

import base64
import binascii
import json
import math
import re
from collections.abc import Mapping, Sequence
from typing import Any

from gometry._lib import CRS, GeometryError, ParseError

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
    rf'^[12]\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)'
    rf'(?:-{_SEMVER_IDENTIFIER}(?:\.{_SEMVER_IDENTIFIER})*)?'
    r'(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$'
)


def _embedded_frame_conflicts(
    embedded_crs: str | None,
    embedded_epoch: float | None,
    declared_crs: str | None,
    declared_epoch: float | None,
    *,
    declared_epoch_present: bool = True,
) -> bool:
    """Whether declared frame metadata contradicts an embedded frame.

    Missing embedded fields are no opinion. CRS labels that name the same
    coordinate frame, including axis-order aliases, are not conflicts.
    """
    if embedded_crs is not None and (
        declared_crs is None
        or not CRS(embedded_crs).same_as(declared_crs, mode='ignore_axis_order')
    ):
        return True
    return (
        embedded_epoch is not None
        and declared_epoch_present
        and (declared_epoch is None or embedded_epoch != declared_epoch)
    )


def _metadata_error(detail: str) -> ParseError:
    error = ParseError(f'malformed GeoParquet geo metadata: {detail}')
    error.format = 'geoparquet'
    error.category = 'metadata'
    return error


def _stored_arrow_schema(pa: Any, serialized: bytes | None) -> Any | None:
    if serialized is None:
        return None
    try:
        return pa.ipc.read_schema(
            pa.BufferReader(base64.b64decode(serialized, validate=True))
        )
    except (binascii.Error, pa.ArrowException, ValueError) as error:
        raise _metadata_error('stored Arrow schema is invalid') from error


def _layout_error(detail: str) -> GeometryError:
    error = GeometryError(detail)
    error.category = 'layout'
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
    """Reject native declarations whose axes exceed structural storage."""
    if encoding not in GEOARROW_ENCODINGS:
        return
    for label in geometry_types:
        has_z, has_m = _geometry_type_axes(label)
        if (has_z and not storage_has_z) or (has_m and not storage_has_m):
            raise _layout_error(
                f'column {column_name!r} geometry_types entry {label!r} exceeds '
                f'structural storage axes (has_z={storage_has_z}, has_m={storage_has_m})'
            )


def _crs_allows_antimeridian_bbox(crs: str | None) -> bool:
    """Whether column bounds may use west > east for a longitude seam crossing."""
    return crs is None or bool(CRS(crs).is_geographic)


def _validate_bbox(
    column_name: str,
    bbox: object,
    *,
    allow_antimeridian: bool = False,
    allow_m: bool = False,
) -> None:
    """Reject non-finite, wrong-length, or unordered GeoParquet bbox values."""
    if (
        not isinstance(bbox, list)
        or len(bbox) not in ({4, 6, 8} if allow_m else {4, 6})
        or any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            for value in bbox
        )
    ):
        lengths = 'four, six, or eight' if allow_m else 'four or six'
        raise _metadata_error(
            f'column {column_name!r} bbox must contain {lengths} finite numbers'
        )
    values = [float(value) for value in bbox]
    dimensions = len(values) // 2
    ordered = (allow_antimeridian or values[0] <= values[dimensions]) and all(
        values[axis] <= values[dimensions + axis] for axis in range(1, dimensions)
    )
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
        raise _metadata_error(
            f'unsupported GeoParquet version: {version!r}; expected 1.x or 2.x'
        )
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
    allowed_types = _GEOMETRY_TYPES
    if metadata.get('version', '').startswith('2.'):
        allowed_types |= {
            _geometry_type_base(kind) + suffix
            for kind in _GEOMETRY_TYPES
            for suffix in (' M', ' ZM')
        }
    if (
        not isinstance(geometry_types, list)
        or any(not isinstance(value, str) for value in geometry_types)
        or len(set(geometry_types)) != len(geometry_types)
        or any(value not in allowed_types for value in geometry_types)
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
    from gometry._arrow import _CRS_DEFINITION_KEY
    from gometry._lib import _parse_geoparquet_column_frame

    frame_payload = {
        key: column_metadata[key]
        for key in ('crs', 'epoch', 'edges', _CRS_DEFINITION_KEY)
        if key in column_metadata
    }
    frame = _parse_geoparquet_column_frame(frame_payload, column_name)
    crs, epoch = frame.crs, frame.epoch
    bbox = column_metadata.get('bbox')
    if bbox is not None:
        _validate_bbox(
            column_name,
            bbox,
            allow_antimeridian=_crs_allows_antimeridian_bbox(crs),
            allow_m=metadata.get('version', '').startswith('2.'),
        )
    return column_metadata, encoding, crs, epoch
