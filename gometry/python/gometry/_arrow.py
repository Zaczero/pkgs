"""GeoArrow bridge between gometry's columnar storage and ``pyarrow``.

The Rust extension calls the ``to_arrow_*`` builders below to wrap raw coordinate
and offset buffers (already laid out in the GeoArrow memory model) as typed
``pyarrow`` ``ExtensionArray`` values, registering the ``geoarrow.*`` extension
types on first use. They are internal plumbing for ``GeometryArray.to_arrow()``,
not part of the public API.
"""

from __future__ import annotations

import json
import math
import sys
from functools import lru_cache
from typing import Any

from gometry._lib import CRS, ParseError
from gometry._optional import missing_optional_dependency

# Runtime import (not TYPE_CHECKING): the raw-buffer annotations below must stay
# resolvable for `get_type_hints`. Imported from the canonical source rather
# than re-exported through `gometry._types` (Buffer is internal there).
if sys.version_info >= (3, 12):
    from collections.abc import Buffer
else:  # Python 3.11: Buffer predates collections.abc
    from typing_extensions import Buffer

TYPE_CHECKING = False

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = sys.modules.get('pyarrow')
    if pa is None:

        class _PyArrowTypes:
            Array = Any
            ExtensionArray = Any

        pa = _PyArrowTypes()

GEOARROW_WKB = 'geoarrow.wkb'
GEOARROW_POINT = 'geoarrow.point'
GEOARROW_MULTIPOINT = 'geoarrow.multipoint'
GEOARROW_LINESTRING = 'geoarrow.linestring'
GEOARROW_MULTILINESTRING = 'geoarrow.multilinestring'
GEOARROW_POLYGON = 'geoarrow.polygon'
GEOARROW_MULTIPOLYGON = 'geoarrow.multipolygon'

#: How many ``list_`` wrappers sit between the extension type and its
#: coordinate struct (``None`` marks the WKB binary layout).
_STORAGE_DEPTH: dict[str, int | None] = {
    GEOARROW_WKB: None,
    GEOARROW_POINT: 0,
    GEOARROW_MULTIPOINT: 1,
    GEOARROW_LINESTRING: 1,
    GEOARROW_MULTILINESTRING: 2,
    GEOARROW_POLYGON: 2,
    GEOARROW_MULTIPOLYGON: 3,
}
_extension_types_registered = False


def _offset_length(offsets: Buffer) -> int:
    """Row count from an i32 offset buffer (``n + 1`` terminals → ``n`` rows).

    Uses ``nbytes`` so both raw ``bytes`` and PEP-3118 int32 views
    (``_Int32Buffer``) report the same length — ``len(memoryview)`` is element
    count on typed views, not bytes.
    """
    return memoryview(offsets).nbytes // 4 - 1


def to_arrow_wkb(
    offsets: Buffer,
    data: Buffer,
    crs: str | None,
    epoch: float | None,
    missing: Buffer | None = None,
) -> pa.Array:
    """Wrap WKB ``offsets``/``data`` buffers as a ``geoarrow.wkb`` array."""
    pa = _pyarrow()
    storage = pa.Array.from_buffers(
        pa.binary(),
        _offset_length(offsets),
        [_validity_buffer(pa, missing), pa.py_buffer(offsets), pa.py_buffer(data)],
    )
    return pa.ExtensionArray.from_storage(
        _extension_type(pa, GEOARROW_WKB, crs, epoch), storage
    )


def to_arrow_point(
    xs: Buffer,
    ys: Buffer,
    z: Buffer | None,
    m: Buffer | None,
    crs: str | None,
    epoch: float | None,
    missing: Buffer | None = None,
) -> pa.Array:
    """Wrap interleaved coordinate buffers as a ``geoarrow.point`` array."""
    pa = _pyarrow()
    storage = _coordinate_values(pa, xs, ys, z, m, missing=missing)
    return pa.ExtensionArray.from_storage(
        _extension_type(pa, GEOARROW_POINT, crs, epoch, z is not None, m is not None),
        storage,
    )


def to_arrow_linestring(
    offsets: Buffer,
    xs: Buffer,
    ys: Buffer,
    z: Buffer | None,
    m: Buffer | None,
    crs: str | None,
    epoch: float | None,
    missing: Buffer | None = None,
) -> pa.Array:
    """Wrap one offset level over coordinates as a ``geoarrow.linestring`` array."""
    return _to_arrow_list(
        offsets, xs, ys, z, m, GEOARROW_LINESTRING, crs, epoch, missing=missing
    )


def to_arrow_multipoint(
    offsets: Buffer,
    xs: Buffer,
    ys: Buffer,
    z: Buffer | None,
    m: Buffer | None,
    crs: str | None,
    epoch: float | None,
    missing: Buffer | None = None,
) -> pa.Array:
    """Wrap one offset level over coordinates as a ``geoarrow.multipoint`` array."""
    return _to_arrow_list(
        offsets, xs, ys, z, m, GEOARROW_MULTIPOINT, crs, epoch, missing=missing
    )


def to_arrow_multilinestring(
    geometry_offsets: Buffer,
    line_offsets: Buffer,
    xs: Buffer,
    ys: Buffer,
    z: Buffer | None,
    m: Buffer | None,
    crs: str | None,
    epoch: float | None,
    missing: Buffer | None = None,
) -> pa.Array:
    """Wrap two offset levels over coordinates as a ``geoarrow.multilinestring`` array."""
    return _to_arrow_nested_list(
        geometry_offsets,
        line_offsets,
        xs,
        ys,
        z,
        m,
        GEOARROW_MULTILINESTRING,
        crs,
        epoch,
        missing=missing,
    )


def to_arrow_polygon(
    polygon_offsets: Buffer,
    ring_offsets: Buffer,
    xs: Buffer,
    ys: Buffer,
    z: Buffer | None,
    m: Buffer | None,
    crs: str | None,
    epoch: float | None,
    missing: Buffer | None = None,
) -> pa.Array:
    """Wrap polygon/ring offset levels over coordinates as a ``geoarrow.polygon`` array."""
    return _to_arrow_nested_list(
        polygon_offsets,
        ring_offsets,
        xs,
        ys,
        z,
        m,
        GEOARROW_POLYGON,
        crs,
        epoch,
        missing=missing,
    )


def to_arrow_multipolygon(
    multipolygon_offsets: Buffer,
    polygon_offsets: Buffer,
    ring_offsets: Buffer,
    xs: Buffer,
    ys: Buffer,
    z: Buffer | None,
    m: Buffer | None,
    crs: str | None,
    epoch: float | None,
    missing: Buffer | None = None,
) -> pa.Array:
    """Wrap multipolygon/polygon/ring offset levels as a ``geoarrow.multipolygon`` array."""
    pa = _pyarrow()
    ring_count = _offset_length(ring_offsets)
    polygon_count = _offset_length(polygon_offsets)
    rings = _coordinate_list(pa, ring_offsets, xs, ys, z, m, ring_count)
    coordinate_type = _coordinate_type(pa, z is not None, m is not None)
    polygon_storage_type = pa.list_(pa.list_(coordinate_type))
    polygons = pa.Array.from_buffers(
        polygon_storage_type,
        polygon_count,
        [None, pa.py_buffer(polygon_offsets)],
        children=[rings],
    )
    storage_type = pa.list_(polygon_storage_type)
    storage = pa.Array.from_buffers(
        storage_type,
        _offset_length(multipolygon_offsets),
        [_validity_buffer(pa, missing), pa.py_buffer(multipolygon_offsets)],
        children=[polygons],
    )
    return pa.ExtensionArray.from_storage(
        _extension_type(
            pa, GEOARROW_MULTIPOLYGON, crs, epoch, z is not None, m is not None
        ),
        storage,
    )


def _to_arrow_list(
    offsets: Buffer,
    xs: Buffer,
    ys: Buffer,
    z: Buffer | None,
    m: Buffer | None,
    extension_name: str,
    crs: str | None,
    epoch: float | None,
    missing: Buffer | None = None,
) -> pa.Array:
    pa = _pyarrow()
    storage = _coordinate_list(
        pa, offsets, xs, ys, z, m, _offset_length(offsets), missing=missing
    )
    return pa.ExtensionArray.from_storage(
        _extension_type(pa, extension_name, crs, epoch, z is not None, m is not None),
        storage,
    )


def _to_arrow_nested_list(
    geometry_offsets: Buffer,
    item_offsets: Buffer,
    xs: Buffer,
    ys: Buffer,
    z: Buffer | None,
    m: Buffer | None,
    extension_name: str,
    crs: str | None,
    epoch: float | None,
    missing: Buffer | None = None,
) -> pa.Array:
    pa = _pyarrow()
    # Offsets may be raw bytes or a PEP-3118 `Int32Buffer` from packed storage.
    item_count = _offset_length(item_offsets)
    items = _coordinate_list(pa, item_offsets, xs, ys, z, m, item_count)
    coordinate_type = _coordinate_type(pa, z is not None, m is not None)
    storage_type = pa.list_(pa.list_(coordinate_type))
    storage = pa.Array.from_buffers(
        storage_type,
        _offset_length(geometry_offsets),
        [_validity_buffer(pa, missing), pa.py_buffer(geometry_offsets)],
        children=[items],
    )
    return pa.ExtensionArray.from_storage(
        _extension_type(pa, extension_name, crs, epoch, z is not None, m is not None),
        storage,
    )


def _coordinate_list(
    pa: Any,
    offsets: Buffer,
    xs: Buffer,
    ys: Buffer,
    z: Buffer | None,
    m: Buffer | None,
    length: int,
    missing: Buffer | None = None,
) -> pa.Array:
    values = _coordinate_values(pa, xs, ys, z, m)
    storage_type = pa.list_(_coordinate_type(pa, z is not None, m is not None))
    return pa.Array.from_buffers(
        storage_type,
        length,
        [_validity_buffer(pa, missing), pa.py_buffer(offsets)],
        children=[values],
    )


def _coordinate_values(
    pa: Any,
    xs: Buffer,
    ys: Buffer,
    z: Buffer | None,
    m: Buffer | None,
    missing: Buffer | None = None,
) -> pa.Array:
    # `xs` is bytes OR any PEP-3118 exporter (zero-copy column objects);
    # the byte length comes from the buffer view either way.
    xs_buffer = pa.py_buffer(xs)
    value_count = xs_buffer.size // 8
    arrays = [
        pa.Array.from_buffers(pa.float64(), value_count, [None, xs_buffer]),
        pa.Array.from_buffers(pa.float64(), value_count, [None, pa.py_buffer(ys)]),
    ]
    names = ['x', 'y']
    if z is not None:
        arrays.append(
            pa.Array.from_buffers(pa.float64(), value_count, [None, pa.py_buffer(z)])
        )
        names.append('z')
    if m is not None:
        arrays.append(
            pa.Array.from_buffers(pa.float64(), value_count, [None, pa.py_buffer(m)])
        )
        names.append('m')
    validity = _validity_buffer(pa, missing)
    if validity is None:
        return pa.StructArray.from_arrays(arrays, names=names)
    struct_type = pa.struct([pa.field(name, pa.float64()) for name in names])
    return pa.Array.from_buffers(struct_type, value_count, [validity], children=arrays)


def _coordinate_type(pa: Any, has_z: bool, has_m: bool) -> Any:
    fields = [pa.field('x', pa.float64()), pa.field('y', pa.float64())]
    if has_z:
        fields.append(pa.field('z', pa.float64()))
    if has_m:
        fields.append(pa.field('m', pa.float64()))
    return pa.struct(fields)


def _storage_axes(storage_type: Any) -> tuple[bool, bool]:
    coordinate_type = storage_type
    while hasattr(coordinate_type, 'value_type'):
        coordinate_type = coordinate_type.value_type
    names = [coordinate_type[index].name for index in range(coordinate_type.num_fields)]
    return 'z' in names, 'm' in names


def _validity_buffer(pa: Any, missing: Buffer | None) -> Any:
    """LSB-packed Arrow validity bitmap from a byte-per-row missing mask
    (``1`` = missing), or ``None`` for a dense array.
    """
    if missing is None:
        return None
    import numpy as np

    mask = np.frombuffer(memoryview(missing), dtype=np.uint8)
    return pa.py_buffer(np.packbits(mask == 0, bitorder='little').tobytes())


def apply_missing(array: pa.ExtensionArray, validity: Buffer) -> pa.ExtensionArray:
    """Rebuild an extension array with a geometry-level validity bitmap.

    The dense storage is reused as-is (missing slots hold placeholder rows);
    only the OUTER validity buffer changes — binary (WKB), list (line/polygon
    families), and struct (point) storages each rebuild their top level.
    """
    pa = _pyarrow()
    storage = array.storage
    validity = pa.py_buffer(validity)
    storage_type = storage.type
    if pa.types.is_binary(storage_type):
        buffers = storage.buffers()
        rebuilt = pa.Array.from_buffers(
            storage_type, len(storage), [validity, buffers[1], buffers[2]]
        )
    elif pa.types.is_list(storage_type):
        rebuilt = pa.Array.from_buffers(
            storage_type,
            len(storage),
            [validity, storage.buffers()[1]],
            children=[storage.values],
        )
    elif pa.types.is_struct(storage_type):
        children = [storage.field(index) for index in range(storage_type.num_fields)]
        rebuilt = pa.Array.from_buffers(
            storage_type, len(storage), [validity], children=children
        )
    else:  # pragma: no cover - the exporters emit exactly these layouts
        raise TypeError(f'unsupported GeoArrow storage layout: {storage_type}')
    return pa.ExtensionArray.from_storage(array.type, rebuilt)


def missing_mask(array: pa.Array) -> bytes:
    """Geometry-level null mask of an (extension) array as one byte per row."""
    import numpy as np

    _pyarrow()
    nulls = array.is_null().to_numpy(zero_copy_only=False)
    return np.asarray(nulls, dtype=np.uint8).tobytes()


def strip_missing(array: pa.Array) -> pa.Array:
    """The array without its null rows (dense import lane input)."""
    pa = _pyarrow()
    if hasattr(array, 'storage'):
        return pa.ExtensionArray.from_storage(array.type, array.storage.drop_null())
    return array.drop_null()


def _pyarrow() -> Any:
    try:
        import pyarrow as pa
    except ModuleNotFoundError as error:
        raise missing_optional_dependency(
            error, 'pyarrow', "arrow interop requires pyarrow; install 'gometry[arrow]'"
        ) from error
    _register_extension_types(pa)
    return pa


def _register_extension_types(pa: Any) -> None:
    global _extension_types_registered
    if _extension_types_registered:
        return
    arrow_key_error = getattr(pa, 'ArrowKeyError', KeyError)
    for extension_name in _STORAGE_DEPTH:
        try:
            pa.register_extension_type(_extension_type(pa, extension_name, None, None))
        except (arrow_key_error, ValueError):
            pass
    _extension_types_registered = True


def _register_extension_types_if_available() -> None:
    # Invoked by name from the Rust capsule exporter (src/py/arrow_c.rs) via
    # `call_method0` so `pa.array(gometry_obj)` reconstructs registered GeoArrow
    # extension arrays — pyright can't see the dynamic Rust call site.
    pa = sys.modules.get('pyarrow')
    if pa is not None:
        _register_extension_types(pa)


def _crs_projjson(crs: str | CRS) -> dict[str, Any]:
    """The PROJJSON form of a canonical CRS string — GeoArrow's recommended
    in-memory CRS encoding, and the one GeoParquet 1.1 requires when columns
    land in a lakehouse.
    """
    return CRS(crs).to_projjson_dict()


def _geoarrow_parse_error(message: str) -> Exception:
    """A ``ParseError`` tagged ``format='geoarrow'`` — the Python GeoArrow
    metadata boundary classifies malformed extension metadata exactly as the
    Rust reader does (``src/py/arrow/metadata.rs`` ``geoarrow_parse_error``).
    """
    error = ParseError(message)
    error.format = 'geoarrow'
    return error


def _metadata_frame(serialized: bytes) -> tuple[str | None, float | None]:
    """Parse GeoArrow extension metadata via the native boundary parser."""
    from gometry._lib import _parse_geoarrow_extension_metadata

    return _parse_geoarrow_extension_metadata(serialized)


@lru_cache(maxsize=256)
def _extension_type(
    pa: Any,
    extension_name: str,
    crs: str | None,
    epoch: float | None,
    has_z: bool = False,
    has_m: bool = False,
) -> Any:
    """One GeoArrow extension type, parametrized by layout and frame metadata.

    The CRS (authority:code string) and coordinate epoch are serialized into
    the extension metadata, so both survive a gometry Arrow round trip. The
    ``epoch`` key is a gometry extension and may be ignored by other readers.
    """
    depth = _STORAGE_DEPTH[extension_name]
    if depth is None:
        storage_type = pa.binary()
    else:
        storage_type = _coordinate_type(pa, has_z, has_m)
        for _ in range(depth):
            storage_type = pa.list_(storage_type)
    return _extension_type_from_storage(pa, extension_name, storage_type, crs, epoch)


def _restore_extension_type(
    extension_name: str,
    storage_type: Any,
    crs: str | None,
    epoch: float | None,
) -> Any:
    """Restore a pickled GeoArrow extension type without pickling ``pyarrow``."""
    return _extension_type_from_storage(
        _pyarrow(), extension_name, storage_type, crs, epoch
    )


@lru_cache(maxsize=256)
def _extension_type_from_storage(
    pa: Any,
    extension_name: str,
    storage_type: Any,
    crs: str | None,
    epoch: float | None,
) -> Any:
    """GeoArrow extension semantics over an exact producer storage type."""
    metadata: dict[str, Any] = {}
    if crs is not None:
        metadata = {'crs': _crs_projjson(crs), 'crs_type': 'projjson'}
    if epoch is not None:
        if not math.isfinite(epoch):
            raise _geoarrow_parse_error(
                'invalid GeoArrow extension metadata: epoch must be finite'
            )
        if crs is None:
            raise _geoarrow_parse_error(
                'invalid GeoArrow extension metadata: a coordinate epoch requires '
                'a CRS; attach one with crs= (or set_crs(...)) before tagging an epoch'
            )
        metadata['epoch'] = epoch

    class GeoArrowType(pa.ExtensionType):
        def __init__(self) -> None:
            super().__init__(storage_type, extension_name)

        def __arrow_ext_serialize__(self) -> bytes:
            return json.dumps(metadata, separators=(',', ':')).encode()

        @classmethod
        def __arrow_ext_deserialize__(
            cls,
            storage_type: Any,
            serialized: bytes,
        ) -> GeoArrowType:
            crs, epoch = _metadata_frame(serialized)
            return _extension_type_from_storage(
                pa, extension_name, storage_type, crs, epoch
            )

        def __reduce__(
            self,
        ) -> tuple[Any, tuple[str, Any, str | None, float | None]]:
            return _restore_extension_type, (
                extension_name,
                storage_type,
                crs,
                epoch,
            )

    return GeoArrowType()
