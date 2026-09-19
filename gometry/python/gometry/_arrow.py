"""GeoArrow extension types for ``pyarrow``.

The Rust extension exports and imports GeoArrow columns natively; this module
owns the ``pyarrow`` side of that contract: the ``geoarrow.*`` extension types
it registers on first use, their CRS and epoch metadata, and their pickling.
It is internal plumbing for ``GeometryArray.to_arrow()`` and ``from_arrow``,
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


def _coordinate_type(pa: Any, has_z: bool, has_m: bool) -> Any:
    fields = [pa.field('x', pa.float64()), pa.field('y', pa.float64())]
    if has_z:
        fields.append(pa.field('z', pa.float64()))
    if has_m:
        fields.append(pa.field('m', pa.float64()))
    return pa.struct(fields)


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
    """The PROJJSON form of a stored CRS definition — GeoArrow's recommended
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

    The CRS definition (as an inline PROJJSON object) and coordinate epoch are serialized into
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
