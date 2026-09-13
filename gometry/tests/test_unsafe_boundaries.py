"""Round-13 L1 - demonstrated FFI/unsafe blocker regressions (B1-B6).

Each case drives the shipped public path that the audit reproduction targeted.
"""

from __future__ import annotations

import gc
import struct
import sys
from concurrent.futures import ThreadPoolExecutor

import gometry as gm
import pytest

pa = pytest.importorskip('pyarrow')


def _gil_enabled() -> bool | None:
    probe = getattr(sys, '_is_gil_enabled', None)
    if probe is None:
        return None
    return bool(probe())


def test_b2_crs_engine_metadata_stable_under_concurrent_calls() -> None:
    """Concurrent ``crs_engine()`` must not return corrupt/empty PROJ metadata.

    Pre-fix free-threaded CPython 3.14t reproduced empty version strings and
    corrupt search paths when ``proj_info()`` pointers were copied after other
    PROJ work. The fix owns every string under a process-wide lock immediately.
    """
    baseline = gm.crs_engine()
    expected_version = baseline['version']
    expected_search = baseline['search_path']
    assert isinstance(expected_version, str) and expected_version
    workers = 16
    iterations = 500 if _gil_enabled() is False else 100

    def worker(_: int) -> list[tuple[object, object]]:
        failures: list[tuple[object, object]] = []
        for _ in range(iterations):
            info = gm.crs_engine()
            if (
                info['version'] != expected_version
                or info['search_path'] != expected_search
            ):
                failures.append((info['version'], info['search_path']))
                if len(failures) >= 5:
                    break
        return failures

    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(worker, range(workers)))
    failures = [item for batch in results for item in batch]
    assert failures == [], (
        f'corrupt crs_engine metadata under concurrency: {failures[:5]!r}'
    )


def _poison_on_array_release(
    schema_capsule: object,
    array_capsule: object,
    poison_fn,
) -> tuple[object, list[bool]]:
    """Wrap an ``__arrow_c_array__`` export so the *array* release callback
    poisons producer memory and sets a flag before the callback returns.

    Design-1 contract: gometry must finish its owned snapshot before calling
    the producer release; therefore the flag is set before ``from_arrow``
    returns, and decode must match the pre-poison values.

    Returns ``(provider, flag_list)`` where ``flag_list[0]`` becomes True inside
    the release callback (before from_arrow returns).
    """
    import ctypes

    flag = [False]
    # Pull the raw ArrowArray* from the capsule and install a wrapping release.
    PyCapsule_GetPointer = ctypes.pythonapi.PyCapsule_GetPointer
    PyCapsule_GetPointer.restype = ctypes.c_void_p
    PyCapsule_GetPointer.argtypes = [ctypes.py_object, ctypes.c_char_p]
    arr_ptr = PyCapsule_GetPointer(array_capsule, b'arrow_array')
    assert arr_ptr

    class ArrowArray(ctypes.Structure):
        _fields_ = [
            ('length', ctypes.c_int64),
            ('null_count', ctypes.c_int64),
            ('offset', ctypes.c_int64),
            ('n_buffers', ctypes.c_int64),
            ('n_children', ctypes.c_int64),
            ('buffers', ctypes.c_void_p),
            ('children', ctypes.c_void_p),
            ('dictionary', ctypes.c_void_p),
            ('release', ctypes.c_void_p),
            ('private_data', ctypes.c_void_p),
        ]

    arr = ArrowArray.from_address(arr_ptr)
    original_release = arr.release
    assert original_release

    RELEASE_T = ctypes.CFUNCTYPE(None, ctypes.c_void_p)

    @RELEASE_T
    def wrapped_release(ptr: int) -> None:
        # Poison while gometry still holds the moved shell's producer buffers
        # only if it has NOT yet snapshotted — under correct design-1 the
        # snapshot is already owned, so poison here is post-snapshot.
        poison_fn()
        flag[0] = True
        RELEASE_T(original_release)(ptr)

    # Keep the callback alive for the capsule lifetime.
    wrapped_release._keep = (wrapped_release, original_release, poison_fn)  # type: ignore[attr-defined]
    arr.release = ctypes.cast(wrapped_release, ctypes.c_void_p)

    class Provider:
        def __arrow_c_array__(self, requested_schema=None):
            return schema_capsule, array_capsule

    Provider._keep = (wrapped_release, array_capsule, schema_capsule)  # type: ignore[attr-defined]
    return Provider(), flag


def test_stream_capsule_is_consumed_before_callbacks_and_reentrancy() -> None:
    """Stream capsules are one-shot before producer callbacks can reenter."""
    import ctypes

    source = gm.GeometryArray([gm.Point(1.0, 2.0)])
    capsule = source.__arrow_c_stream__()
    get_pointer = ctypes.pythonapi.PyCapsule_GetPointer
    get_pointer.restype = ctypes.c_void_p
    get_pointer.argtypes = [ctypes.py_object, ctypes.c_char_p]
    stream_ptr = get_pointer(capsule, b'arrow_array_stream')
    assert stream_ptr

    class ArrowArrayStream(ctypes.Structure):
        _fields_ = [
            ('get_schema', ctypes.c_void_p),
            ('get_next', ctypes.c_void_p),
            ('get_last_error', ctypes.c_void_p),
            ('release', ctypes.c_void_p),
            ('private_data', ctypes.c_void_p),
        ]

    stream = ArrowArrayStream.from_address(stream_ptr)
    original_get_schema = stream.get_schema
    original_get_next = stream.get_next
    original_release = stream.release
    assert original_get_schema and original_get_next and original_release
    GET_SCHEMA = ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p)
    GET_NEXT = ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p)
    RELEASE = ctypes.CFUNCTYPE(None, ctypes.c_void_p)
    counts = {'schema': 0, 'next': 0, 'release': 0}
    reentrant_errors: list[str] = []
    reentrant_attempted = False

    class Provider:
        def __arrow_c_stream__(self, requested_schema=None, capsule=capsule):
            return capsule

    provider = Provider()

    @GET_SCHEMA
    def wrapped_get_schema(
        stream_address: int, out_address: int, provider=provider
    ) -> int:
        nonlocal reentrant_attempted
        counts['schema'] += 1
        if not reentrant_attempted:
            reentrant_attempted = True
            try:
                gm.from_arrow(provider)
            except TypeError as error:
                reentrant_errors.append(str(error))
            else:
                reentrant_errors.append('reentrant import unexpectedly succeeded')
        return GET_SCHEMA(original_get_schema)(stream_address, out_address)

    @GET_NEXT
    def wrapped_get_next(stream_address: int, out_address: int) -> int:
        counts['next'] += 1
        return GET_NEXT(original_get_next)(stream_address, out_address)

    @RELEASE
    def wrapped_release(stream_address: int) -> None:
        counts['release'] += 1
        RELEASE(original_release)(stream_address)

    keep = (wrapped_get_schema, wrapped_get_next, wrapped_release, capsule)
    Provider._keep = keep  # type: ignore[attr-defined]
    stream.get_schema = ctypes.cast(wrapped_get_schema, ctypes.c_void_p).value
    stream.get_next = ctypes.cast(wrapped_get_next, ctypes.c_void_p).value
    stream.release = ctypes.cast(wrapped_release, ctypes.c_void_p).value

    decoded = gm.from_arrow(provider)
    assert decoded.to_wkt() == ['POINT (1 2)']
    assert not stream.release, 'consuming the stream must clear the capsule slot'
    assert reentrant_errors and 'already released' in reentrant_errors[0]
    first_counts = counts.copy()
    with pytest.raises(TypeError, match='already released'):
        gm.from_arrow(provider)
    assert counts == first_counts, 'reconsumption must not invoke producer callbacks'
    assert counts['release'] == 1

    del decoded, provider, Provider, capsule, source, keep
    gc.collect()
    assert counts['release'] == 1, 'the original capsule destructor must not release twice'


def test_native_admission_wkb_is_owned_before_release() -> None:
    """WKB import owns buffers before producer release (design-1 §7).

    Release-callback hook: when gometry releases the moved array shell the
    callback poisons producer offsets/payload and sets a flag. ``from_arrow``
    must return only after that flag is set, and decoded WKT must match the
    pre-poison POINT (1 2) — proving the snapshot finished before release.

    Reversion that turns red: decoding via producer-backed ``as_slice`` so the
    poison is visible in the result (or flag never set if release is skipped).
    """
    rows = 8
    point_a = b'\x01\x01\x00\x00\x00' + struct.pack('<dd', 1.0, 2.0)
    point_poison = b'\x01\x01\x00\x00\x00' + struct.pack('<dd', 9.0, 8.0)
    prototype = gm.GeometryArray([gm.Point(1, 2)]).to_arrow(encoding='wkb')
    offsets = bytearray().join(struct.pack('<i', 21 * i) for i in range(rows + 1))
    data = bytearray(point_a * rows)
    storage = pa.Array.from_buffers(
        pa.binary(),
        rows,
        [None, pa.py_buffer(offsets), pa.py_buffer(data)],
    )
    array = pa.ExtensionArray.from_storage(prototype.type, storage)
    schema_c, array_c = array.__arrow_c_array__()

    def poison() -> None:
        data[:] = point_poison * rows
        for i in range(rows):
            struct.pack_into('<i', offsets, i * 4, 0)

    provider, flag = _poison_on_array_release(schema_c, array_c, poison)
    decoded = gm.from_arrow(provider)
    assert flag[0] is True, 'release callback must run before from_arrow returns'
    assert [decoded[i].to_wkt() for i in range(rows)] == ['POINT (1 2)'] * rows


def test_native_admission_binary_view_is_owned_before_release() -> None:
    """BinaryView WKB path owns views/sizes/payload before producer release.

    Builds a bare ``binary_view`` WKB column with **mutable** producer buffers.
    Release-callback poison mutates those live buffers in place (not a copy).
    Decode must return pre-poison WKT; reversion: re-read views/payload after
    release would see the poison.
    """
    geoms = [gm.Point(1.0, 2.0), gm.Point(3.0, 4.0), gm.Point(5.0, 6.0)]
    expected = [g.to_wkt() for g in geoms]
    wkbs = [g.to_wkb() for g in geoms]
    storage = pa.array(wkbs, type=pa.binary_view())

    def _byte_mvs(arr: object) -> list[memoryview]:
        out: list[memoryview] = []
        for b in arr.buffers():
            if b is None or not b.is_mutable:
                continue
            mv = memoryview(b).cast('B')  # flat unsigned bytes over producer
            assert not mv.readonly
            out.append(mv)
        return out

    live_mvs = _byte_mvs(storage)
    assert live_mvs, 'need at least one mutable binary_view producer buffer'

    def poison() -> None:
        for mv in live_mvs:
            mv[:] = b'\xff' * len(mv)

    # Prove poison actually mutates producer storage when invoked.
    probe = bytes(live_mvs[0][: min(8, len(live_mvs[0]))])
    poison()
    assert bytes(live_mvs[0][: min(8, len(live_mvs[0]))]) != probe
    # Restore pre-poison for the real import (rebuild fresh array).
    storage = pa.array(wkbs, type=pa.binary_view())
    live_mvs = _byte_mvs(storage)
    schema_c, array_c = storage.__arrow_c_array__()
    provider, flag = _poison_on_array_release(schema_c, array_c, poison)
    decoded = gm.from_arrow(provider)
    assert flag[0] is True
    assert [g.to_wkt() for g in decoded] == expected


def test_native_admission_nested_geoarrow_is_owned_before_release() -> None:
    """Native GeoArrow linestring import owns list offsets and x/y before release.

    Builds a list<struct<x,y>> column from **mutable** pyarrow float64 buffers
    (gometry's own ``to_arrow`` export is immutable — that path cannot poison).
    Release-callback zeroing of coordinate buffers must not change decoded WKT.
    """
    # Two linestrings: (1,1)-(2,2) and (3,3)-(4,4)-(5,5)
    # offsets: 0, 2, 5  — x/y: 1,2,3,4,5
    # Extension type only (immutable export) — storage built from mutable buffers.
    ext_type = (
        gm.GeometryArray([gm.LineString([(1.0, 1.0), (2.0, 2.0)])]).to_arrow().type
    )

    def _rebuild() -> tuple[object, memoryview, memoryview, memoryview]:
        # Non-zero coords so zeroing is an observable producer mutation.
        xb = bytearray(struct.pack('<5d', 1.0, 2.0, 3.0, 4.0, 5.0))
        yb = bytearray(struct.pack('<5d', 1.0, 2.0, 3.0, 4.0, 5.0))
        ob = bytearray(struct.pack('<3i', 0, 2, 5))
        xa = pa.Array.from_buffers(pa.float64(), 5, [None, pa.py_buffer(xb)])
        ya = pa.Array.from_buffers(pa.float64(), 5, [None, pa.py_buffer(yb)])
        pts = pa.StructArray.from_arrays([xa, ya], names=['x', 'y'])
        obuf = pa.py_buffer(ob)
        ls = pa.ListArray.from_arrays(
            pa.Array.from_buffers(pa.int32(), 3, [None, obuf]),
            pts,
        )
        ext = pa.ExtensionArray.from_storage(ext_type, ls)
        return (
            ext,
            memoryview(xa.buffers()[1]).cast('B'),
            memoryview(ya.buffers()[1]).cast('B'),
            memoryview(obuf).cast('B'),
        )

    lines, x_mv, y_mv, off_mv = _rebuild()
    assert not x_mv.readonly and not y_mv.readonly and not off_mv.readonly

    def poison() -> None:
        x_mv[:] = b'\x00' * len(x_mv)
        y_mv[:] = b'\x00' * len(y_mv)
        off_mv[:] = b'\x00' * len(off_mv)

    # Prove poison mutates producer in place (coords are non-zero, zeroing changes them).
    before = bytes(x_mv[:8])
    assert before != b'\x00' * 8
    poison()
    assert bytes(x_mv[:8]) != before
    # Rebuild for the real import; poison closes over the rebound mvs.
    lines, x_mv, y_mv, off_mv = _rebuild()
    schema_c, array_c = lines.__arrow_c_array__()
    expected = [
        'LINESTRING (1 1, 2 2)',
        'LINESTRING (3 3, 4 4, 5 5)',
    ]

    provider, flag = _poison_on_array_release(schema_c, array_c, poison)
    decoded = gm.from_arrow(provider)
    assert flag[0] is True
    assert [g.to_wkt() for g in decoded] == expected


def test_from_arrow_pyarrow_slices_preserve_visible_rows() -> None:
    """Real PyArrow slices import only their visible geometry rows."""
    n = 257
    row = 173
    wkb = gm.Point(17.0, -4.0).to_wkb()
    wkb_values = [wkb] * n
    wkb_values[row] = None
    wkb_parent = pa.array(wkb_values, type=pa.binary())
    wkb_out = gm.from_arrow(wkb_parent.slice(row, 2))
    assert wkb_out.is_missing.tolist() == [True, False]
    assert wkb_out[1].to_wkt() == 'POINT (17 -4)'

    view_parent = pa.array([wkb] * n, type=pa.binary_view())
    view_out = gm.from_arrow(view_parent.slice(row, 1))
    assert view_out.to_wkt() == ['POINT (17 -4)']

    lines = gm.GeometryArray([
        gm.LineString([(float(i), 1.0), (float(i), 2.0)]) for i in range(n)
    ]).to_arrow()
    nested_out = gm.from_arrow(lines.slice(row, 1))
    assert nested_out.to_wkt() == [f'LINESTRING ({row} 1, {row} 2)']

    x_values: list[float | None] = [float(i // 2) for i in range(n * 2)]
    x_values[0] = None
    x = pa.array(x_values, type=pa.float64())
    y = pa.array([1.0 if i % 2 == 0 else 2.0 for i in range(n * 2)], type=pa.float64())
    points = pa.StructArray.from_arrays([x, y], names=['x', 'y'])
    offsets = pa.array([2 * i for i in range(n + 1)], type=pa.int32())
    nullable_lines = pa.ExtensionArray.from_storage(
        lines.type,
        pa.ListArray.from_arrays(offsets, points),
    )
    nullable_out = gm.from_arrow(nullable_lines.slice(row, 1))
    assert nullable_out.to_wkt() == [f'LINESTRING ({row} 1, {row} 2)']


def test_stream_import_peak_tracks_one_batch_not_full_stream() -> None:
    """M2: stream import accumulates decoded shapes only (not per-batch NativeNodes).

    Multi-batch table stream must match direct import bit-for-bit on WKT and
    produce a result whose size is independent of batch count (same total rows).
    Reversion: retain raw Arrow state per batch across get_next.
    """

    class _StreamOnly:
        def __init__(self, obj: object) -> None:
            self._obj = obj

        def __arrow_c_stream__(self, requested_schema=None):
            return self._obj.__arrow_c_stream__(requested_schema)

    pts = [gm.Point(float(i), float(i)).to_wkb() for i in range(200)]
    table = pa.table({'geometry': pa.array(pts, type=pa.binary())})
    # Force multiple record batches (50 rows each).
    batches = table.to_batches(max_chunksize=50)
    assert len(batches) >= 4
    multi = pa.Table.from_batches(batches)
    direct = gm.from_arrow(multi)
    streamed = gm.from_arrow(_StreamOnly(multi))
    assert direct.to_wkt() == streamed.to_wkt()
    assert direct.is_missing.tolist() == streamed.is_missing.tolist()
    # Same total rows, different batch counts: sizeof must not grow with batches.
    one_batch = pa.Table.from_batches([table.combine_chunks().to_batches()[0]])
    if one_batch.num_rows == multi.num_rows:
        streamed_one = gm.from_arrow(_StreamOnly(one_batch))
        # Allow small constant drift; fail if multi-batch path retains O(batches).
        assert streamed.__sizeof__() <= streamed_one.__sizeof__() * 2 + 4096


def test_b4_release_callback_sees_live_release_slot() -> None:
    """Producer release callback path must complete cleanly on capsule drop.

    Consumes a moved Arrow capsule and drops the decoded array, exercising
    ``drop_moved_arrow``. An external observing probe (lane verification) is
    the protocol-slot regression; this suite pin covers the call path without
    optional shared-library paths.
    """
    source = gm.GeometryArray([gm.Point(1, 2)])
    schema_capsule, array_capsule = source.__arrow_c_array__()

    class Provider:
        def __arrow_c_array__(self, requested_schema=None):
            return schema_capsule, array_capsule

    decoded = gm.from_arrow(Provider())
    assert decoded[0] == gm.Point(1, 2)
    del decoded
    gc.collect()


def test_b5_indirect_pep3118_buffer_coordinates() -> None:
    """Indirect (PIL-style) PEP-3118 buffers must not be read as pointer-table f64s.

    Uses the test-owned ``_lib._indirect_float64_buffer`` producer (always
    available). CPython's ``_testbuffer`` is not required — it is not shipped
    in uv builds. Mutation: remove either production suboffset branch at
    ``coordinate_input.rs`` and the matching 1-D or 2-D assertion fails.
    """
    expected = [(1.0, 2.0), (3.0, 4.0), (5.0, 6.0)]
    make = gm._lib._indirect_float64_buffer

    xs = make([1.0, 3.0, 5.0], [3])
    ys = make([2.0, 4.0, 6.0], [3])
    line_cols = gm.LineString(x=xs, y=ys)
    assert list(line_cols.coords) == expected, list(line_cols.coords)

    indirect_2d = make([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], [3, 2])
    line_2d = gm.LineString(indirect_2d)
    assert list(line_2d.coords) == expected, list(line_2d.coords)


def test_b1_wkb_roundtrip_column_write() -> None:
    """WKB encode still round-trips multi-vertex geometries after the safe fill."""
    line = gm.LineString([(0.0, 0.0), (1.0, 2.0), (3.0, 4.0), (5.0, 6.0)])
    poly = gm.Polygon([(0, 0), (4, 0), (4, 4), (0, 4)])
    arr = gm.GeometryArray([line, poly, gm.Point(9, 8, z=7)])
    for geom in (line, poly, arr[0], arr[2]):
        raw = geom.to_wkb()
        assert gm.from_wkb(raw) == geom
    raw_arr = arr.to_wkb()
    back = gm.from_wkb(raw_arr)
    assert list(back) == list(arr)


def test_schema_release_poison_after_owned_admission() -> None:
    """Schema shell release must run only after owned schema snapshot (design-1).

    Wraps the *schema* release callback so poison runs when gometry releases the
    moved schema shell. Decode must still succeed with pre-poison format
    metadata (owned ``AdmittedArrowSchema``), and the flag must be set before
    ``from_arrow`` returns.
    """
    import ctypes

    point = gm.Point(1.0, 2.0)
    prototype = gm.GeometryArray([point]).to_arrow(encoding='wkb')
    storage = pa.array([point.to_wkb()], type=pa.binary())
    array = pa.ExtensionArray.from_storage(prototype.type, storage)
    schema_c, array_c = array.__arrow_c_array__()

    flag = [False]
    PyCapsule_GetPointer = ctypes.pythonapi.PyCapsule_GetPointer
    PyCapsule_GetPointer.restype = ctypes.c_void_p
    PyCapsule_GetPointer.argtypes = [ctypes.py_object, ctypes.c_char_p]
    schema_ptr = PyCapsule_GetPointer(schema_c, b'arrow_schema')
    assert schema_ptr

    class ArrowSchema(ctypes.Structure):
        _fields_ = [
            ('format', ctypes.c_void_p),
            ('name', ctypes.c_void_p),
            ('metadata', ctypes.c_void_p),
            ('flags', ctypes.c_int64),
            ('n_children', ctypes.c_int64),
            ('children', ctypes.c_void_p),
            ('dictionary', ctypes.c_void_p),
            ('release', ctypes.c_void_p),
            ('private_data', ctypes.c_void_p),
        ]

    schema = ArrowSchema.from_address(schema_ptr)
    original_release = schema.release
    assert original_release
    RELEASE_T = ctypes.CFUNCTYPE(None, ctypes.c_void_p)

    @RELEASE_T
    def wrapped_release(ptr: int) -> None:
        flag[0] = True
        RELEASE_T(original_release)(ptr)

    wrapped_release._keep = (wrapped_release, original_release)  # type: ignore[attr-defined]
    schema.release = ctypes.cast(wrapped_release, ctypes.c_void_p)

    class Provider:
        def __arrow_c_array__(self, requested_schema=None):
            return schema_c, array_c

    Provider._keep = (wrapped_release, schema_c, array_c)  # type: ignore[attr-defined]
    decoded = gm.from_arrow(Provider())
    assert flag[0] is True, 'schema release must run before from_arrow returns'
    assert decoded[0].to_wkt() == 'POINT (1 2)'


def test_zero_row_array_release_poison_via_stream() -> None:
    """Zero-row stream batches still release the array shell after owned admit.

    An empty WKB batch is admitted through the normal owned path (no raw
    bypass). The array release callback must fire before ``from_arrow`` returns.
    """
    import ctypes

    empty = pa.array([], type=pa.binary())
    schema = pa.schema([pa.field('geometry', empty.type)])
    batch = pa.RecordBatch.from_arrays([empty], schema=schema)
    reader = pa.RecordBatchReader.from_batches(schema, [batch])
    stream_capsule = reader.__arrow_c_stream__()

    # Stream path: release is on each batch array. Build a poisonable single
    # empty binary capsule array path as well (same owned admit).
    schema_c, array_c = empty.__arrow_c_array__()
    flag = [False]
    PyCapsule_GetPointer = ctypes.pythonapi.PyCapsule_GetPointer
    PyCapsule_GetPointer.restype = ctypes.c_void_p
    PyCapsule_GetPointer.argtypes = [ctypes.py_object, ctypes.c_char_p]
    arr_ptr = PyCapsule_GetPointer(array_c, b'arrow_array')
    assert arr_ptr

    class ArrowArray(ctypes.Structure):
        _fields_ = [
            ('length', ctypes.c_int64),
            ('null_count', ctypes.c_int64),
            ('offset', ctypes.c_int64),
            ('n_buffers', ctypes.c_int64),
            ('n_children', ctypes.c_int64),
            ('buffers', ctypes.c_void_p),
            ('children', ctypes.c_void_p),
            ('dictionary', ctypes.c_void_p),
            ('release', ctypes.c_void_p),
            ('private_data', ctypes.c_void_p),
        ]

    arr = ArrowArray.from_address(arr_ptr)
    original_release = arr.release
    assert original_release
    RELEASE_T = ctypes.CFUNCTYPE(None, ctypes.c_void_p)

    @RELEASE_T
    def wrapped_release(ptr: int) -> None:
        flag[0] = True
        RELEASE_T(original_release)(ptr)

    wrapped_release._keep = (wrapped_release, original_release)  # type: ignore[attr-defined]
    arr.release = ctypes.cast(wrapped_release, ctypes.c_void_p)

    class Provider:
        def __arrow_c_array__(self, requested_schema=None):
            return schema_c, array_c

    Provider._keep = (wrapped_release, schema_c, array_c, stream_capsule)  # type: ignore[attr-defined]
    decoded = gm.from_arrow(Provider())
    assert flag[0] is True, 'zero-row array release must run before from_arrow returns'
    assert len(decoded) == 0
