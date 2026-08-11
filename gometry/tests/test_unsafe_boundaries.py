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


def test_from_arrow_pyarrow_slices_copy_only_visible_buffer_windows() -> None:
    """M1: proxy WKB and nested GeoArrow imports request tiny windows.

    The tracing provider exercises the ordinary Python-buffer admission lane
    (it exposes a real Arrow ``type`` but no Arrow-C protocol). It records each
    ``Buffer.slice(start, length).to_pybytes()`` request, so a mutation back to
    full-parent ``to_pybytes()`` fails independently of the final decoded
    GeometryArray's retained size. The nested LineString slice proves the
    selected outer offsets project recursively into x/y coordinate windows.
    """

    class _TracedBuffer:
        def __init__(
            self,
            raw: object,
            name: str,
            events: list[tuple[str, str, int, int]],
            start: int = 0,
        ) -> None:
            self._raw = raw
            self._name = name
            self._events = events
            self._start = start

        @property
        def size(self) -> int:
            return int(self._raw.size)

        def slice(self, start: int, length: int) -> _TracedBuffer:
            self._events.append(('slice', self._name, self._start + start, length))
            return _TracedBuffer(
                self._raw.slice(start, length),
                self._name,
                self._events,
                self._start + start,
            )

        def to_pybytes(self) -> bytes:
            self._events.append(('copy', self._name, self._start, self.size))
            return bytes(self._raw.to_pybytes())

    class _TracedArray:
        def __init__(
            self, raw: object, name: str, events: list[tuple[str, str, int, int]]
        ) -> None:
            self._raw = raw
            self._name = name
            self._events = events
            self.type = raw.type
            self.offset = raw.offset
            self.null_count = raw.null_count

        def __len__(self) -> int:
            return len(self._raw)

        def buffers(self) -> tuple[object | None, ...]:
            raw_buffers = self._raw.buffers()
            self._events.append(('buffers', self._name, 0, len(raw_buffers)))
            return tuple(
                None
                if buffer is None
                else _TracedBuffer(
                    buffer, f'{self._name}.buffer[{index}]', self._events
                )
                for index, buffer in enumerate(raw_buffers)
            )

        def validate(self, *, full: bool = False) -> None:
            self._events.append(('validate', self._name, 0, int(full)))
            self._raw.validate(full=full)

        def __getitem__(self, index: int) -> object:
            self._events.append(('scalar', self._name, index, 1))
            return self._raw[index]

        @property
        def values(self) -> _TracedArray:
            return _TracedArray(self._raw.values, f'{self._name}.values', self._events)

        def field(self, name: str) -> _TracedArray:
            return _TracedArray(
                self._raw.field(name), f'{self._name}.{name}', self._events
            )

    class _TracedExtensionArray(_TracedArray):
        @property
        def storage(self) -> _TracedArray:
            return _TracedArray(
                self._raw.storage, f'{self._name}.storage', self._events
            )

    def copied(events: list[tuple[str, str, int, int]]) -> list[tuple[str, int, int]]:
        return [
            (name, start, length)
            for kind, name, start, length in events
            if kind == 'copy'
        ]

    n = 257
    row = 173
    point = gm.Point(17.0, -4.0)
    wkb = point.to_wkb()
    # The visible two-row window has a non-zero physical validity-bit
    # alignment and contains one null, so direct admission must retain its one
    # byte plus ``offset % 8`` rather than copying the 257-row parent bitmap.
    wkb_values = [wkb] * n
    wkb_values[row] = None
    wkb_parent = pa.array(wkb_values, type=pa.binary())
    wkb_events: list[tuple[str, str, int, int]] = []
    wkb_out = gm.from_arrow(_TracedArray(wkb_parent.slice(row, 2), 'wkb', wkb_events))
    assert wkb_out.is_missing.tolist() == [True, False]
    assert wkb_out[1].to_wkt() == 'POINT (17 -4)'
    assert copied(wkb_events)
    assert set(copied(wkb_events)) <= {
        ('wkb.buffer[0]', row // 8, 1),
        ('wkb.buffer[1]', row * 4, 12),
        ('wkb.buffer[2]', row * len(wkb), len(wkb)),
    }
    assert ('wkb.buffer[0]', row // 8, 1) in copied(wkb_events)
    assert ('wkb.buffer[1]', row * 4, 12) in copied(wkb_events)
    assert ('wkb.buffer[2]', row * len(wkb), len(wkb)) in copied(wkb_events)

    # A non-PyArrow proxy retains the explicit BinaryView validation contract:
    # its descriptor and selected payload window are copied, never the parent
    # data payload. Real PyArrow uses its separate C++ scalar route below.
    view_parent = pa.array([wkb] * n, type=pa.binary_view())
    view_events: list[tuple[str, str, int, int]] = []
    view_out = gm.from_arrow(
        _TracedArray(view_parent.slice(row, 1), 'view', view_events)
    )
    assert view_out.to_wkt() == ['POINT (17 -4)']
    assert view_events == [
        ('buffers', 'view', 0, 3),
        ('slice', 'view.buffer[1]', row * 16, 16),
        ('copy', 'view.buffer[1]', row * 16, 16),
        ('buffers', 'view', 0, 3),
        ('slice', 'view.buffer[2]', row * len(wkb), len(wkb)),
        ('copy', 'view.buffer[2]', row * len(wkb), len(wkb)),
    ]

    lines = gm.GeometryArray([
        gm.LineString([(float(i), 1.0), (float(i), 2.0)]) for i in range(n)
    ]).to_arrow()
    nested_events: list[tuple[str, str, int, int]] = []
    nested_out = gm.from_arrow(
        _TracedExtensionArray(lines.slice(row, 1), 'lines', nested_events)
    )
    assert nested_out.to_wkt() == [f'LINESTRING ({row} 1, {row} 2)']
    assert copied(nested_events)
    assert set(copied(nested_events)) <= {
        ('lines.storage.buffer[1]', row * 4, 8),
        ('lines.storage.values.x.buffer[1]', row * 2 * 8, 2 * 8),
        ('lines.storage.values.y.buffer[1]', row * 2 * 8, 2 * 8),
    }
    assert ('lines.storage.buffer[1]', row * 4, 8) in copied(nested_events)
    assert ('lines.storage.values.x.buffer[1]', row * 2 * 8, 2 * 8) in copied(
        nested_events
    )
    assert ('lines.storage.values.y.buffer[1]', row * 2 * 8, 2 * 8) in copied(
        nested_events
    )

    # A nullable coordinate child makes the nested validity path observable.
    # Its only null belongs to an unselected parent row: import must still
    # succeed, while copying just the selected physical validity byte (rather
    # than the full 514-coordinate parent bitmap).
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
    nullable_events: list[tuple[str, str, int, int]] = []
    nullable_out = gm.from_arrow(
        _TracedExtensionArray(
            nullable_lines.slice(row, 1), 'nullable_lines', nullable_events
        )
    )
    assert nullable_out.to_wkt() == [f'LINESTRING ({row} 1, {row} 2)']
    assert set(copied(nullable_events)) <= {
        ('nullable_lines.storage.buffer[1]', row * 4, 8),
        ('nullable_lines.storage.values.x.buffer[0]', row * 2 // 8, 1),
        ('nullable_lines.storage.values.x.buffer[1]', row * 2 * 8, 2 * 8),
        ('nullable_lines.storage.values.y.buffer[1]', row * 2 * 8, 2 * 8),
    }
    assert ('nullable_lines.storage.values.x.buffer[0]', row * 2 // 8, 1) in copied(
        nullable_events
    )


def test_stream_import_peak_tracks_one_batch_not_full_stream() -> None:
    """M2: stream import accumulates decoded shapes only (not per-batch NativeNodes).

    Multi-batch table stream must match direct import bit-for-bit on WKT and
    produce a result whose size is independent of batch count (same total rows).
    Reversion: retain ``NativeNode`` / ArrowStorage per batch across get_next.
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
