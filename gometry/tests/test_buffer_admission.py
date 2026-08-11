"""R15-H: executable suboffset coverage, Arrow admission C10-C12, release-once.

Deterministic fixtures only. No timing assertions. No ``_testbuffer`` dependency -
indirect buffers come from the test-owned ``_lib._indirect_float64_buffer``.
"""

from __future__ import annotations

import ctypes
import gc
import subprocess
import sys
import textwrap

import gometry as gm
import pytest

pa = pytest.importorskip('pyarrow')

_lib = gm._lib


# ---------------------------------------------------------------------------
# A — PEP-3118 suboffset soundness (executable without _testbuffer)
# ---------------------------------------------------------------------------


def _indirect(values: list[float], shape: tuple[int, ...] | list[int]):
    """Test-owned indirect f64 buffer with genuine PEP-3118 suboffsets."""
    return _lib._indirect_float64_buffer(values, list(shape))


def test_indirect_buffer_reports_suboffsets() -> None:
    buf = _indirect([1.0, 2.0, 3.0], (3,))
    assert buf.is_indirect
    assert list(buf.shape) == [3]
    # memoryview must surface suboffsets (the production guard key).
    mv = memoryview(buf)
    assert mv.suboffsets is not None
    assert mv.suboffsets[0] >= 0
    assert mv.tolist() == [1.0, 2.0, 3.0]


def test_b5_indirect_1d_columns_via_test_owned_producer() -> None:
    """1-D indirect columns must yield real coordinates, not pointer-table bytes.

    Mutation: remove ``coordinate_input.rs`` 1-D suboffset early-return →
    this assertion fails (coords become garbage pointer bits).
    """
    expected = [(1.0, 2.0), (3.0, 4.0), (5.0, 6.0)]
    xs = _indirect([1.0, 3.0, 5.0], (3,))
    ys = _indirect([2.0, 4.0, 6.0], (3,))
    line = gm.LineString(x=xs, y=ys)
    assert list(line.coords) == expected, list(line.coords)


def test_b5_indirect_2d_matrix_via_test_owned_producer() -> None:
    """2-D indirect N x D matrix must yield real coordinates, not pointer-table bytes.

    Mutation: remove ``coordinate_input.rs`` 2-D suboffset early-return →
    this assertion fails (coords become garbage pointer bits).
    """
    expected = [(1.0, 2.0), (3.0, 4.0), (5.0, 6.0)]
    matrix = _indirect([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], (3, 2))
    mv = memoryview(matrix)
    assert mv.ndim == 2
    assert mv.suboffsets is not None
    line = gm.LineString(matrix)
    assert list(line.coords) == expected, list(line.coords)


def test_b5_legacy_testbuffer_path_still_works_when_present() -> None:
    """If CPython ships ``_testbuffer``, keep the original path green too."""
    _testbuffer = pytest.importorskip('_testbuffer')
    expected = [(1.0, 2.0), (3.0, 4.0), (5.0, 6.0)]
    xs = _testbuffer.ndarray(
        [1.0, 3.0, 5.0], shape=(3,), format='d', flags=_testbuffer.ND_PIL
    )
    ys = _testbuffer.ndarray(
        [2.0, 4.0, 6.0], shape=(3,), format='d', flags=_testbuffer.ND_PIL
    )
    assert list(gm.LineString(x=xs, y=ys).coords) == expected
    matrix = _testbuffer.ndarray(
        [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        shape=(3, 2),
        format='d',
        flags=_testbuffer.ND_PIL,
    )
    assert list(gm.LineString(matrix).coords) == expected


# ---------------------------------------------------------------------------
# B / C10 — FixedSizeList child nulls under outer-valid rows
# ---------------------------------------------------------------------------


def _geoarrow_point_batch(fsl: pa.Array) -> pa.RecordBatch:
    field = pa.field(
        'geometry',
        fsl.type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.point',
            b'ARROW:extension:metadata': b'{}',
        },
    )
    return pa.RecordBatch.from_arrays([fsl], schema=pa.schema([field]))


def test_c10_interleaved_child_null_under_outer_valid_rejects() -> None:
    """Referenced FixedSizeList child nulls must reject (not become zero)."""
    flat = pa.array([1.0, None, 3.0, 4.0], type=pa.float64())
    fsl = pa.FixedSizeListArray.from_arrays(flat, 2)
    assert fsl.null_count == 0
    with pytest.raises((TypeError, gm.ParseError, ValueError), match=r'null'):
        gm.from_arrow(_geoarrow_point_batch(fsl))


def test_c10_interleaved_child_null_hidden_by_outer_null_ok() -> None:
    """Child nulls wholly hidden by an outer null may be ignored."""
    flat = pa.array([1.0, 2.0, 99.0, None], type=pa.float64())
    fsl = pa.FixedSizeListArray.from_arrays(flat, 2, mask=pa.array([False, True]))
    arr = gm.from_arrow(_geoarrow_point_batch(fsl))
    assert arr.to_wkt() == ['POINT (1 2)', None]
    assert arr.is_missing.tolist() == [False, True]


# ---------------------------------------------------------------------------
# B / C11 — parent capture/snapshot errors must reject
# ---------------------------------------------------------------------------


class _ArrowArray(ctypes.Structure):
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


def _wkb_table() -> pa.Table:
    pts = pa.array([gm.Point(1, 2).to_wkb(), gm.Point(3, 4).to_wkb()], type=pa.binary())
    schema = pa.schema([
        pa.field(
            'geometry',
            pa.binary(),
            metadata={
                b'ARROW:extension:name': b'geoarrow.wkb',
                b'ARROW:extension:metadata': b'{}',
            },
        ),
        pa.field('x', pa.int64()),
    ])
    return pa.Table.from_arrays(
        [pts, pa.array([10, 20], type=pa.int64())], schema=schema
    )


def _capsule_provider(schema_c, array_c):
    class Provider:
        def __arrow_c_array__(self, requested_schema=None):
            return schema_c, array_c

    Provider._keep = (schema_c, array_c)  # type: ignore[attr-defined]
    return Provider()


def test_c11_parent_forged_n_buffers_rejects_capsule() -> None:
    """Struct parent n_buffers must be 1; forging to 2 must reject (not accept)."""
    table = _wkb_table()
    # RecordBatch has __arrow_c_array__
    batch = table.to_batches()[0]
    schema_c, array_c = batch.__arrow_c_array__()
    PyCapsule_GetPointer = ctypes.pythonapi.PyCapsule_GetPointer
    PyCapsule_GetPointer.restype = ctypes.c_void_p
    PyCapsule_GetPointer.argtypes = [ctypes.py_object, ctypes.c_char_p]
    arr_ptr = PyCapsule_GetPointer(array_c, b'arrow_array')
    arr = _ArrowArray.from_address(arr_ptr)
    assert arr.n_buffers == 1
    arr.n_buffers = 2
    with pytest.raises((TypeError, gm.ParseError, ValueError)):
        gm.from_arrow(_capsule_provider(schema_c, array_c))


def test_c11_parent_null_count_validity_mismatch_rejects_capsule() -> None:
    """Parent null_count that disagrees with validity must reject."""
    table = _wkb_table()
    batch = table.to_batches()[0]
    schema_c, array_c = batch.__arrow_c_array__()
    PyCapsule_GetPointer = ctypes.pythonapi.PyCapsule_GetPointer
    PyCapsule_GetPointer.restype = ctypes.c_void_p
    PyCapsule_GetPointer.argtypes = [ctypes.py_object, ctypes.c_char_p]
    arr_ptr = PyCapsule_GetPointer(array_c, b'arrow_array')
    arr = _ArrowArray.from_address(arr_ptr)
    # Struct with n_buffers=1 and null_count=0 is fine; claim nulls without a
    # validity bitmap (or with a mismatch) must reject.
    arr.null_count = 1
    with pytest.raises((TypeError, gm.ParseError, ValueError)):
        gm.from_arrow(_capsule_provider(schema_c, array_c))


def test_c11_parent_forged_n_buffers_rejects_stream() -> None:
    """Stream path must also reject a forged parent n_buffers (not accept)."""
    table = _wkb_table()
    batch = table.to_batches()[0]
    schema_c, array_c = batch.__arrow_c_array__()

    # Pull the live schema/array so a custom stream can re-export them with a
    # forged parent n_buffers on get_next.
    PyCapsule_GetPointer = ctypes.pythonapi.PyCapsule_GetPointer
    PyCapsule_GetPointer.restype = ctypes.c_void_p
    PyCapsule_GetPointer.argtypes = [ctypes.py_object, ctypes.c_char_p]
    schema_ptr = PyCapsule_GetPointer(schema_c, b'arrow_schema')
    array_ptr = PyCapsule_GetPointer(array_c, b'arrow_array')

    class ArrowSchema(ctypes.Structure):
        _fields_ = [
            ('format', ctypes.c_char_p),
            ('name', ctypes.c_char_p),
            ('metadata', ctypes.c_void_p),
            ('flags', ctypes.c_int64),
            ('n_children', ctypes.c_int64),
            ('children', ctypes.c_void_p),
            ('dictionary', ctypes.c_void_p),
            ('release', ctypes.c_void_p),
            ('private_data', ctypes.c_void_p),
        ]

    class ArrowArrayStream(ctypes.Structure):
        _fields_ = [
            ('get_schema', ctypes.c_void_p),
            ('get_next', ctypes.c_void_p),
            ('get_last_error', ctypes.c_void_p),
            ('release', ctypes.c_void_p),
            ('private_data', ctypes.c_void_p),
        ]

    src_schema = ArrowSchema.from_address(schema_ptr)
    src_array = _ArrowArray.from_address(array_ptr)
    assert src_array.n_buffers == 1

    @ctypes.CFUNCTYPE(None, ctypes.c_void_p)
    def _noop_release(_ptr: int) -> None:
        pass

    release_fn = ctypes.cast(_noop_release, ctypes.c_void_p)
    stream = ArrowArrayStream()
    state = {'done': False}

    @ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p)
    def get_schema(_stream: int, out_schema: int) -> int:
        out = ArrowSchema.from_address(out_schema)
        out.format = src_schema.format
        out.name = src_schema.name
        out.metadata = src_schema.metadata
        out.flags = src_schema.flags
        out.n_children = src_schema.n_children
        out.children = src_schema.children
        out.dictionary = src_schema.dictionary
        out.release = release_fn
        out.private_data = None
        return 0

    @ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p)
    def get_next(_stream: int, out_array: int) -> int:
        out = _ArrowArray.from_address(out_array)
        if state['done']:
            out.length = 0
            out.null_count = 0
            out.offset = 0
            out.n_buffers = 0
            out.n_children = 0
            out.buffers = None
            out.children = None
            out.dictionary = None
            out.release = None
            out.private_data = None
            return 0
        state['done'] = True
        out.length = src_array.length
        out.null_count = src_array.null_count
        out.offset = src_array.offset
        # Forge: struct parent must have n_buffers=1; 2 must reject (C11).
        out.n_buffers = 2
        out.n_children = src_array.n_children
        out.buffers = src_array.buffers
        out.children = src_array.children
        out.dictionary = src_array.dictionary
        out.release = release_fn
        out.private_data = None
        return 0

    @ctypes.CFUNCTYPE(ctypes.c_char_p, ctypes.c_void_p)
    def get_last_error(_stream: int) -> None:
        return None

    stream.get_schema = ctypes.cast(get_schema, ctypes.c_void_p)
    stream.get_next = ctypes.cast(get_next, ctypes.c_void_p)
    stream.get_last_error = ctypes.cast(get_last_error, ctypes.c_void_p)
    stream.release = release_fn
    stream.private_data = None
    stream._keep = (  # type: ignore[attr-defined]
        get_schema,
        get_next,
        get_last_error,
        _noop_release,
        schema_c,
        array_c,
        state,
    )

    PyCapsule_New = ctypes.pythonapi.PyCapsule_New
    PyCapsule_New.restype = ctypes.py_object
    PyCapsule_New.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
    stream_cap = PyCapsule_New(ctypes.pointer(stream), b'arrow_array_stream', None)

    class StreamOnly:
        def __arrow_c_stream__(self, requested_schema=None):
            return stream_cap

    StreamOnly._keep = (stream_cap, stream)  # type: ignore[attr-defined]
    with pytest.raises((TypeError, gm.ParseError, ValueError)):
        gm.from_arrow(StreamOnly())

    # Honest stream path still succeeds.
    class HonestStream:
        def __init__(self, obj: object) -> None:
            self._obj = obj

        def __arrow_c_stream__(self, requested_schema=None):
            return self._obj.__arrow_c_stream__(requested_schema)

    assert gm.from_arrow(HonestStream(table)).to_wkt() == [
        'POINT (1 2)',
        'POINT (3 4)',
    ]


# ---------------------------------------------------------------------------
# B / C12 — deep irrelevant schema sibling must not stack-overflow
# ---------------------------------------------------------------------------


def test_c12_deep_unused_struct_sibling_imports_in_subprocess() -> None:
    """Valid geometry beside a ~50k-level unused Struct must import (exit 0)."""
    script = textwrap.dedent(
        r"""
        import ctypes
        import sys

        import gometry as gm
        import pyarrow as pa

        class ArrowSchema(ctypes.Structure):
            pass

        ArrowSchemaPtr = ctypes.POINTER(ArrowSchema)
        ArrowSchema._fields_ = [
            ("format", ctypes.c_char_p),
            ("name", ctypes.c_char_p),
            # Metadata is a length-prefixed binary blob, NOT a C string —
            # must be void* so ctypes does not stop at the first NUL.
            ("metadata", ctypes.c_void_p),
            ("flags", ctypes.c_int64),
            ("n_children", ctypes.c_int64),
            ("children", ctypes.POINTER(ArrowSchemaPtr)),
            ("dictionary", ArrowSchemaPtr),
            ("release", ctypes.c_void_p),
            ("private_data", ctypes.c_void_p),
        ]

        class ArrowArray(ctypes.Structure):
            pass

        ArrowArrayPtr = ctypes.POINTER(ArrowArray)
        ArrowArray._fields_ = [
            ("length", ctypes.c_int64),
            ("null_count", ctypes.c_int64),
            ("offset", ctypes.c_int64),
            ("n_buffers", ctypes.c_int64),
            ("n_children", ctypes.c_int64),
            ("buffers", ctypes.POINTER(ctypes.c_void_p)),
            ("children", ctypes.POINTER(ArrowArrayPtr)),
            ("dictionary", ArrowArrayPtr),
            ("release", ctypes.c_void_p),
            ("private_data", ctypes.c_void_p),
        ]

        SCHEMA_RELEASE = ctypes.CFUNCTYPE(None, ArrowSchemaPtr)
        ARRAY_RELEASE = ctypes.CFUNCTYPE(None, ArrowArrayPtr)

        @SCHEMA_RELEASE
        def release_schema(ptr):
            ptr.contents.release = None

        @ARRAY_RELEASE
        def release_array(ptr):
            ptr.contents.release = None

        def deep_struct_schema(depth: int):
            keep = []
            leaf = ArrowSchema(
                b"g", b"leaf", None, 0, 0, None, None,
                ctypes.cast(release_schema, ctypes.c_void_p), None,
            )
            keep.append(leaf)
            current = leaf
            for i in range(depth):
                children = (ArrowSchemaPtr * 1)(ctypes.pointer(current))
                keep.append(children)
                parent = ArrowSchema(
                    b"+s", f"L{i}".encode(), None, 0, 1, children, None,
                    ctypes.cast(release_schema, ctypes.c_void_p), None,
                )
                keep.append(parent)
                current = parent
            return current, keep

        def deep_struct_array(depth: int, length: int):
            keep = []
            leaf = ArrowArray(
                length, 0, 0, 1, 0, None, None, None,
                ctypes.cast(release_array, ctypes.c_void_p), None,
            )
            keep.append(leaf)
            current = leaf
            for _ in range(depth):
                children = (ArrowArrayPtr * 1)(ctypes.pointer(current))
                keep.append(children)
                parent = ArrowArray(
                    length, 0, 0, 1, 1, None, children, None,
                    ctypes.cast(release_array, ctypes.c_void_p), None,
                )
                keep.append(parent)
                current = parent
            return current, keep

        depth = 50_000
        # Export a bare WKB geometry array (format "z" + geoarrow.wkb extension),
        # not a RecordBatch struct root — then wrap it as the geometry sibling
        # next to a deep unused Struct.
        point = gm.Point(1.0, 2.0)
        prototype = gm.GeometryArray([point]).to_arrow(encoding="wkb")
        storage = pa.array([point.to_wkb()], type=pa.binary())
        ext = pa.ExtensionArray.from_storage(prototype.type, storage)
        schema_c, array_c = ext.__arrow_c_array__()

        PyCapsule_GetPointer = ctypes.pythonapi.PyCapsule_GetPointer
        PyCapsule_GetPointer.restype = ctypes.c_void_p
        PyCapsule_GetPointer.argtypes = [ctypes.py_object, ctypes.c_char_p]
        schema_ptr = PyCapsule_GetPointer(schema_c, b"arrow_schema")
        array_ptr = PyCapsule_GetPointer(array_c, b"arrow_array")
        root_schema = ArrowSchema.from_address(schema_ptr)
        root_array = ArrowArray.from_address(array_ptr)
        assert root_schema.format == b"z", root_schema.format

        deep_s, keep_s = deep_struct_schema(depth)
        deep_a, keep_a = deep_struct_array(depth, 1)

        # Steal the exported geometry schema/array as the named geometry child
        # (null their release so the parent struct owns the lifetime).
        root_schema.release = None
        root_array.release = None
        geo_schema = ArrowSchema(
            root_schema.format, b"geometry", root_schema.metadata,
            root_schema.flags, root_schema.n_children, root_schema.children,
            root_schema.dictionary,
            ctypes.cast(release_schema, ctypes.c_void_p), None,
        )
        geo_array = ArrowArray(
            root_array.length, root_array.null_count, root_array.offset,
            root_array.n_buffers, root_array.n_children, root_array.buffers,
            root_array.children, root_array.dictionary,
            ctypes.cast(release_array, ctypes.c_void_p), None,
        )
        keep_s.extend([geo_schema, deep_s])
        keep_a.extend([geo_array, deep_a])

        children_s = (ArrowSchemaPtr * 2)(
            ctypes.pointer(geo_schema), ctypes.pointer(deep_s)
        )
        children_a = (ArrowArrayPtr * 2)(
            ctypes.pointer(geo_array), ctypes.pointer(deep_a)
        )
        keep_s.append(children_s)
        keep_a.append(children_a)

        parent_s = ArrowSchema(
            b"+s", b"", None, 0, 2, children_s, None,
            ctypes.cast(release_schema, ctypes.c_void_p), None,
        )
        # Struct arrays require n_buffers=1 and a non-null buffer table
        # (validity may be a null pointer entry).
        parent_bufs = (ctypes.c_void_p * 1)(None)
        keep_a.append(parent_bufs)
        parent_a = ArrowArray(
            1, 0, 0, 1, 2,
            ctypes.cast(parent_bufs, ctypes.POINTER(ctypes.c_void_p)),
            children_a, None,
            ctypes.cast(release_array, ctypes.c_void_p), None,
        )
        keep_s.append(parent_s)
        keep_a.append(parent_a)

        PyCapsule_New = ctypes.pythonapi.PyCapsule_New
        PyCapsule_New.restype = ctypes.py_object
        PyCapsule_New.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]

        schema_cap = PyCapsule_New(ctypes.addressof(parent_s), b"arrow_schema", None)
        array_cap = PyCapsule_New(ctypes.addressof(parent_a), b"arrow_array", None)

        class Provider:
            def __arrow_c_array__(self, requested_schema=None):
                return schema_cap, array_cap

        Provider._keep = (
            keep_s, keep_a, schema_cap, array_cap,
            release_schema, release_array, schema_c, array_c, ext,
        )

        arr = gm.from_arrow(Provider())
        assert arr.to_wkt() == ["POINT (1 2)"], arr.to_wkt()
        print("OK", arr.to_wkt())
        """
    )
    result = subprocess.run(
        [sys.executable, '-c', script],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, (
        f'expected exit 0, got {result.returncode}\n'
        f'stdout={result.stdout!r}\nstderr={result.stderr!r}'
    )
    assert 'OK' in result.stdout


# ---------------------------------------------------------------------------
# C13 — selected deeply nested schema must reject before native stack growth
# ---------------------------------------------------------------------------


def test_c13_deep_selected_schema_rejects_in_subprocess() -> None:
    """A selected 20k-deep schema must raise cleanly, never crash the process.

    Mutation: remove the selected-capture depth guard in ``admitted.rs``. This
    producer then recurses until the native stack overflows (the pre-fix
    failure), so the subprocess exits nonzero instead of printing ``REJECTED``.
    """
    script = textwrap.dedent(
        r"""
        import ctypes
        import sys

        import gometry as gm

        class ArrowSchema(ctypes.Structure):
            pass

        ArrowSchemaPtr = ctypes.POINTER(ArrowSchema)
        ArrowSchema._fields_ = [
            ("format", ctypes.c_char_p),
            ("name", ctypes.c_char_p),
            ("metadata", ctypes.c_void_p),
            ("flags", ctypes.c_int64),
            ("n_children", ctypes.c_int64),
            ("children", ctypes.POINTER(ArrowSchemaPtr)),
            ("dictionary", ArrowSchemaPtr),
            ("release", ctypes.c_void_p),
            ("private_data", ctypes.c_void_p),
        ]

        class ArrowArray(ctypes.Structure):
            pass

        ArrowArrayPtr = ctypes.POINTER(ArrowArray)
        ArrowArray._fields_ = [
            ("length", ctypes.c_int64),
            ("null_count", ctypes.c_int64),
            ("offset", ctypes.c_int64),
            ("n_buffers", ctypes.c_int64),
            ("n_children", ctypes.c_int64),
            ("buffers", ctypes.POINTER(ctypes.c_void_p)),
            ("children", ctypes.POINTER(ArrowArrayPtr)),
            ("dictionary", ArrowArrayPtr),
            ("release", ctypes.c_void_p),
            ("private_data", ctypes.c_void_p),
        ]

        SCHEMA_RELEASE = ctypes.CFUNCTYPE(None, ArrowSchemaPtr)
        ARRAY_RELEASE = ctypes.CFUNCTYPE(None, ArrowArrayPtr)

        @SCHEMA_RELEASE
        def release_schema(ptr):
            ptr.contents.release = None

        @ARRAY_RELEASE
        def release_array(ptr):
            ptr.contents.release = None

        def nested_schema(depth):
            keep = []
            current = ArrowSchema(
                b"z", b"leaf", None, 0, 0, None, None,
                ctypes.cast(release_schema, ctypes.c_void_p), None,
            )
            keep.append(current)
            for index in range(depth):
                children = (ArrowSchemaPtr * 1)(ctypes.pointer(current))
                keep.append(children)
                current = ArrowSchema(
                    b"+s", b"geometry" if index + 1 == depth else b"nested", None,
                    0, 1, children, None,
                    ctypes.cast(release_schema, ctypes.c_void_p), None,
                )
                keep.append(current)
            return current, keep

        def nested_array(depth):
            keep = []
            current = ArrowArray(
                0, 0, 0, 1, 0, None, None, None,
                ctypes.cast(release_array, ctypes.c_void_p), None,
            )
            keep.append(current)
            for _ in range(depth):
                children = (ArrowArrayPtr * 1)(ctypes.pointer(current))
                keep.append(children)
                current = ArrowArray(
                    0, 0, 0, 1, 1, None, children, None,
                    ctypes.cast(release_array, ctypes.c_void_p), None,
                )
                keep.append(current)
            return current, keep

        depth = 20_000
        selected_schema, schemas = nested_schema(depth)
        selected_array, arrays = nested_array(depth)
        schema_children = (ArrowSchemaPtr * 1)(ctypes.pointer(selected_schema))
        array_children = (ArrowArrayPtr * 1)(ctypes.pointer(selected_array))
        root_schema = ArrowSchema(
            b"+s", b"", None, 0, 1, schema_children, None,
            ctypes.cast(release_schema, ctypes.c_void_p), None,
        )
        root_array = ArrowArray(
            0, 0, 0, 1, 1, None, array_children, None,
            ctypes.cast(release_array, ctypes.c_void_p), None,
        )

        PyCapsule_New = ctypes.pythonapi.PyCapsule_New
        PyCapsule_New.restype = ctypes.py_object
        PyCapsule_New.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
        schema_cap = PyCapsule_New(ctypes.addressof(root_schema), b"arrow_schema", None)
        array_cap = PyCapsule_New(ctypes.addressof(root_array), b"arrow_array", None)

        class Provider:
            def __arrow_c_array__(self, requested_schema=None):
                return schema_cap, array_cap

        Provider._keep = (
            schemas, arrays, schema_children, array_children, root_schema,
            root_array, schema_cap, array_cap, release_schema, release_array,
        )
        try:
            gm.from_arrow(Provider())
        except TypeError as exc:
            assert "selected schema nesting exceeds maximum depth" in str(exc), exc
            print("REJECTED", exc)
            sys.exit(0)
        raise SystemExit("deep selected schema was accepted")
        """
    )
    result = subprocess.run(
        [sys.executable, '-c', script],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, (
        f'expected clean rejection, got {result.returncode}\n'
        f'stdout={result.stdout!r}\nstderr={result.stderr!r}'
    )
    assert 'REJECTED' in result.stdout


# ---------------------------------------------------------------------------
# B — release callbacks run exactly once
# ---------------------------------------------------------------------------


def _wkb_extension_array():
    """Foreign WKB Arrow array with a live release slot (not gometry-native)."""
    point = gm.Point(1.0, 2.0)
    prototype = gm.GeometryArray([point]).to_arrow(encoding='wkb')
    storage = pa.array([point.to_wkb()], type=pa.binary())
    return pa.ExtensionArray.from_storage(prototype.type, storage)


def test_release_callback_once_on_success() -> None:
    """Producer release must run exactly once on successful admission."""
    array = _wkb_extension_array()
    schema_c, array_c = array.__arrow_c_array__()
    counts = [0]
    PyCapsule_GetPointer = ctypes.pythonapi.PyCapsule_GetPointer
    PyCapsule_GetPointer.restype = ctypes.c_void_p
    PyCapsule_GetPointer.argtypes = [ctypes.py_object, ctypes.c_char_p]
    arr_ptr = PyCapsule_GetPointer(array_c, b'arrow_array')
    arr = _ArrowArray.from_address(arr_ptr)
    original = arr.release
    assert original
    RELEASE_T = ctypes.CFUNCTYPE(None, ctypes.c_void_p)

    @RELEASE_T
    def wrapped(ptr: int) -> None:
        counts[0] += 1
        RELEASE_T(original)(ptr)

    wrapped._keep = (wrapped, original)  # type: ignore[attr-defined]
    arr.release = ctypes.cast(wrapped, ctypes.c_void_p)

    class Provider:
        def __arrow_c_array__(self, requested_schema=None):
            return schema_c, array_c

    Provider._keep = (wrapped, schema_c, array_c)  # type: ignore[attr-defined]
    decoded = gm.from_arrow(Provider())
    assert decoded[0] == gm.Point(1, 2)
    del decoded
    gc.collect()
    assert counts[0] == 1, f'release count={counts[0]}'


def test_release_callback_once_on_rejection() -> None:
    """Producer release must run exactly once even when admission rejects."""
    array = _wkb_extension_array()
    schema_c, array_c = array.__arrow_c_array__()
    counts = [0]
    PyCapsule_GetPointer = ctypes.pythonapi.PyCapsule_GetPointer
    PyCapsule_GetPointer.restype = ctypes.c_void_p
    PyCapsule_GetPointer.argtypes = [ctypes.py_object, ctypes.c_char_p]
    arr_ptr = PyCapsule_GetPointer(array_c, b'arrow_array')
    arr = _ArrowArray.from_address(arr_ptr)
    original = arr.release
    RELEASE_T = ctypes.CFUNCTYPE(None, ctypes.c_void_p)

    @RELEASE_T
    def wrapped(ptr: int) -> None:
        counts[0] += 1
        RELEASE_T(original)(ptr)

    wrapped._keep = (wrapped, original)  # type: ignore[attr-defined]
    arr.release = ctypes.cast(wrapped, ctypes.c_void_p)
    # Forge length to force a layout/content rejection after move.
    arr.length = -1

    class Provider:
        def __arrow_c_array__(self, requested_schema=None):
            return schema_c, array_c

    Provider._keep = (wrapped, schema_c, array_c)  # type: ignore[attr-defined]
    with pytest.raises((TypeError, gm.ParseError, ValueError, OverflowError)):
        gm.from_arrow(Provider())
    gc.collect()
    assert counts[0] == 1, f'release count on reject={counts[0]}'
