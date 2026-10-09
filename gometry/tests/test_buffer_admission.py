"""R15-H: executable suboffset coverage, Arrow admission C10-C12, release-once.

Deterministic fixtures only. No timing assertions. No ``_testbuffer`` dependency -
indirect buffers come from the test-owned ``_lib._indirect_float64_buffer``.
"""

from __future__ import annotations

import ctypes
import errno
import gc
import struct
import subprocess
import sys
import textwrap

import gometry as gm
import pytest

from tests._support import _capsule_only_from_pyarrow, _CapsuleOnly, _StreamOnly

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


@pytest.mark.parametrize('axis', ['z', 'm'])
def test_b5_indirect_empty_matrix_checks_optional_axis_length(axis: str) -> None:
    matrix = _indirect([], (0, 2))
    with pytest.raises(gm.InvalidGeometryError):
        gm.LineString(matrix, **{axis: [1.0]})

    empty = gm.LineString(_indirect([], (0, 2)), **{axis: []})
    assert empty.coordinate_axes == ('XYZ' if axis == 'z' else 'XYM')


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


def test_interleaved_child_null_under_outer_valid_rejects() -> None:
    """Referenced FixedSizeList child nulls must reject (not become zero)."""
    flat = pa.array([1.0, None, 3.0, 4.0], type=pa.float64())
    fsl = pa.FixedSizeListArray.from_arrays(flat, 2)
    assert fsl.null_count == 0
    with pytest.raises((TypeError, gm.ParseError, ValueError), match=r'null'):
        gm.from_arrow(_geoarrow_point_batch(fsl))


def test_interleaved_child_null_hidden_by_outer_null_ok() -> None:
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


def test_parent_forged_n_buffers_rejects_capsule() -> None:
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


def test_parent_null_count_validity_mismatch_rejects_capsule() -> None:
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


def test_parent_forged_n_buffers_rejects_stream() -> None:
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
        # The source capsule owns these borrowed pointers. Keep it live through
        # import and make the shallow copy non-owning to avoid releasing it.
        ctypes.memmove(
            out_schema,
            ctypes.byref(src_schema),
            ctypes.sizeof(src_schema),
        )
        out = ArrowSchema.from_address(out_schema)
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


def test_deep_unused_struct_sibling_imports_in_subprocess() -> None:
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


def test_deep_selected_schema_rejects_in_subprocess() -> None:
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


def test_arrow_c_malformed_payload_rejected() -> None:
    # Truncated WKB payload must fail as a parse error, never UB.
    # Schema-driven offset/overflow rejection is covered by the Rust
    # layout_validation_tests battery (native_buffer_len / checked_byte_span /
    # binary terminal-offset width; fallible format parse rejects Other).
    with pytest.raises((gm.ParseError, TypeError, ValueError)):
        gm.from_arrow(pa.array([b'\x01'], type=pa.binary()))


def test_arrow_c_sliced_struct_child_null_count_raw_length() -> None:
    """Struct-child null_count is validated against the raw child length, not
    the projected parent-visible length.
    """
    values = gm.points([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], [0] * 10, crs=4326)
    arrow = values.to_arrow()
    sliced = arrow.slice(2, 3)
    restored = gm.from_arrow(sliced)
    assert restored.to_wkt() == ['POINT (2 0)', 'POINT (3 0)', 'POINT (4 0)']


def test_binary_view_multi_buffer_external_wkb_import() -> None:
    """Pin true multi-data-buffer BinaryView import (not a single external buffer).

    PyArrow 24 packs one array into one data buffer; concatenating two
    BinaryView arrays yields ≥2 external data buffers end-to-end.
    """
    if not hasattr(pa, 'binary_view'):
        pytest.skip('pyarrow binary_view unavailable')
    # Long WKB forces external (non-inline) BinaryView descriptors on each side.
    left = [
        gm.LineString([(float(j), float(j)) for j in range(30)]).to_wkb()
        for _ in range(4)
    ]
    right = [
        gm.LineString([(float(j + 10), float(j)) for j in range(30)]).to_wkb()
        for _ in range(4)
    ]
    assert all(len(w) > 12 for w in left + right)
    arrow = pa.concat_arrays([
        pa.array(left, type=pa.binary_view()),
        pa.array(right, type=pa.binary_view()),
    ])
    # ≥4 Arrow buffers: validity, views, data0, data1 (true multi-buffer).
    assert len(arrow.buffers()) >= 4, (
        f'expected multi-buffer BinaryView, got {len(arrow.buffers())}'
    )
    restored = gm.from_arrow(arrow)
    assert len(restored) == 8
    assert (
        restored[0].to_wkt()
        == gm.LineString([(float(j), float(j)) for j in range(30)]).to_wkt()
    )
    assert (
        restored[4].to_wkt()
        == gm.LineString([(float(j + 10), float(j)) for j in range(30)]).to_wkt()
    )
    # Capsule-only forces native BinaryView materialization over multi buffers.
    restored_c = gm.from_arrow(_capsule_only_from_pyarrow(arrow))
    assert restored_c.to_wkt() == restored.to_wkt()


def _arrow_c_binary_capsule_provider(length: int):
    """Build a retained foreign Binary capsule pair for length-boundary tests."""
    import ctypes

    class ArrowSchema(ctypes.Structure):
        _fields_ = [
            ('format', ctypes.c_char_p),
            ('name', ctypes.c_char_p),
            ('metadata', ctypes.c_char_p),
            ('flags', ctypes.c_int64),
            ('n_children', ctypes.c_int64),
            ('children', ctypes.POINTER(ctypes.c_void_p)),
            ('dictionary', ctypes.c_void_p),
            ('release', ctypes.c_void_p),
            ('private_data', ctypes.c_void_p),
        ]

    class ArrowArray(ctypes.Structure):
        _fields_ = [
            ('length', ctypes.c_int64),
            ('null_count', ctypes.c_int64),
            ('offset', ctypes.c_int64),
            ('n_buffers', ctypes.c_int64),
            ('n_children', ctypes.c_int64),
            ('buffers', ctypes.POINTER(ctypes.c_void_p)),
            ('children', ctypes.POINTER(ctypes.c_void_p)),
            ('dictionary', ctypes.c_void_p),
            ('release', ctypes.c_void_p),
            ('private_data', ctypes.c_void_p),
        ]

    counts = {'schema': 0, 'array': 0}

    @ctypes.CFUNCTYPE(None, ctypes.c_void_p)
    def schema_release(pointer: int) -> None:
        counts['schema'] += 1
        ArrowSchema.from_address(pointer).release = None

    @ctypes.CFUNCTYPE(None, ctypes.c_void_p)
    def array_release(pointer: int) -> None:
        counts['array'] += 1
        ArrowArray.from_address(pointer).release = None

    buffer_table = (ctypes.c_void_p * 3)(0, 0, 0)
    schema = ArrowSchema(
        format=b'z',
        name=b'',
        metadata=None,
        flags=0,
        n_children=0,
        children=None,
        dictionary=None,
        release=ctypes.cast(schema_release, ctypes.c_void_p),
        private_data=None,
    )
    array = ArrowArray(
        length=length,
        null_count=0,
        offset=0,
        n_buffers=3,
        n_children=0,
        buffers=ctypes.cast(buffer_table, ctypes.POINTER(ctypes.c_void_p)),
        children=None,
        dictionary=None,
        release=ctypes.cast(array_release, ctypes.c_void_p),
        private_data=None,
    )
    schema_box = ctypes.pointer(schema)
    array_box = ctypes.pointer(array)

    PyCapsule_New = ctypes.pythonapi.PyCapsule_New
    PyCapsule_GetPointer = ctypes.pythonapi.PyCapsule_GetPointer
    PyCapsule_New.restype = ctypes.py_object
    PyCapsule_New.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
    PyCapsule_GetPointer.restype = ctypes.c_void_p
    PyCapsule_GetPointer.argtypes = [ctypes.c_void_p, ctypes.c_char_p]

    @ctypes.CFUNCTYPE(None, ctypes.c_void_p)
    def schema_capsule_destructor(capsule: int) -> None:
        pointer = PyCapsule_GetPointer(capsule, b'arrow_schema')
        if pointer and ArrowSchema.from_address(pointer).release:
            schema_release(pointer)

    @ctypes.CFUNCTYPE(None, ctypes.c_void_p)
    def array_capsule_destructor(capsule: int) -> None:
        pointer = PyCapsule_GetPointer(capsule, b'arrow_array')
        if pointer and ArrowArray.from_address(pointer).release:
            array_release(pointer)

    schema_cap = PyCapsule_New(
        ctypes.cast(schema_box, ctypes.c_void_p),
        b'arrow_schema',
        ctypes.cast(schema_capsule_destructor, ctypes.c_void_p),
    )
    array_cap = PyCapsule_New(
        ctypes.cast(array_box, ctypes.c_void_p),
        b'arrow_array',
        ctypes.cast(array_capsule_destructor, ctypes.c_void_p),
    )
    provider = _CapsuleOnly(schema_cap, array_cap)
    keep_alive = (
        schema,
        array,
        buffer_table,
        schema_release,
        array_release,
        schema_capsule_destructor,
        array_capsule_destructor,
        schema_cap,
        array_cap,
    )
    return provider, counts, keep_alive


def test_from_arrow_positive_unrepresentable_span_is_overflow_error() -> None:
    """A positive i64 length whose Binary offsets span cannot fit usize."""
    provider, counts, _keepalive = _arrow_c_binary_capsule_provider(2**63 - 1)
    with pytest.raises(OverflowError, match='Arrow offset window length overflows'):
        gm.from_arrow(provider)
    assert counts == {'schema': 1, 'array': 1}


def test_from_arrow_negative_length_remains_type_error() -> None:
    """Negative C fields remain malformed input, not positive-size overflow."""
    provider, counts, _keepalive = _arrow_c_binary_capsule_provider(-1)
    with pytest.raises(TypeError, match='Arrow array length is negative'):
        gm.from_arrow(provider)
    assert counts == {'schema': 1, 'array': 1}


def test_zero_batch_binary_with_illegal_child_rejected() -> None:
    """R14: schema-only/zero-batch must reject Binary/`z` with n_children=1.

    Nonempty arrays already reject impossible child cardinality via layout
    validation; a zero-batch stream must reject the same impossible binary
    schema without dereferencing its declared child table.
    """
    import ctypes

    class ArrowSchema(ctypes.Structure):
        pass

    ArrowSchema._fields_ = [
        ('format', ctypes.c_char_p),
        ('name', ctypes.c_char_p),
        ('metadata', ctypes.c_void_p),
        ('flags', ctypes.c_int64),
        ('n_children', ctypes.c_int64),
        ('children', ctypes.POINTER(ctypes.POINTER(ArrowSchema))),
        ('dictionary', ctypes.c_void_p),
        ('release', ctypes.c_void_p),
        ('private_data', ctypes.c_void_p),
    ]

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

    class ArrowArrayStream(ctypes.Structure):
        _fields_ = [
            ('get_schema', ctypes.c_void_p),
            ('get_next', ctypes.c_void_p),
            ('get_last_error', ctypes.c_void_p),
            ('release', ctypes.c_void_p),
            ('private_data', ctypes.c_void_p),
        ]

    @ctypes.CFUNCTYPE(None, ctypes.c_void_p)
    def _noop_release(_ptr: int) -> None:
        pass

    release_fn = ctypes.cast(_noop_release, ctypes.c_void_p)

    # Child: real float64 leaf (format "g").
    child = ArrowSchema(
        format=b'g',
        name=b'x',
        metadata=None,
        flags=0,
        n_children=0,
        children=None,
        dictionary=None,
        release=release_fn,
        private_data=None,
    )
    child_ptr = ctypes.pointer(child)
    children_table = (ctypes.POINTER(ArrowSchema) * 1)(child_ptr)

    # Root: format "z" (Binary) with illegal n_children=1 + float64 child.
    root = ArrowSchema(
        format=b'z',
        name=b'geometry',
        metadata=None,
        flags=0,
        n_children=1,
        children=ctypes.cast(
            children_table, ctypes.POINTER(ctypes.POINTER(ArrowSchema))
        ),
        dictionary=None,
        release=release_fn,
        private_data=None,
    )

    # Zero-batch stream: schema is the impossible Binary tree; get_next ends.
    stream = ArrowArrayStream()

    @ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p)
    def get_schema(_stream: int, out_schema: int) -> int:
        ctypes.memmove(out_schema, ctypes.byref(root), ctypes.sizeof(root))
        return 0

    @ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p)
    def get_next(_stream: int, out_array: int) -> int:
        # End of stream: leave release=None (Arrow C convention).
        out = ArrowArray.from_address(out_array)
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

    @ctypes.CFUNCTYPE(ctypes.c_char_p, ctypes.c_void_p)
    def get_last_error(_stream: int) -> None:
        return None

    stream.get_schema = ctypes.cast(get_schema, ctypes.c_void_p)
    stream.get_next = ctypes.cast(get_next, ctypes.c_void_p)
    stream.get_last_error = ctypes.cast(get_last_error, ctypes.c_void_p)
    stream.release = release_fn
    stream.private_data = None

    # Keep live refs so GC cannot free the CFUNCTYPE objects mid-call.
    stream._keep = (
        get_schema,
        get_next,
        get_last_error,
        _noop_release,
        child,
        children_table,
        root,
    )  # type: ignore[attr-defined]

    PyCapsule_New = ctypes.pythonapi.PyCapsule_New
    PyCapsule_New.restype = ctypes.py_object
    PyCapsule_New.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
    stream_box = ctypes.pointer(stream)
    stream_cap = PyCapsule_New(stream_box, b'arrow_array_stream', None)

    class _StreamCapsuleOnly:
        def __arrow_c_stream__(self, requested_schema=None):
            return stream_cap

    with pytest.raises((
        TypeError,
        gm.ParseError,
        gm.GeometryError,
        ValueError,
    )) as exc_info:
        gm.from_arrow(_StreamCapsuleOnly())
    assert type(exc_info.value).__name__ != 'PanicException'
    msg = str(exc_info.value).lower()
    assert 'child' in msg or 'children' in msg or 'binary' in msg or '0' in msg

    # Positive: legal zero-batch Binary WKB schema still accepts.
    ok = pa.Table.from_batches(
        [],
        schema=pa.schema([
            pa.field(
                'geometry',
                pa.binary(),
                metadata={b'ARROW:extension:name': b'geoarrow.wkb'},
            )
        ]),
    )
    empty = gm.from_arrow(_StreamOnly(ok))
    assert len(empty) == 0

    # Nonempty array with the same illegal layout is still rejected (parity).
    class _ArrayCapsuleOnly:
        def __init__(self, schema_cap: object, array_cap: object) -> None:
            self._schema = schema_cap
            self._array = array_cap

        def __arrow_c_array__(self, requested_schema=None):
            return (self._schema, self._array)

    # Reuse the illegal schema with a length=0 array (layout still sees n_children).
    buffer_table = (ctypes.c_void_p * 3)(0, 0, 0)
    arr = ArrowArray(
        length=0,
        null_count=0,
        offset=0,
        n_buffers=3,
        n_children=1,
        buffers=ctypes.cast(buffer_table, ctypes.c_void_p),
        children=ctypes.cast(children_table, ctypes.c_void_p),
        dictionary=None,
        release=release_fn,
        private_data=None,
    )
    schema_cap = PyCapsule_New(ctypes.pointer(root), b'arrow_schema', None)
    array_cap = PyCapsule_New(ctypes.pointer(arr), b'arrow_array', None)
    with pytest.raises((
        TypeError,
        gm.ParseError,
        gm.GeometryError,
        ValueError,
    )) as exc2:
        gm.from_arrow(_ArrayCapsuleOnly(schema_cap, array_cap))
    assert type(exc2.value).__name__ != 'PanicException'


def _f5_arrow_c_structs():
    """Ctypes ArrowSchema/ArrowArray/Stream types for F5 harnesses."""
    import ctypes

    class ArrowSchema(ctypes.Structure):
        pass

    ArrowSchema._fields_ = [
        ('format', ctypes.c_char_p),
        ('name', ctypes.c_char_p),
        ('metadata', ctypes.c_void_p),
        ('flags', ctypes.c_int64),
        ('n_children', ctypes.c_int64),
        ('children', ctypes.POINTER(ctypes.POINTER(ArrowSchema))),
        ('dictionary', ctypes.c_void_p),
        ('release', ctypes.c_void_p),
        ('private_data', ctypes.c_void_p),
    ]

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

    class ArrowArrayStream(ctypes.Structure):
        _fields_ = [
            ('get_schema', ctypes.c_void_p),
            ('get_next', ctypes.c_void_p),
            ('get_last_error', ctypes.c_void_p),
            ('release', ctypes.c_void_p),
            ('private_data', ctypes.c_void_p),
        ]

    return ctypes, ArrowSchema, ArrowArray, ArrowArrayStream


def _f5_build_id_geometry_schema(ctypes, ArrowSchema, release_fn):
    """Struct schema with child0=id, child1=geoarrow.point geometry."""

    def _meta(pairs):
        body = b''
        for key, value in pairs:
            body += struct.pack('<i', len(key)) + key
            body += struct.pack('<i', len(value)) + value
        blob = struct.pack('<i', len(pairs)) + body
        return (ctypes.c_uint8 * len(blob))(*blob)

    child0 = ArrowSchema(
        format=b'l',
        name=b'id',
        metadata=None,
        flags=0,
        n_children=0,
        children=None,
        dictionary=None,
        release=release_fn,
        private_data=None,
    )
    x_schema = ArrowSchema(
        format=b'g',
        name=b'x',
        metadata=None,
        flags=0,
        n_children=0,
        children=None,
        dictionary=None,
        release=release_fn,
        private_data=None,
    )
    y_schema = ArrowSchema(
        format=b'g',
        name=b'y',
        metadata=None,
        flags=0,
        n_children=0,
        children=None,
        dictionary=None,
        release=release_fn,
        private_data=None,
    )
    xy_ptrs = (ctypes.POINTER(ArrowSchema) * 2)(
        ctypes.pointer(x_schema), ctypes.pointer(y_schema)
    )
    geom_meta = _meta([
        (b'ARROW:extension:name', b'geoarrow.point'),
        (b'ARROW:extension:metadata', b'{}'),
    ])
    child1 = ArrowSchema(
        format=b'+s',
        name=b'geometry',
        metadata=ctypes.cast(geom_meta, ctypes.c_void_p),
        flags=0,
        n_children=2,
        children=ctypes.cast(xy_ptrs, ctypes.POINTER(ctypes.POINTER(ArrowSchema))),
        dictionary=None,
        release=release_fn,
        private_data=None,
    )
    schema_children = (ctypes.POINTER(ArrowSchema) * 2)(
        ctypes.pointer(child0), ctypes.pointer(child1)
    )
    root_schema = ArrowSchema(
        format=b'+s',
        name=b'',
        metadata=None,
        flags=0,
        n_children=2,
        children=ctypes.cast(
            schema_children, ctypes.POINTER(ctypes.POINTER(ArrowSchema))
        ),
        dictionary=None,
        release=release_fn,
        private_data=None,
    )
    keep = (
        geom_meta,
        x_schema,
        y_schema,
        xy_ptrs,
        child0,
        child1,
        schema_children,
        root_schema,
    )
    return root_schema, keep


@pytest.mark.parametrize(
    ('status', 'expected'),
    [(errno.ENOMEM, MemoryError), (errno.EIO, OSError), (errno.EINVAL, OSError)],
    ids=['enomem', 'eio', 'errno_fallback'],
)
def test_arrow_c_stream_callback_errno_maps_to_python_error(
    status: int, expected: type[Exception]
) -> None:
    """Callback return codes remain observable; no failure becomes TypeError."""
    ctypes, _, _, ArrowArrayStream = _f5_arrow_c_structs()

    @ctypes.CFUNCTYPE(None, ctypes.c_void_p)
    def release(_ptr: int) -> None:
        pass

    @ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p)
    def get_schema(_stream: int, _out_schema: int) -> int:
        return status

    @ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p)
    def get_next(_stream: int, _out_array: int) -> int:
        return 0

    error_detail = ctypes.create_string_buffer(b'producer callback failed')

    @ctypes.CFUNCTYPE(ctypes.c_void_p, ctypes.c_void_p)
    def get_last_error(_stream: int) -> int:
        return ctypes.addressof(error_detail)

    stream = ArrowArrayStream(
        ctypes.cast(get_schema, ctypes.c_void_p),
        ctypes.cast(get_next, ctypes.c_void_p),
        ctypes.cast(get_last_error, ctypes.c_void_p),
        ctypes.cast(release, ctypes.c_void_p),
        None,
    )
    PyCapsule_New = ctypes.pythonapi.PyCapsule_New
    PyCapsule_New.restype = ctypes.py_object
    PyCapsule_New.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
    capsule = PyCapsule_New(ctypes.pointer(stream), b'arrow_array_stream', None)

    class _ErrorStream:
        def __arrow_c_stream__(self, requested_schema=None):
            del requested_schema
            return capsule

    _ErrorStream._keep = (  # type: ignore[attr-defined]
        stream,
        get_schema,
        get_next,
        get_last_error,
        release,
        error_detail,
        capsule,
    )
    with pytest.raises(expected, match='producer callback failed') as error:
        gm.from_arrow(_ErrorStream())
    if expected is OSError:
        assert error.value.errno == status


def test_arrow_c_child_index_out_of_range_rejects() -> None:
    """F5: schema geometry child index >= array.n_children must ParseError.

    Stream zero-row path: schema has geometry at index 1, batch declares
    n_children=1 — children.add(1) was OOB before the bounds gate. Point
    zero-row validation returned immediately after the bad add.
    """
    ctypes, ArrowSchema, ArrowArray, ArrowArrayStream = _f5_arrow_c_structs()

    @ctypes.CFUNCTYPE(None, ctypes.c_void_p)
    def _noop_release(_ptr: int) -> None:
        pass

    release_fn = ctypes.cast(_noop_release, ctypes.c_void_p)
    root_schema, schema_keep = _f5_build_id_geometry_schema(
        ctypes, ArrowSchema, release_fn
    )

    id_child_arr = ArrowArray(
        length=0,
        null_count=0,
        offset=0,
        n_buffers=2,
        n_children=0,
        buffers=None,
        children=None,
        dictionary=None,
        release=release_fn,
        private_data=None,
    )
    only_one = (ctypes.POINTER(ArrowArray) * 1)(ctypes.pointer(id_child_arr))

    stream = ArrowArrayStream()
    state = {'done': False}

    @ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p)
    def get_schema(_stream: int, out_schema: int) -> int:
        ctypes.memmove(
            out_schema,
            ctypes.byref(root_schema),
            ctypes.sizeof(root_schema),
        )
        return 0

    @ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p)
    def get_next(_stream: int, out_array: int) -> int:
        out = ArrowArray.from_address(out_array)
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
        # Zero-row batch: n_children=1 while schema geometry is at index 1.
        out.length = 0
        out.null_count = 0
        out.offset = 0
        out.n_buffers = 1
        out.n_children = 1
        out.buffers = None
        out.children = ctypes.cast(only_one, ctypes.c_void_p)
        out.dictionary = None
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
        schema_keep,
        id_child_arr,
        only_one,
        state,
    )

    PyCapsule_New = ctypes.pythonapi.PyCapsule_New
    PyCapsule_New.restype = ctypes.py_object
    PyCapsule_New.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
    stream_cap = PyCapsule_New(ctypes.pointer(stream), b'arrow_array_stream', None)

    class _StreamCapsuleOnly:
        def __arrow_c_stream__(self, requested_schema=None):
            return stream_cap

    with pytest.raises(
        gm.ParseError, match=r'child index|n_children|out of range'
    ) as exc:
        gm.from_arrow(_StreamCapsuleOnly())
    assert type(exc.value).__name__ != 'PanicException'
    assert exc.value.format == 'geoarrow'

    # Positive: conforming multi-child table still imports (array + stream).
    ok = gm.GeometryArray([gm.Point(1.0, 2.0)])
    table = pa.table({
        'id': pa.array([1], type=pa.int64()),
        'geometry': ok.to_arrow(),
    })
    batch = table.to_batches()[0]
    restored = gm.from_arrow(_CapsuleOnly(*batch.__arrow_c_array__()))
    assert restored.to_wkt() == ['POINT (1 2)']
    restored_s = gm.from_arrow(_StreamOnly(table))
    assert restored_s.to_wkt() == ['POINT (1 2)']


def test_arrow_c_null_child_pointer_rejects() -> None:
    """F5: null child pointer at a valid index must ParseError on stream path."""
    ctypes, ArrowSchema, ArrowArray, ArrowArrayStream = _f5_arrow_c_structs()

    @ctypes.CFUNCTYPE(None, ctypes.c_void_p)
    def _noop_release(_ptr: int) -> None:
        pass

    release_fn = ctypes.cast(_noop_release, ctypes.c_void_p)
    root_schema, schema_keep = _f5_build_id_geometry_schema(
        ctypes, ArrowSchema, release_fn
    )

    id_arr = ArrowArray(
        length=0,
        null_count=0,
        offset=0,
        n_buffers=2,
        n_children=0,
        buffers=None,
        children=None,
        dictionary=None,
        release=release_fn,
        private_data=None,
    )
    # n_children=2 matches schema; child[1] (geometry) is NULL.
    child_table = (ctypes.POINTER(ArrowArray) * 2)(ctypes.pointer(id_arr), None)

    stream = ArrowArrayStream()
    state = {'done': False}

    @ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p)
    def get_schema(_stream: int, out_schema: int) -> int:
        ctypes.memmove(
            out_schema,
            ctypes.byref(root_schema),
            ctypes.sizeof(root_schema),
        )
        return 0

    @ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p)
    def get_next(_stream: int, out_array: int) -> int:
        out = ArrowArray.from_address(out_array)
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
        out.length = 0
        out.null_count = 0
        out.offset = 0
        out.n_buffers = 1
        out.n_children = 2
        out.buffers = None
        out.children = ctypes.cast(child_table, ctypes.c_void_p)
        out.dictionary = None
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
        schema_keep,
        id_arr,
        child_table,
        state,
    )

    PyCapsule_New = ctypes.pythonapi.PyCapsule_New
    PyCapsule_New.restype = ctypes.py_object
    PyCapsule_New.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
    stream_cap = PyCapsule_New(ctypes.pointer(stream), b'arrow_array_stream', None)

    class _StreamCapsuleOnly:
        def __arrow_c_stream__(self, requested_schema=None):
            return stream_cap

    with pytest.raises(gm.ParseError, match=r'child.*null|null.*child') as exc:
        gm.from_arrow(_StreamCapsuleOnly())
    assert type(exc.value).__name__ != 'PanicException'
    assert exc.value.format == 'geoarrow'


def _selected_struct_child_count_provider(protocol: str, array_children: int):
    """Retain one schema child and a selectable table-array child table."""
    ctypes, ArrowSchema, ArrowArray, ArrowArrayStream = _f5_arrow_c_structs()
    releases = {'schema': 0, 'array': 0, 'stream': 0}

    @ctypes.CFUNCTYPE(None, ctypes.c_void_p)
    def release_schema(pointer: int) -> None:
        schema = ArrowSchema.from_address(pointer)
        releases['schema'] += 1
        if schema.n_children and schema.children:
            for index in range(schema.n_children):
                child = schema.children[index]
                if child and child.contents.release:
                    release_schema(ctypes.cast(child, ctypes.c_void_p))
        schema.release = None

    @ctypes.CFUNCTYPE(None, ctypes.c_void_p)
    def release_array(pointer: int) -> None:
        array = ArrowArray.from_address(pointer)
        releases['array'] += 1
        if array.n_children and array.children:
            children = ctypes.cast(
                array.children, ctypes.POINTER(ctypes.POINTER(ArrowArray))
            )
            for index in range(array.n_children):
                child = children[index]
                if child and child.contents.release:
                    release_array(ctypes.cast(child, ctypes.c_void_p))
        array.release = None

    metadata_pairs = [
        (b'ARROW:extension:name', b'geoarrow.point'),
        (b'ARROW:extension:metadata', b'{}'),
    ]
    metadata_body = b''
    for key, value in metadata_pairs:
        metadata_body += struct.pack('=i', len(key)) + key
        metadata_body += struct.pack('=i', len(value)) + value
    metadata_blob = struct.pack('=i', len(metadata_pairs)) + metadata_body
    metadata = (ctypes.c_uint8 * len(metadata_blob))(*metadata_blob)
    schema_release_pointer = ctypes.cast(release_schema, ctypes.c_void_p)
    array_release_pointer = ctypes.cast(release_array, ctypes.c_void_p)

    x_schema = ArrowSchema(
        b'g', b'x', None, 0, 0, None, None, schema_release_pointer, None
    )
    y_schema = ArrowSchema(
        b'g', b'y', None, 0, 0, None, None, schema_release_pointer, None
    )
    coordinate_schemas = (ctypes.POINTER(ArrowSchema) * 2)(
        ctypes.pointer(x_schema), ctypes.pointer(y_schema)
    )
    geometry_schema = ArrowSchema(
        b'+s',
        b'geometry',
        ctypes.cast(metadata, ctypes.c_void_p),
        0,
        2,
        ctypes.cast(coordinate_schemas, ctypes.POINTER(ctypes.POINTER(ArrowSchema))),
        None,
        schema_release_pointer,
        None,
    )
    schema_children = (ctypes.POINTER(ArrowSchema) * 1)(ctypes.pointer(geometry_schema))
    root_schema = ArrowSchema(
        b'+s',
        b'',
        None,
        0,
        1,
        ctypes.cast(schema_children, ctypes.POINTER(ctypes.POINTER(ArrowSchema))),
        None,
        schema_release_pointer,
        None,
    )

    x_values = (ctypes.c_double * 1)(1.0)
    y_values = (ctypes.c_double * 1)(2.0)
    x_buffers = (ctypes.c_void_p * 2)(None, ctypes.addressof(x_values))
    y_buffers = (ctypes.c_void_p * 2)(None, ctypes.addressof(y_values))
    x_array = ArrowArray(
        1,
        0,
        0,
        2,
        0,
        ctypes.cast(x_buffers, ctypes.c_void_p),
        None,
        None,
        array_release_pointer,
        None,
    )
    y_array = ArrowArray(
        1,
        0,
        0,
        2,
        0,
        ctypes.cast(y_buffers, ctypes.c_void_p),
        None,
        None,
        array_release_pointer,
        None,
    )
    geometry_buffers = (ctypes.c_void_p * 1)(None)
    coordinate_arrays = (ctypes.POINTER(ArrowArray) * 2)(
        ctypes.pointer(x_array), ctypes.pointer(y_array)
    )
    geometry_array = ArrowArray(
        1,
        0,
        0,
        1,
        2,
        ctypes.cast(geometry_buffers, ctypes.c_void_p),
        ctypes.cast(coordinate_arrays, ctypes.c_void_p),
        None,
        array_release_pointer,
        None,
    )
    extra_values = (ctypes.c_double * 1)(9.0)
    extra_buffers = (ctypes.c_void_p * 2)(None, ctypes.addressof(extra_values))
    extra_array = ArrowArray(
        1,
        0,
        0,
        2,
        0,
        ctypes.cast(extra_buffers, ctypes.c_void_p),
        None,
        None,
        array_release_pointer,
        None,
    )
    root_buffers = (ctypes.c_void_p * 1)(None)
    array_children_values = [ctypes.pointer(geometry_array)]
    if array_children == 2:
        array_children_values.append(ctypes.pointer(extra_array))
    array_child_table = (ctypes.POINTER(ArrowArray) * array_children)(
        *array_children_values
    )
    root_array = ArrowArray(
        1,
        0,
        0,
        1,
        array_children,
        ctypes.cast(root_buffers, ctypes.c_void_p),
        ctypes.cast(array_child_table, ctypes.c_void_p),
        None,
        array_release_pointer,
        None,
    )

    keep_alive = (
        metadata,
        x_schema,
        y_schema,
        coordinate_schemas,
        geometry_schema,
        schema_children,
        root_schema,
        x_values,
        y_values,
        x_buffers,
        y_buffers,
        x_array,
        y_array,
        geometry_buffers,
        coordinate_arrays,
        geometry_array,
        extra_values,
        extra_buffers,
        extra_array,
        root_buffers,
        array_child_table,
        root_array,
        release_schema,
        release_array,
    )
    PyCapsule_New = ctypes.pythonapi.PyCapsule_New
    PyCapsule_GetPointer = ctypes.pythonapi.PyCapsule_GetPointer
    PyCapsule_New.restype = ctypes.py_object
    PyCapsule_New.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
    PyCapsule_GetPointer.restype = ctypes.c_void_p
    PyCapsule_GetPointer.argtypes = [ctypes.c_void_p, ctypes.c_char_p]

    @ctypes.CFUNCTYPE(None, ctypes.c_void_p)
    def schema_capsule_destructor(capsule: int) -> None:
        pointer = PyCapsule_GetPointer(capsule, b'arrow_schema')
        if pointer and ArrowSchema.from_address(pointer).release:
            release_schema(pointer)

    @ctypes.CFUNCTYPE(None, ctypes.c_void_p)
    def array_capsule_destructor(capsule: int) -> None:
        pointer = PyCapsule_GetPointer(capsule, b'arrow_array')
        if pointer and ArrowArray.from_address(pointer).release:
            release_array(pointer)

    if protocol == 'array':
        schema_capsule = PyCapsule_New(
            ctypes.cast(ctypes.pointer(root_schema), ctypes.c_void_p),
            b'arrow_schema',
            ctypes.cast(schema_capsule_destructor, ctypes.c_void_p),
        )
        array_capsule = PyCapsule_New(
            ctypes.cast(ctypes.pointer(root_array), ctypes.c_void_p),
            b'arrow_array',
            ctypes.cast(array_capsule_destructor, ctypes.c_void_p),
        )
        keep_alive += (
            schema_capsule_destructor,
            array_capsule_destructor,
            schema_capsule,
            array_capsule,
        )
        return _CapsuleOnly(schema_capsule, array_capsule), releases, keep_alive

    if protocol != 'stream':
        raise AssertionError(f'unexpected Arrow-C protocol {protocol!r}')

    state = {'done': False}
    stream = ArrowArrayStream()

    @ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p)
    def get_schema(_stream: int, output: int) -> int:
        ctypes.memmove(output, ctypes.byref(root_schema), ctypes.sizeof(root_schema))
        # Move the schema tree to the callback output without releasing it.
        root_schema.release = None
        return 0

    @ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p)
    def get_next(_stream: int, output: int) -> int:
        if state['done']:
            ctypes.memset(output, 0, ctypes.sizeof(ArrowArray))
            return 0
        state['done'] = True
        ctypes.memmove(output, ctypes.byref(root_array), ctypes.sizeof(root_array))
        # Move the array tree to the callback output without releasing it.
        root_array.release = None
        return 0

    @ctypes.CFUNCTYPE(ctypes.c_char_p, ctypes.c_void_p)
    def get_last_error(_stream: int) -> None:
        return None

    @ctypes.CFUNCTYPE(None, ctypes.c_void_p)
    def release_stream(pointer: int) -> None:
        stream_owner = ArrowArrayStream.from_address(pointer)
        releases['stream'] += 1
        if root_schema.release:
            release_schema(ctypes.addressof(root_schema))
        if root_array.release:
            release_array(ctypes.addressof(root_array))
        stream_owner.release = None

    stream.get_schema = ctypes.cast(get_schema, ctypes.c_void_p)
    stream.get_next = ctypes.cast(get_next, ctypes.c_void_p)
    stream.get_last_error = ctypes.cast(get_last_error, ctypes.c_void_p)
    stream.release = ctypes.cast(release_stream, ctypes.c_void_p)
    stream.private_data = None

    @ctypes.CFUNCTYPE(None, ctypes.c_void_p)
    def stream_capsule_destructor(capsule: int) -> None:
        pointer = PyCapsule_GetPointer(capsule, b'arrow_array_stream')
        if pointer and ArrowArrayStream.from_address(pointer).release:
            release_stream(pointer)

    stream_capsule = PyCapsule_New(
        ctypes.cast(ctypes.pointer(stream), ctypes.c_void_p),
        b'arrow_array_stream',
        ctypes.cast(stream_capsule_destructor, ctypes.c_void_p),
    )

    class _StreamCapsuleOnly:
        __slots__ = ('_capsule',)

        def __init__(self, capsule: object) -> None:
            self._capsule = capsule

        def __arrow_c_stream__(self, requested_schema=None):
            del requested_schema
            return self._capsule

    keep_alive += (
        state,
        stream,
        get_schema,
        get_next,
        get_last_error,
        release_stream,
        stream_capsule_destructor,
        stream_capsule,
    )
    return _StreamCapsuleOnly(stream_capsule), releases, keep_alive


@pytest.mark.parametrize(
    'protocol',
    ['array', 'stream'],
    ids=['arrow-c-array', 'arrow-c-stream'],
)
def test_arrow_c_selected_struct_child_count_matches_schema(protocol: str) -> None:
    """Selected struct roots reject mismatched schema/array counts and release once."""
    invalid, invalid_releases, _keepinvalid = _selected_struct_child_count_provider(
        protocol, array_children=2
    )
    # Schema callbacks cover root + point struct + x/y; the extra array child
    # makes the mismatched array tree one callback longer than the valid tree.
    with pytest.raises(TypeError, match=r'n_children.*schema'):
        gm.from_arrow(invalid)
    assert invalid_releases == {
        'schema': 4,
        'array': 5,
        'stream': int(protocol == 'stream'),
    }

    valid, valid_releases, _keepvalid = _selected_struct_child_count_provider(
        protocol, array_children=1
    )
    assert gm.from_arrow(valid).to_wkt() == ['POINT (1 2)']
    assert valid_releases == {
        'schema': 4,
        'array': 4,
        'stream': int(protocol == 'stream'),
    }


def _schema_child_table_provider(
    protocol: str,
    *,
    n_children: int,
    children_present: bool,
    extension: bool,
    child_entry_present: bool = True,
):
    """Retain a foreign schema shell around a configurable child-table boundary."""
    ctypes, ArrowSchema, ArrowArray, ArrowArrayStream = _f5_arrow_c_structs()
    releases = {'schema': 0, 'array': 0, 'stream': 0, 'get_next': 0}

    @ctypes.CFUNCTYPE(None, ctypes.c_void_p)
    def release_schema(pointer: int) -> None:
        schema = ArrowSchema.from_address(pointer)
        releases['schema'] += 1
        # This malformed table is deliberately not traversed during release.
        schema.release = None

    @ctypes.CFUNCTYPE(None, ctypes.c_void_p)
    def release_array(pointer: int) -> None:
        array = ArrowArray.from_address(pointer)
        releases['array'] += 1
        array.release = None

    metadata = None
    if extension:
        key = b'ARROW:extension:name'
        value = b'geoarrow.point'
        metadata_blob = (
            struct.pack('=i', 1)
            + struct.pack('=i', len(key))
            + key
            + struct.pack('=i', len(value))
            + value
        )
        metadata = (ctypes.c_uint8 * len(metadata_blob))(*metadata_blob)
    placeholder = ArrowSchema(b'g', b'placeholder', None, 0, 0, None, None, None, None)
    placeholder_children = (ctypes.POINTER(ArrowSchema) * 1)(
        ctypes.pointer(placeholder) if child_entry_present else None
    )
    root_schema = ArrowSchema(
        b'+s',
        b'geometry' if extension else b'',
        ctypes.cast(metadata, ctypes.c_void_p) if metadata is not None else None,
        0,
        n_children,
        (
            ctypes.cast(
                placeholder_children, ctypes.POINTER(ctypes.POINTER(ArrowSchema))
            )
            if children_present
            else None
        ),
        None,
        ctypes.cast(release_schema, ctypes.c_void_p),
        None,
    )

    array_buffers = (ctypes.c_void_p * 1)(None)
    root_array = ArrowArray(
        0,
        0,
        0,
        1,
        0,
        ctypes.cast(array_buffers, ctypes.c_void_p),
        None,
        None,
        ctypes.cast(release_array, ctypes.c_void_p),
        None,
    )

    PyCapsule_New = ctypes.pythonapi.PyCapsule_New
    PyCapsule_GetPointer = ctypes.pythonapi.PyCapsule_GetPointer
    PyCapsule_New.restype = ctypes.py_object
    PyCapsule_New.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
    PyCapsule_GetPointer.restype = ctypes.c_void_p
    PyCapsule_GetPointer.argtypes = [ctypes.c_void_p, ctypes.c_char_p]

    @ctypes.CFUNCTYPE(None, ctypes.c_void_p)
    def schema_capsule_destructor(capsule: int) -> None:
        pointer = PyCapsule_GetPointer(capsule, b'arrow_schema')
        if pointer and ArrowSchema.from_address(pointer).release:
            release_schema(pointer)

    @ctypes.CFUNCTYPE(None, ctypes.c_void_p)
    def array_capsule_destructor(capsule: int) -> None:
        pointer = PyCapsule_GetPointer(capsule, b'arrow_array')
        if pointer and ArrowArray.from_address(pointer).release:
            release_array(pointer)

    # Keep all source structures and callbacks alive, but not the capsules:
    # dropping the provider below must run each remaining capsule destructor.
    keep_alive = (
        metadata,
        placeholder,
        placeholder_children,
        root_schema,
        array_buffers,
        root_array,
        release_schema,
        release_array,
        schema_capsule_destructor,
        array_capsule_destructor,
    )
    if protocol == 'array':
        schema_capsule = PyCapsule_New(
            ctypes.cast(ctypes.pointer(root_schema), ctypes.c_void_p),
            b'arrow_schema',
            ctypes.cast(schema_capsule_destructor, ctypes.c_void_p),
        )
        array_capsule = PyCapsule_New(
            ctypes.cast(ctypes.pointer(root_array), ctypes.c_void_p),
            b'arrow_array',
            ctypes.cast(array_capsule_destructor, ctypes.c_void_p),
        )
        return _CapsuleOnly(schema_capsule, array_capsule), releases, keep_alive

    if protocol != 'stream':
        raise AssertionError(f'unexpected Arrow-C protocol {protocol!r}')

    stream = ArrowArrayStream()

    @ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p)
    def get_schema(_stream: int, output: int) -> int:
        ctypes.memmove(output, ctypes.byref(root_schema), ctypes.sizeof(root_schema))
        root_schema.release = None
        return 0

    @ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p)
    def get_next(_stream: int, output: int) -> int:
        releases['get_next'] += 1
        ctypes.memset(output, 0, ctypes.sizeof(ArrowArray))
        return 0

    @ctypes.CFUNCTYPE(ctypes.c_char_p, ctypes.c_void_p)
    def get_last_error(_stream: int) -> None:
        return None

    @ctypes.CFUNCTYPE(None, ctypes.c_void_p)
    def release_stream(pointer: int) -> None:
        stream_owner = ArrowArrayStream.from_address(pointer)
        releases['stream'] += 1
        stream_owner.release = None

    stream.get_schema = ctypes.cast(get_schema, ctypes.c_void_p)
    stream.get_next = ctypes.cast(get_next, ctypes.c_void_p)
    stream.get_last_error = ctypes.cast(get_last_error, ctypes.c_void_p)
    stream.release = ctypes.cast(release_stream, ctypes.c_void_p)
    stream.private_data = None

    @ctypes.CFUNCTYPE(None, ctypes.c_void_p)
    def stream_capsule_destructor(capsule: int) -> None:
        pointer = PyCapsule_GetPointer(capsule, b'arrow_array_stream')
        if pointer and ArrowArrayStream.from_address(pointer).release:
            release_stream(pointer)

    stream_capsule = PyCapsule_New(
        ctypes.cast(ctypes.pointer(stream), ctypes.c_void_p),
        b'arrow_array_stream',
        ctypes.cast(stream_capsule_destructor, ctypes.c_void_p),
    )
    keep_alive += (
        stream,
        get_schema,
        get_next,
        get_last_error,
        release_stream,
        stream_capsule_destructor,
    )

    class _StreamCapsuleOnly:
        __slots__ = ('_capsule',)

        def __init__(self, capsule: object) -> None:
            self._capsule = capsule

        def __arrow_c_stream__(self, requested_schema=None):
            del requested_schema
            return self._capsule

    return _StreamCapsuleOnly(stream_capsule), releases, keep_alive


@pytest.mark.parametrize(
    'protocol',
    ['array', 'stream'],
    ids=['arrow-c-array', 'arrow-c-stream'],
)
def test_arrow_c_foreign_schema_child_table_span_overflow_is_type_error(
    protocol: str,
) -> None:
    """A pointer table beyond Rust's byte-span bound is rejected before traversal."""
    import gc

    child_count = sys.maxsize // ctypes.sizeof(ctypes.c_void_p) + 1
    provider, releases, _keepalive = _schema_child_table_provider(
        protocol,
        n_children=child_count,
        children_present=True,
        extension=True,
    )
    with pytest.raises(
        TypeError, match='Arrow schema n_children is negative or too large'
    ):
        gm.from_arrow(provider)

    del provider
    gc.collect()
    assert releases == {
        'schema': 1,
        'array': int(protocol == 'array'),
        'stream': int(protocol == 'stream'),
        'get_next': 0,
    }


@pytest.mark.parametrize(
    'protocol',
    ['array', 'stream'],
    ids=['arrow-c-array', 'arrow-c-stream'],
)
@pytest.mark.parametrize(
    ('n_children', 'children_present', 'child_entry_present', 'message'),
    [
        pytest.param(
            -1,
            False,
            True,
            'Arrow schema n_children is negative or too large',
            id='negative-count',
        ),
        pytest.param(
            1,
            False,
            True,
            'Arrow schema children pointer is null while n_children > 0',
            id='nonzero-count-null-table',
        ),
        pytest.param(
            0,
            False,
            True,
            'Arrow table or record batch has no columns',
            id='zero-count-null-table',
        ),
        pytest.param(
            1,
            True,
            False,
            'Arrow child schema is null',
            id='null-child-entry',
        ),
    ],
)
def test_arrow_c_schema_child_table_admission_preserves_count_and_null_rules(
    protocol: str,
    n_children: int,
    children_present: bool,
    child_entry_present: bool,
    message: str,
) -> None:
    import gc

    provider, releases, _keepalive = _schema_child_table_provider(
        protocol,
        n_children=n_children,
        children_present=children_present,
        extension=False,
        child_entry_present=child_entry_present,
    )
    with pytest.raises(TypeError, match=message):
        gm.from_arrow(provider)

    del provider
    gc.collect()
    assert releases == {
        'schema': 1,
        'array': int(protocol == 'array'),
        'stream': int(protocol == 'stream'),
        'get_next': 0,
    }
