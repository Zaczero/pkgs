"""Non-native-endian numeric buffer ingest must not silently corrupt values.

D03: f64 buffers (`>f8` on little-endian) through the typed-buffer fast paths
used by `points`, `with_coordinates`, and `crs_transform`.
D04: u64 id buffers for CellArray / H3 vertex / H3 edge.
D01: Arrow stream `read_all` errors must propagate (no silent empty array).
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from typing import TYPE_CHECKING

import gometry as gm
import numpy as np
import pytest

if TYPE_CHECKING:
    from pathlib import Path


def test_big_endian_float64_points_match_native() -> None:
    """Exact repro: BE f64 columns into points must equal LE values bit-exactly."""
    x_be = np.array([1.0], dtype='>f8')
    y_be = np.array([2.0], dtype='>f8')
    x_le = np.array([1.0], dtype='<f8')
    y_le = np.array([2.0], dtype='<f8')

    be = gm.points(x_be, y_be)[0]
    le = gm.points(x_le, y_le)[0]
    assert be.x == 1.0
    assert be.y == 2.0
    assert be.x == le.x and be.y == le.y


def test_big_endian_float64_with_coordinates_and_crs_transform() -> None:
    x_be = np.array([1.0], dtype='>f8')
    y_be = np.array([2.0], dtype='>f8')
    x_le = np.array([1.0], dtype='<f8')
    y_le = np.array([2.0], dtype='<f8')

    pt = gm.Point(0, 0).with_coordinates(x=x_be, y=y_be)
    assert pt.x == 1.0 and pt.y == 2.0

    out_be = gm.crs_transform(4326, 4326, x_be, y_be)
    out_le = gm.crs_transform(4326, 4326, x_le, y_le)
    np.testing.assert_array_equal(out_be, out_le)
    assert float(out_be[0, 0]) == 1.0
    assert float(out_be[0, 1]) == 2.0


def test_native_endian_float64_unchanged() -> None:
    native = '>' if sys.byteorder == 'big' else '<'
    x = np.array([3.5, -7.25], dtype=f'{native}f8')
    y = np.array([9.0, 0.125], dtype=f'{native}f8')
    arr = gm.points(x, y)
    assert arr[0].x == 3.5 and arr[0].y == 9.0
    assert arr[1].x == -7.25 and arr[1].y == 0.125


def test_zero_dimensional_float64_buffers_remain_single_coordinate_columns() -> None:
    points = gm.points(np.array(1.25), [2.5, 3.5])
    assert points.to_wkt() == ['POINT (1.25 2.5)', 'POINT (1.25 3.5)']


@pytest.mark.parametrize(
    ('dtype', 'value', 'expected'),
    [
        (
            np.float64,
            1.25,
            ['POINT (1.25 2.5)', 'POINT (1.25 3.5)'],
        ),
        (
            np.int64,
            1,
            ['POINT (1 2.5)', 'POINT (1 3.5)'],
        ),
        (
            object,
            1.25,
            ['POINT (1.25 2.5)', 'POINT (1.25 3.5)'],
        ),
    ],
    ids=['float64', 'int64', 'object-float'],
)
def test_zero_dimensional_ndarray_subclass_broadcasts_as_scalar(
    dtype: object,
    value: float,
    expected: list[str],
) -> None:
    class ScalarArray(np.ndarray):
        pass

    x = np.array(value, dtype=dtype).view(ScalarArray)
    assert gm.points(x, [2.5, 3.5]).to_wkt() == expected


@pytest.mark.skipif(
    sys.version_info < (3, 12),
    reason='Python-level buffer protocol overrides require Python 3.12',
)
def test_zero_dimensional_ndarray_subclass_buffer_exception_keeps_identity() -> None:
    error = RuntimeError('ndarray subclass buffer sentinel')

    class RaisingBufferArray(np.ndarray):
        def __buffer__(self, flags: int) -> memoryview:
            raise error

    x = np.array(1.25).view(RaisingBufferArray)
    with pytest.raises(RuntimeError) as excinfo:
        gm.points(x, [2.5, 3.5])

    assert excinfo.value is error


def test_big_endian_misread_would_be_wrong_without_swap() -> None:
    """Sanity: raw BE→host reinterpret of 1.0 is not 1.0; our path must still yield 1.0."""
    import struct

    misread = struct.unpack(
        '<d' if sys.byteorder == 'little' else '>d', struct.pack('>d', 1.0)
    )[0]
    if sys.byteorder == 'little':
        assert misread != 1.0
    x = np.array([1.0], dtype='>f8')
    y = np.array([2.0], dtype='>f8')
    p = gm.points(x, y)[0]
    assert p.x == 1.0 and p.y == 2.0


def test_big_endian_u64_cell_array_matches_native() -> None:
    """Exact repro: CellArray from `>u8` ids must equal native-endian ids."""
    cell = gm.S2Cell(0, 0, level=1)
    be = gm.CellArray(np.array([cell.id], dtype='>u8'), type=gm.S2Cell)
    le = gm.CellArray(np.array([cell.id], dtype='<u8'), type=gm.S2Cell)
    assert be[0].id == cell.id
    assert le[0].id == cell.id
    assert be[0].id == le[0].id


def test_big_endian_h3_vertex_and_edge_arrays_accepted() -> None:
    """BE u64 vertex/edge id arrays must decode, not over-reject as invalid ids."""
    cell = gm.H3Cell(0x8928308280FFFFF)
    vertex = next(iter(cell.vertices))
    edge = next(iter(cell.edges))

    for entity, id_, ctor in (
        ('vertex', vertex.id, gm.H3VertexArray),
        ('edge', edge.id, gm.H3EdgeArray),
    ):
        be = ctor(np.array([id_], dtype='>u8'))
        le = ctor(np.array([id_], dtype='<u8'))
        assert be[0].id == id_, entity
        assert le[0].id == id_, entity
        assert be[0].id == le[0].id, entity


def test_native_endian_u64_unchanged() -> None:
    native = '>' if sys.byteorder == 'big' else '<'
    cell = gm.S2Cell(0, 0, level=2)
    arr = gm.CellArray(np.array([cell.id], dtype=f'{native}u8'), type=gm.S2Cell)
    assert arr[0].id == cell.id


@pytest.mark.parametrize(
    ('case', 'pep_format', 'consumer', 'expected'),
    [
        ('negative_len', 'd', 'line_column', 'BufferError'),
        ('partial_item', 'd', 'line_column', 'BufferError'),
        ('negative_shape', 'd', 'line_column', 'BufferError'),
        ('negative_ndim', 'd', 'line_column', 'BufferError'),
        ('shape_mismatch', 'd', 'line_matrix', 'BufferError'),
        ('shape_product_overflow', 'd', 'line_matrix', 'BufferError'),
        ('negative_len', 'd', 'points_fast_path', 'BufferError'),
        ('null_buffer', 'd', 'line_column', 'BufferError'),
        ('null_strided', 'd', 'line_strided', 'BufferError'),
        ('null_matrix', 'd', 'line_matrix', 'BufferError'),
        ('null_buffer', 'd', 'crs_transform', 'BufferError'),
        ('negative_len', 'B', 'wkb', 'BufferError'),
        ('negative_len', 'b', 'wkb', 'BufferError'),
        ('null_buffer', 'B', 'wkb', 'BufferError'),
        ('negative_len', 'Q', 'cell_ids', 'BufferError'),
        ('negative_len', 'Q', 'h3_ids', 'BufferError'),
        ('null_buffer', 'Q', 'cell_ids', 'BufferError'),
        ('null_buffer', 'Q', 'h3_ids', 'BufferError'),
    ],
    ids=[
        'negative-byte-length',
        'partial-f64-item',
        'negative-shape-dimension',
        'negative-dimension-count',
        'shape-count-disagrees-with-length',
        'shape-product-overflow',
        'point-buffer-fast-path',
        'null-data-f64-column',
        'null-data-strided-f64-column',
        'null-data-f64-matrix',
        'null-data-crs-coordinate',
        'one-byte-buffer-copy',
        'signed-one-byte-buffer-copy',
        'null-data-one-byte-copy',
        'cell-u64-buffer-copy',
        'h3-u64-buffer-copy',
        'null-data-cell-u64-copy',
        'null-data-h3-u64-copy',
    ],
)
def test_malformed_buffer_metadata_is_rejected_before_consumers_read(
    case: str,
    pep_format: str,
    consumer: str,
    expected: str,
    tmp_path: Path,
) -> None:
    # ruff: ignore[missing-f-string-syntax] - Source executed by the child interpreter.
    script = textwrap.dedent(
        """
        import array
        import sys

        import gometry as gm

        case, format, consumer = sys.argv[1:]
        bad = gm._lib._malformed_buffer(case, format)
        try:
            if consumer == 'line_column':
                gm.LineString(x=bad, y=[1.0])
            elif consumer == 'line_strided':
                gm.LineString(x=bad, y=[1.0, 2.0, 3.0])
            elif consumer == 'line_matrix':
                gm.LineString(bad)
            elif consumer == 'points_fast_path':
                gm.points(bad, array.array('d', [1.0]))
            elif consumer == 'crs_transform':
                gm.crs_transform(4326, 4326, bad, [1.0])
            elif consumer == 'wkb':
                gm.from_wkb(bad)
            elif consumer == 'cell_ids':
                gm.CellArray(bad, type=gm.H3Cell)
            elif consumer == 'h3_ids':
                gm.H3VertexArray(bad)
            else:
                raise SystemExit(f'unknown consumer: {consumer}')
        except BufferError as error:
            print('REJECTED BufferError', error)
        except OverflowError as error:
            print('REJECTED OverflowError', error)
        else:
            raise SystemExit('malformed buffer was accepted')
        """
    )
    result = subprocess.run(
        [sys.executable, '-c', script, case, pep_format, consumer],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=8,
        check=False,
    )

    assert result.returncode == 0, (
        f'{case}/{pep_format} through {consumer}: rc={result.returncode}\n'
        f'stdout={result.stdout!r}\nstderr={result.stderr!r}'
    )
    assert result.stdout.startswith(f'REJECTED {expected}'), result.stdout


def test_strided_byte_offset_overflow_is_rejected_before_pointer_read(
    tmp_path: Path,
) -> None:
    script = textwrap.dedent(
        """
        import sys

        import gometry as gm

        bad = gm._lib._malformed_buffer('stride_span_overflow', 'd')
        try:
            gm.LineString(x=bad, y=[1.0, 2.0, 3.0])
        except OverflowError as error:
            print('REJECTED OverflowError', error)
        else:
            raise SystemExit('unrepresentable stride was accepted')
        """
    )
    result = subprocess.run(
        [sys.executable, '-c', script],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=8,
        check=False,
    )

    assert result.returncode == 0, (
        f'rc={result.returncode}\nstdout={result.stdout!r}\nstderr={result.stderr!r}'
    )
    assert result.stdout.startswith('REJECTED OverflowError'), result.stdout


def test_empty_null_buffers_keep_empty_results(tmp_path: Path) -> None:
    script = textwrap.dedent(
        """
        import numpy as np
        import gometry as gm

        null_f64 = gm._lib._malformed_buffer('null_empty', 'd')
        null_u64 = gm._lib._malformed_buffer('null_empty', 'Q')
        null_bytes = gm._lib._malformed_buffer('null_empty', 'B')

        assert gm.LineString(x=null_f64, y=[]).to_wkt() == 'LINESTRING EMPTY'
        assert gm.LineString(x=np.array([], dtype=np.float64), y=[]).to_wkt() == (
            'LINESTRING EMPTY'
        )

        assert len(gm.CellArray(null_u64, type=gm.H3Cell)) == 0
        assert len(
            gm.CellArray(np.array([], dtype=np.uint64), type=gm.H3Cell)
        ) == 0

        def from_empty_wkb(data):
            try:
                gm.from_wkb(data)
            except Exception as error:
                return type(error).__name__, str(error)
            raise AssertionError('empty WKB unexpectedly parsed')

        expected = ('ParseError', 'invalid WKB: unexpected end of WKB')
        assert from_empty_wkb(b'') == expected
        assert from_empty_wkb(null_bytes) == expected
        print('empty buffers preserved')
        """
    )
    result = subprocess.run(
        [sys.executable, '-c', script],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=8,
        check=False,
    )

    assert result.returncode == 0, (
        f'rc={result.returncode}\nstdout={result.stdout!r}\nstderr={result.stderr!r}'
    )
    assert result.stdout.strip() == 'empty buffers preserved'


@pytest.mark.parametrize(
    'consumer',
    [
        'float_column',
        'coordinate_matrix',
        'points_constructor',
        'cell_array_ids',
        'cell_array_inferred_ids',
        'h3_array_ids',
    ],
    ids=[
        'float-column',
        'nd-matrix',
        'points-constructor',
        'cell-array-u64',
        'cell-array-inferred-u64',
        'h3-array-u64',
    ],
)
def test_typed_buffer_provider_exception_instance_is_preserved(
    consumer: str,
) -> None:
    error = RuntimeError(f'buffer exporter sentinel for {consumer}')
    bad = gm._lib._RaisingBuffer(error)
    operation = {
        'float_column': lambda: gm.LineString(x=bad, y=[0.0, 1.0]),
        'coordinate_matrix': lambda: gm.LineString(bad),
        'points_constructor': lambda: gm.points(bad, [0.0, 1.0]),
        'cell_array_ids': lambda: gm.CellArray(bad, type=gm.H3Cell),
        'cell_array_inferred_ids': lambda: gm.CellArray(bad),
        'h3_array_ids': lambda: gm.H3VertexArray(bad),
    }[consumer]
    with pytest.raises(RuntimeError) as excinfo:
        operation()
    assert excinfo.value is error


def test_byte_buffer_provider_error_is_not_misclassified_as_an_iterable() -> None:
    error = RuntimeError('byte buffer exporter sentinel')
    with pytest.raises(RuntimeError) as excinfo:
        gm.from_wkb(gm._lib._RaisingBuffer(error))
    assert excinfo.value is error


def test_erroring_arrow_stream_propagates() -> None:
    """Exact repro: read_all() failure must raise, not return an empty array."""
    pa = pytest.importorskip('pyarrow')

    arr = gm.points([1.0], [2.0], crs=4326).to_arrow()
    batch = pa.record_batch([arr], names=['geometry'])

    def batches():
        yield batch
        raise RuntimeError('boom')

    reader = pa.RecordBatchReader.from_batches(batch.schema, batches())
    with pytest.raises(RuntimeError, match='boom'):
        gm.from_arrow(reader)


def test_arrow_stream_returns_all_rows() -> None:
    pa = pytest.importorskip('pyarrow')

    arr = gm.points([1.0, 3.0], [2.0, 4.0], crs=4326).to_arrow()
    batch = pa.record_batch([arr], names=['geometry'])
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    restored = gm.from_arrow(reader)
    assert len(restored) == 2
    assert restored.to_wkt() == ['POINT (1 2)', 'POINT (3 4)']
