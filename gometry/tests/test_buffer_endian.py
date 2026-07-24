"""Non-native-endian numeric buffer ingest must not silently corrupt values.

D03: f64 buffers (`>f8` on little-endian) through the typed-buffer fast paths
used by `points`, `set_coordinates`, and `crs_transform`.
D04: u64 id buffers for CellArray / H3 vertex / H3 edge.
D01: Arrow stream `read_all` errors must propagate (no silent empty array).
"""

from __future__ import annotations

import sys

import gometry as gm
import numpy as np
import pytest


def test_d03_big_endian_f64_points_match_native() -> None:
    """Exact repro: BE f64 columns into points must equal LE values bit-exactly."""
    x_be = np.array([1.0], dtype=">f8")
    y_be = np.array([2.0], dtype=">f8")
    x_le = np.array([1.0], dtype="<f8")
    y_le = np.array([2.0], dtype="<f8")

    be = gm.points(x_be, y_be)[0]
    le = gm.points(x_le, y_le)[0]
    assert be.x == 1.0
    assert be.y == 2.0
    assert be.x == le.x and be.y == le.y


def test_d03_big_endian_f64_set_coordinates_and_crs_transform() -> None:
    x_be = np.array([1.0], dtype=">f8")
    y_be = np.array([2.0], dtype=">f8")
    x_le = np.array([1.0], dtype="<f8")
    y_le = np.array([2.0], dtype="<f8")

    pt = gm.Point(0, 0).set_coordinates(x=x_be, y=y_be)
    assert pt.x == 1.0 and pt.y == 2.0

    out_be = gm.crs_transform(4326, 4326, x_be, y_be)
    out_le = gm.crs_transform(4326, 4326, x_le, y_le)
    np.testing.assert_array_equal(out_be, out_le)
    assert float(out_be[0, 0]) == 1.0
    assert float(out_be[0, 1]) == 2.0


def test_d03_native_endian_f64_unchanged() -> None:
    native = ">" if sys.byteorder == "big" else "<"
    x = np.array([3.5, -7.25], dtype=f"{native}f8")
    y = np.array([9.0, 0.125], dtype=f"{native}f8")
    arr = gm.points(x, y)
    assert arr[0].x == 3.5 and arr[0].y == 9.0
    assert arr[1].x == -7.25 and arr[1].y == 0.125


def test_d03_big_endian_misread_would_be_wrong_without_swap() -> None:
    """Sanity: raw BE→host reinterpret of 1.0 is not 1.0; our path must still yield 1.0."""
    import struct

    misread = struct.unpack(
        "<d" if sys.byteorder == "little" else ">d", struct.pack(">d", 1.0)
    )[0]
    if sys.byteorder == "little":
        assert misread != 1.0
    x = np.array([1.0], dtype=">f8")
    y = np.array([2.0], dtype=">f8")
    p = gm.points(x, y)[0]
    assert p.x == 1.0 and p.y == 2.0


def test_d04_big_endian_u64_cell_array_matches_native() -> None:
    """Exact repro: CellArray from `>u8` ids must equal native-endian ids."""
    cell = gm.S2Cell(0, 0, level=1)
    be = gm.CellArray(np.array([cell.id], dtype=">u8"), type=gm.S2Cell)
    le = gm.CellArray(np.array([cell.id], dtype="<u8"), type=gm.S2Cell)
    assert be[0].id == cell.id
    assert le[0].id == cell.id
    assert be[0].id == le[0].id


def test_d04_big_endian_h3_vertex_and_edge_arrays_accepted() -> None:
    """BE u64 vertex/edge id arrays must decode, not over-reject as invalid ids."""
    cell = gm.H3Cell(0x8928308280FFFFF)
    vertex = next(iter(cell.vertices))
    edge = next(iter(cell.edges))

    for entity, id_, ctor in (
        ("vertex", vertex.id, gm.H3VertexArray),
        ("edge", edge.id, gm.H3EdgeArray),
    ):
        be = ctor(np.array([id_], dtype=">u8"))
        le = ctor(np.array([id_], dtype="<u8"))
        assert be[0].id == id_, entity
        assert le[0].id == id_, entity
        assert be[0].id == le[0].id, entity


def test_d04_native_endian_u64_unchanged() -> None:
    native = ">" if sys.byteorder == "big" else "<"
    cell = gm.S2Cell(0, 0, level=2)
    arr = gm.CellArray(np.array([cell.id], dtype=f"{native}u8"), type=gm.S2Cell)
    assert arr[0].id == cell.id


def test_d01_erroring_arrow_stream_propagates() -> None:
    """Exact repro: read_all() failure must raise, not return an empty array."""
    pa = pytest.importorskip("pyarrow")

    arr = gm.points([1.0], [2.0], crs=4326).to_arrow()
    batch = pa.record_batch([arr], names=["geometry"])

    def batches():
        yield batch
        raise RuntimeError("boom")

    reader = pa.RecordBatchReader.from_batches(batch.schema, batches())
    with pytest.raises(RuntimeError, match="boom"):
        gm.from_arrow(reader)


def test_d01_happy_arrow_stream_returns_all_rows() -> None:
    pa = pytest.importorskip("pyarrow")

    arr = gm.points([1.0, 3.0], [2.0, 4.0], crs=4326).to_arrow()
    batch = pa.record_batch([arr], names=["geometry"])
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    restored = gm.from_arrow(reader)
    assert len(restored) == 2
    assert restored.to_wkt() == ["POINT (1 2)", "POINT (3 4)"]
