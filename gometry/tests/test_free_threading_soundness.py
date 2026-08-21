"""Free-threading soundness: concurrent mutation of list fancy-index and PEP-3118 buffers.

These tests prove the two v1 free-threading blockers stay fixed:

1. Exact-list fancy indexing must not read past a list that another thread
   shrinks (no length-snapshot + unsynchronized ``get_item_unchecked``).
2. Numeric / one-byte PEP-3118 buffers must not form Rust ``&[T]`` over memory
   another thread may mutate (ReadOnlyCell / copy-on-mutable).

On a free-threaded build (``sys._is_gil_enabled() is False``) a mutator thread
races the reader. On a GIL-enabled build the concurrent cases **skip** with an
explicit free-threading reason so they never pass as a false free-threading
proof. Deterministic single-thread safe-path tests always run.
"""

from __future__ import annotations

import sys
import threading
import time
from typing import Any

import gometry as gm
import numpy as np
import pytest

_BARRIER_TIMEOUT = 30.0
_JOIN_TIMEOUT = 60.0
_STRESS_SECONDS = 0.35


def _gil_enabled() -> bool | None:
    probe = getattr(sys, '_is_gil_enabled', None)
    if probe is None:
        return None
    return bool(probe())


def _require_free_threading() -> None:
    gil = _gil_enabled()
    if gil is None:
        pytest.skip('sys._is_gil_enabled unavailable on this interpreter')
    if gil is True:
        pytest.skip(
            'free-threading soundness stress requires CPython with the GIL disabled '
            '(e.g. 3.14t); this build has the GIL enabled'
        )


# ---------------------------------------------------------------------------
# Deterministic safe-path coverage (always on — GIL or free-threaded)
# ---------------------------------------------------------------------------


def test_exact_list_and_tuple_fancy_index_all_receivers() -> None:
    """Owned-snapshot list path and immutable-tuple path for every receiver family."""
    geom = gm.GeometryArray([gm.Point(float(i), float(i)) for i in range(8)])
    cell = gm.H3Cell('8928308280fffff')
    cells = gm.CellArray([cell, cell.parent(resolution=8), cell, cell], type=gm.H3Cell)
    edges = gm.H3EdgeArray(list(cell.edges))
    verts = gm.H3VertexArray(list(cell.vertices))

    receivers: list[tuple[str, Any]] = [
        ('GeometryArray', geom),
        ('CellArray', cells),
        ('H3EdgeArray', edges),
        ('H3VertexArray', verts),
    ]
    for name, arr in receivers:
        n = len(arr)
        idx_list = list(range(0, n, 2))
        mask_list = [i % 2 == 0 for i in range(n)]
        via_list_idx = arr[idx_list]
        via_tuple_idx = arr[tuple(idx_list)]
        via_list_mask = arr[mask_list]
        via_tuple_mask = arr[tuple(mask_list)]
        assert len(via_list_idx) == len(via_tuple_idx) == (n + 1) // 2, name
        assert len(via_list_mask) == len(via_tuple_mask) == (n + 1) // 2, name
        # Mixed / non-bool-leading list stays invalid (raises IndexError/TypeError path).
        with pytest.raises((TypeError, IndexError, ValueError, gm.GeometryError)):
            _ = arr[[0, True]]  # type: ignore[list-item]


def test_buffer_ingest_shapes_deterministic() -> None:
    """Contiguous / strided / N x D / bytes / bytearray / memoryview all parse correctly."""
    n = 64
    x = np.arange(n, dtype=np.float64)
    y = np.arange(n, dtype=np.float64) * 0.25
    pts = gm.points(x, y)
    assert len(pts) == n
    assert float(pts[0].x) == 0.0
    assert float(pts[-1].y) == pytest.approx((n - 1) * 0.25)

    # Strided 1-D columns from a C-order (n, 2) matrix: each column view has
    # stride = 2 * itemsize (not unit-strided contiguous).
    storage = np.zeros((n, 2), dtype=np.float64)
    storage[:, 0] = x
    storage[:, 1] = y
    xs = storage[:, 0]
    ys = storage[:, 1]
    assert not xs.flags['C_CONTIGUOUS']
    assert xs.strides[0] == 16, (
        f'expected strided column (stride 16 for float64 pair), got {xs.strides}'
    )
    pts_s = gm.points(xs, ys)
    assert len(pts_s) == n
    assert float(pts_s[3].x) == 3.0

    # N x D LineString constructor.
    nd = np.column_stack([x, y])
    line = gm.LineString(nd)
    assert len(line.coords) == n

    pt = gm.Point(1.0, 2.0)
    wkb = pt.to_wkb()
    assert gm.from_wkb(wkb) == pt
    assert gm.from_wkb(bytearray(wkb)) == pt
    assert gm.from_wkb(memoryview(wkb)) == pt
    assert gm.from_wkb(memoryview(bytearray(wkb))) == pt


@pytest.mark.parametrize(
    'shape',
    [
        pytest.param('contiguous', id='buffer-contiguous-f64-ndarray'),
        pytest.param('strided', id='buffer-strided-f64-ndarray'),
        pytest.param('nd', id='buffer-NxD-f64-ndarray'),
    ],
)
def test_mutable_numeric_buffers_are_snapshotted(shape: str) -> None:
    """A writable exporter is copied at capture — neither aliased nor refused.

    The paths that would form a Rust slice over the exporter take CPython's
    contiguous copy when its storage may change under them, so ingest is a
    snapshot and a later write cannot reach an already-built geometry. Refusing
    the input instead would make NumPy, the one required runtime dependency,
    unusable on the supported ``cp314t`` target.
    """
    n = 256
    if shape == 'contiguous':
        x = np.arange(n, dtype=np.float64)
        y = np.arange(n, dtype=np.float64) * 0.5
        built = gm.points(x, y)
        before = [point.x for point in built[:4]]
        x[:4] = -1.0
        assert [point.x for point in built[:4]] == before

    elif shape == 'strided':
        storage = np.zeros((n, 2), dtype=np.float64)
        storage[:, 0] = np.arange(n, dtype=np.float64)
        storage[:, 1] = np.arange(n, dtype=np.float64) * 0.5
        built = gm.points(storage[:, 0], storage[:, 1])
        before = [point.x for point in built[:4]]
        storage[:4, 0] = -1.0
        assert [point.x for point in built[:4]] == before

    else:
        nd = np.column_stack([
            np.arange(n, dtype=np.float64),
            np.arange(n, dtype=np.float64) * 0.5,
        ])
        built = gm.LineString(nd)
        before = str(built)
        nd[0, 0] = -1.0
        assert str(built) == before


def test_readonly_facade_over_mutable_buffer_is_snapshotted() -> None:
    """A readonly memoryview is a facade: the storage behind it still moves.

    It carries no immutable provenance, so it takes the copying path — which is
    a snapshot, not an error.
    """
    storage = np.arange(32, dtype=np.float64)
    view = memoryview(storage).toreadonly()
    built = gm.points(view, view)
    before = [point.x for point in built[:4]]
    storage[:4] = -1.0
    assert [point.x for point in built[:4]] == before


@pytest.mark.parametrize('format', ['d', 'Q'])
def test_free_threaded_bytes_backed_typed_memoryview_is_accepted(format: str) -> None:
    """Exact built-in memoryview views over bytes have immutable provenance."""
    _require_free_threading()
    import struct

    endian = '<' if sys.byteorder == 'little' else '>'
    if format == 'd':
        view = memoryview(struct.pack(f'{endian}2d', 1.5, -2.25)).cast('d')
        arr = gm.points(view, view)
        assert [point.x for point in arr] == [1.5, -2.25]
    else:
        cell = gm.S2Cell(0, 0, level=1)
        view = memoryview(struct.pack(f'{endian}Q', cell.id)).cast('Q')
        arr = gm.CellArray(view, type=gm.S2Cell)
        assert arr[0].id == cell.id


def test_uint64_buffer_is_snapshotted() -> None:
    """The cell and H3 u64 paths capture through ``tobytes()``, so they own their ids."""
    cell = gm.H3Cell('8928308280fffff')
    ids = np.array([cell.id], dtype=np.uint64)
    built = gm.CellArray(ids, type=gm.H3Cell)
    ids[0] = 0
    assert built[0].id == cell.id


# ---------------------------------------------------------------------------
# Concurrent stress (free-threaded only)
# ---------------------------------------------------------------------------


def _run_mutator_reader(
    reader: Any,
    mutator: Any,
    *,
    seconds: float = _STRESS_SECONDS,
) -> list[str]:
    """Run mutator + reader concurrently; return error messages."""
    _require_free_threading()
    barrier = threading.Barrier(2)
    stop = threading.Event()
    errors: list[str] = []
    errors_lock = threading.Lock()

    def wrap(name: str, fn: Any) -> None:
        try:
            barrier.wait(timeout=_BARRIER_TIMEOUT)
            deadline = time.monotonic() + seconds
            while time.monotonic() < deadline and not stop.is_set():
                fn()
        except Exception as exc:  # pragma: no cover - surfaced via errors
            with errors_lock:
                errors.append(f'{name}: {type(exc).__name__}: {exc}')
            stop.set()

    t_mut = threading.Thread(target=wrap, args=('mutator', mutator), name='ft-mutator')
    t_read = threading.Thread(target=wrap, args=('reader', reader), name='ft-reader')
    t_mut.start()
    t_read.start()
    t_mut.join(timeout=_JOIN_TIMEOUT)
    t_read.join(timeout=_JOIN_TIMEOUT)
    stop.set()
    if t_mut.is_alive() or t_read.is_alive():
        errors.append('thread did not finish (possible hang/deadlock)')
    return errors


@pytest.mark.parametrize(
    'receiver_kind',
    [
        pytest.param('GeometryArray', id='receiver-GeometryArray'),
        pytest.param('CellArray', id='receiver-CellArray'),
        pytest.param('H3EdgeArray', id='receiver-H3EdgeArray'),
        pytest.param('H3VertexArray', id='receiver-H3VertexArray'),
    ],
)
def test_concurrent_list_fancy_index_mutation(receiver_kind: str) -> None:
    """Mutator shrinks/extends a shared index list while the reader fancy-indexes.

    Without the owned-snapshot fix, free-threaded CPython can observe
    length+unchecked OOB reads (SIGSEGV / abort / corrupt results). This race is
    **probabilistic** without a sanitizer — a green run does not prove absence of
    the old bug; the deterministic companion tests pin the safe path, and a
    deliberate revert of the list snapshot reintroduces the unchecked loop.
    """
    _require_free_threading()

    if receiver_kind == 'GeometryArray':
        arr: Any = gm.GeometryArray([gm.Point(float(i), float(i)) for i in range(64)])
    elif receiver_kind == 'CellArray':
        cell = gm.H3Cell('8928308280fffff')
        arr = gm.CellArray([cell] * 64, type=gm.H3Cell)
    elif receiver_kind == 'H3EdgeArray':
        edges = list(gm.H3Cell('8928308280fffff').edges)
        arr = gm.H3EdgeArray(edges * 11)  # 66
        arr = arr[:64]
    else:
        verts = list(gm.H3Cell('8928308280fffff').vertices)
        arr = gm.H3VertexArray(verts * 11)
        arr = arr[:64]

    n = len(arr)
    # Shared mutable list used as fancy index — starts valid, mutator thrashing size.
    index_list: list[int] = list(range(n))
    mask_list: list[bool] = [True] * n

    def mutator() -> None:
        # Shrink and regrow so a concurrent unchecked read past the new length races.
        if len(index_list) > 1:
            del index_list[-1]
            del mask_list[-1]
        else:
            index_list[:] = list(range(n))
            mask_list[:] = [True] * n
        # Occasional clear → empty fancy index.
        if len(index_list) < n // 4:
            index_list.clear()
            mask_list.clear()
            index_list.extend(range(n))
            mask_list.extend([True] * n)

    def reader() -> None:
        # Either path is fine: success, or a clean Python error if the list
        # became empty/inconsistent mid-call. Memory unsafety must not happen.
        try:
            _ = arr[index_list]
        except (IndexError, TypeError, ValueError, gm.GeometryError):
            pass
        try:
            _ = arr[mask_list]
        except (IndexError, TypeError, ValueError, gm.GeometryError):
            pass

    errors = _run_mutator_reader(reader, mutator)
    if errors:
        pytest.fail('\n'.join(errors))


@pytest.mark.parametrize(
    'shape',
    [
        pytest.param('columns', id='buffer-columns-f64-ndarray'),
        pytest.param('nd', id='buffer-NxD-f64-ndarray'),
    ],
)
def test_concurrent_numeric_buffer_mutation(shape: str) -> None:
    """A mutator rewrites the source array while the reader captures from it.

    Refusing writable exporters left this race with no coverage at all: the
    reader could only raise. A torn snapshot is an acceptable outcome for a
    caller that mutates its own input mid-call; memory unsafety is not.
    """
    _require_free_threading()

    n = 256
    xs = np.arange(n, dtype=np.float64)
    ys = np.arange(n, dtype=np.float64) * 0.5
    nd = np.column_stack([xs, ys])

    def mutator() -> None:
        # Slice assignment writes through; `xs += 1.0` would rebind a local.
        xs[:] += 1.0
        ys[:] -= 0.5
        nd[:, 0] += 1.0

    if shape == 'columns':

        def reader() -> None:
            _ = gm.points(xs, ys)

    else:

        def reader() -> None:
            _ = gm.LineString(nd)

    errors = _run_mutator_reader(reader, mutator)
    if errors:
        pytest.fail('\n'.join(errors))


def test_proj_info_snapshot_survives_concurrent_reconfigure() -> None:
    """``crs_engine()`` must never expose PROJ's mutable global strings.

    ``proj_info()`` returns pointers that alias libPROJ's own mutable globals: a
    concurrent ``crs_configure`` rewrites ``version`` and frees the previously
    reported search path. gometry copies that struct's strings into owned
    storage under the lock at acquisition, so a reader can only ever observe a
    complete snapshot.

    Reverting that copy (holding the raw ``PJ_INFO`` pointers and reading them
    after the lock is released) turns this red with empty version strings and
    freed search-path bytes.
    """
    _require_free_threading()

    stop = threading.Event()
    anomalies: list[tuple[str, str]] = []

    def reader() -> None:
        while not stop.is_set():
            info = gm.crs_engine()
            version = info['version']
            if not isinstance(version, str) or not version.strip():
                anomalies.append(('empty version', repr(version)))
            search_path = info['search_path']
            if not isinstance(search_path, str):
                anomalies.append(('corrupt search_path', repr(search_path)))

    def reconfigure() -> None:
        try:
            for index in range(200):
                gm.crs_configure(
                    search_paths=[f'gometry-ft-proj-{index}']
                )  # synthetic path for race only
        finally:
            stop.set()

    readers = [threading.Thread(target=reader) for _ in range(6)]
    writer = threading.Thread(target=reconfigure)
    for thread in readers:
        thread.start()
    writer.start()
    writer.join(_JOIN_TIMEOUT)
    for thread in readers:
        thread.join(_JOIN_TIMEOUT)
    gm.crs_clear_cache()

    assert anomalies == []
