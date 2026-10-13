use std::convert::Infallible;
use std::ops::ControlFlow;
use std::sync::Arc;

use pyo3::exceptions::PyOverflowError;
use pyo3::{PyResult, Python};

use super::equality::{borrowed_axis, packed_contiguous_coord_span};
use super::point_access::{packed_line_point_at, packed_polygon_point_at};
use super::{
    ArrayRows, CoordSeq, CoordinateAxis, CoordinateColumnRef, CoordinateOwner,
    CoordinatePointCursor, CoordinateSource, CoordinateView, Point, Shape, ShapeData,
    checked_vector_len,
};
use crate::geometry::storage::{self, GeometryArrayStorage, PackedStorage};
use crate::geometry::{CoordRun, CoordWindow, MOrdinate, ZOrdinate};

impl CoordinateView {
    pub const fn from_shape(shape: Arc<ShapeData>) -> Self {
        Self {
            owner: CoordinateOwner::Shape(shape),
        }
    }

    pub(crate) const fn from_array_rows(rows: ArrayRows) -> Self {
        Self {
            owner: CoordinateOwner::Array(rows),
        }
    }

    /// The geometry source, for topology-shaped (nested) rendering.
    pub(crate) fn source(&self) -> CoordinateSource<'_> {
        match &self.owner {
            CoordinateOwner::Shape(shape) => CoordinateSource::Shape(shape),
            CoordinateOwner::Array(rows) => CoordinateSource::Array(rows),
        }
    }

    /// Drive `visit` over every run / scalar point of the owner, dispatching
    /// scalar shapes vs array rows. The traversal primitive behind every
    /// method. Prefer [`try_for_each`] when the visitor may stop early.
    pub(crate) fn for_each<F>(&self, visit: &mut F)
    where
        F: for<'x> FnMut(Option<usize>, CoordRun<'x>),
    {
        let ControlFlow::Continue(()): ControlFlow<Infallible> =
            self.try_for_each(&mut |geometry, run| {
                visit(geometry, run);
                ControlFlow::Continue(())
            });
    }

    /// Breakable traversal: `ControlFlow::Break` stops at the current run.
    #[expect(
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        reason = "admitted polygon CSR rows bound both ring endpoints; retain direct hot-path column reads"
    )]
    fn try_for_each<B, F>(&self, visit: &mut F) -> ControlFlow<B>
    where
        F: for<'x> FnMut(Option<usize>, CoordRun<'x>) -> ControlFlow<B>,
    {
        match &self.owner {
            CoordinateOwner::Shape(shape) => shape.for_each_coord_run(&mut |run| visit(None, run)),
            CoordinateOwner::Array(array_rows) => match array_rows.storage() {
                GeometryArrayStorage::Mixed(shapes) => {
                    for (row, shape) in shapes.iter().enumerate() {
                        if array_rows
                            .missing()
                            .is_some_and(|mask| mask.is_missing(row))
                        {
                            continue;
                        }
                        shape.for_each_coord_run(&mut |run| visit(Some(row), run))?;
                    }
                    ControlFlow::Continue(())
                },
                // Packed points: each row is one scalar point geometry, so it
                // contributes one coordinate tagged with its own row index.
                GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) => {
                    let map = row_map.as_deref();
                    if array_rows.is_compact_nullable() {
                        let mask = array_rows
                            .missing()
                            .expect("compact nullable rows carry validity");
                        for (row, present) in mask.cursor() {
                            let Some(present) = present else { continue };
                            visit(
                                Some(row),
                                CoordRun::Point(
                                    coords.point_at(storage::physical_row(map, present)),
                                ),
                            )?;
                        }
                        return ControlFlow::Continue(());
                    }
                    for row in 0..storage::point_logical_len(coords, map) {
                        if array_rows
                            .missing()
                            .is_some_and(|mask| mask.is_missing(row))
                        {
                            continue;
                        }
                        visit(
                            Some(row),
                            CoordRun::Point(coords.point_at(storage::physical_row(map, row))),
                        )?;
                    }
                    ControlFlow::Continue(())
                },
                // Packed lines: each row contributes its zero-copy column
                // window as one run, exactly like a scalar LineString.
                GeometryArrayStorage::Packed(PackedStorage::Lines {
                    coords,
                    offsets,
                    row_map,
                }) => {
                    let map = row_map.as_deref();
                    let rows = storage::line_logical_len(offsets.as_slice(), map);
                    for row in 0..rows {
                        if array_rows
                            .missing()
                            .is_some_and(|mask| mask.is_missing(row))
                        {
                            continue;
                        }
                        let window = map.csr_window(offsets.as_slice(), row);
                        let view = coords.view(CoordWindow::from_range(window, coords.len()));
                        visit(Some(row), CoordRun::Seq(&view))?;
                    }
                    ControlFlow::Continue(())
                },
                // Packed polygons: walk the two CSR levels directly. Building
                // a temporary Polygon view per row would allocate its hole Arc
                // and then make `walk` traverse the same offsets again.
                GeometryArrayStorage::Packed(PackedStorage::Polygons {
                    coords,
                    ring_offsets,
                    polygon_offsets,
                    row_map,
                }) => {
                    let map = row_map.as_deref();
                    let ring_offsets = ring_offsets.as_slice();
                    let rows = storage::polygon_logical_len(polygon_offsets.as_slice(), map);
                    for row in 0..rows {
                        if array_rows
                            .missing()
                            .is_some_and(|mask| mask.is_missing(row))
                        {
                            continue;
                        }
                        for physical_ring in map.csr_window(polygon_offsets.as_slice(), row) {
                            let window = ring_offsets[physical_ring] as usize
                                ..ring_offsets[physical_ring + 1] as usize;
                            let view = coords.view(CoordWindow::from_range(window, coords.len()));
                            visit(Some(row), CoordRun::Seq(&view))?;
                        }
                    }
                    ControlFlow::Continue(())
                },
            },
        }
    }

    pub(crate) fn point_cursor(&self, reverse: bool) -> CoordinatePointCursor {
        CoordinatePointCursor::new(self.clone(), reverse)
    }

    pub(crate) fn iterator_length_hint(&self) -> usize {
        let hint = self.single_seq().map_or_else(
            || match &self.owner {
                CoordinateOwner::Shape(shape) => match shape.shape() {
                    Shape::Point(_) => 1,
                    Shape::Empty(..)
                    | Shape::MultiPoint(_)
                    | Shape::LineString(_)
                    | Shape::MultiLineString(_)
                    | Shape::Polygon(_)
                    | Shape::MultiPolygon(_)
                    | Shape::GeometryCollection(_) => 0,
                },
                CoordinateOwner::Array(rows) => match rows.storage() {
                    GeometryArrayStorage::Mixed(shapes) => shapes.len(),
                    GeometryArrayStorage::Packed(PackedStorage::Points { .. }) => {
                        rows.storage().len()
                    },
                    GeometryArrayStorage::Packed(
                        PackedStorage::Lines { coords, .. }
                        | PackedStorage::Polygons { coords, .. },
                    ) => coords.len(),
                },
            },
            |seq| seq.len(),
        );
        hint.min(isize::MAX as usize)
    }

    #[cfg(test)]
    pub(super) fn attach_test_python<T>(f: impl for<'py> FnOnce(Python<'py>) -> T) -> T {
        Python::initialize();
        Python::attach(f)
    }

    #[cfg(test)]
    pub(crate) fn column(&self, axis: CoordinateAxis) -> CoordinateColumnRef<'_> {
        Self::attach_test_python(|py| {
            self.try_column(py, axis)
                .expect("test coordinate column materializes")
        })
    }

    #[cfg(test)]
    pub fn collect_points(&self) -> Vec<Point> {
        Self::attach_test_python(|py| {
            let count = self
                .len_for_python(py)
                .expect("test coordinate count fits Python sequence length");
            let capacity = checked_vector_len::<Point>(count, "coordinate")
                .expect("test coordinate points fit Vec layout");
            self.collect_points_admitted(capacity)
        })
    }

    #[cfg(test)]
    pub fn row_index_i64(&self) -> Vec<i64> {
        Self::attach_test_python(|py| {
            self.try_row_index_i64(py)
                .expect("test coordinate row indexes materialize")
        })
    }

    /// Logical Rust-side heap retained by the view's owner.
    pub fn logical_heap_bytes(&self) -> usize {
        match &self.owner {
            CoordinateOwner::Shape(shape) => shape.shape().logical_heap_bytes(),
            CoordinateOwner::Array(rows) => rows.storage().logical_heap_bytes(),
        }
    }

    /// `Some` only when the whole view is exactly one contiguous [`CoordSeq`]
    /// (`MultiPoint`/`LineString`), enabling zero-copy columns. `None` for
    /// `Point` and any multipart view.
    pub fn single_seq(&self) -> Option<&CoordSeq> {
        match &self.owner {
            CoordinateOwner::Shape(shape) => match shape.shape() {
                Shape::MultiPoint(seq) => Some(seq),
                Shape::LineString(seq) => Some(seq.as_coords()),
                Shape::Point(_)
                | Shape::MultiLineString(_)
                | Shape::Polygon(_)
                | Shape::MultiPolygon(_)
                | Shape::GeometryCollection(_)
                | Shape::Empty(..) => None,
            },
            // A packed point array's coordinates are exactly its shared column,
            // so `.x`/`.y` can borrow it directly (zero-copy on the Rust side).
            CoordinateOwner::Array(rows) => {
                if rows.missing().is_some() {
                    return None;
                }
                match rows.storage() {
                    GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) => {
                        row_map.is_identity().then_some(coords)
                    },
                    // Packed line/polygon coordinate columns borrow only when the
                    // logical rows are the whole physical storage. Row-selected
                    // arrays materialize through `for_each`, so public buffers
                    // match logical row order and never expose excluded parent
                    // coordinates.
                    GeometryArrayStorage::Packed(
                        PackedStorage::Lines {
                            coords, row_map, ..
                        }
                        | PackedStorage::Polygons {
                            coords, row_map, ..
                        },
                    ) => row_map.is_identity().then_some(coords),
                    GeometryArrayStorage::Mixed(_) => None,
                }
            },
        }
    }

    /// Resolve one axis to a borrowed or materialized column.
    pub(crate) fn try_column(
        &self,
        py: Python<'_>,
        axis: CoordinateAxis,
    ) -> PyResult<CoordinateColumnRef<'_>> {
        if let CoordinateOwner::Array(rows) = &self.owner
            && rows.missing().is_none()
            && let GeometryArrayStorage::Packed(packed) = rows.storage()
            && let Some((coords, span)) = packed_contiguous_coord_span(packed)
        {
            return Ok(borrowed_axis(coords, span, axis));
        }
        if let CoordinateOwner::Array(rows) = &self.owner
            && let GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) =
                rows.storage()
            && rows.missing().is_none()
            && let Some(map) = row_map.explicit_indices()
        {
            let capacity = checked_vector_len::<f64>(map.len(), "coordinate column")?;
            let values = match axis {
                CoordinateAxis::X => coords.xs(),
                CoordinateAxis::Y => coords.ys(),
                CoordinateAxis::Z => {
                    let Some(zs) = coords.zs() else {
                        return Ok(CoordinateColumnRef::Missing);
                    };
                    zs
                },
                CoordinateAxis::M => {
                    let Some(ms) = coords.ms() else {
                        return Ok(CoordinateColumnRef::Missing);
                    };
                    ms
                },
            };
            let mut out = Vec::with_capacity(capacity);
            out.extend(map.iter().map(|&row| {
                *values
                    .get(row)
                    .expect("point row selection is bounded by its coordinate owner")
            }));
            return Ok(CoordinateColumnRef::Dense(out));
        }
        if let Some(seq) = self.single_seq() {
            return Ok(match axis {
                CoordinateAxis::X => CoordinateColumnRef::Borrowed(seq.xs()),
                CoordinateAxis::Y => CoordinateColumnRef::Borrowed(seq.ys()),
                CoordinateAxis::Z => seq
                    .zs()
                    .map_or(CoordinateColumnRef::Missing, CoordinateColumnRef::Borrowed),
                CoordinateAxis::M => seq
                    .ms()
                    .map_or(CoordinateColumnRef::Missing, CoordinateColumnRef::Borrowed),
            });
        }
        Ok(match axis {
            CoordinateAxis::X => {
                CoordinateColumnRef::Dense(self.gather_ordinate(py, |point| point.x)?)
            },
            CoordinateAxis::Y => {
                CoordinateColumnRef::Dense(self.gather_ordinate(py, |point| point.y)?)
            },
            CoordinateAxis::Z => self.gather_optional(py, Point::z)?,
            CoordinateAxis::M => self.gather_optional(py, Point::m)?,
        })
    }

    /// Stream every flattened coordinate without
    /// materializing a `Vec<Point>` — the engine behind the list /
    /// numpy / row-index surfaces. Use [`collect_points`](Self::collect_points)
    /// only where random access is genuinely needed (slicing).
    #[expect(
        clippy::impl_trait_in_params,
        reason = "visitor type is intentionally opaque at this one-pass traversal boundary"
    )]
    pub fn for_each_point(&self, mut visit: impl FnMut(Point)) {
        let ControlFlow::Continue(()): ControlFlow<Infallible> =
            self.try_for_each_point(&mut |coord| {
                visit(coord);
                ControlFlow::Continue(())
            });
    }

    /// Breakable point stream: `ControlFlow::Break` stops at the current
    /// coordinate. Used by `index` / membership / equality so a first hit or
    /// first mismatch never walks the tail.
    #[expect(
        clippy::impl_trait_in_params,
        reason = "visitor type is intentionally opaque at this one-pass traversal boundary"
    )]
    #[expect(
        clippy::indexing_slicing,
        reason = "CoordSeq admits equal-length ordinate columns and this loop is bounded by their shared length"
    )]
    pub fn try_for_each_point<B>(
        &self,
        visit: &mut impl FnMut(Point) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        self.try_for_each(&mut |_, run| match run {
            CoordRun::Seq(seq) => {
                // Length-exact column windows: loop bound IS each slice's len,
                // so per-vertex access carries no bounds checks (elision).
                let xs = seq.xs();
                let ys = seq.ys();
                let width = xs.len();
                let ys = &ys[..width];
                let zs = seq.zs().map(|column| &column[..width]);
                let ms = seq.ms().map(|column| &column[..width]);
                for coord in 0..width {
                    visit(Point::new_raw_axes(
                        xs[coord],
                        ys[coord],
                        ZOrdinate(zs.map(|column| column[coord])),
                        MOrdinate(ms.map(|column| column[coord])),
                    ))?;
                }
                ControlFlow::Continue(())
            },
            CoordRun::Point(point) => visit(point),
        })
    }

    /// Flattened coordinates.
    #[cfg(test)]
    pub(crate) fn collect_points_admitted(&self, capacity: usize) -> Vec<Point> {
        let mut points = Vec::with_capacity(capacity);
        self.for_each_point(|coord| points.push(coord));
        points
    }

    /// Per-coordinate geometry-row index (all `0` for a scalar geometry; the
    /// logical source row for an array owner). Backs `coords.row_index`.
    #[cfg(test)]
    pub fn row_index(&self) -> Vec<usize> {
        Self::attach_test_python(|py| {
            let count = self
                .len_for_python(py)
                .expect("test coordinates fit Python length");
            let capacity = checked_vector_len::<usize>(count, "coordinate row index")
                .expect("test row index fits Vec layout");
            self.row_index_cast(capacity, |row| row)
        })
    }

    /// Per-coordinate geometry-row index as NumPy's `int64` lane, built at
    /// coordinate-run altitude (CSR window fills / one fill per run) rather
    /// than per-vertex push. Backs the public `Coordinates.row_index` getter.
    pub(crate) fn try_row_index_i64(&self, py: Python<'_>) -> PyResult<Vec<i64>> {
        let count = self.len_for_python(py)?;
        let capacity = checked_vector_len::<i64>(count, "coordinate row index")?;
        let rows = match &self.owner {
            CoordinateOwner::Shape(_) => 1,
            CoordinateOwner::Array(rows) => rows.len(),
        };
        if rows.saturating_sub(1) > i64::MAX as usize {
            return Err(PyOverflowError::new_err(
                "coordinate row index exceeds the int64 output range",
            ));
        }
        Ok(self.row_index_cast(capacity, |row| row as i64))
    }

    /// Shared run-wise row-index emitter. Identity packed storage fills from
    /// CSR offsets (or the point column length); every other owner walks
    /// coordinate runs once and fills each run's span with its logical row id.
    #[expect(
        clippy::indexing_slicing,
        reason = "identity row indexes and CSR endpoints are admitted against the exact output capacity"
    )]
    fn row_index_cast<T: Copy>(&self, capacity: usize, cast: impl Fn(usize) -> T) -> Vec<T> {
        let zero = cast(0);

        if let CoordinateOwner::Array(rows) = &self.owner
            && rows.missing().is_none()
        {
            match rows.storage() {
                GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map })
                    if row_map.is_identity() =>
                {
                    return (0..coords.len()).map(cast).collect();
                },
                GeometryArrayStorage::Packed(PackedStorage::Lines {
                    offsets, row_map, ..
                }) if row_map.is_identity() => {
                    let offsets = offsets.as_slice();
                    let mut indexes = vec![zero; capacity];
                    for (row, &[start, end]) in offsets.array_windows::<2>().enumerate() {
                        let start = start as usize;
                        let end = end as usize;
                        indexes[start..end].fill(cast(row));
                    }
                    return indexes;
                },
                GeometryArrayStorage::Packed(PackedStorage::Polygons {
                    ring_offsets,
                    polygon_offsets,
                    row_map,
                    ..
                }) if row_map.is_identity() => {
                    let rings = ring_offsets.as_slice();
                    let polys = polygon_offsets.as_slice();
                    let mut indexes = vec![zero; capacity];
                    for (row, &[ring_start, ring_end]) in polys.array_windows::<2>().enumerate() {
                        let ring_start = ring_start as usize;
                        let ring_end = ring_end as usize;
                        if ring_start == ring_end {
                            continue;
                        }
                        let start = rings[ring_start] as usize;
                        let end = rings[ring_end] as usize;
                        indexes[start..end].fill(cast(row));
                    }
                    return indexes;
                },
                GeometryArrayStorage::Mixed(_) | GeometryArrayStorage::Packed(_) => {},
            }
        }

        // Scalar geometry: every coordinate belongs to the single owner (id 0).
        if matches!(&self.owner, CoordinateOwner::Shape(_)) {
            return vec![zero; capacity];
        }

        // Mask / gather / window / mixed: one fill per coordinate run, logical
        // row ids, missing rows skipped by `for_each`.
        let mut indexes = vec![zero; capacity];
        let mut remaining = indexes.as_mut_slice();
        self.for_each(&mut |geometry, run| {
            let len = match run {
                CoordRun::Seq(seq) => seq.len(),
                CoordRun::Point(_) => 1,
            };
            let id = cast(geometry.unwrap_or(0));
            let (output, rest) = std::mem::take(&mut remaining).split_at_mut(len);
            output.fill(id);
            remaining = rest;
        });
        debug_assert!(remaining.is_empty());
        indexes
    }

    /// The coordinate at `index` (flattened), or `None` if out of range.
    ///
    /// Packed identity columns and single-sequence shapes resolve in O(1) (or
    /// O(log runs) via CSR binary search). General owners walk runs with early
    /// break so `coords[0]` never scans past the first hit.
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "each remaining-index decrement is guarded by a comparison proving the minuend is large enough"
    )]
    pub fn point_at(&self, index: usize) -> Option<Point> {
        if let Some(point) = self.point_at_fast(index) {
            return Some(point);
        }
        let mut remaining = index;
        let result = self.try_for_each(&mut |_, run| match run {
            CoordRun::Seq(seq) => {
                let len = seq.len();
                if remaining < len {
                    ControlFlow::Break(seq.point_at(remaining))
                } else {
                    remaining -= len;
                    ControlFlow::Continue(())
                }
            },
            CoordRun::Point(point) => {
                if remaining == 0 {
                    ControlFlow::Break(point)
                } else {
                    remaining -= 1;
                    ControlFlow::Continue(())
                }
            },
        });
        match result {
            ControlFlow::Break(point) => Some(point),
            ControlFlow::Continue(()) => None,
        }
    }

    /// Storage-shaped random access for packed identity / window / gather
    /// columns and single-run shapes. O(1) for points; O(log rows) for
    /// contiguous CSR; gather of lines/polys falls through to the run walker.
    fn point_at_fast(&self, index: usize) -> Option<Point> {
        match &self.owner {
            CoordinateOwner::Shape(shape) => match shape.shape() {
                Shape::Point(point) if index == 0 => Some(*point),
                Shape::LineString(seq) if index < seq.len() => Some(seq.point_at(index)),
                Shape::MultiPoint(seq) if index < seq.len() => Some(seq.point_at(index)),
                Shape::Point(_)
                | Shape::MultiPoint(_)
                | Shape::LineString(_)
                | Shape::MultiLineString(_)
                | Shape::Polygon(_)
                | Shape::MultiPolygon(_)
                | Shape::GeometryCollection(_)
                | Shape::Empty(..) => None,
            },
            CoordinateOwner::Array(rows) if rows.missing().is_none() => match rows.storage() {
                GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) => {
                    let map = row_map.as_deref();
                    let len = storage::point_logical_len(coords, map);
                    if index >= len {
                        return None;
                    }
                    let physical = map.physical(index);
                    Some(coords.point_at(physical))
                },
                GeometryArrayStorage::Packed(PackedStorage::Lines {
                    coords,
                    offsets,
                    row_map,
                }) => {
                    let map = row_map.as_deref();
                    let offsets = offsets.as_slice();
                    match map {
                        storage::RowSelectionRef::Identity
                        | crate::geometry::storage::RowSelectionRef::Window { .. } => {
                            packed_line_point_at(coords, offsets, map, index)
                        },
                        storage::RowSelectionRef::Gather(_) => None,
                    }
                },
                GeometryArrayStorage::Packed(PackedStorage::Polygons {
                    coords,
                    ring_offsets,
                    polygon_offsets,
                    row_map,
                }) => {
                    let map = row_map.as_deref();
                    match map {
                        storage::RowSelectionRef::Identity
                        | crate::geometry::storage::RowSelectionRef::Window { .. } => {
                            packed_polygon_point_at(
                                coords,
                                ring_offsets.as_slice(),
                                polygon_offsets.as_slice(),
                                map,
                                index,
                            )
                        },
                        storage::RowSelectionRef::Gather(_) => None,
                    }
                },
                // Mixed / masked / gather-of-ragged: flat index is not a
                // storage-shaped O(1) key — fall through to the run walker.
                GeometryArrayStorage::Mixed(_) => None,
            },
            CoordinateOwner::Array(_) => None,
        }
    }

    fn gather_ordinate(
        &self,
        py: Python<'_>,
        project: impl Fn(Point) -> f64,
    ) -> PyResult<Vec<f64>> {
        let count = self.len_for_python(py)?;
        let capacity = checked_vector_len::<f64>(count, "coordinate column")?;
        let mut out = Vec::with_capacity(capacity);
        self.for_each_point(|point| out.push(project(point)));
        Ok(out)
    }
}
