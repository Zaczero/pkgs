use std::sync::Arc;

use crate::HeapSize;
use crate::array::storage_helpers::column_window;
use crate::array::{
    CoordSeq, CsrOffsetColumn, GeometryError, Point, PolygonLevel, PyResult, RingLevel, Shape,
    ShapeRow,
};
use crate::geometry::{CoordWindow, LineSeq};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckedPhysicalRows {
    rows: Arc<[usize]>,
}

impl CheckedPhysicalRows {
    fn trusted(rows: Arc<[usize]>, physical_len: usize) -> Self {
        debug_assert!(
            rows.iter().all(|&physical| physical < physical_len),
            "RowSelection::Gather index must reference an existing physical row",
        );
        Self { rows }
    }

    fn checked(rows: Arc<[usize]>, physical_len: usize, label: &str) -> PyResult<Self> {
        if rows.iter().any(|&physical| physical >= physical_len) {
            return Err(GeometryError::new_err(format!(
                "{label} row_map references a nonexistent physical row"
            )));
        }
        Ok(Self::trusted(rows, physical_len))
    }

    fn as_slice(&self) -> &[usize] {
        &self.rows
    }
}

impl HeapSize for CheckedPhysicalRows {
    fn heap_bytes(&self) -> usize {
        self.rows.heap_bytes()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RowWindow {
    start: usize,
    len: usize,
}

impl RowWindow {
    /// `physical_len` MUST be the backing storage's true row count. A
    /// `RowSelection::window(start, len)` constructor used to pass
    /// `start + len` here, which made the assertion `start <= start + len &&
    /// len <= len` — vacuously true in every build, so it could never fire.
    /// Callers now pass the real length and the tripwire means something.
    fn trusted(start: usize, len: usize, physical_len: usize) -> Self {
        debug_assert!(
            start <= physical_len && len <= physical_len - start,
            "RowSelection::Window must reference existing physical rows",
        );
        Self { start, len }
    }

    pub(crate) const fn start(self) -> usize {
        self.start
    }

    pub(crate) const fn len(self) -> usize {
        self.len
    }

    pub(crate) const fn end(self) -> usize {
        self.start + self.len
    }

    pub(crate) const fn as_range(self) -> std::ops::Range<usize> {
        self.start()..self.end()
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum RowSelection {
    #[default]
    Identity,
    Window(RowWindow),
    Gather(CheckedPhysicalRows),
}

#[derive(Clone, Copy, Debug)]
pub enum RowSelectionRef<'a> {
    Identity,
    Window { start: usize, len: usize },
    Gather(&'a [usize]),
}

impl RowSelection {
    pub(crate) fn gather_trusted(map: Arc<[usize]>, physical_len: usize) -> Self {
        Self::Gather(CheckedPhysicalRows::trusted(map, physical_len))
    }

    pub(crate) fn gather_checked(
        map: Arc<[usize]>,
        physical_len: usize,
        label: &str,
    ) -> PyResult<Self> {
        Ok(Self::Gather(CheckedPhysicalRows::checked(
            map,
            physical_len,
            label,
        )?))
    }

    pub fn window_trusted(start: usize, len: usize, physical_len: usize) -> Self {
        Self::Window(RowWindow::trusted(start, len, physical_len))
    }

    pub fn as_deref(&self) -> RowSelectionRef<'_> {
        match self {
            Self::Identity => RowSelectionRef::Identity,
            Self::Window(window) => RowSelectionRef::Window {
                start: window.start(),
                len: window.len(),
            },
            Self::Gather(map) => RowSelectionRef::Gather(map.as_slice()),
        }
    }

    pub const fn is_identity(&self) -> bool {
        matches!(self, Self::Identity)
    }

    pub const fn reorders(&self) -> bool {
        !self.is_identity()
    }

    #[expect(
        clippy::same_name_method,
        reason = "the inherent operation deliberately shares the domain vocabulary of its trait contract"
    )]
    pub(crate) fn heap_bytes(&self) -> usize {
        HeapSize::heap_bytes(self)
    }

    pub(crate) fn explicit_indices(&self) -> Option<std::borrow::Cow<'_, [usize]>> {
        match self {
            Self::Identity | Self::Window(_) => None,
            Self::Gather(map) => Some(std::borrow::Cow::Borrowed(map.as_slice())),
        }
    }

    pub(crate) fn pickle_row_map_indices(&self) -> Option<std::borrow::Cow<'_, [usize]>> {
        match self {
            Self::Identity => None,
            Self::Window(window) => Some(std::borrow::Cow::Owned(window.as_range().collect())),
            Self::Gather(map) => Some(std::borrow::Cow::Borrowed(map.as_slice())),
        }
    }
}

impl HeapSize for RowSelection {
    fn heap_bytes(&self) -> usize {
        match self {
            Self::Identity | Self::Window(_) => 0,
            Self::Gather(map) => map.heap_bytes(),
        }
    }
}

impl<'a> From<Option<&'a [usize]>> for RowSelectionRef<'a> {
    fn from(value: Option<&'a [usize]>) -> Self {
        value.map_or(Self::Identity, Self::Gather)
    }
}

impl RowSelectionRef<'_> {
    pub(crate) const fn len(self, physical_len: usize) -> usize {
        match self {
            Self::Identity => physical_len,
            Self::Window { len, .. } => len,
            Self::Gather(map) => map.len(),
        }
    }

    pub(crate) const fn physical(self, logical: usize) -> usize {
        match self {
            Self::Identity => logical,
            Self::Window { start, .. } => start + logical,
            Self::Gather(map) => map[logical],
        }
    }

    pub(crate) const fn csr_window(
        self,
        offsets: &[i32],
        logical: usize,
    ) -> std::ops::Range<usize> {
        let physical = self.physical(logical);
        offsets[physical] as usize..offsets[physical + 1] as usize
    }

    pub(crate) const fn is_identity(self) -> bool {
        matches!(self, Self::Identity)
    }

    pub(crate) const fn contiguous_window(self) -> Option<std::ops::Range<usize>> {
        match self {
            Self::Window { start, len } => Some(start..start + len),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub enum GeometryArrayStorage {
    /// Heterogeneous (or non-packable) rows owned as plain shapes.
    ///
    /// The array frame, missing mask, prepared-row cache, and frame-cache
    /// sidecars live on [`PyGeometryArray`] — not per-row scalar wrappers.
    /// Scalar extract (`arr[i]`) and prepared kernels materialize
    /// [`ShapeData`] once into the array-side prepared cache.
    Mixed(Vec<Shape>),
    Points {
        coords: Arc<CoordSeq>,
        row_map: RowSelection,
    },
    Lines {
        coords: Arc<CoordSeq>,
        offsets: CsrOffsetColumn,
        row_map: RowSelection,
    },
    Polygons {
        coords: Arc<CoordSeq>,
        ring_offsets: CsrOffsetColumn<RingLevel>,
        polygon_offsets: CsrOffsetColumn<PolygonLevel>,
        row_map: RowSelection,
    },
}

pub enum PointRows<'a> {
    Packed {
        coords: &'a CoordSeq,
        row_map: RowSelectionRef<'a>,
    },
    Gathered(Vec<Point>),
}

impl PointRows<'_> {
    pub fn len(&self) -> usize {
        match self {
            Self::Packed { coords, row_map } => point_logical_len(coords, *row_map),
            Self::Gathered(points) => points.len(),
        }
    }
    /// Per-geometry emptiness test (see `Geometry.is_empty`).
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     One result per input geometry.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, index: usize) -> Point {
        match self {
            Self::Packed { coords, row_map } => coords.point_at(physical_row(*row_map, index)),
            Self::Gathered(points) => points[index],
        }
    }

    pub fn first(&self) -> Option<Point> {
        match self {
            Self::Packed { .. } if self.is_empty() => None,
            Self::Packed { .. } => Some(self.get(0)),
            Self::Gathered(points) => points.first().copied(),
        }
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = Point> + '_ {
        let len = self.len();
        (0..len).map(move |index| self.get(index))
    }

    pub fn into_vec(self) -> Vec<Point> {
        self.iter().collect()
    }
}

pub enum LineRows<'a> {
    Packed {
        coords: &'a CoordSeq,
        offsets: &'a [i32],
        row_map: RowSelectionRef<'a>,
    },
    Gathered(Vec<LineSeq>),
}

pub(crate) fn physical_row<'a>(row_map: impl Into<RowSelectionRef<'a>>, logical: usize) -> usize {
    row_map.into().physical(logical)
}

pub(crate) fn line_logical_len<'a>(
    offsets: &[i32],
    row_map: impl Into<RowSelectionRef<'a>>,
) -> usize {
    row_map.into().len(offsets.len().saturating_sub(1))
}

pub(crate) fn row_map_is_identity<'a>(row_map: impl Into<RowSelectionRef<'a>>) -> bool {
    row_map.into().is_identity()
}

pub(crate) fn polygon_logical_len<'a>(
    polygon_offsets: &[i32],
    row_map: impl Into<RowSelectionRef<'a>>,
) -> usize {
    row_map.into().len(polygon_offsets.len().saturating_sub(1))
}

pub(crate) fn point_logical_len<'a>(
    coords: &CoordSeq,
    row_map: impl Into<RowSelectionRef<'a>>,
) -> usize {
    row_map.into().len(coords.len())
}

pub(crate) fn packed_lines_coord_len<'a>(
    offsets: &[i32],
    row_map: impl Into<RowSelectionRef<'a>>,
) -> usize {
    match row_map.into() {
        RowSelectionRef::Identity => offsets.last().map_or(0, |&end| end as usize),
        RowSelectionRef::Window { start, len } => {
            if len == 0 {
                0
            } else {
                offsets[start + len] as usize - offsets[start] as usize
            }
        },
        RowSelectionRef::Gather(map) => map
            .iter()
            .map(|&physical| {
                let begin = offsets[physical] as usize;
                let end = offsets[physical + 1] as usize;
                end - begin
            })
            .sum(),
    }
}

pub(crate) fn packed_polygons_coord_len<'a>(
    ring_offsets: &[i32],
    polygon_offsets: &[i32],
    row_map: impl Into<RowSelectionRef<'a>>,
) -> usize {
    match row_map.into() {
        RowSelectionRef::Identity => ring_offsets.last().map_or(0, |&end| end as usize),
        RowSelectionRef::Window { start, len } => {
            if len == 0 {
                0
            } else {
                let ring_start = polygon_offsets[start] as usize;
                let ring_end = polygon_offsets[start + len] as usize;
                ring_offsets[ring_end] as usize - ring_offsets[ring_start] as usize
            }
        },
        RowSelectionRef::Gather(map) => map
            .iter()
            .map(|&physical| {
                let ring_start = polygon_offsets[physical] as usize;
                let ring_end = polygon_offsets[physical + 1] as usize;
                ring_offsets[ring_end] as usize - ring_offsets[ring_start] as usize
            })
            .sum(),
    }
}

pub(crate) fn packed_polygons_ring_len<'a>(
    polygon_offsets: &[i32],
    row_map: impl Into<RowSelectionRef<'a>>,
) -> usize {
    match row_map.into() {
        RowSelectionRef::Identity => polygon_offsets.last().map_or(0, |&end| end as usize),
        RowSelectionRef::Window { start, len } => {
            if len == 0 {
                0
            } else {
                polygon_offsets[start + len] as usize - polygon_offsets[start] as usize
            }
        },
        RowSelectionRef::Gather(map) => map
            .iter()
            .map(|&physical| {
                polygon_offsets[physical + 1] as usize - polygon_offsets[physical] as usize
            })
            .sum(),
    }
}

#[derive(Default)]
struct RowSelectionBuild {
    state: RowSelectionBuildState,
}

#[derive(Default)]
enum RowSelectionBuildState {
    #[default]
    Empty,
    Contiguous {
        start: usize,
        len: usize,
    },
    Gathered(Vec<usize>),
}

impl RowSelectionBuild {
    fn push(&mut self, index: usize) {
        match &mut self.state {
            RowSelectionBuildState::Empty => {
                self.state = RowSelectionBuildState::Contiguous {
                    start: index,
                    len: 1,
                };
            },
            RowSelectionBuildState::Contiguous { start, len } => {
                if index == *start + *len {
                    *len += 1;
                } else {
                    let mut gathered = Vec::with_capacity(*len + 1);
                    gathered.extend(*start..*start + *len);
                    gathered.push(index);
                    self.state = RowSelectionBuildState::Gathered(gathered);
                }
            },
            RowSelectionBuildState::Gathered(indices) => indices.push(index),
        }
    }

    const fn contiguous_range(&self) -> Option<std::ops::Range<usize>> {
        match self.state {
            RowSelectionBuildState::Contiguous { start, len } => Some(start..start + len),
            _ => None,
        }
    }

    fn finish(self, physical_len: usize) -> RowSelection {
        match self.state {
            RowSelectionBuildState::Empty => {
                RowSelection::gather_trusted(Arc::from([] as [usize; 0]), physical_len)
            },
            RowSelectionBuildState::Contiguous { start, len } => {
                RowSelection::window_trusted(start, len, physical_len)
            },
            RowSelectionBuildState::Gathered(indices) => {
                RowSelection::gather_trusted(indices.into(), physical_len)
            },
        }
    }
}

pub(crate) fn row_selection_from_logical_rows(
    row_map: RowSelectionRef<'_>,
    physical_len: usize,
    rows: impl IntoIterator<Item = usize>,
) -> RowSelection {
    let mut build = RowSelectionBuild::default();
    for logical in rows {
        build.push(physical_row(row_map, logical));
    }
    build.finish(physical_len)
}

pub(crate) fn contiguous_physical_range(
    row_map: RowSelectionRef<'_>,
    rows: impl IntoIterator<Item = usize>,
) -> Option<std::ops::Range<usize>> {
    let mut build = RowSelectionBuild::default();
    for logical in rows {
        build.push(physical_row(row_map, logical));
    }
    build.contiguous_range()
}

pub(crate) fn polygon_rings_range<'a>(
    polygon_offsets: &[i32],
    row_map: impl Into<RowSelectionRef<'a>>,
    logical: usize,
) -> std::ops::Range<usize> {
    row_map.into().csr_window(polygon_offsets, logical)
}

impl LineRows<'_> {
    pub fn len(&self) -> usize {
        match self {
            Self::Packed {
                offsets, row_map, ..
            } => line_logical_len(offsets, *row_map),
            Self::Gathered(lines) => lines.len(),
        }
    }

    pub const fn is_packed_pair(&self, other: &Self) -> bool {
        matches!((self, other), (Self::Packed { .. }, Self::Packed { .. }))
    }

    pub fn row_xy(&self, index: usize) -> (&[f64], &[f64]) {
        match self {
            Self::Packed {
                coords,
                offsets,
                row_map,
            } => {
                let window = row_map.csr_window(offsets, index);
                (
                    column_window(coords.xs(), &window),
                    column_window(coords.ys(), &window),
                )
            },
            Self::Gathered(lines) => {
                let seq = &lines[index];
                (seq.xs(), seq.ys())
            },
        }
    }

    pub const fn packed_column_view(&self) -> Option<crate::geometry::PackedLineColumnView<'_>> {
        match self {
            Self::Packed {
                coords,
                offsets,
                row_map,
            } => Some(crate::geometry::PackedLineColumnView {
                coords,
                offsets,
                row_map: *row_map,
            }),
            Self::Gathered(_) => None,
        }
    }
}

enum PhysicalRow<'a> {
    Mixed(&'a Shape),
    Point(Point),
    Line(&'a CoordSeq, std::ops::Range<usize>),
    Polygon(&'a CoordSeq, &'a [i32], std::ops::Range<usize>),
}

enum PhysicalRowsIter<'a> {
    Mixed {
        items: &'a [Shape],
        index: usize,
    },
    Points {
        coords: &'a CoordSeq,
        row_map: RowSelectionRef<'a>,
        index: usize,
        len: usize,
    },
    Lines {
        coords: &'a CoordSeq,
        offsets: &'a [i32],
        row_map: RowSelectionRef<'a>,
        index: usize,
        len: usize,
    },
    Polygons {
        coords: &'a CoordSeq,
        ring_offsets: &'a [i32],
        polygon_offsets: &'a [i32],
        row_map: RowSelectionRef<'a>,
        index: usize,
        len: usize,
    },
}

impl<'a> PhysicalRowsIter<'a> {
    fn new(storage: &'a GeometryArrayStorage) -> Self {
        match storage {
            GeometryArrayStorage::Mixed(items) => Self::Mixed { items, index: 0 },
            GeometryArrayStorage::Points { coords, row_map } => {
                let row_map = row_map.as_deref();
                Self::Points {
                    coords,
                    row_map,
                    index: 0,
                    len: point_logical_len(coords, row_map),
                }
            },
            GeometryArrayStorage::Lines {
                coords,
                offsets,
                row_map,
            } => {
                let row_map = row_map.as_deref();
                Self::Lines {
                    coords,
                    offsets: offsets.as_slice(),
                    row_map,
                    index: 0,
                    len: line_logical_len(offsets.as_slice(), row_map),
                }
            },
            GeometryArrayStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            } => {
                let row_map = row_map.as_deref();
                Self::Polygons {
                    coords,
                    ring_offsets: ring_offsets.as_slice(),
                    polygon_offsets: polygon_offsets.as_slice(),
                    row_map,
                    index: 0,
                    len: polygon_logical_len(polygon_offsets.as_slice(), row_map),
                }
            },
        }
    }
}

impl<'a> Iterator for PhysicalRowsIter<'a> {
    type Item = PhysicalRow<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Mixed { items, index } => {
                if *index >= items.len() {
                    return None;
                }
                let item = PhysicalRow::Mixed(&items[*index]);
                *index += 1;
                Some(item)
            },
            Self::Points {
                coords,
                row_map,
                index,
                len,
            } => {
                if *index >= *len {
                    return None;
                }
                let point = coords.point_at(physical_row(*row_map, *index));
                *index += 1;
                Some(PhysicalRow::Point(point))
            },
            Self::Lines {
                coords,
                offsets,
                row_map,
                index,
                len,
            } => {
                if *index >= *len {
                    return None;
                }
                let window = row_map.csr_window(offsets, *index);
                *index += 1;
                Some(PhysicalRow::Line(coords, window))
            },
            Self::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
                index,
                len,
            } => {
                if *index >= *len {
                    return None;
                }
                let window = polygon_rings_range(polygon_offsets, *row_map, *index);
                *index += 1;
                Some(PhysicalRow::Polygon(coords, ring_offsets, window))
            },
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = match self {
            Self::Mixed { items, index } => items.len().saturating_sub(*index),
            Self::Points { index, len, .. }
            | Self::Lines { index, len, .. }
            | Self::Polygons { index, len, .. } => len.saturating_sub(*index),
        };
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for PhysicalRowsIter<'_> {
    fn len(&self) -> usize {
        self.size_hint().0
    }
}

pub struct ShapesIter<'a> {
    rows: PhysicalRowsIter<'a>,
}

impl<'a> ShapesIter<'a> {
    pub(crate) fn new(storage: &'a GeometryArrayStorage) -> Self {
        Self {
            rows: PhysicalRowsIter::new(storage),
        }
    }
}

impl<'a> Iterator for ShapesIter<'a> {
    type Item = std::borrow::Cow<'a, Shape>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.rows.next()? {
            PhysicalRow::Mixed(shape) => Some(std::borrow::Cow::Borrowed(shape)),
            PhysicalRow::Point(point) => Some(std::borrow::Cow::Owned(Shape::Point(point))),
            PhysicalRow::Line(coords, window) => Some(std::borrow::Cow::Owned(Shape::LineString(
                LineSeq::from_trusted(coords.view(CoordWindow::trusted(window, coords.len()))),
            ))),
            PhysicalRow::Polygon(coords, ring_offsets, rings) => {
                Some(std::borrow::Cow::Owned(Shape::Polygon(
                    GeometryArrayStorage::polygon_view(coords, ring_offsets, rings),
                )))
            },
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.rows.size_hint()
    }
}

impl ExactSizeIterator for ShapesIter<'_> {
    fn len(&self) -> usize {
        self.size_hint().0
    }
}

// SAFETY: `ShapesIter` always yields exactly `size_hint().0` items — never
// fewer. The lower/upper bounds come from `PhysicalRowsIter`, which tracks a
// fixed logical length set once at construction and advances a monotoic
// `index` by exactly one on every successful `next`:
//
// - Mixed: `len = items.len()`; yields `items[index]` while `index < len`.
//   Missing array rows are still present as placeholder `Shape` entries in
//   the mixed column — they count toward len and are yielded (the array
//   missing-mask is orthogonal and not consulted here).
// - Points / Lines / Polygons: `len = {point,line,polygon}_logical_len(...)`
//   which is `RowSelectionRef::len(physical_len)`:
//     Identity → physical_len
//     Window { len } → len
//     Gather(map) → map.len()
//   Each `next` resolves `physical_row(row_map, index)` (or the CSR window
//   for that logical row) and always returns `Some` while `index < len`.
//
// There is no filtering, short-circuit, or fallible yield path that can
// return `None` before exhaustion. A bad gather index is a construction-
// time/`debug_assert` defect, not a shorter iterator. Do not spread this
// impl to other iterators without an independent exactness proof.
unsafe impl std::iter::TrustedLen for ShapesIter<'_> {}

pub struct RowsIter<'a> {
    rows: PhysicalRowsIter<'a>,
}

impl<'a> RowsIter<'a> {
    pub(crate) fn new(storage: &'a GeometryArrayStorage) -> Self {
        Self {
            rows: PhysicalRowsIter::new(storage),
        }
    }
}

impl<'a> Iterator for RowsIter<'a> {
    type Item = ShapeRow<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.rows.next()? {
            PhysicalRow::Mixed(shape) => Some(ShapeRow::Shape(shape)),
            PhysicalRow::Point(point) => Some(ShapeRow::Point(point)),
            PhysicalRow::Line(coords, window) => {
                Some(ShapeRow::Line(coords, window.start, window.end))
            },
            PhysicalRow::Polygon(coords, ring_offsets, rings) => Some(ShapeRow::Rings(
                coords,
                ring_offsets,
                rings.start,
                rings.end,
            )),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.rows.size_hint()
    }
}

impl ExactSizeIterator for RowsIter<'_> {
    fn len(&self) -> usize {
        self.size_hint().0
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::geometry::{CoordSeq, Point, Shape};

    /// Exhaust `ShapesIter` and assert every remaining `size_hint` equals the
    /// actual remaining yield count (TrustedLen contract).
    fn assert_shapes_iter_exact(storage: &GeometryArrayStorage) {
        let expected_len = storage.len();
        let mut iter = storage.iter_shapes();
        let (lo, hi) = iter.size_hint();
        assert_eq!(lo, expected_len);
        assert_eq!(hi, Some(expected_len));
        let mut yielded = 0_usize;
        while iter.next().is_some() {
            yielded += 1;
            let remaining = expected_len - yielded;
            let (lo, hi) = iter.size_hint();
            assert_eq!(lo, remaining, "size_hint lower after {yielded} yields");
            assert_eq!(
                hi,
                Some(remaining),
                "size_hint upper after {yielded} yields"
            );
        }
        assert_eq!(yielded, expected_len);
        assert_eq!(iter.size_hint(), (0, Some(0)));
        assert!(iter.next().is_none());
    }

    #[test]
    fn checked_gather_rejects_out_of_range_physical_rows() {
        let _ = RowSelection::gather_checked(Arc::from([0_usize, 3]), 3, "test").unwrap_err();
    }

    #[test]
    fn checked_gather_preserves_valid_physical_rows() {
        let row_map = RowSelection::gather_checked(Arc::from([2_usize, 0]), 3, "test").unwrap();
        assert_eq!(row_map.as_deref().len(3), 2);
        assert_eq!(physical_row(row_map.as_deref(), 0), 2);
        assert_eq!(physical_row(row_map.as_deref(), 1), 0);
    }

    #[test]
    fn shapes_iter_size_hint_exact_mixed() {
        let storage = GeometryArrayStorage::Mixed(vec![
            Shape::Point(Point::new_unchecked_xy(0.0, 0.0)),
            Shape::Point(Point::new_unchecked_xy(1.0, 1.0)),
            Shape::typed_empty(
                crate::geometry::EmptyKind::Point,
                crate::geometry::CoordinateAxes::XY,
            ),
        ]);
        assert_shapes_iter_exact(&storage);
    }

    #[test]
    fn shapes_iter_size_hint_exact_points_identity_window_gather() {
        let coords = Arc::new(CoordSeq::from_vecs(
            vec![0.0, 1.0, 2.0, 3.0],
            vec![0.0, 1.0, 2.0, 3.0],
            None,
            None,
        ));
        let identity = GeometryArrayStorage::Points {
            coords: Arc::clone(&coords),
            row_map: RowSelection::Identity,
        };
        assert_shapes_iter_exact(&identity);

        let window = GeometryArrayStorage::Points {
            coords: Arc::clone(&coords),
            row_map: RowSelection::window_trusted(1, 2, 3),
        };
        assert_shapes_iter_exact(&window);

        let gather = GeometryArrayStorage::Points {
            coords,
            row_map: RowSelection::gather_checked(Arc::from([3_usize, 0, 2]), 4, "test").unwrap(),
        };
        assert_shapes_iter_exact(&gather);
    }

    #[test]
    fn shapes_iter_size_hint_exact_lines_identity_window_gather() {
        let coords = Arc::new(CoordSeq::from_vecs(
            vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0],
            vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0],
            None,
            None,
        ));
        // Three lines: verts [0,2), [2,4), [4,6)
        let offsets = crate::geometry::CsrOffsetColumn::try_new(vec![0, 2, 4, 6], 6).unwrap();
        let identity = GeometryArrayStorage::Lines {
            coords: Arc::clone(&coords),
            offsets: offsets.clone(),
            row_map: RowSelection::Identity,
        };
        assert_shapes_iter_exact(&identity);

        let window = GeometryArrayStorage::Lines {
            coords: Arc::clone(&coords),
            offsets: offsets.clone(),
            row_map: RowSelection::window_trusted(0, 2, 2),
        };
        assert_shapes_iter_exact(&window);

        let gather = GeometryArrayStorage::Lines {
            coords,
            offsets,
            row_map: RowSelection::gather_checked(Arc::from([2_usize, 0]), 3, "test").unwrap(),
        };
        assert_shapes_iter_exact(&gather);
    }

    #[test]
    fn shapes_iter_size_hint_exact_polygons_identity_window_gather() {
        // Two triangles: ring verts 0..4 and 4..8 (closed).
        let coords = Arc::new(CoordSeq::from_vecs(
            vec![0.0, 1.0, 1.0, 0.0, 10.0, 11.0, 11.0, 10.0],
            vec![0.0, 0.0, 1.0, 0.0, 10.0, 10.0, 11.0, 10.0],
            None,
            None,
        ));
        let ring_offsets = crate::geometry::CsrOffsetColumn::<crate::geometry::RingLevel>::try_new(
            vec![0, 4, 8],
            8,
        )
        .unwrap();
        let polygon_offsets =
            crate::geometry::CsrOffsetColumn::<crate::geometry::PolygonLevel>::try_new(
                vec![0, 1, 2],
                2,
            )
            .unwrap();
        let identity = GeometryArrayStorage::Polygons {
            coords: Arc::clone(&coords),
            ring_offsets: ring_offsets.clone(),
            polygon_offsets: polygon_offsets.clone(),
            row_map: RowSelection::Identity,
        };
        assert_shapes_iter_exact(&identity);

        let window = GeometryArrayStorage::Polygons {
            coords: Arc::clone(&coords),
            ring_offsets: ring_offsets.clone(),
            polygon_offsets: polygon_offsets.clone(),
            row_map: RowSelection::window_trusted(1, 1, 2),
        };
        assert_shapes_iter_exact(&window);

        let gather = GeometryArrayStorage::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
            row_map: RowSelection::gather_checked(Arc::from([1_usize, 0]), 2, "test").unwrap(),
        };
        assert_shapes_iter_exact(&gather);
    }

    #[test]
    fn shapes_iter_size_hint_exact_empty_storages() {
        assert_shapes_iter_exact(&GeometryArrayStorage::Mixed(Vec::new()));
        let empty_pts = GeometryArrayStorage::Points {
            coords: Arc::new(CoordSeq::from_vecs(vec![], vec![], None, None)),
            row_map: RowSelection::Identity,
        };
        assert_shapes_iter_exact(&empty_pts);
    }
}

#[cfg(test)]
mod row_window_assertion_tests {
    use super::*;

    /// The `RowWindow::trusted` tripwire must be able to FIRE.
    ///
    /// It previously could not: `RowSelection::window(start, len)` passed
    /// `start + len` as `physical_len`, reducing the assertion to
    /// `start <= start + len && len <= len`. This pins that a window past the
    /// end of the backing storage is now rejected in a debug build.
    // The tripwire is a `debug_assert!`, so it exists only where
    // `debug-assertions` is on. Without this gate the test cannot pass under
    // `cargo nextest run --release`, which CI runs as a second pass.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "must reference existing physical rows")]
    fn window_assertion_is_not_vacuous() {
        let _ = RowSelection::window_trusted(2, 5, 4);
    }

    #[test]
    fn window_inside_physical_rows_is_accepted() {
        let selection = RowSelection::window_trusted(1, 2, 4);
        assert!(matches!(selection, RowSelection::Window(_)));
    }
}
