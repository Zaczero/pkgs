#[cfg(test)]
use std::cell::Cell;
use std::sync::Arc;

use crate::HeapSize;
use crate::array::storage_helpers::column_window;
use crate::array::{
    CoordSeq, CsrOffsetColumn, GeometryKind, Point, PolygonLevel, RingLevel, Shape, ShapeRow,
};
use crate::geometry::{CoordWindow, GeometryErrorKind, LineSeq, SharedSlice};

#[cfg(test)]
std::thread_local! {
    static SHAPE_ITER_MATERIALIZATIONS: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_shape_iter_materializations() {
    SHAPE_ITER_MATERIALIZATIONS.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn shape_iter_materializations() -> usize {
    SHAPE_ITER_MATERIALIZATIONS.with(Cell::get)
}

#[cfg(test)]
fn record_shape_iter_materialization() {
    SHAPE_ITER_MATERIALIZATIONS.with(|count| count.set(count.get().saturating_add(1)));
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CheckedPhysicalRows {
    rows: SharedSlice<usize>,
}

impl CheckedPhysicalRows {
    #[cfg(test)]
    fn checked(rows: Vec<usize>, physical_len: usize) -> Option<Self> {
        if rows.iter().any(|&physical| physical >= physical_len) {
            return None;
        }
        Some(Self {
            rows: SharedSlice::from_vec(rows),
        })
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
pub(crate) struct RowWindow {
    start: usize,
    len: usize,
}

impl RowWindow {
    pub(crate) const fn start(self) -> usize {
        self.start
    }

    pub(crate) const fn len(self) -> usize {
        self.len
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) enum RowSelection {
    #[default]
    Identity,
    Window(RowWindow),
    Gather(CheckedPhysicalRows),
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum RowSelectionRef<'a> {
    Identity,
    Window { start: usize, len: usize },
    Gather(&'a [usize]),
}

impl RowSelection {
    /// A gather over `map`, or `None` when a row lies outside `physical_len`.
    #[cfg(test)]
    pub(crate) fn gather_checked_native(map: Vec<usize>, physical_len: usize) -> Option<Self> {
        CheckedPhysicalRows::checked(map, physical_len).map(Self::Gather)
    }

    pub(crate) fn as_deref(&self) -> RowSelectionRef<'_> {
        match self {
            Self::Identity => RowSelectionRef::Identity,
            Self::Window(window) => RowSelectionRef::Window {
                start: window.start(),
                len: window.len(),
            },
            Self::Gather(map) => RowSelectionRef::Gather(map.as_slice()),
        }
    }

    pub(crate) const fn is_identity(&self) -> bool {
        matches!(self, Self::Identity)
    }

    pub(crate) const fn reorders(&self) -> bool {
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
}

/// Vertex interval covered by a contiguous range of admitted CSR rows.
pub(crate) fn csr_value_window(
    offsets: &[i32],
    rows: std::ops::Range<usize>,
) -> std::ops::Range<usize> {
    let endpoints = offsets
        .get(rows.start..=rows.end)
        .expect("CSR row range is bounded by its admitted offset column");
    let start = endpoints
        .first()
        .expect("CSR row range includes its start boundary");
    let end = endpoints
        .last()
        .expect("CSR row range includes its end boundary");
    (*start as usize)..(*end as usize)
}

#[derive(Clone, Debug)]
pub(crate) enum GeometryArrayStorage {
    /// Heterogeneous (or non-packable) rows owned as plain shapes.
    ///
    /// The array frame, missing mask, prepared-row cache, and frame-cache
    /// sidecars live on [`PyGeometryArray`] — not per-row scalar wrappers.
    /// Scalar extract (`arr[i]`) and prepared kernels materialize
    /// [`crate::geometry::PreparedPlan`] once into the array-side prepared
    /// cache.
    Mixed(Vec<Shape>),
    /// Homogeneous rows in one columnar layout. Packed-only code takes the
    /// inner type directly, so it can never be handed `Mixed`.
    Packed(PackedStorage),
}

/// The three columnar layouts. A value of this type is packed by construction:
/// every operation that only makes sense on columns takes it, and the `Mixed`
/// arms those operations used to bail on are gone with the case.
#[derive(Clone, Debug)]
pub(crate) enum PackedStorage {
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

pub(crate) struct PointRows<'a>(PointRowSource<'a>);

enum PointRowSource<'a> {
    Packed {
        coords: &'a CoordSeq,
        row_map: RowSelectionRef<'a>,
    },
    Mixed(&'a [Shape]),
}

impl PointRows<'_> {
    pub(crate) fn len(&self) -> usize {
        match &self.0 {
            PointRowSource::Packed { coords, row_map } => point_logical_len(coords, *row_map),
            PointRowSource::Mixed(points) => points.len(),
        }
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn get(&self, index: usize) -> Point {
        match &self.0 {
            PointRowSource::Packed { coords, row_map } => {
                coords.point_at(physical_row(*row_map, index))
            },
            PointRowSource::Mixed(points) => {
                let Shape::Point(point) = points
                    .get(index)
                    .expect("point row index is bounded by its admitted mixed slice")
                else {
                    unreachable!("point row admission excludes other shapes");
                };
                *point
            },
        }
    }

    pub(crate) fn iter(&self) -> impl ExactSizeIterator<Item = Point> + '_ {
        let len = self.len();
        (0..len).map(move |index| self.get(index))
    }
}

pub(crate) struct LineRows<'a>(LineRowSource<'a>);

enum LineRowSource<'a> {
    Packed {
        coords: &'a CoordSeq,
        offsets: &'a [i32],
        row_map: RowSelectionRef<'a>,
    },
    Mixed(&'a [Shape]),
}

impl GeometryArrayStorage {
    pub(crate) fn point_rows(&self) -> Option<PointRows<'_>> {
        match self {
            Self::Mixed(shapes) => shapes
                .iter()
                .all(|shape| matches!(shape, Shape::Point(_)))
                .then_some(PointRows(PointRowSource::Mixed(shapes))),
            Self::Packed(PackedStorage::Points { coords, row_map }) => {
                Some(PointRows(PointRowSource::Packed {
                    coords,
                    row_map: row_map.as_deref(),
                }))
            },
            Self::Packed(PackedStorage::Lines { .. } | PackedStorage::Polygons { .. }) => None,
        }
    }

    pub(crate) fn line_rows(&self) -> Option<LineRows<'_>> {
        match self {
            Self::Mixed(shapes) => shapes
                .iter()
                .all(|shape| matches!(shape, Shape::LineString(_)))
                .then_some(LineRows(LineRowSource::Mixed(shapes))),
            Self::Packed(PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            }) => Some(LineRows(LineRowSource::Packed {
                coords,
                offsets: offsets.as_slice(),
                row_map: row_map.as_deref(),
            })),
            Self::Packed(PackedStorage::Points { .. } | PackedStorage::Polygons { .. }) => None,
        }
    }
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

pub(crate) fn admit_packed_coord_count(count: usize) -> crate::error::Result<usize> {
    if count > i32::MAX as usize {
        return Err(GeometryErrorKind::OffsetCapacityExceeded.into());
    }
    Ok(count)
}

pub(crate) fn admit_packed_total(total: usize, additional: usize) -> crate::error::Result<usize> {
    let count = total
        .checked_add(additional)
        .ok_or(GeometryErrorKind::OffsetCapacityExceeded)?;
    admit_packed_coord_count(count)
}

pub(crate) fn materialized_packed_lines_len<'a>(
    offsets: &[i32],
    row_map: impl Into<RowSelectionRef<'a>>,
) -> crate::error::Result<usize> {
    let map = row_map.into();
    let rows = line_logical_len(offsets, map);
    let mut vertices = 0_u128;
    for row in 0..rows {
        vertices = vertices
            .checked_add(map.csr_window(offsets, row).len() as u128)
            .expect("packed line gather count fits u128");
        if vertices > i32::MAX as u128 {
            return Err(GeometryErrorKind::OffsetCapacityExceeded.into());
        }
    }
    Ok(vertices as usize)
}

pub(crate) fn materialized_packed_polygons_len<'a>(
    ring_offsets: &[i32],
    polygon_offsets: &[i32],
    row_map: impl Into<RowSelectionRef<'a>>,
) -> crate::error::Result<(usize, usize)> {
    let map = row_map.into();
    let rows = polygon_logical_len(polygon_offsets, map);
    let mut rings = 0_u128;
    let mut vertices = 0_u128;
    for row in 0..rows {
        let row_rings = map.csr_window(polygon_offsets, row);
        rings = rings
            .checked_add(row_rings.len() as u128)
            .expect("packed polygon ring count fits u128");
        if rings > i32::MAX as u128 {
            return Err(GeometryErrorKind::OffsetCapacityExceeded.into());
        }
        if !row_rings.is_empty() {
            let vertex_window = csr_value_window(ring_offsets, row_rings);
            vertices = vertices
                .checked_add(
                    u128::try_from(vertex_window.len())
                        .expect("packed polygon vertex count fits u128"),
                )
                .expect("packed polygon vertex count fits u128");
            if vertices > i32::MAX as u128 {
                return Err(GeometryErrorKind::OffsetCapacityExceeded.into());
            }
        }
    }
    Ok((vertices as usize, rings as usize))
}

#[derive(Default)]
struct RowSelectionBuild {
    state: RowSelectionBuildState,
}

#[derive(Default)]
enum RowSelectionBuildState {
    #[default]
    Empty,
    Contiguous(std::ops::Range<usize>),
    Gathered(Vec<usize>),
}

impl RowSelectionBuild {
    fn push(&mut self, index: usize) {
        match &mut self.state {
            RowSelectionBuildState::Empty => {
                self.state = RowSelectionBuildState::Contiguous(index..index.wrapping_add(1));
            },
            RowSelectionBuildState::Contiguous(window) => {
                if index == window.end {
                    window.end = index.wrapping_add(1);
                } else {
                    let mut gathered = Vec::with_capacity(window.len().wrapping_add(1));
                    gathered.extend(window.clone());
                    gathered.push(index);
                    self.state = RowSelectionBuildState::Gathered(gathered);
                }
            },
            RowSelectionBuildState::Gathered(indices) => indices.push(index),
        }
    }

    fn finish(self) -> RowSelection {
        match self.state {
            RowSelectionBuildState::Empty => RowSelection::Gather(CheckedPhysicalRows {
                rows: SharedSlice::empty(),
            }),
            RowSelectionBuildState::Contiguous(window) => RowSelection::Window(RowWindow {
                start: window.start,
                len: window.len(),
            }),
            RowSelectionBuildState::Gathered(indices) => {
                RowSelection::Gather(CheckedPhysicalRows {
                    rows: SharedSlice::from_vec(indices),
                })
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
        assert!(
            logical < row_map.len(physical_len),
            "logical row is outside selected row map"
        );
        let physical = physical_row(row_map, logical);
        assert!(
            physical < physical_len,
            "physical row is outside backing storage"
        );
        build.push(physical);
    }
    build.finish()
}

pub(crate) fn contiguous_physical_range(
    row_map: RowSelectionRef<'_>,
    rows: impl IntoIterator<Item = usize>,
) -> Option<std::ops::Range<usize>> {
    let mut rows = rows.into_iter();
    let start = physical_row(row_map, rows.next()?);
    let mut end = start.checked_add(1)?;
    for logical in rows {
        if physical_row(row_map, logical) != end {
            return None;
        }
        end = end.checked_add(1)?;
    }
    Some(start..end)
}

pub(crate) fn polygon_rings_range<'a>(
    polygon_offsets: &[i32],
    row_map: impl Into<RowSelectionRef<'a>>,
    logical: usize,
) -> std::ops::Range<usize> {
    row_map.into().csr_window(polygon_offsets, logical)
}

impl LineRows<'_> {
    pub(crate) fn len(&self) -> usize {
        match &self.0 {
            LineRowSource::Packed {
                offsets, row_map, ..
            } => line_logical_len(offsets, *row_map),
            LineRowSource::Mixed(lines) => lines.len(),
        }
    }

    pub(crate) const fn is_packed_pair(&self, other: &Self) -> bool {
        matches!(
            (&self.0, &other.0),
            (LineRowSource::Packed { .. }, LineRowSource::Packed { .. })
        )
    }

    pub(crate) fn row_xy(&self, index: usize) -> (&[f64], &[f64]) {
        match &self.0 {
            LineRowSource::Packed {
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
            LineRowSource::Mixed(lines) => {
                let Shape::LineString(seq) = lines
                    .get(index)
                    .expect("line row index is bounded by its admitted mixed slice")
                else {
                    unreachable!("line row admission excludes other shapes");
                };
                (seq.xs(), seq.ys())
            },
        }
    }

    pub(crate) const fn packed_column_view(
        &self,
    ) -> Option<crate::geometry::PackedLineColumnView<'_>> {
        match &self.0 {
            LineRowSource::Packed {
                coords,
                offsets,
                row_map,
            } => Some(crate::geometry::PackedLineColumnView {
                coords,
                offsets,
                row_map: *row_map,
            }),
            LineRowSource::Mixed(_) => None,
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
            GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) => {
                let row_map = row_map.as_deref();
                Self::Points {
                    coords,
                    row_map,
                    index: 0,
                    len: point_logical_len(coords, row_map),
                }
            },
            GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            }) => {
                let row_map = row_map.as_deref();
                Self::Lines {
                    coords,
                    offsets: offsets.as_slice(),
                    row_map,
                    index: 0,
                    len: line_logical_len(offsets.as_slice(), row_map),
                }
            },
            GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            }) => {
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
                let item = PhysicalRow::Mixed(
                    items
                        .get(*index)
                        .expect("mixed row index is bounded by its admitted slice"),
                );
                *index = (*index).wrapping_add(1);
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
                *index = (*index).wrapping_add(1);
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
                *index = (*index).wrapping_add(1);
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
                *index = (*index).wrapping_add(1);
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

pub(crate) struct ShapesIter<'a> {
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
    type Item = crate::error::Result<std::borrow::Cow<'a, Shape>>;

    fn next(&mut self) -> Option<Self::Item> {
        let row = self.rows.next()?;
        #[cfg(test)]
        record_shape_iter_materialization();
        match row {
            PhysicalRow::Mixed(shape) => Some(Ok(std::borrow::Cow::Borrowed(shape))),
            PhysicalRow::Point(point) => Some(Ok(std::borrow::Cow::Owned(Shape::Point(point)))),
            PhysicalRow::Line(coords, window) => {
                Some(Ok(std::borrow::Cow::Owned(if window.is_empty() {
                    Shape::typed_empty(GeometryKind::LineString, coords.axes())
                } else {
                    Shape::LineString(LineSeq::row_view(
                        coords,
                        CoordWindow::from_range(window, coords.len()),
                    ))
                })))
            },
            PhysicalRow::Polygon(coords, ring_offsets, rings) => Some(
                GeometryArrayStorage::polygon_view(coords, ring_offsets, rings)
                    .map(|polygon| std::borrow::Cow::Owned(Shape::Polygon(polygon))),
            ),
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

pub(crate) struct RowsIter<'a> {
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
            PhysicalRow::Line(coords, window) => Some(ShapeRow::line(
                coords,
                CoordWindow::from_range(window, coords.len()),
            )),
            PhysicalRow::Polygon(coords, ring_offsets, rings) => {
                Some(ShapeRow::rings(coords, ring_offsets, rings))
            },
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

    use super::*;
    use crate::error::ErrorKind;

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
            yielded = yielded
                .checked_add(1)
                .expect("an iterator cannot yield more than usize::MAX rows");
            let remaining = expected_len
                .checked_sub(yielded)
                .expect("ShapesIter cannot yield beyond its exact size");
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
    fn selected_packed_materializations_admit_duplicate_output_width() {
        let maximum = i32::MAX as usize;
        let duplicate_rows = [0, 0];
        let selection = RowSelectionRef::Gather(&duplicate_rows);
        let line_offsets = [0, i32::MAX];

        assert_eq!(
            packed_lines_coord_len(&line_offsets, selection),
            maximum * 2
        );
        assert!(matches!(
            materialized_packed_lines_len(&line_offsets, selection)
                .unwrap_err()
                .kind(),
            ErrorKind::Geometry(GeometryErrorKind::OffsetCapacityExceeded)
        ));

        let ring_offsets = [0, i32::MAX];
        let polygon_offsets = [0, 1];
        assert_eq!(
            packed_polygons_coord_len(&ring_offsets, &polygon_offsets, selection),
            maximum * 2
        );
        assert_eq!(packed_polygons_ring_len(&polygon_offsets, selection), 2);
        assert!(matches!(
            materialized_packed_polygons_len(&ring_offsets, &polygon_offsets, selection)
                .unwrap_err()
                .kind(),
            ErrorKind::Geometry(GeometryErrorKind::OffsetCapacityExceeded)
        ));

        assert_eq!(admit_packed_coord_count(maximum).unwrap(), maximum);
        assert!(matches!(
            admit_packed_coord_count(maximum + 1).unwrap_err().kind(),
            ErrorKind::Geometry(GeometryErrorKind::OffsetCapacityExceeded)
        ));
    }

    #[test]
    fn checked_gather_rejects_out_of_range_physical_rows() {
        assert!(RowSelection::gather_checked_native(vec![0_usize, 3], 3).is_none());
    }

    #[test]
    fn checked_gather_preserves_valid_physical_rows() {
        let row_map = RowSelection::gather_checked_native(vec![2_usize, 0], 3).unwrap();
        assert_eq!(row_map.as_deref().len(3), 2);
        assert_eq!(physical_row(row_map.as_deref(), 0), 2);
        assert_eq!(physical_row(row_map.as_deref(), 1), 0);
    }

    #[test]
    fn mixed_row_views_borrow_without_allocating_and_reject_other_kinds() {
        let coords =
            CoordSeq::try_from_vecs(vec![2.0, 3.0], vec![4.0, 5.0], Some(vec![6.0, 7.0]), None)
                .unwrap();
        let point = Point::new_raw_xy(0.0, 1.0);
        let points = GeometryArrayStorage::Mixed(vec![
            Shape::Point(point),
            Shape::Point(coords.point_at(1)),
        ]);
        let line = Shape::LineString(LineSeq::from_proven(coords.clone()));
        let lines = GeometryArrayStorage::Mixed(vec![line.clone()]);
        let other = GeometryArrayStorage::Mixed(vec![Shape::Point(point), line]);
        let ((point_rows, line_rows), allocations) = crate::test_support::allocation_count(|| {
            (points.point_rows().unwrap(), lines.line_rows().unwrap())
        });
        assert_eq!(allocations, 0);
        assert_eq!(point_rows.len(), 2);
        assert_eq!(point_rows.get(0), point);
        assert_eq!(point_rows.get(1), coords.point_at(1));
        assert_eq!(line_rows.row_xy(0), (coords.xs(), coords.ys()));
        assert!(line_rows.packed_column_view().is_none());
        assert!(other.point_rows().is_none());
        assert!(other.line_rows().is_none());
    }

    #[test]
    fn shapes_iter_size_hint_exact_mixed() {
        let storage = GeometryArrayStorage::Mixed(vec![
            Shape::Point(Point::new_raw_xy(0.0, 0.0)),
            Shape::Point(Point::new_raw_xy(1.0, 1.0)),
            Shape::typed_empty(
                crate::geometry::GeometryKind::Point,
                crate::geometry::CoordinateAxes::XY,
            ),
        ]);
        assert_shapes_iter_exact(&storage);
    }

    #[test]
    fn shapes_iter_size_hint_exact_points_identity_window_gather() {
        let coords = Arc::new(
            CoordSeq::try_from_vecs(
                vec![0.0, 1.0, 2.0, 3.0],
                vec![0.0, 1.0, 2.0, 3.0],
                None,
                None,
            )
            .unwrap(),
        );
        let identity = GeometryArrayStorage::Packed(PackedStorage::Points {
            coords: Arc::clone(&coords),
            row_map: RowSelection::Identity,
        });
        assert_shapes_iter_exact(&identity);

        let window = GeometryArrayStorage::Packed(PackedStorage::Points {
            coords: Arc::clone(&coords),
            row_map: row_selection_from_logical_rows(RowSelectionRef::Identity, 4, 1..3),
        });
        assert_shapes_iter_exact(&window);

        let gather = GeometryArrayStorage::Packed(PackedStorage::Points {
            coords,
            row_map: RowSelection::gather_checked_native(vec![3_usize, 0, 2], 4).unwrap(),
        });
        assert_shapes_iter_exact(&gather);
    }

    #[test]
    fn shapes_iter_size_hint_exact_lines_identity_window_gather() {
        let coords = Arc::new(
            CoordSeq::try_from_vecs(
                vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0],
                vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0],
                None,
                None,
            )
            .unwrap(),
        );
        // Three lines: verts [0,2), [2,4), [4,6)
        let offsets = crate::geometry::CsrOffsetColumn::try_new(vec![0, 2, 4, 6], 6).unwrap();
        let identity = GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords: Arc::clone(&coords),
            offsets: offsets.clone(),
            row_map: RowSelection::Identity,
        });
        assert_shapes_iter_exact(&identity);

        let window = GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords: Arc::clone(&coords),
            offsets: offsets.clone(),
            row_map: row_selection_from_logical_rows(RowSelectionRef::Identity, 3, 0..2),
        });
        assert_shapes_iter_exact(&window);

        let gather = GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords,
            offsets,
            row_map: RowSelection::gather_checked_native(vec![2_usize, 0], 3).unwrap(),
        });
        assert_shapes_iter_exact(&gather);
    }

    #[test]
    fn shapes_iter_size_hint_exact_polygons_identity_window_gather() {
        // Two triangles: ring verts 0..4 and 4..8 (closed).
        let coords = Arc::new(
            CoordSeq::try_from_vecs(
                vec![0.0, 1.0, 1.0, 0.0, 10.0, 11.0, 11.0, 10.0],
                vec![0.0, 0.0, 1.0, 0.0, 10.0, 10.0, 11.0, 10.0],
                None,
                None,
            )
            .unwrap(),
        );
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
        let identity = GeometryArrayStorage::Packed(PackedStorage::Polygons {
            coords: Arc::clone(&coords),
            ring_offsets: ring_offsets.clone(),
            polygon_offsets: polygon_offsets.clone(),
            row_map: RowSelection::Identity,
        });
        assert_shapes_iter_exact(&identity);

        let window = GeometryArrayStorage::Packed(PackedStorage::Polygons {
            coords: Arc::clone(&coords),
            ring_offsets: ring_offsets.clone(),
            polygon_offsets: polygon_offsets.clone(),
            row_map: row_selection_from_logical_rows(RowSelectionRef::Identity, 2, 1..2),
        });
        assert_shapes_iter_exact(&window);

        let gather = GeometryArrayStorage::Packed(PackedStorage::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
            row_map: RowSelection::gather_checked_native(vec![1_usize, 0], 2).unwrap(),
        });
        assert_shapes_iter_exact(&gather);
    }

    #[test]
    fn shapes_iter_size_hint_exact_empty_storages() {
        assert_shapes_iter_exact(&GeometryArrayStorage::Mixed(Vec::new()));
        let empty_pts = GeometryArrayStorage::Packed(PackedStorage::Points {
            coords: Arc::new(CoordSeq::try_from_vecs(vec![], vec![], None, None).unwrap()),
            row_map: RowSelection::Identity,
        });
        assert_shapes_iter_exact(&empty_pts);
    }
}

#[cfg(test)]
mod row_selection_admission_tests {
    use super::*;

    #[test]
    fn checked_gather_rejects_out_of_range_physical_rows() {
        assert!(RowSelection::gather_checked_native(vec![4_usize], 4).is_none());
    }

    #[test]
    fn logical_row_composition_builds_a_private_window() {
        let selection = row_selection_from_logical_rows(RowSelectionRef::Identity, 4, 1..3);
        assert!(matches!(selection, RowSelection::Window(_)));
    }

    #[test]
    fn logical_row_composition_preserves_empty_range_at_physical_end() {
        let selection = row_selection_from_logical_rows(RowSelectionRef::Identity, 4, 4..4);
        assert_eq!(selection.as_deref().len(4), 0);
    }

    #[test]
    #[should_panic(expected = "logical row is outside selected row map")]
    fn logical_row_composition_rejects_identity_logical_end() {
        drop(row_selection_from_logical_rows(
            RowSelectionRef::Identity,
            4,
            [4],
        ));
    }

    #[test]
    #[should_panic(expected = "logical row is outside selected row map")]
    fn logical_row_composition_rejects_window_logical_end_before_resolution() {
        let row_map = RowSelectionRef::Window {
            start: usize::MAX - 1,
            len: 1,
        };
        drop(row_selection_from_logical_rows(row_map, usize::MAX, [1]));
    }

    #[test]
    #[should_panic(expected = "physical row is outside backing storage")]
    fn logical_row_composition_rejects_gathered_physical_end() {
        let row_map = RowSelectionRef::Gather(&[4]);
        drop(row_selection_from_logical_rows(row_map, 4, [0]));
    }
}
