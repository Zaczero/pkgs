//! Detached packed-column execution over borrowed physical columns and a logical row selection.
//!
//! Present rows flow through `PackedRow`, which resolves admitted selectors
//! and outer CSR rows once; polygon coordinate windows are derived on demand.
//! Only Identity and Window selections expose contiguous slices; Gather never
//! rebuilds an input CSR payload here.
//! `collect_rows` carries missingness separately as `Option<PackedRow>`.
//! Output-producing kernels write logical rows once into their final coordinate
//! and offset columns.

use std::borrow::Cow;
use std::sync::Arc;

use crate::array::missing::MissingIter;
#[cfg(test)]
use crate::array::packed_column_kernels;
use crate::array::{
    ComputedCaches, CoordSeq, CoordinateAxes, CoordinateReplacement, CsrOffsetBuilder,
    CsrOffsetColumn, Frame, GeometryArrayStorage, PackedColumnBuilder, PackedRow, PackedStorage,
    Point, PointRowLayout, PolygonLevel, PyErr, PyGeometryArray, PyResult, Python, ReplacementAxis,
    RingLevel, RowSelectionRef, StorageLayout, ValidityMask, admit_packed_total, csr_value_window,
    map_masked_point_columns_to_crs, materialized_packed_lines_len,
    materialized_packed_polygons_len, rows_err,
};
use crate::crs::TransformFailure;
use crate::geometry::{CoordSeqBuilder, CoordWindow, MOrdinate, ZOrdinate};

pub(crate) enum PackedColumnError {
    Batch(crate::error::Error),
    Row {
        row: usize,
        error: crate::error::Error,
    },
}

impl From<crate::error::Error> for PackedColumnError {
    fn from(error: crate::error::Error) -> Self {
        Self::Batch(error)
    }
}

pub(crate) type PackedColumnResult<T> = std::result::Result<T, PackedColumnError>;

/// Logical point rows over packed coordinates.
///
/// The cursor owns the only sequential validity/rank walk used by point
/// kernels. Row-aligned masks map a present logical row directly; compact
/// masks feed the present rank through the selected physical-row map. The
/// layout is supplied by the owning `ArrayRows` variant, never inferred from
/// mask and selection lengths.
#[derive(Clone, Copy)]
pub(crate) struct PointRows<'a> {
    logical_len: usize,
    row_map: RowSelectionRef<'a>,
    missing: Option<&'a ValidityMask>,
    layout: PointRowLayout,
}

impl<'a> PointRows<'a> {
    pub(crate) fn new(
        coords_len: usize,
        row_map: RowSelectionRef<'a>,
        missing: Option<&'a ValidityMask>,
        layout: PointRowLayout,
    ) -> Self {
        let selected_len = row_map.len(coords_len);
        let logical_len = missing.map_or(selected_len, ValidityMask::len);
        debug_assert!(
            !layout.is_compact() || missing.is_some(),
            "compact point rows require a validity mask"
        );
        Self {
            logical_len,
            row_map,
            missing,
            layout,
        }
    }

    pub(crate) const fn len(self) -> usize {
        self.logical_len
    }

    pub(crate) const fn present_count(self) -> usize {
        match self.missing {
            Some(mask) => mask.present_count(),
            None => self.logical_len,
        }
    }

    pub(crate) const fn is_compact(self) -> bool {
        self.layout.is_compact()
    }

    pub(crate) fn cursor(self) -> PointRowCursor<'a> {
        PointRowCursor {
            logical: 0,
            remaining: self.logical_len,
            row_map: self.row_map,
            layout: self.layout,
            validity: self.missing.map(ValidityMask::iter),
            present: 0,
        }
    }
}

pub(crate) struct PointRowCursor<'a> {
    logical: usize,
    remaining: usize,
    row_map: RowSelectionRef<'a>,
    layout: PointRowLayout,
    validity: Option<MissingIter<'a>>,
    present: usize,
}

impl Iterator for PointRowCursor<'_> {
    type Item = (usize, Option<usize>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let logical = self.logical;
        self.logical = self.logical.wrapping_add(1);
        self.remaining = self.remaining.wrapping_sub(1);
        let physical = match &mut self.validity {
            Some(validity) => {
                let missing = validity.next().expect("point validity length is exact");
                if missing {
                    None
                } else {
                    let present = self.present;
                    self.present = self.present.wrapping_add(1);
                    Some(self.row_map.physical(if self.layout.is_compact() {
                        present
                    } else {
                        logical
                    }))
                }
            },
            None => Some(self.row_map.physical(logical)),
        };
        Some((logical, physical))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for PointRowCursor<'_> {}

pub(crate) struct PointColumns<'a> {
    coords: &'a CoordSeq,
    row_map: RowSelectionRef<'a>,
    missing: Option<&'a ValidityMask>,
    layout: PointRowLayout,
}

pub(crate) struct XyColumns<'a> {
    pub xs: &'a [f64],
    pub ys: &'a [f64],
}

impl PointColumns<'_> {
    pub(crate) const fn row_layout(&self) -> PointRowLayout {
        self.layout
    }

    pub(crate) const fn selection(&self) -> RowSelectionRef<'_> {
        self.row_map
    }
    pub(crate) const fn coords(&self) -> &CoordSeq {
        self.coords
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "physical is yielded by PointRows; XY-only kernels avoid loading unused Z/M ordinates"
    )]
    pub(crate) fn physical_xy_at(&self, physical: usize) -> (f64, f64) {
        (self.coords.xs()[physical], self.coords.ys()[physical])
    }

    pub(crate) fn physical_xy(&self) -> XyColumns<'_> {
        let coords = self.coords();
        XyColumns {
            xs: coords.xs(),
            ys: coords.ys(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.missing
            .map_or_else(|| self.row_map.len(self.coords().len()), ValidityMask::len)
    }

    pub(crate) fn rows(&self) -> PointRows<'_> {
        PointRows::new(self.coords.len(), self.row_map, self.missing, self.layout)
    }

    pub(crate) fn contiguous_coords(&self) -> Option<CoordSeq> {
        if self.missing.is_some() {
            return None;
        }
        match self.row_map {
            RowSelectionRef::Identity => Some(self.coords().clone()),
            RowSelectionRef::Window { start, len } => Some(self.coords.view(
                CoordWindow::from_range(start..start.wrapping_add(len), self.coords.len()),
            )),
            RowSelectionRef::Gather(_) => None,
        }
    }

    pub(crate) fn contiguous_xy(&self) -> Option<XyColumns<'_>> {
        if self.missing.is_some() {
            return None;
        }
        let range = match self.row_map {
            RowSelectionRef::Identity => 0..self.coords.len(),
            RowSelectionRef::Window { start, len } => start..start.wrapping_add(len),
            RowSelectionRef::Gather(_) => return None,
        };
        Some(XyColumns {
            xs: crate::array::column_window(self.coords.xs(), &range),
            ys: crate::array::column_window(self.coords.ys(), &range),
        })
    }

    pub(crate) fn geodesic_xy_lanes(&self) -> (Cow<'_, [f64]>, Cow<'_, [f64]>) {
        debug_assert!(
            self.missing.is_none(),
            "geodesic lanes require present rows"
        );
        if let Some(columns) = self.contiguous_xy() {
            return (Cow::Borrowed(columns.xs), Cow::Borrowed(columns.ys));
        }
        let mut xs = Vec::with_capacity(self.len());
        let mut ys = Vec::with_capacity(self.len());
        for (_, physical) in self.rows().cursor() {
            let physical = physical.expect("geodesic lanes require present rows");
            let (x, y) = self.physical_xy_at(physical);
            xs.push(x);
            ys.push(y);
        }
        (Cow::Owned(xs), Cow::Owned(ys))
    }

    pub(crate) fn iter(&self) -> impl ExactSizeIterator<Item = Point> + '_ {
        let coords = self.coords();
        let axes = coords.axes();
        self.rows().cursor().map(move |(_, physical)| {
            physical.map_or_else(|| missing_point(axes), |physical| coords.point_at(physical))
        })
    }
}

pub(crate) struct LineColumns<'a> {
    coords: &'a CoordSeq,
    offsets: &'a CsrOffsetColumn,
    row_map: RowSelectionRef<'a>,
    missing: Option<&'a ValidityMask>,
}

impl<'a> LineColumns<'a> {
    pub(crate) const fn selection(&self) -> RowSelectionRef<'a> {
        self.row_map
    }

    pub(crate) const fn coords(&self) -> &CoordSeq {
        self.coords
    }

    pub(crate) fn offsets(&self) -> &[i32] {
        self.offsets.as_slice()
    }

    pub(crate) fn physical_xy(&self) -> XyColumns<'_> {
        let coords = self.coords();
        XyColumns {
            xs: coords.xs(),
            ys: coords.ys(),
        }
    }

    pub(crate) fn rows(&self) -> usize {
        self.row_map.len(self.offsets().len().saturating_sub(1))
    }

    pub(crate) fn is_missing(&self, logical: usize) -> bool {
        self.missing.is_some_and(|mask| mask.is_missing(logical))
    }

    pub(crate) fn row_window(&self, logical: usize) -> std::ops::Range<usize> {
        self.packed_row(logical)
            .vertex_window()
            .expect("packed line row has a coordinate window")
    }

    pub(crate) fn packed_row(&self, logical: usize) -> PackedRow<'a> {
        PackedRow::line(logical, self.coords, self.offsets, self.row_map)
    }

    pub(crate) const fn from_parts(
        coords: &'a CoordSeq,
        offsets: &'a CsrOffsetColumn,
        row_map: RowSelectionRef<'a>,
    ) -> Self {
        resolve_line_columns(coords, offsets, row_map, None)
    }

    pub(crate) fn materialized_vertex_len(&self) -> crate::error::Result<usize> {
        materialized_packed_lines_len(self.offsets(), self.row_map)
    }

    pub(crate) fn materialized_present_vertices(
        &self,
        minimum_row_len: usize,
    ) -> crate::error::Result<usize> {
        let mut vertices = 0;
        for row in 0..self.rows() {
            if self.is_missing(row) {
                continue;
            }
            let len = self.row_window(row).len();
            if len >= minimum_row_len {
                vertices = admit_packed_total(vertices, len)?;
            }
        }
        Ok(vertices)
    }

    /// Reverse each selected CSR window into identity columns. Identity
    /// reverse keeps sharing the source offset table via
    /// [`crate::array::reverse_coord_windows`] instead.
    pub(crate) fn reverse_selected(&self) -> crate::error::Result<(CoordSeq, CsrOffsetColumn)> {
        let vertices = self.materialized_vertex_len()?;
        let mut builder = PackedColumnBuilder::like(self.coords, vertices, self.rows());
        for logical in 0..self.rows() {
            builder.push_reversed_window_admitted(self.coords, self.row_window(logical))?;
        }
        let cap = builder.vertex_len();
        builder.finish(cap)
    }
}

pub(crate) struct PolygonColumns<'a> {
    coords: &'a CoordSeq,
    ring_offsets: &'a CsrOffsetColumn<RingLevel>,
    polygon_offsets: &'a CsrOffsetColumn<PolygonLevel>,
    row_map: RowSelectionRef<'a>,
    missing: Option<&'a ValidityMask>,
}

/// Topology-only descriptor for dual-strategy columnar reduction over packed
/// line/ring storage.
///
/// Owns **offsets and missingness only** — arithmetic stays monomorphized in
/// the domain kernel (length, perimeter, …). Leaf offsets bound contiguous
/// vertex runs (one line, one ring). Group offsets map output rows onto leaf
/// ranges (`None` = identity for lines; `Some` = polygon→ring CSR). Group-level
/// missing masks mark NaN output slots without exposing placeholder coordinates.
#[derive(Clone, Copy)]
pub(crate) struct SegmentedRuns<'a> {
    /// Leaf run boundaries in the physical vertex column (`len = n_leaves + 1`).
    pub leaf_offsets: &'a [i32],
    /// Group boundaries in leaf space (`len = n_groups + 1`). `None` means
    /// identity: group `i` owns only leaf `i`.
    pub group_offsets: Option<&'a [i32]>,
    /// Per-group missing mask (`None` = all present). Length = `n_groups`.
    pub group_missing: Option<&'a ValidityMask>,
    pub row_map: RowSelectionRef<'a>,
}

impl SegmentedRuns<'_> {
    pub(crate) fn n_groups(&self) -> usize {
        let physical = self.group_offsets.map_or_else(
            || self.n_leaves(),
            |offsets| offsets.len().saturating_sub(1),
        );
        self.row_map.len(physical)
    }

    pub(crate) const fn n_leaves(&self) -> usize {
        self.leaf_offsets.len().saturating_sub(1)
    }

    /// Leaf-index range owned by `group`.
    pub(crate) fn group_leaves(&self, group: usize) -> std::ops::Range<usize> {
        let group = self.row_map.physical(group);
        self.group_offsets.map_or_else(
            || group..group.wrapping_add(1),
            |offsets| RowSelectionRef::Identity.csr_window(offsets, group),
        )
    }

    /// Vertex window for leaf `leaf` in the physical column.
    pub(crate) const fn leaf_vertices(&self, leaf: usize) -> std::ops::Range<usize> {
        RowSelectionRef::Identity.csr_window(self.leaf_offsets, leaf)
    }

    /// True when the group is marked missing.
    pub(crate) fn is_missing(&self, group: usize) -> bool {
        self.group_missing
            .is_some_and(|mask| mask.is_missing(group))
    }
}

impl LineColumns<'_> {
    /// Topology descriptor for planar line-length dual reduction. Group space
    /// is identity over leaves (one line row = one leaf).
    pub(crate) fn segmented_runs<'a>(
        &'a self,
        missing: Option<&'a ValidityMask>,
    ) -> SegmentedRuns<'a> {
        SegmentedRuns {
            leaf_offsets: self.offsets(),
            group_offsets: None,
            group_missing: missing,
            row_map: self.row_map,
        }
    }
}

impl PolygonColumns<'_> {
    /// Topology descriptor for planar polygon perimeter dual reduction.
    pub(crate) fn segmented_runs<'a>(
        &'a self,
        missing: Option<&'a ValidityMask>,
    ) -> SegmentedRuns<'a> {
        SegmentedRuns {
            leaf_offsets: self.ring_offsets(),
            group_offsets: Some(self.polygon_offsets()),
            group_missing: missing,
            row_map: self.row_map,
        }
    }
}

impl<'a> PolygonColumns<'a> {
    pub(crate) const fn coords(&self) -> &CoordSeq {
        self.coords
    }

    pub(crate) fn ring_offsets(&self) -> &[i32] {
        self.ring_offsets.as_slice()
    }

    pub(crate) fn polygon_offsets(&self) -> &[i32] {
        self.polygon_offsets.as_slice()
    }

    pub(crate) fn physical_xy(&self) -> XyColumns<'_> {
        let coords = self.coords();
        XyColumns {
            xs: coords.xs(),
            ys: coords.ys(),
        }
    }

    pub(crate) fn rows(&self) -> usize {
        self.row_map
            .len(self.polygon_offsets().len().saturating_sub(1))
    }

    pub(crate) fn is_missing(&self, logical: usize) -> bool {
        self.missing.is_some_and(|mask| mask.is_missing(logical))
    }

    pub(crate) fn polygon_rings(&self, logical: usize) -> std::ops::Range<usize> {
        self.row_map.csr_window(self.polygon_offsets(), logical)
    }

    pub(crate) fn packed_row(&self, logical: usize) -> PackedRow<'a> {
        PackedRow::polygon(
            logical,
            self.coords,
            self.ring_offsets,
            self.polygon_offsets,
            self.row_map,
        )
    }

    pub(crate) const fn from_parts(
        coords: &'a CoordSeq,
        ring_offsets: &'a CsrOffsetColumn<RingLevel>,
        polygon_offsets: &'a CsrOffsetColumn<PolygonLevel>,
        row_map: RowSelectionRef<'a>,
    ) -> Self {
        resolve_polygon_columns(coords, ring_offsets, polygon_offsets, row_map, None)
    }

    pub(crate) fn materialized_sizes(&self) -> crate::error::Result<(usize, usize)> {
        materialized_packed_polygons_len(self.ring_offsets(), self.polygon_offsets(), self.row_map)
    }

    pub(crate) fn materialized_present_sizes(&self) -> crate::error::Result<(usize, usize)> {
        let mut vertices = 0;
        let mut rings = 0;
        for row in 0..self.rows() {
            if self.is_missing(row) {
                continue;
            }
            let row_rings = self.polygon_rings(row);
            if !row_rings.is_empty() {
                let offsets = self.ring_offsets();
                vertices = admit_packed_total(
                    vertices,
                    csr_value_window(offsets, row_rings.clone()).len(),
                )?;
            }
            rings = admit_packed_total(rings, row_rings.len())?;
        }
        Ok((vertices, rings))
    }

    pub(crate) fn reverse_selected(
        &self,
    ) -> crate::error::Result<(
        CoordSeq,
        CsrOffsetColumn<RingLevel>,
        CsrOffsetColumn<PolygonLevel>,
    )> {
        let (vertices, rings) = self.materialized_sizes()?;
        let mut builder = PackedColumnBuilder::like(self.coords, vertices, rings);
        let mut polygon_builder = CsrOffsetBuilder::with_capacity(self.rows());
        let mut ring_index = 0_usize;
        for logical in 0..self.rows() {
            let row = self.packed_row(logical);
            let row_rings = row
                .polygon_rings()
                .expect("packed polygon row has a ring range");
            for ring in row_rings.clone() {
                let window = row
                    .polygon_ring_window(ring)
                    .expect("ring lies in its admitted polygon row");
                builder.push_reversed_window_admitted(self.coords, window)?;
            }
            ring_index = ring_index.wrapping_add(row_rings.len());
            polygon_builder.push_end(ring_index, ring_index)?;
        }
        let cap = builder.vertex_len();
        let (out_coords, out_ring_offsets) = builder.finish(cap)?;
        let out_ring_offsets = out_ring_offsets.cast_level::<RingLevel>();
        let out_polygon_offsets = polygon_builder
            .finish(out_ring_offsets.len().saturating_sub(1))?
            .cast_level::<PolygonLevel>();
        Ok((out_coords, out_ring_offsets, out_polygon_offsets))
    }
}

pub(crate) enum PackedColumns<'a> {
    Points(PointColumns<'a>),
    Lines(LineColumns<'a>),
    Polygons(PolygonColumns<'a>),
}

impl PackedColumns<'_> {
    pub(crate) fn physical_xy(&self) -> XyColumns<'_> {
        match self {
            Self::Points(columns) => columns.physical_xy(),
            Self::Lines(columns) => columns.physical_xy(),
            Self::Polygons(columns) => columns.physical_xy(),
        }
    }

    pub(crate) fn row_count(&self) -> usize {
        match self {
            Self::Points(columns) => columns.len(),
            Self::Lines(columns) => columns.rows(),
            Self::Polygons(columns) => columns.rows(),
        }
    }

    pub(crate) fn collect_rows<T, C: FromIterator<T>>(
        &self,
        mut f: impl FnMut(Option<PackedRow<'_>>) -> T,
    ) -> C {
        match self {
            Self::Points(columns) => columns
                .rows()
                .cursor()
                .map(|(logical, physical)| {
                    f(physical
                        .map(|physical| PackedRow::point_at(logical, columns.coords, physical)))
                })
                .collect(),
            Self::Lines(columns) => (0..columns.rows())
                .map(|logical| {
                    f((!columns.is_missing(logical)).then(|| columns.packed_row(logical)))
                })
                .collect(),
            Self::Polygons(columns) => (0..columns.rows())
                .map(|logical| {
                    f((!columns.is_missing(logical)).then(|| columns.packed_row(logical)))
                })
                .collect(),
        }
    }
}

pub(crate) enum PackedColumnOutput {
    Points(CoordSeq),
    Lines {
        coords: CoordSeq,
        offsets: CsrOffsetColumn,
    },
    Polygons {
        coords: CoordSeq,
        ring_offsets: CsrOffsetColumn<RingLevel>,
        polygon_offsets: CsrOffsetColumn<PolygonLevel>,
    },
}

pub(crate) fn packed_columns_err(error: PackedColumnError) -> PyErr {
    match error {
        PackedColumnError::Batch(error) => error.into(),
        PackedColumnError::Row { row, error } => rows_err((row, error)),
    }
}

pub(super) const fn batch_err(error: crate::error::Error) -> PackedColumnError {
    PackedColumnError::Batch(error)
}

pub(super) const fn row_err(row: usize, error: crate::error::Error) -> PackedColumnError {
    PackedColumnError::Row { row, error }
}

const fn resolve_point_columns<'a>(
    coords: &'a CoordSeq,
    row_map: RowSelectionRef<'a>,
    missing: Option<&'a ValidityMask>,
    layout: PointRowLayout,
) -> PointColumns<'a> {
    PointColumns {
        coords,
        row_map,
        missing,
        layout,
    }
}

const fn resolve_line_columns<'a>(
    coords: &'a CoordSeq,
    offsets: &'a CsrOffsetColumn,
    row_map: RowSelectionRef<'a>,
    missing: Option<&'a ValidityMask>,
) -> LineColumns<'a> {
    LineColumns {
        coords,
        offsets,
        row_map,
        missing,
    }
}

const fn resolve_polygon_columns<'a>(
    coords: &'a CoordSeq,
    ring_offsets: &'a CsrOffsetColumn<RingLevel>,
    polygon_offsets: &'a CsrOffsetColumn<PolygonLevel>,
    row_map: RowSelectionRef<'a>,
    missing: Option<&'a ValidityMask>,
) -> PolygonColumns<'a> {
    PolygonColumns {
        coords,
        ring_offsets,
        polygon_offsets,
        row_map,
        missing,
    }
}

fn resolve_packed_columns<'a>(
    storage: &'a GeometryArrayStorage,
    missing: Option<&'a ValidityMask>,
    point_layout: PointRowLayout,
) -> Option<PackedColumns<'a>> {
    match storage {
        GeometryArrayStorage::Packed(packed) => {
            Some(resolve_columns(packed, missing, point_layout))
        },
        GeometryArrayStorage::Mixed(_) => None,
    }
}

fn resolve_columns<'a>(
    storage: &'a PackedStorage,
    missing: Option<&'a ValidityMask>,
    point_layout: PointRowLayout,
) -> PackedColumns<'a> {
    match storage {
        PackedStorage::Points { coords, row_map } => PackedColumns::Points(resolve_point_columns(
            coords,
            row_map.as_deref(),
            missing,
            point_layout,
        )),
        PackedStorage::Lines {
            coords,
            offsets,
            row_map,
        } => PackedColumns::Lines(resolve_line_columns(
            coords,
            offsets,
            row_map.as_deref(),
            missing,
        )),
        PackedStorage::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
            row_map,
        } => PackedColumns::Polygons(resolve_polygon_columns(
            coords,
            ring_offsets,
            polygon_offsets,
            row_map.as_deref(),
            missing,
        )),
    }
}

fn assemble_packed_output(output: PackedColumnOutput) -> GeometryArrayStorage {
    match output {
        PackedColumnOutput::Points(coords) => GeometryArrayStorage::Packed(PackedStorage::Points {
            coords: Arc::new(coords),
            row_map: crate::array::RowSelection::Identity,
        }),
        PackedColumnOutput::Lines { coords, offsets } => {
            GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords: Arc::new(coords),
                offsets,
                row_map: crate::array::RowSelection::Identity,
            })
        },
        PackedColumnOutput::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
        } => GeometryArrayStorage::Packed(PackedStorage::Polygons {
            coords: Arc::new(coords),
            ring_offsets,
            polygon_offsets,
            row_map: crate::array::RowSelection::Identity,
        }),
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "replacement lengths are admitted for the selected output, and old_index belongs to a resolved packed row window"
)]
fn replacement_axis_value(
    axis: &ReplacementAxis,
    replacement_index: usize,
    old: Option<&[f64]>,
    old_index: usize,
) -> Option<f64> {
    match axis {
        ReplacementAxis::Replace(values) => Some(values[replacement_index]),
        ReplacementAxis::Carry => old.map(|column| column[old_index]),
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "replacement X/Y columns are admitted for the selected output vertex count"
)]
fn push_replacement_vertex(
    builder: &mut CoordSeqBuilder,
    old: &CoordSeq,
    old_index: usize,
    replacement: &CoordinateReplacement,
    replacement_index: usize,
) {
    builder.push_xyzm(
        replacement.xs[replacement_index],
        replacement.ys[replacement_index],
        replacement_axis_value(&replacement.zs, replacement_index, old.zs(), old_index),
        replacement_axis_value(&replacement.ms, replacement_index, old.ms(), old_index),
    );
}

fn validate_replacement_ring(
    coords: &CoordSeq,
    window: std::ops::Range<usize>,
) -> PackedColumnResult<()> {
    match crate::array::CheckedRingWindow::checked(coords, window) {
        Ok(_) => Ok(()),
        Err(crate::array::RingWindowError::TooShort(len)) => Err(PackedColumnError::Batch(
            crate::geometry::GeometryErrorKind::RingTooShort(len).into(),
        )),
        Err(crate::array::RingWindowError::Open) => Err(PackedColumnError::Batch(
            crate::geometry::GeometryErrorKind::message("polygon ring must be closed"),
        )),
        Err(
            crate::array::RingWindowError::Inverted | crate::array::RingWindowError::OutOfRange,
        ) => Err(PackedColumnError::Batch(
            crate::geometry::GeometryErrorKind::message("polygon ring range is invalid"),
        )),
    }
}

fn finish_replacement_builder(
    builder: CoordSeqBuilder,
    allow_mask_placeholders: bool,
) -> PackedColumnResult<CoordSeq> {
    if allow_mask_placeholders {
        builder
            .finish_with_missing_placeholders()
            .map_err(batch_err)
    } else {
        builder.finish().map_err(batch_err)
    }
}

fn packed_row_missing<const HAS_MISSING: bool>(missing: Option<&ValidityMask>, row: usize) -> bool {
    if HAS_MISSING {
        missing
            .expect("HAS_MISSING rows carry a mask")
            .is_missing(row)
    } else {
        false
    }
}

fn replace_point_columns<const HAS_MISSING: bool>(
    point_columns: &PointColumns<'_>,
    replacement: &CoordinateReplacement,
    replacement_index: &mut usize,
) -> PackedColumnResult<PackedColumnOutput> {
    let old = point_columns.coords();
    let rows = point_columns.rows();
    let capacity = if rows.is_compact() {
        rows.present_count()
    } else {
        rows.len()
    };
    crate::array::admit_packed_coord_count(capacity).map_err(batch_err)?;
    let mut builder = CoordSeqBuilder::with_capacity(old.axes(), capacity);
    for (_row, physical) in rows.cursor() {
        if let Some(physical) = physical {
            push_replacement_vertex(&mut builder, old, physical, replacement, *replacement_index);
            *replacement_index = (*replacement_index).wrapping_add(1);
        } else if !rows.is_compact() {
            debug_assert!(HAS_MISSING);
            builder.push(missing_point(old.axes()));
        }
    }
    let coords = if rows.is_compact() {
        builder.finish().map_err(batch_err)?
    } else {
        finish_replacement_builder(builder, HAS_MISSING)?
    };
    Ok(PackedColumnOutput::Points(coords))
}

fn replace_line_columns<const HAS_MISSING: bool>(
    line_columns: &LineColumns<'_>,
    replacement: &CoordinateReplacement,
    replacement_index: &mut usize,
    missing: Option<&ValidityMask>,
) -> PackedColumnResult<PackedColumnOutput> {
    let old = line_columns.coords();
    let capacity = line_columns.materialized_present_vertices(2)?;
    let mut builder = CoordSeqBuilder::with_capacity(old.axes(), capacity);
    let mut offsets = crate::array::CsrOffsetBuilder::with_capacity(line_columns.rows());
    for row in 0..line_columns.rows() {
        if packed_row_missing::<HAS_MISSING>(missing, row) {
            // Missing rows carry no logical child coordinates.
        } else {
            let window = line_columns.row_window(row);
            if window.len() >= 2 {
                for old_index in window {
                    push_replacement_vertex(
                        &mut builder,
                        old,
                        old_index,
                        replacement,
                        *replacement_index,
                    );
                    *replacement_index = (*replacement_index).wrapping_add(1);
                }
            }
        }
        offsets
            .push_end(builder.len(), i32::MAX as usize)
            .map_err(batch_err)?;
    }
    let coords = finish_replacement_builder(builder, HAS_MISSING)?;
    let cap = coords.len();
    Ok(PackedColumnOutput::Lines {
        coords,
        offsets: offsets.finish(cap).map_err(batch_err)?,
    })
}

fn replace_polygon_columns<const HAS_MISSING: bool>(
    polygon_columns: &PolygonColumns<'_>,
    replacement: &CoordinateReplacement,
    replacement_index: &mut usize,
    missing: Option<&ValidityMask>,
) -> PackedColumnResult<PackedColumnOutput> {
    let old = polygon_columns.coords();
    let (vertex_capacity, ring_capacity) = polygon_columns.materialized_present_sizes()?;
    let mut builder = CoordSeqBuilder::with_capacity(old.axes(), vertex_capacity);
    let mut ring_builder = crate::array::CsrOffsetBuilder::with_capacity(ring_capacity);
    let mut polygon_builder = crate::array::CsrOffsetBuilder::with_capacity(polygon_columns.rows());
    let mut ring_count = 0_usize;
    let mut validate_windows = Vec::with_capacity(ring_capacity);
    for row in 0..polygon_columns.rows() {
        if packed_row_missing::<HAS_MISSING>(missing, row) {
            polygon_builder
                .push_end(ring_count, i32::MAX as usize)
                .map_err(batch_err)?;
            continue;
        }
        let packed_row = polygon_columns.packed_row(row);
        let row_rings = packed_row
            .polygon_rings()
            .expect("packed polygon row has a ring window");
        for ring in row_rings.clone() {
            let window = packed_row
                .polygon_ring_window(ring)
                .expect("ring lies in its admitted polygon row");
            let output_start = builder.len();
            for old_index in window {
                push_replacement_vertex(
                    &mut builder,
                    old,
                    old_index,
                    replacement,
                    *replacement_index,
                );
                *replacement_index = (*replacement_index).wrapping_add(1);
            }
            validate_windows.push(output_start..builder.len());
            ring_builder
                .push_end(builder.len(), i32::MAX as usize)
                .map_err(batch_err)?;
        }
        ring_count = ring_count.wrapping_add(row_rings.len());
        polygon_builder
            .push_end(ring_count, i32::MAX as usize)
            .map_err(batch_err)?;
    }
    let coords = finish_replacement_builder(builder, HAS_MISSING)?;
    let coord_len = coords.len();
    for window in validate_windows {
        validate_replacement_ring(&coords, window)?;
    }
    Ok(PackedColumnOutput::Polygons {
        coords,
        ring_offsets: ring_builder
            .finish(coord_len)
            .map_err(batch_err)?
            .cast_level(),
        polygon_offsets: polygon_builder
            .finish(ring_count)
            .map_err(batch_err)?
            .cast_level(),
    })
}

fn replace_packed_columns_for_mask<const HAS_MISSING: bool>(
    columns: PackedColumns<'_>,
    replacement: &CoordinateReplacement,
    replacement_index: &mut usize,
    missing: Option<&ValidityMask>,
) -> PackedColumnResult<PackedColumnOutput> {
    match columns {
        PackedColumns::Points(point_columns) => {
            replace_point_columns::<HAS_MISSING>(&point_columns, replacement, replacement_index)
        },
        PackedColumns::Lines(line_columns) => replace_line_columns::<HAS_MISSING>(
            &line_columns,
            replacement,
            replacement_index,
            missing,
        ),
        PackedColumns::Polygons(polygon_columns) => replace_polygon_columns::<HAS_MISSING>(
            &polygon_columns,
            replacement,
            replacement_index,
            missing,
        ),
    }
}

fn replace_packed_columns_dispatch(
    columns: PackedColumns<'_>,
    replacement: &CoordinateReplacement,
    replacement_index: &mut usize,
    missing: Option<&ValidityMask>,
) -> PackedColumnResult<PackedColumnOutput> {
    match missing {
        Some(mask) => replace_packed_columns_for_mask::<true>(
            columns,
            replacement,
            replacement_index,
            Some(mask),
        ),
        None => {
            replace_packed_columns_for_mask::<false>(columns, replacement, replacement_index, None)
        },
    }
}

fn finish_pointwise_coords(
    builder: CoordSeqBuilder,
    has_missing: bool,
) -> PackedColumnResult<CoordSeq> {
    if has_missing {
        builder
            .finish_with_missing_placeholders()
            .map_err(batch_err)
    } else {
        builder.finish().map_err(batch_err)
    }
}

fn missing_point(axes: CoordinateAxes) -> Point {
    Point::new_raw_axes(
        f64::NAN,
        f64::NAN,
        ZOrdinate(axes.has_z().then_some(f64::NAN)),
        MOrdinate(axes.has_m().then_some(f64::NAN)),
    )
}

fn map_selected_points(
    columns: PackedColumns<'_>,
    output_axes: CoordinateAxes,
    mut map: impl FnMut(Point) -> crate::error::Result<Point>,
) -> PackedColumnResult<PackedColumnOutput> {
    match columns {
        PackedColumns::Points(points) => {
            let rows = points.rows();
            let capacity = if rows.is_compact() {
                rows.present_count()
            } else {
                rows.len()
            };
            crate::array::admit_packed_coord_count(capacity).map_err(batch_err)?;
            let mut builder = CoordSeqBuilder::with_capacity(output_axes, capacity);
            let mut has_missing = false;
            for (logical, physical) in rows.cursor() {
                let Some(physical) = physical else {
                    if rows.is_compact() {
                        continue;
                    }
                    has_missing = true;
                    builder.push(missing_point(output_axes));
                    continue;
                };
                builder.push(
                    map(points.coords().point_at(physical))
                        .map_err(|error| row_err(logical, error))?,
                );
            }
            let coords = if rows.is_compact() {
                builder.finish().map_err(batch_err)?
            } else {
                finish_pointwise_coords(builder, has_missing)?
            };
            Ok(PackedColumnOutput::Points(coords))
        },
        PackedColumns::Lines(lines) => {
            let capacity = lines.materialized_present_vertices(2)?;
            let mut builder = CoordSeqBuilder::with_capacity(output_axes, capacity);
            let mut offsets = CsrOffsetBuilder::with_capacity(lines.rows());
            for logical in 0..lines.rows() {
                if !lines.is_missing(logical) {
                    let window = lines.row_window(logical);
                    if window.len() >= 2 {
                        for physical in window {
                            builder.push(
                                map(lines.coords().point_at(physical))
                                    .map_err(|error| row_err(logical, error))?,
                            );
                        }
                    }
                }
                offsets
                    .push_end(builder.len(), i32::MAX as usize)
                    .map_err(batch_err)?;
            }
            let coords = finish_pointwise_coords(builder, lines.missing.is_some())?;
            let len = coords.len();
            Ok(PackedColumnOutput::Lines {
                coords,
                offsets: offsets.finish(len).map_err(batch_err)?,
            })
        },
        PackedColumns::Polygons(polygons) => {
            let (capacity, ring_capacity) = polygons.materialized_present_sizes()?;
            let mut builder = CoordSeqBuilder::with_capacity(output_axes, capacity);
            let mut rings = CsrOffsetBuilder::with_capacity(ring_capacity);
            let mut polygon_offsets = CsrOffsetBuilder::with_capacity(polygons.rows());
            let mut ring_count = 0_usize;
            for logical in 0..polygons.rows() {
                if !polygons.is_missing(logical) {
                    let packed_row = polygons.packed_row(logical);
                    let row_rings = packed_row
                        .polygon_rings()
                        .expect("packed polygon row has a ring range");
                    for ring in row_rings.clone() {
                        let window = packed_row
                            .polygon_ring_window(ring)
                            .expect("ring lies in its admitted polygon row");
                        for physical in window {
                            builder.push(
                                map(polygons.coords().point_at(physical))
                                    .map_err(|error| row_err(logical, error))?,
                            );
                        }
                        rings
                            .push_end(builder.len(), i32::MAX as usize)
                            .map_err(batch_err)?;
                    }
                    ring_count = ring_count.wrapping_add(row_rings.len());
                }
                polygon_offsets
                    .push_end(ring_count, i32::MAX as usize)
                    .map_err(batch_err)?;
            }
            let coords = finish_pointwise_coords(builder, polygons.missing.is_some())?;
            let len = coords.len();
            Ok(PackedColumnOutput::Polygons {
                coords,
                ring_offsets: rings.finish(len).map_err(batch_err)?.cast_level(),
                polygon_offsets: polygon_offsets
                    .finish(ring_count)
                    .map_err(batch_err)?
                    .cast_level(),
            })
        },
    }
}

fn transform_selected_coords(
    transformer: &crate::crs::TransformPlan,
    coords: &CoordSeq,
    indices: impl IntoIterator<Item = usize>,
    capacity: usize,
) -> std::result::Result<CoordSeq, TransformFailure> {
    let mut xs = Vec::with_capacity(capacity);
    let mut ys = Vec::with_capacity(capacity);
    let mut zs = coords.zs().map(|_| Vec::with_capacity(capacity));
    let mut ms = coords.ms().map(|_| Vec::with_capacity(capacity));
    for physical in indices {
        let point = coords.point_at(physical);
        xs.push(point.x);
        ys.push(point.y);
        if let Some(output) = zs.as_mut() {
            output.push(point.z().expect("Z output preserves the source axis"));
        }
        if let Some(output) = ms.as_mut() {
            output.push(point.m().expect("M output preserves the source axis"));
        }
    }
    let zt = zs.as_mut().map_or(crate::Zt::None, |values| {
        crate::Zt::Z(values.as_mut_slice())
    });
    transformer.transform_coordinates_indexed(&mut xs, &mut ys, zt)?;
    Ok(CoordSeq::try_from_vecs(xs, ys, zs, ms)?)
}

fn packed_transform_failure(failure: TransformFailure, row: Option<usize>) -> PackedColumnError {
    match (failure.coordinate, row) {
        (Some(_), Some(row)) => row_err(row, failure.error),
        _ => batch_err(failure.error),
    }
}

fn point_row_for_coordinate(rows: PointRows<'_>, coordinate: usize) -> Option<usize> {
    rows.cursor()
        .filter_map(|(logical, physical)| physical.map(|_| logical))
        .nth(coordinate)
}

fn offset_row_for_coordinate(offsets: &[i32], coordinate: usize) -> Option<usize> {
    offsets
        .partition_point(|&end| end as usize <= coordinate)
        .checked_sub(1)
        .filter(|&row| row < offsets.len().saturating_sub(1))
}

fn polygon_row_for_coordinate(
    ring_offsets: &[i32],
    polygon_offsets: &[i32],
    coordinate: usize,
) -> Option<usize> {
    let ring = offset_row_for_coordinate(ring_offsets, coordinate)?;
    offset_row_for_coordinate(polygon_offsets, ring)
}

fn map_selected_to_crs(
    columns: PackedColumns<'_>,
    transformer: &crate::crs::TransformPlan,
) -> PackedColumnResult<PackedColumnOutput> {
    match columns {
        PackedColumns::Points(points) => {
            let rows = points.rows();
            if let Some(missing) = points.missing {
                let coords = map_masked_point_columns_to_crs(
                    transformer,
                    points.coords(),
                    points.selection(),
                    missing,
                    points.row_layout(),
                )
                .map_err(|failure| {
                    let row = failure
                        .coordinate
                        .and_then(|coordinate| point_row_for_coordinate(rows, coordinate));
                    packed_transform_failure(failure, row)
                })?;
                return Ok(PackedColumnOutput::Points(coords));
            }
            crate::array::admit_packed_coord_count(points.len()).map_err(batch_err)?;
            let coords = transform_selected_coords(
                transformer,
                points.coords(),
                points
                    .rows()
                    .cursor()
                    .map(|(_, physical)| physical.expect("dense point rows are present")),
                points.len(),
            )
            .map_err(|failure| {
                let row = failure
                    .coordinate
                    .and_then(|coordinate| point_row_for_coordinate(rows, coordinate));
                packed_transform_failure(failure, row)
            })?;
            Ok(PackedColumnOutput::Points(coords))
        },
        PackedColumns::Lines(lines) => {
            let coordinate_count = lines.materialized_present_vertices(2).map_err(batch_err)?;
            let mut offsets = CsrOffsetBuilder::with_capacity(lines.rows());
            let mut output_coordinate_count = 0_usize;
            for logical in 0..lines.rows() {
                if !lines.is_missing(logical) {
                    let len = lines.row_window(logical).len();
                    if len >= 2 {
                        output_coordinate_count = output_coordinate_count.wrapping_add(len);
                    }
                }
                offsets
                    .push_end(output_coordinate_count, i32::MAX as usize)
                    .map_err(batch_err)?;
            }
            debug_assert_eq!(output_coordinate_count, coordinate_count);
            let offsets = offsets.finish(coordinate_count).map_err(batch_err)?;
            let indices = (0..lines.rows()).flat_map(|logical| {
                if lines.is_missing(logical) || lines.row_window(logical).len() < 2 {
                    0..0
                } else {
                    lines.row_window(logical)
                }
            });
            let coords =
                transform_selected_coords(transformer, lines.coords(), indices, coordinate_count)
                    .map_err(|failure| {
                    let row = failure.coordinate.and_then(|coordinate| {
                        offset_row_for_coordinate(offsets.as_slice(), coordinate)
                    });
                    packed_transform_failure(failure, row)
                })?;
            Ok(PackedColumnOutput::Lines { coords, offsets })
        },
        PackedColumns::Polygons(polygons) => {
            let (coordinate_count, ring_count) =
                polygons.materialized_present_sizes().map_err(batch_err)?;
            let mut rings = CsrOffsetBuilder::with_capacity(ring_count);
            let mut polygon_offsets = CsrOffsetBuilder::with_capacity(polygons.rows());
            let mut output_ring_count = 0_usize;
            let mut output_coordinate_count = 0_usize;
            for logical in 0..polygons.rows() {
                if !polygons.is_missing(logical) {
                    let row_rings = polygons.polygon_rings(logical);
                    if !row_rings.is_empty() {
                        let source = polygons.ring_offsets();
                        let source_coordinates = csr_value_window(source, row_rings.clone());
                        let output_start = output_coordinate_count;
                        let mut output_row_end = 0_usize;
                        for ring in row_rings.clone() {
                            output_row_end = output_row_end.wrapping_add(
                                csr_value_window(source, ring..ring.wrapping_add(1)).len(),
                            );
                            rings
                                .push_end(
                                    output_start.wrapping_add(output_row_end),
                                    i32::MAX as usize,
                                )
                                .map_err(batch_err)?;
                        }
                        output_coordinate_count =
                            output_coordinate_count.wrapping_add(source_coordinates.len());
                    }
                    output_ring_count = output_ring_count.wrapping_add(row_rings.len());
                }
                polygon_offsets
                    .push_end(output_ring_count, i32::MAX as usize)
                    .map_err(batch_err)?;
            }
            debug_assert_eq!(output_coordinate_count, coordinate_count);
            debug_assert_eq!(output_ring_count, ring_count);
            let ring_offsets = rings.finish(coordinate_count).map_err(batch_err)?;
            let polygon_offsets = polygon_offsets.finish(ring_count).map_err(batch_err)?;
            let indices = (0..polygons.rows())
                .filter(|&logical| !polygons.is_missing(logical))
                .flat_map(|logical| polygons.polygon_rings(logical))
                .flat_map(|ring| {
                    let source = polygons.ring_offsets();
                    csr_value_window(source, ring..ring.wrapping_add(1))
                });
            let coords = transform_selected_coords(
                transformer,
                polygons.coords(),
                indices,
                coordinate_count,
            )
            .map_err(|failure| {
                let row = failure.coordinate.and_then(|coordinate| {
                    polygon_row_for_coordinate(
                        ring_offsets.as_slice(),
                        polygon_offsets.as_slice(),
                        coordinate,
                    )
                });
                packed_transform_failure(failure, row)
            })?;
            Ok(PackedColumnOutput::Polygons {
                coords,
                ring_offsets: ring_offsets.cast_level(),
                polygon_offsets: polygon_offsets.cast_level(),
            })
        },
    }
}

impl PyGeometryArray {
    fn assemble_packed_result(
        &self,
        output: PackedColumnOutput,
        output_frame: Frame,
    ) -> crate::error::Result<Self> {
        let layout = match (&output, self.storage()) {
            (
                PackedColumnOutput::Points(_),
                GeometryArrayStorage::Packed(PackedStorage::Points { .. }),
            ) if self.is_compact_nullable() => StorageLayout::PresentOnly,
            _ => StorageLayout::LogicalRows,
        };
        Self::from_result_storage(
            Arc::new(assemble_packed_output(output)),
            output_frame,
            self.missing().cloned(),
            layout,
            ComputedCaches::fresh(),
        )
    }

    pub(crate) fn reduce_packed_columns_detached<R>(
        &self,
        py: Python<'_>,
        kernel: impl for<'a> FnOnce(PackedColumns<'a>) -> PackedColumnResult<R> + Send,
    ) -> PyResult<Option<R>>
    where
        R: Send,
    {
        let Some(columns) =
            resolve_packed_columns(self.storage(), self.missing(), self.point_row_layout())
        else {
            return Ok(None);
        };
        let result = py
            .detach(move || kernel(columns))
            .map_err(packed_columns_err)?;
        Ok(Some(result))
    }

    pub(crate) fn map_packed_columns_detached(
        &self,
        py: Python<'_>,
        output_frame: Frame,
        kernel: impl for<'a> FnOnce(PackedColumns<'a>) -> PackedColumnResult<PackedColumnOutput> + Send,
    ) -> PyResult<Option<Self>> {
        let output = self
            .reduce_packed_columns_detached(py, kernel)?
            .map(|output| self.assemble_packed_result(output, output_frame))
            .transpose()?;
        Ok(output)
    }

    /// Like [`Self::map_packed_columns_detached`], but lets a packed kernel
    /// decline a row layout it cannot represent without materializing Shapes.
    /// `None` means the caller should use its general path.
    pub(crate) fn try_map_packed_columns_detached(
        &self,
        py: Python<'_>,
        output_frame: Frame,
        kernel: impl for<'a> FnOnce(PackedColumns<'a>) -> PackedColumnResult<Option<PackedColumnOutput>>
        + Send,
    ) -> PyResult<Option<Self>> {
        let Some(output) = self.reduce_packed_columns_detached(py, kernel)? else {
            return Ok(None);
        };
        let Some(output) = output else {
            return Ok(None);
        };
        Ok(Some(
            self.assemble_packed_result(output, output_frame)
                .map_err(crate::PyErr::from)?,
        ))
    }

    pub(crate) fn map_packed_points_detached(
        &self,
        py: Python<'_>,
        output_frame: Frame,
        output_axes: CoordinateAxes,
        map: impl Fn(Point) -> crate::error::Result<Point> + Send,
    ) -> PyResult<Option<Self>> {
        let Some(columns) =
            resolve_packed_columns(self.storage(), self.missing(), self.point_row_layout())
        else {
            return Ok(None);
        };
        let output = py
            .detach(move || map_selected_points(columns, output_axes, map))
            .map_err(packed_columns_err)?;
        Ok(Some(
            self.assemble_packed_result(output, output_frame)
                .map_err(crate::PyErr::from)?,
        ))
    }

    pub(crate) fn map_packed_to_crs_detached(
        &self,
        py: Python<'_>,
        output_frame: Frame,
        transformer: crate::crs::TransformPlan,
    ) -> PyResult<Option<Self>> {
        let Some(columns) =
            resolve_packed_columns(self.storage(), self.missing(), self.point_row_layout())
        else {
            return Ok(None);
        };
        let output = py
            .detach(move || map_selected_to_crs(columns, &transformer))
            .map_err(packed_columns_err)?;
        Ok(Some(
            self.assemble_packed_result(output, output_frame)
                .map_err(crate::PyErr::from)?,
        ))
    }

    pub(crate) fn replace_packed_coords_detached(
        &self,
        py: Python<'_>,
        output_frame: Frame,
        replacement: CoordinateReplacement,
    ) -> PyResult<Option<Self>> {
        let Some(columns) =
            resolve_packed_columns(self.storage(), self.missing(), self.point_row_layout())
        else {
            return Ok(None);
        };
        let output = py
            .detach(move || -> PackedColumnResult<PackedColumnOutput> {
                let mut replacement_index = 0;
                let output = replace_packed_columns_dispatch(
                    columns,
                    &replacement,
                    &mut replacement_index,
                    self.missing(),
                )?;
                if replacement_index != replacement.len {
                    return Err(PackedColumnError::Batch(
                        crate::geometry::GeometryErrorKind::message(format!(
                            "coordinates must have length {}; got length {replacement_index}",
                            replacement.len
                        )),
                    ));
                }
                Ok(output)
            })
            .map_err(packed_columns_err)?;
        Ok(Some(
            self.assemble_packed_result(output, output_frame)
                .map_err(crate::PyErr::from)?,
        ))
    }

    pub(crate) fn pair_packed_columns_detached<R>(
        py: Python<'_>,
        left: &Self,
        right: &Self,
        kernel: impl for<'a, 'b> FnOnce(PackedColumns<'a>, PackedColumns<'b>) -> PackedColumnResult<R>
        + Send,
    ) -> PyResult<Option<R>>
    where
        R: Send,
    {
        let Some(left_columns) =
            resolve_packed_columns(left.storage(), left.missing(), left.point_row_layout())
        else {
            return Ok(None);
        };
        let Some(right_columns) =
            resolve_packed_columns(right.storage(), right.missing(), right.point_row_layout())
        else {
            return Ok(None);
        };
        let result = py
            .detach(move || kernel(left_columns, right_columns))
            .map_err(packed_columns_err)?;
        Ok(Some(result))
    }

    /// Points-only reduce: returns `None` for mixed or line/polygon packed storage.
    pub(crate) fn reduce_packed_points_detached<R>(
        &self,
        py: Python<'_>,
        kernel: impl FnOnce(PointColumns<'_>) -> PackedColumnResult<R> + Send,
    ) -> PyResult<Option<R>>
    where
        R: Send,
    {
        let GeometryArrayStorage::Packed(PackedStorage::Points { .. }) = self.storage() else {
            return Ok(None);
        };
        let storage = Arc::clone(self.storage_arc());
        let missing = self.missing().cloned();
        let point_layout = self.point_row_layout();
        let result = py
            .detach(move || -> PackedColumnResult<R> {
                let columns =
                    resolve_packed_columns(storage.as_ref(), missing.as_ref(), point_layout)
                        .expect("points");
                let PackedColumns::Points(point_columns) = columns else {
                    unreachable!("reduce_packed_points_detached requires point storage");
                };
                kernel(point_columns)
            })
            .map_err(packed_columns_err)?;
        Ok(Some(result))
    }

    pub(crate) fn pair_packed_points_detached<T>(
        &self,
        py: Python<'_>,
        right: &Self,
        kernel: impl FnOnce(&[f64], &[f64], &[f64], &[f64]) -> crate::error::Result<Vec<T>> + Send,
    ) -> PyResult<Option<Vec<T>>>
    where
        T: Send,
    {
        let (
            GeometryArrayStorage::Packed(PackedStorage::Points { .. }),
            GeometryArrayStorage::Packed(PackedStorage::Points { .. }),
        ) = (self.storage(), right.storage())
        else {
            return Ok(None);
        };
        let left_storage = Arc::clone(self.storage_arc());
        let right_storage = Arc::clone(right.storage_arc());
        let left_missing = self.missing().cloned();
        let right_missing = right.missing().cloned();
        let left_point_layout = self.point_row_layout();
        let right_point_layout = right.point_row_layout();
        let values = py
            .detach(move || -> PackedColumnResult<Option<Vec<T>>> {
                let left_columns = resolve_packed_columns(
                    left_storage.as_ref(),
                    left_missing.as_ref(),
                    left_point_layout,
                )
                .expect("points");
                let right_columns = resolve_packed_columns(
                    right_storage.as_ref(),
                    right_missing.as_ref(),
                    right_point_layout,
                )
                .expect("points");
                let PackedColumns::Points(left) = left_columns else {
                    unreachable!("pair_packed_points_detached requires point storage");
                };
                let PackedColumns::Points(right) = right_columns else {
                    unreachable!("pair_packed_points_detached requires point storage");
                };
                let (Some(left_xy), Some(right_xy)) = (left.contiguous_xy(), right.contiguous_xy())
                else {
                    return Ok(None);
                };
                kernel(left_xy.xs, left_xy.ys, right_xy.xs, right_xy.ys)
                    .map(Some)
                    .map_err(batch_err)
            })
            .map_err(packed_columns_err)?;
        Ok(values)
    }

    /// O(1) packed-storage X/Y swap — narrow escape hatch, not a general scan.
    pub(crate) fn swap_xy_packed_storage(&self) -> crate::error::Result<Option<Self>> {
        Ok(match self.storage() {
            GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) => Some(
                Self::packed_points_mapped(coords.swap_xy(), self.frame.clone(), row_map.clone())?,
            ),
            GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            }) => Some(Self::packed_lines_mapped(
                coords.swap_xy(),
                offsets.clone(),
                self.frame.clone(),
                row_map.clone(),
            )?),
            GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            }) => Some(Self::packed_polygons_mapped(
                coords.swap_xy(),
                ring_offsets.clone(),
                polygon_offsets.clone(),
                self.frame.clone(),
                row_map.clone(),
            )?),
            GeometryArrayStorage::Mixed(_) => None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::RowSelection;
    use crate::test_support::allocation_count;

    fn coords() -> CoordSeq {
        CoordSeq::try_from_vecs(
            vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0],
            vec![0.0, 1.0, 0.0, 3.0, 4.0, 3.0],
            None,
            None,
        )
        .expect("valid coordinates")
    }

    #[test]
    fn point_buffers_reserve_all_output_columns_once() {
        let allocations = [2_usize, 128].map(|rows| {
            let storage = GeometryArrayStorage::Packed(PackedStorage::Points {
                coords: Arc::new(
                    CoordSeq::try_from_vecs(vec![0.0; rows], vec![0.0; rows], None, None).unwrap(),
                ),
                row_map: RowSelection::Identity,
            });
            let Some(PackedColumns::Points(points)) =
                resolve_packed_columns(&storage, None, PointRowLayout::Dense)
            else {
                panic!("point fixture must resolve packed points");
            };
            let (output, count) = allocation_count(|| {
                packed_column_kernels::buffer_point_columns(
                    &points,
                    1.0,
                    std::num::NonZeroU32::new(4).unwrap(),
                    None,
                )
            });
            let Ok(Some(PackedColumnOutput::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
            })) = output
            else {
                panic!("point buffers must produce packed polygons");
            };
            assert_eq!(coords.len(), rows.wrapping_mul(17));
            assert_eq!(ring_offsets.len(), rows.wrapping_add(1));
            assert_eq!(polygon_offsets.len(), rows.wrapping_add(1));
            assert_eq!(coords.xs().first(), coords.xs().last());
            assert_eq!(coords.ys().first(), coords.ys().last());
            count
        });
        assert_eq!(allocations.first(), allocations.last());
    }

    #[test]
    fn geodesic_xy_lanes_borrow_contiguous_rows_and_copy_gathers() {
        let coords = coords();
        let gather = RowSelection::gather_checked_native(vec![4, 1, 4], coords.len()).unwrap();
        for (selection, expected_xs, expected_ys, expected_allocations) in [
            (
                gather.as_deref(),
                &[4.0, 1.0, 4.0][..],
                &[4.0, 1.0, 4.0][..],
                2,
            ),
            (RowSelectionRef::Identity, coords.xs(), coords.ys(), 0),
            (
                RowSelectionRef::Window { start: 2, len: 3 },
                &[2.0, 3.0, 4.0][..],
                &[0.0, 3.0, 4.0][..],
                0,
            ),
            (
                RowSelectionRef::Window { start: 6, len: 0 },
                &[][..],
                &[][..],
                0,
            ),
        ] {
            let columns =
                resolve_point_columns(&coords, selection, None, PointRowLayout::RowAligned);
            let ((xs, ys), allocations) = allocation_count(|| columns.geodesic_xy_lanes());
            assert_eq!(allocations, expected_allocations);
            assert_eq!(&*xs, expected_xs);
            assert_eq!(&*ys, expected_ys);
        }
    }

    #[test]
    fn geographic_point_bounds_allocate_only_the_output() {
        let coords = coords();
        let gather = RowSelection::gather_checked_native(vec![4, 1, 4], coords.len()).unwrap();
        let missing = ValidityMask::from_vec(3, vec![false, true, false]).unwrap();
        for mask in [None, Some(&missing)] {
            let columns = PackedColumns::Points(resolve_point_columns(
                &coords,
                gather.as_deref(),
                mask,
                PointRowLayout::RowAligned,
            ));
            let (bounds, allocations) = allocation_count(|| {
                packed_column_kernels::geographic_bounds_values_from_columns_masked(
                    &columns,
                    crate::geographic::GeographicFrame::crs84(),
                    mask,
                )
            });
            let bounds = bounds.unwrap();
            assert_eq!(allocations, 1);
            let expected = if mask.is_some() {
                [
                    4.0,
                    4.0,
                    4.0,
                    4.0,
                    f64::NAN,
                    f64::NAN,
                    f64::NAN,
                    f64::NAN,
                    4.0,
                    4.0,
                    4.0,
                    4.0,
                ]
            } else {
                [4.0, 4.0, 4.0, 4.0, 1.0, 1.0, 1.0, 1.0, 4.0, 4.0, 4.0, 4.0]
            };
            assert_eq!(bounds.len(), expected.len());
            for (actual, expected) in bounds.iter().zip(expected) {
                if expected.is_nan() {
                    assert!(actual.is_nan());
                } else {
                    assert_eq!(*actual, expected);
                }
            }
        }
    }

    #[test]
    fn selected_views_resolve_physical_rows_directly() {
        let selection = || RowSelection::gather_checked_native(vec![1_usize, 0, 1], 2).unwrap();
        let point_storage = GeometryArrayStorage::Packed(PackedStorage::Points {
            coords: Arc::new(coords()),
            row_map: RowSelection::gather_checked_native(vec![2_usize, 0, 2], 6).unwrap(),
        });
        let line_storage = GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords: Arc::new(coords()),
            offsets: CsrOffsetColumn::try_new(vec![0, 3, 6], 6).expect("valid offsets"),
            row_map: selection(),
        });
        let polygon_storage = GeometryArrayStorage::Packed(PackedStorage::Polygons {
            coords: Arc::new(coords()),
            ring_offsets: CsrOffsetColumn::<RingLevel>::try_new(vec![0, 3, 6], 6)
                .expect("valid ring offsets"),
            polygon_offsets: CsrOffsetColumn::<PolygonLevel>::try_new(vec![0, 1, 2], 2)
                .expect("valid polygon offsets"),
            row_map: selection(),
        });

        let Some(points) = resolve_packed_columns(&point_storage, None, PointRowLayout::Dense)
        else {
            panic!("packed point view resolution failed");
        };
        assert_eq!(points.row_count(), 3);
        assert_eq!(
            points.collect_rows::<_, Vec<_>>(|row| {
                row.map(|row| (row.index(), row.vertex_window()))
            }),
            [
                Some((0, Some(2..3))),
                Some((1, Some(0..1))),
                Some((2, Some(2..3))),
            ]
        );
        drop(packed_column_kernels::bounds_values_from_columns(&points));

        for storage in [&line_storage, &polygon_storage] {
            let Some(columns) = resolve_packed_columns(storage, None, PointRowLayout::Dense) else {
                panic!("packed view resolution failed");
            };
            let rows = columns
                .collect_rows::<_, Vec<_>>(|row| row.map(|row| (row.index(), row.vertex_window())));
            assert_eq!(rows, [
                Some((0, Some(3..6))),
                Some((1, Some(0..3))),
                Some((2, Some(3..6))),
            ]);
        }
    }

    #[test]
    fn point_row_cursor_maps_present_ranks_only_for_compact_payloads() {
        let compact_map = RowSelection::gather_checked_native(vec![5_usize, 2, 5], 6).unwrap();
        let compact_mask = ValidityMask::from_vec(6, vec![true, false, true, true, false, false])
            .expect("compact mask");
        let compact = PointRows::new(
            6,
            compact_map.as_deref(),
            Some(&compact_mask),
            PointRowLayout::Compact,
        );
        assert!(compact.is_compact());
        assert_eq!(compact.present_count(), 3);
        assert_eq!(compact.cursor().collect::<Vec<_>>(), [
            (0, None),
            (1, Some(5)),
            (2, None),
            (3, None),
            (4, Some(2)),
            (5, Some(5))
        ]);

        let row_aligned_map = RowSelection::gather_checked_native(vec![2_usize, 0, 2], 3).unwrap();
        let row_aligned_mask =
            ValidityMask::from_vec(3, vec![false, true, false]).expect("row-aligned mask");
        let row_aligned = PointRows::new(
            3,
            row_aligned_map.as_deref(),
            Some(&row_aligned_mask),
            PointRowLayout::RowAligned,
        );
        assert!(!row_aligned.is_compact());
        assert_eq!(row_aligned.cursor().collect::<Vec<_>>(), [
            (0, Some(2)),
            (1, None),
            (2, Some(2))
        ]);

        let equal_length_compact_mask = ValidityMask::from_sparse(3, &[1]).expect("compact mask");
        let equal_length_compact_map =
            RowSelection::gather_checked_native(vec![0_usize, 1, 0], 2).unwrap();
        let equal_length_compact = PointRows::new(
            2,
            equal_length_compact_map.as_deref(),
            Some(&equal_length_compact_mask),
            PointRowLayout::Compact,
        );
        assert!(equal_length_compact.is_compact());
        assert_eq!(equal_length_compact.cursor().collect::<Vec<_>>(), [
            (0, Some(0)),
            (1, None),
            (2, Some(1))
        ]);
    }

    #[test]
    fn point_row_cursor_decodes_non_byte_aligned_validity_windows() {
        let parent = ValidityMask::from_sparse(80, &[4, 7, 11, 19, 27, 32, 40, 63, 72]).unwrap();
        let window = parent.window(3, 65).expect("window contains missing rows");
        assert_eq!(window.arrow_bitmap().1, 3);
        let missing = [1_usize, 4, 8, 16, 24, 29, 37, 60];

        let row_aligned = PointRows::new(
            65,
            RowSelectionRef::Identity,
            Some(&window),
            PointRowLayout::RowAligned,
        );
        let expected_row_aligned: Vec<_> = (0..65)
            .map(|logical| (logical, (!missing.contains(&logical)).then_some(logical)))
            .collect();
        assert_eq!(
            row_aligned.cursor().collect::<Vec<_>>(),
            expected_row_aligned
        );

        let compact = PointRows::new(
            57,
            RowSelectionRef::Identity,
            Some(&window),
            PointRowLayout::Compact,
        );
        let mut present = 0;
        let expected_compact: Vec<_> = (0..65)
            .map(|logical| {
                if missing.contains(&logical) {
                    (logical, None)
                } else {
                    let physical = present;
                    present += 1;
                    (logical, Some(physical))
                }
            })
            .collect();
        assert_eq!(compact.cursor().collect::<Vec<_>>(), expected_compact);
        assert_eq!(present, compact.present_count());
    }

    #[test]
    fn point_row_cursor_preserves_selection_order_for_empty_and_gathered_rows() {
        let empty = PointRows::new(0, RowSelectionRef::Identity, None, PointRowLayout::Dense);
        assert_eq!(
            empty.cursor().collect::<Vec<_>>(),
            Vec::<(usize, Option<usize>)>::new()
        );

        let all_missing = ValidityMask::from_vec(9, vec![true; 9]).unwrap();
        let row_aligned_missing = PointRows::new(
            9,
            RowSelectionRef::Identity,
            Some(&all_missing),
            PointRowLayout::RowAligned,
        );
        assert_eq!(
            row_aligned_missing.cursor().collect::<Vec<_>>(),
            (0..9).map(|logical| (logical, None)).collect::<Vec<_>>()
        );
        let compact_missing = PointRows::new(
            0,
            RowSelectionRef::Identity,
            Some(&all_missing),
            PointRowLayout::Compact,
        );
        assert_eq!(
            compact_missing.cursor().collect::<Vec<_>>(),
            (0..9).map(|logical| (logical, None)).collect::<Vec<_>>()
        );

        let dense_map = RowSelection::gather_checked_native(vec![4_usize, 1, 4, 0], 5).unwrap();
        let dense_gather = PointRows::new(5, dense_map.as_deref(), None, PointRowLayout::Dense);
        assert_eq!(dense_gather.cursor().collect::<Vec<_>>(), [
            (0, Some(4)),
            (1, Some(1)),
            (2, Some(4)),
            (3, Some(0))
        ]);

        let row_aligned_mask = ValidityMask::from_vec(4, vec![false, true, false, false]).unwrap();
        let row_aligned_gather = PointRows::new(
            5,
            dense_map.as_deref(),
            Some(&row_aligned_mask),
            PointRowLayout::RowAligned,
        );
        assert_eq!(row_aligned_gather.cursor().collect::<Vec<_>>(), [
            (0, Some(4)),
            (1, None),
            (2, Some(4)),
            (3, Some(0))
        ]);

        let compact_map = RowSelection::gather_checked_native(vec![2_usize, 1, 2, 0], 3).unwrap();
        let compact_gather = PointRows::new(
            3,
            compact_map.as_deref(),
            Some(&row_aligned_mask),
            PointRowLayout::Compact,
        );
        assert_eq!(compact_gather.cursor().collect::<Vec<_>>(), [
            (0, Some(2)),
            (1, None),
            (2, Some(1)),
            (3, Some(2))
        ]);

        let dense_window = PointRows::new(
            8,
            RowSelectionRef::Window { start: 3, len: 5 },
            None,
            PointRowLayout::Dense,
        );
        assert_eq!(dense_window.cursor().collect::<Vec<_>>(), [
            (0, Some(3)),
            (1, Some(4)),
            (2, Some(5)),
            (3, Some(6)),
            (4, Some(7))
        ]);
    }

    fn gathered_lines(rows: usize) -> GeometryArrayStorage {
        GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords: Arc::new(coords()),
            offsets: CsrOffsetColumn::try_new(vec![0, 3, 6], 6).expect("valid offsets"),
            row_map: RowSelection::gather_checked_native(
                (0..rows).map(|row| row & 1).collect::<Vec<_>>(),
                2,
            )
            .unwrap(),
        })
    }

    #[test]
    fn selected_validation_and_total_bounds_do_not_stage_rows() {
        let storage = gathered_lines(4);
        let missing = ValidityMask::from_vec(4, vec![false, true, false, true]).unwrap();
        for mask in [None, Some(&missing)] {
            let columns = resolve_packed_columns(&storage, mask, PointRowLayout::Dense)
                .expect("packed columns");
            let (admission, admission_allocations) = allocation_count(|| {
                packed_column_kernels::ensure_geographic_columns_present(
                    &columns,
                    crate::geographic::GeographicFrame::crs84(),
                )
            });
            admission.unwrap();
            let (bounds, bounds_allocations) =
                allocation_count(|| packed_column_kernels::total_bounds_from_columns(&columns));
            assert_eq!((admission_allocations, bounds_allocations), (0, 0));
            assert_eq!(
                bounds.unwrap().into_array(),
                if mask.is_some() {
                    [0.0, 0.0, 2.0, 1.0]
                } else {
                    [0.0, 0.0, 5.0, 4.0]
                }
            );
        }
    }

    #[test]
    fn selected_reduction_allocates_only_its_result() {
        let storage = gathered_lines(32);
        let columns =
            resolve_packed_columns(&storage, None, PointRowLayout::Dense).expect("packed columns");
        let (values, allocations) =
            allocation_count(|| packed_column_kernels::bounds_values_from_columns(&columns));
        assert_eq!(values.len(), 32 * 4);
        assert_eq!(allocations, 1);
    }

    #[test]
    fn selected_reverse_reserves_coordinate_and_offset_columns() {
        let counts = [2, 128].map(|rows| {
            let storage = gathered_lines(rows);
            let Some(PackedColumns::Lines(columns)) =
                resolve_packed_columns(&storage, None, PointRowLayout::Dense)
            else {
                panic!("line fixture requires line columns");
            };
            let (output, allocations) = allocation_count(|| columns.reverse_selected());
            let (coords, offsets) = output.unwrap();
            assert_eq!(coords.len(), rows.wrapping_mul(3));
            assert_eq!(offsets.len(), rows.wrapping_add(1));
            assert_eq!(coords.xs().first(), Some(&2.0));
            assert_eq!(coords.xs().last(), Some(&3.0));
            allocations
        });
        assert_eq!(counts.first(), counts.last());
    }

    #[test]
    fn selected_affine_reserves_only_visible_coordinates() {
        use crate::HeapSize as _;

        let source = coords();
        let prefix = source.view(CoordWindow::from_range(0..3, source.len()));
        let source_offsets = CsrOffsetColumn::try_new(vec![0, 3, 6], source.len()).unwrap();
        let prefix_offsets = CsrOffsetColumn::try_new(vec![0, 3], prefix.len()).unwrap();
        let linear = crate::geometry::AffineLinear::new([2.0, 0.0, 0.0, 2.0]).unwrap();
        let footprints = [
            LineColumns::from_parts(&source, &source_offsets, RowSelectionRef::Window {
                start: 0,
                len: 1,
            }),
            LineColumns::from_parts(&prefix, &prefix_offsets, RowSelectionRef::Identity),
        ]
        .map(|columns| {
            let Ok(PackedColumnOutput::Lines { coords, offsets }) =
                packed_column_kernels::packed_per_row_self_origin_affine_columns(
                    PackedColumns::Lines(columns),
                    crate::array::OriginSpec::Center,
                    &linear,
                )
            else {
                panic!("line affine must produce line columns");
            };
            assert_eq!(coords.xs(), &[-1.0, 1.0, 3.0]);
            assert_eq!(coords.ys(), &[-0.5, 1.5, -0.5]);
            assert_eq!(offsets.as_slice(), &[0, 3]);
            coords.heap_bytes()
        });
        assert_eq!(footprints.first(), footprints.last());
    }

    #[test]
    fn selected_affine_does_not_allocate_per_row_coordinates() {
        let counts = [2, 128].map(|rows| {
            let storage = gathered_lines(rows);
            let columns = resolve_packed_columns(&storage, None, PointRowLayout::Dense)
                .expect("packed columns");
            let linear = crate::geometry::AffineLinear::new([2.0, 0.0, 0.0, 2.0]).unwrap();
            let (output, allocations) = allocation_count(|| {
                packed_column_kernels::packed_per_row_self_origin_affine_columns(
                    columns,
                    crate::array::OriginSpec::Center,
                    &linear,
                )
            });
            let Ok(PackedColumnOutput::Lines { coords, offsets }) = output else {
                panic!("line affine must produce line columns");
            };
            assert_eq!(coords.len(), rows.wrapping_mul(3));
            assert_eq!(offsets.len(), rows.wrapping_add(1));
            assert_eq!(coords.xs().first(), Some(&-1.0));
            assert_eq!(coords.xs().last(), Some(&6.0));
            allocations
        });
        assert_eq!(counts.first(), counts.last());
    }

    #[test]
    fn selected_pointwise_map_has_constant_output_owner_allocations() {
        fn count(rows: usize) -> usize {
            let storage = gathered_lines(rows);
            let columns = resolve_packed_columns(&storage, None, PointRowLayout::Dense)
                .expect("packed columns");
            let (output, allocations) = allocation_count(|| {
                let Ok(output) = map_selected_points(columns, CoordinateAxes::XY, |point| {
                    point.translate_xy(1.0, -1.0)
                }) else {
                    panic!("pointwise map failed");
                };
                output
            });
            let PackedColumnOutput::Lines { coords, offsets } = output else {
                panic!("line input must produce line output");
            };
            assert_eq!(coords.len(), rows * 3);
            assert_eq!(offsets.len(), rows + 1);
            allocations
        }

        let small = count(2);
        let large = count(128);
        assert_eq!(small, large);
    }

    fn gather_map(rows: Vec<usize>, physical_len: usize) -> RowSelection {
        RowSelection::gather_checked_native(rows, physical_len).unwrap()
    }

    #[test]
    fn reverse_selected_lines_copies_only_gathered_windows() {
        let coords = CoordSeq::try_from_vecs(
            vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0],
            vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0],
            None,
            None,
        )
        .unwrap();
        let offsets = CsrOffsetColumn::try_new(vec![0, 3, 6], 6).unwrap();
        let map = gather_map(vec![1, 1], 2);
        let (reversed, out_offsets) = LineColumns::from_parts(&coords, &offsets, map.as_deref())
            .reverse_selected()
            .unwrap();
        assert_eq!(reversed.xs(), &[5.0, 4.0, 3.0, 5.0, 4.0, 3.0]);
        assert_eq!(out_offsets.as_slice(), &[0, 3, 6]);
    }
}
