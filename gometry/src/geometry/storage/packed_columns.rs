//! Detached packed-column execution over borrowed physical columns and a logical row selection.
//!
//! Present rows flow through `PackedRow`, which resolves admitted selectors
//! and outer CSR rows once; polygon coordinate windows are derived on demand.
//! Only Identity and Window selections expose contiguous slices; Gather never
//! rebuilds an input CSR payload here.
//! `collect_rows` carries missingness separately as `Option<PackedRow>`.
//! Output-producing kernels write logical rows once into their final coordinate
//! and offset columns.

mod geographic;

use std::borrow::Cow;
use std::sync::Arc;

use super::admission::{CheckedRingWindow, RingWindowError};
use super::builder::PackedColumnBuilder;
#[cfg(test)]
use super::kernels as packed_column_kernels;
use super::kernels::map_masked_point_columns_to_crs;
use super::replacement::{CoordinateReplacement, ReplacementAxis};
use crate::crs::TransformFailure;
use crate::geographic::GeographicFrame;
use crate::geometry::storage::validity::MissingIter;
use crate::geometry::storage::{
    ArrayRows, GeometryArrayStorage, PackedRow, PackedStorage, RowSelectionRef, ValidityMask,
    admit_packed_total, csr_value_window, materialized_packed_lines_len,
    materialized_packed_polygons_len,
};
use crate::geometry::{
    CoordSeq, CoordSeqBuilder, CoordWindow, CoordinateAxes, CsrOffsetBuilder, CsrOffsetColumn,
    GeometryErrorKind, Point, PolygonLevel, RingLevel, storage,
};

#[derive(Debug)]
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
/// The cursor owns the sequential validity/rank walk used by point kernels.
/// Nullable point admission guarantees present-only physical coordinates, so
/// every present bit resolves through the selected present-rank mapping.
#[derive(Clone, Copy)]
pub(crate) struct PointRows<'a> {
    logical_len: usize,
    row_map: RowSelectionRef<'a>,
    missing: Option<&'a ValidityMask>,
}

impl<'a> PointRows<'a> {
    fn new(
        coords_len: usize,
        row_map: RowSelectionRef<'a>,
        missing: Option<&'a ValidityMask>,
    ) -> Self {
        let selected_len = row_map.len(coords_len);
        let logical_len = missing.map_or(selected_len, ValidityMask::len);
        Self {
            logical_len,
            row_map,
            missing,
        }
    }

    pub(crate) const fn present_count(self) -> usize {
        match self.missing {
            Some(mask) => mask.present_count(),
            None => self.logical_len,
        }
    }

    pub(crate) fn cursor(self) -> PointRowCursor<'a> {
        PointRowCursor {
            logical: 0,
            remaining: self.logical_len,
            row_map: self.row_map,
            validity: self.missing.map(ValidityMask::iter),
            present: 0,
        }
    }
}

#[derive(Clone)]
pub(crate) struct PointRowCursor<'a> {
    logical: usize,
    remaining: usize,
    row_map: RowSelectionRef<'a>,
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
                    Some(self.row_map.physical(present))
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
}

pub(crate) struct XyColumns<'a> {
    pub xs: &'a [f64],
    pub ys: &'a [f64],
}

impl PointColumns<'_> {
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
        PointRows::new(self.coords.len(), self.row_map, self.missing)
    }

    pub(crate) fn present_points(&self) -> impl Iterator<Item = Point> + '_ {
        let coords = self.coords;
        self.rows()
            .cursor()
            .filter_map(move |(_, physical)| physical.map(|physical| coords.point_at(physical)))
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
            xs: storage::column_window(self.coords.xs(), &range),
            ys: storage::column_window(self.coords.ys(), &range),
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
    /// [`crate::geometry::storage::reverse_coord_windows`] instead.
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

    /// Visit present logical rows in display order without materializing their
    /// geometry shapes. The row index remains the logical row even when point
    /// storage is compact or selected through a gather.
    pub(crate) fn try_for_each_present_row<E>(
        &self,
        mut visit: impl FnMut(usize, PackedRow<'_>) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        match self {
            Self::Points(columns) => {
                for (logical, physical) in columns.rows().cursor() {
                    if let Some(physical) = physical {
                        visit(
                            logical,
                            PackedRow::point_at(logical, columns.coords, physical),
                        )?;
                    }
                }
            },
            Self::Lines(columns) => {
                for logical in 0..columns.rows() {
                    if !columns.is_missing(logical) {
                        visit(logical, columns.packed_row(logical))?;
                    }
                }
            },
            Self::Polygons(columns) => {
                for logical in 0..columns.rows() {
                    if !columns.is_missing(logical) {
                        visit(logical, columns.packed_row(logical))?;
                    }
                }
            },
        }
        Ok(())
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
) -> PointColumns<'a> {
    PointColumns {
        coords,
        row_map,
        missing,
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

pub(super) fn resolve_packed_columns(owner: &ArrayRows) -> Option<PackedColumns<'_>> {
    match owner.storage() {
        GeometryArrayStorage::Packed(packed) => Some(resolve_columns(packed, owner.missing())),
        GeometryArrayStorage::Mixed(_) => None,
    }
}

fn resolve_columns<'a>(
    storage: &'a PackedStorage,
    missing: Option<&'a ValidityMask>,
) -> PackedColumns<'a> {
    match storage {
        PackedStorage::Points { coords, row_map } => {
            PackedColumns::Points(resolve_point_columns(coords, row_map.as_deref(), missing))
        },
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

pub(crate) fn assemble_packed_output(output: PackedColumnOutput) -> GeometryArrayStorage {
    match output {
        PackedColumnOutput::Points(coords) => GeometryArrayStorage::Packed(PackedStorage::Points {
            coords: Arc::new(coords),
            row_map: storage::RowSelection::Identity,
        }),
        PackedColumnOutput::Lines { coords, offsets } => {
            GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords: Arc::new(coords),
                offsets,
                row_map: storage::RowSelection::Identity,
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
            row_map: storage::RowSelection::Identity,
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
    match CheckedRingWindow::checked(coords, window) {
        Ok(_) => Ok(()),
        Err(RingWindowError::TooShort(len)) => Err(PackedColumnError::Batch(
            crate::geometry::GeometryErrorKind::RingTooShort(len).into(),
        )),
        Err(RingWindowError::Open) => Err(PackedColumnError::Batch(
            crate::geometry::GeometryErrorKind::message("polygon ring must be closed"),
        )),
        Err(RingWindowError::Inverted | RingWindowError::OutOfRange) => {
            Err(PackedColumnError::Batch(
                crate::geometry::GeometryErrorKind::message("polygon ring range is invalid"),
            ))
        },
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

fn replace_point_columns(
    point_columns: &PointColumns<'_>,
    replacement: &CoordinateReplacement,
    replacement_index: &mut usize,
) -> PackedColumnResult<PackedColumnOutput> {
    let old = point_columns.coords();
    let rows = point_columns.rows();
    let capacity = rows.present_count();
    storage::admit_packed_coord_count(capacity).map_err(batch_err)?;
    let mut builder = CoordSeqBuilder::with_capacity(old.axes(), capacity);
    let mut replace_present = |physical| {
        push_replacement_vertex(&mut builder, old, physical, replacement, *replacement_index);
        *replacement_index = (*replacement_index).wrapping_add(1);
    };
    if point_columns.missing.is_some() {
        for (_row, physical) in rows.cursor() {
            if let Some(physical) = physical {
                replace_present(physical);
            }
        }
    } else {
        for logical in 0..point_columns.len() {
            replace_present(point_columns.row_map.physical(logical));
        }
    }
    let coords = builder.finish().map_err(batch_err)?;
    Ok(PackedColumnOutput::Points(coords))
}

fn replace_line_columns<const HAS_MISSING: bool>(
    line_columns: &LineColumns<'_>,
    replacement: &CoordinateReplacement,
    replacement_index: &mut usize,
) -> PackedColumnResult<PackedColumnOutput> {
    let old = line_columns.coords();
    let capacity = line_columns.materialized_present_vertices(2)?;
    let mut builder = CoordSeqBuilder::with_capacity(old.axes(), capacity);
    let mut offsets = CsrOffsetBuilder::with_capacity(line_columns.rows());
    for row in 0..line_columns.rows() {
        if HAS_MISSING && line_columns.is_missing(row) {
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
) -> PackedColumnResult<PackedColumnOutput> {
    let old = polygon_columns.coords();
    let (vertex_capacity, ring_capacity) = polygon_columns.materialized_present_sizes()?;
    let mut builder = CoordSeqBuilder::with_capacity(old.axes(), vertex_capacity);
    let mut ring_builder = CsrOffsetBuilder::with_capacity(ring_capacity);
    let mut polygon_builder = CsrOffsetBuilder::with_capacity(polygon_columns.rows());
    let mut ring_count = 0_usize;
    let mut validate_windows = Vec::with_capacity(ring_capacity);
    for row in 0..polygon_columns.rows() {
        if HAS_MISSING && polygon_columns.is_missing(row) {
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
) -> PackedColumnResult<PackedColumnOutput> {
    match columns {
        PackedColumns::Points(point_columns) => {
            replace_point_columns(&point_columns, replacement, replacement_index)
        },
        PackedColumns::Lines(line_columns) => {
            replace_line_columns::<HAS_MISSING>(&line_columns, replacement, replacement_index)
        },
        PackedColumns::Polygons(polygon_columns) => {
            replace_polygon_columns::<HAS_MISSING>(&polygon_columns, replacement, replacement_index)
        },
    }
}

pub(crate) fn replace_packed_columns_dispatch(
    columns: PackedColumns<'_>,
    replacement: &CoordinateReplacement,
    replacement_index: &mut usize,
) -> PackedColumnResult<PackedColumnOutput> {
    let has_missing = match &columns {
        PackedColumns::Points(columns) => columns.missing.is_some(),
        PackedColumns::Lines(columns) => columns.missing.is_some(),
        PackedColumns::Polygons(columns) => columns.missing.is_some(),
    };
    if has_missing {
        replace_packed_columns_for_mask::<true>(columns, replacement, replacement_index)
    } else {
        replace_packed_columns_for_mask::<false>(columns, replacement, replacement_index)
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

pub(crate) fn map_selected_points(
    columns: PackedColumns<'_>,
    output_axes: CoordinateAxes,
    mut map: impl FnMut(Point) -> crate::error::Result<Point>,
) -> PackedColumnResult<PackedColumnOutput> {
    match columns {
        PackedColumns::Points(points) => {
            let rows = points.rows();
            let capacity = rows.present_count();
            storage::admit_packed_coord_count(capacity).map_err(batch_err)?;
            let mut builder = CoordSeqBuilder::with_capacity(output_axes, capacity);
            for (logical, physical) in rows.cursor() {
                let Some(physical) = physical else {
                    continue;
                };
                builder.push(
                    map(points.coords().point_at(physical))
                        .map_err(|error| row_err(logical, error))?,
                );
            }
            let coords = builder.finish().map_err(batch_err)?;
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

fn canonicalize_transformed_points(
    coordinates: &CoordSeq,
    frame: GeographicFrame,
) -> crate::error::Result<CoordSeq> {
    coordinates.try_map_points(|point| {
        frame
            .canonical_computed_point(point)
            .map_err(|error| crate::crs::CrsError::invalid(error.to_string()))
    })
}

fn transform_selected_coords(
    transformer: &crate::crs::TransformPlan,
    coords: &CoordSeq,
    indices: impl IntoIterator<Item = usize>,
    capacity: usize,
    target_output: TargetLongitudeOutput,
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
    match target_output {
        TargetLongitudeOutput::Canonical => {
            transformer.transform_coordinates_indexed(&mut xs, &mut ys, zt)?;
        },
        TargetLongitudeOutput::ContinuousChart => {
            transformer.transform_coordinates_indexed_for_shape(&mut xs, &mut ys, zt)?;
        },
    }
    Ok(CoordSeq::try_from_vecs(xs, ys, zs, ms)?)
}

#[derive(Clone, Copy)]
enum TargetLongitudeOutput {
    Canonical,
    ContinuousChart,
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

pub(crate) fn map_selected_to_crs(
    columns: PackedColumns<'_>,
    transformer: &crate::crs::TransformPlan,
) -> PackedColumnResult<PackedColumnOutput> {
    match columns {
        PackedColumns::Points(points) => {
            let rows = points.rows();
            if points.missing.is_some() {
                let mut coords =
                    map_masked_point_columns_to_crs(transformer, &points).map_err(|failure| {
                        let row = failure
                            .coordinate
                            .and_then(|coordinate| point_row_for_coordinate(rows, coordinate));
                        packed_transform_failure(failure, row)
                    })?;
                if let Some(frame) = transformer.computed_geographic_frame() {
                    coords = canonicalize_transformed_points(&coords, frame).map_err(batch_err)?;
                }
                return Ok(PackedColumnOutput::Points(coords));
            }
            storage::admit_packed_coord_count(points.len()).map_err(batch_err)?;
            let coords = transform_selected_coords(
                transformer,
                points.coords(),
                points
                    .rows()
                    .cursor()
                    .map(|(_, physical)| physical.expect("dense point rows are present")),
                points.len(),
                TargetLongitudeOutput::Canonical,
            )
            .map_err(|failure| {
                let row = failure
                    .coordinate
                    .and_then(|coordinate| point_row_for_coordinate(rows, coordinate));
                packed_transform_failure(failure, row)
            })?;
            let coords = if let Some(frame) = transformer.computed_geographic_frame() {
                canonicalize_transformed_points(&coords, frame).map_err(batch_err)?
            } else {
                coords
            };
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
            let indices = (0..lines.rows()).flat_map(|logical| {
                if lines.is_missing(logical) || lines.row_window(logical).len() < 2 {
                    0..0
                } else {
                    lines.row_window(logical)
                }
            });
            let selected_offsets = offsets.finish(coordinate_count).map_err(batch_err)?;
            let coords = transform_selected_coords(
                transformer,
                lines.coords(),
                indices,
                coordinate_count,
                TargetLongitudeOutput::ContinuousChart,
            )
            .map_err(|failure| {
                let row = failure.coordinate.and_then(|coordinate| {
                    offset_row_for_coordinate(selected_offsets.as_slice(), coordinate)
                });
                packed_transform_failure(failure, row)
            })?;
            let (coords, offsets) = if let Some(frame) = transformer.computed_geographic_frame() {
                let source_frame = transformer.source_geographic_frame();
                let mut metric = transformer.target_metric_scratch();
                let mut output = CoordSeqBuilder::with_capacity(coords.axes(), coordinate_count);
                let mut output_offsets = CsrOffsetBuilder::with_capacity(lines.rows());
                for logical in 0..lines.rows() {
                    let row_start = usize::try_from(
                        *selected_offsets
                            .as_slice()
                            .get(logical)
                            .expect("selected line row has a start offset"),
                    )
                    .map_err(|_| row_err(logical, GeometryErrorKind::MalformedCsrOffsets.into()))?;
                    let row_end = usize::try_from(
                        *selected_offsets
                            .as_slice()
                            .get(logical.wrapping_add(1))
                            .expect("selected line row has an end offset"),
                    )
                    .map_err(|_| row_err(logical, GeometryErrorKind::MalformedCsrOffsets.into()))?;
                    let output_window =
                        coords.view(CoordWindow::from_range(row_start..row_end, coords.len()));
                    let source_range = lines.row_window(logical);
                    let published = if !lines.is_missing(logical) && source_range.len() >= 2 {
                        let source_window = lines.coords().view(CoordWindow::from_range(
                            source_range.clone(),
                            lines.coords().len(),
                        ));
                        let mut budget =
                            crate::geometry::ExpansionBudget::new("to_crs", "computed edges");
                        budget
                            .add(output_window.len())
                            .map_err(|error| row_err(logical, error))?;
                        let closed = source_range.len() > 2
                            && source_window.point_at(0)
                                == source_window.point_at(source_range.len().wrapping_sub(1));
                        crate::geometry::publish_transformed_chain(
                            &source_window,
                            &output_window,
                            closed,
                            source_frame,
                            frame,
                            &mut budget,
                            &mut metric,
                        )
                        .map_err(|error| row_err(logical, error))?
                    } else {
                        None
                    };
                    if let Some(points) = published {
                        for point in points {
                            output.push(point);
                        }
                    } else {
                        output.extend_window(&coords, row_start..row_end);
                    }
                    output_offsets
                        .push_end(output.len(), i32::MAX as usize)
                        .map_err(|error| row_err(logical, error))?;
                }
                let coords = output.finish().map_err(batch_err)?;
                let offsets = output_offsets.finish(coords.len()).map_err(batch_err)?;
                (coords, offsets)
            } else {
                (coords, selected_offsets)
            };
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
            let selected_ring_offsets = rings.finish(coordinate_count).map_err(batch_err)?;
            let selected_polygon_offsets = polygon_offsets.finish(ring_count).map_err(batch_err)?;
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
                TargetLongitudeOutput::ContinuousChart,
            )
            .map_err(|failure| {
                let row = failure.coordinate.and_then(|coordinate| {
                    polygon_row_for_coordinate(
                        selected_ring_offsets.as_slice(),
                        selected_polygon_offsets.as_slice(),
                        coordinate,
                    )
                });
                packed_transform_failure(failure, row)
            })?;
            if let Some(frame) = transformer.computed_geographic_frame() {
                let source_frame = transformer.source_geographic_frame();
                let mut metric = transformer.target_metric_scratch();
                let mut output = CoordSeqBuilder::with_capacity(coords.axes(), coordinate_count);
                let mut output_rings = CsrOffsetBuilder::with_capacity(ring_count);
                let mut output_polygons = CsrOffsetBuilder::with_capacity(polygons.rows());
                let source_ring_offsets = polygons.ring_offsets();
                for logical in 0..polygons.rows() {
                    let row_ring_start = usize::try_from(
                        *selected_polygon_offsets
                            .as_slice()
                            .get(logical)
                            .expect("selected polygon row has a start ring"),
                    )
                    .map_err(|_| row_err(logical, GeometryErrorKind::MalformedCsrOffsets.into()))?;
                    let row_ring_end = usize::try_from(
                        *selected_polygon_offsets
                            .as_slice()
                            .get(logical.wrapping_add(1))
                            .expect("selected polygon row has an end ring"),
                    )
                    .map_err(|_| row_err(logical, GeometryErrorKind::MalformedCsrOffsets.into()))?;
                    let row_coordinate_start = usize::try_from(
                        *selected_ring_offsets
                            .as_slice()
                            .get(row_ring_start)
                            .expect("selected polygon row has a start coordinate"),
                    )
                    .map_err(|_| row_err(logical, GeometryErrorKind::MalformedCsrOffsets.into()))?;
                    let row_coordinate_end = usize::try_from(
                        *selected_ring_offsets
                            .as_slice()
                            .get(row_ring_end)
                            .expect("selected polygon row has an end coordinate"),
                    )
                    .map_err(|_| row_err(logical, GeometryErrorKind::MalformedCsrOffsets.into()))?;
                    let mut budget =
                        crate::geometry::ExpansionBudget::new("to_crs", "computed edges");
                    budget
                        .add(row_coordinate_end.wrapping_sub(row_coordinate_start))
                        .map_err(|error| row_err(logical, error))?;

                    let source_rings = if polygons.is_missing(logical) {
                        0..0
                    } else {
                        polygons.polygon_rings(logical)
                    };
                    debug_assert_eq!(
                        source_rings.len(),
                        row_ring_end.wrapping_sub(row_ring_start),
                        "selected source and provisional output polygon rows retain the same rings"
                    );
                    for (output_ring, source_ring) in
                        (row_ring_start..row_ring_end).zip(source_rings)
                    {
                        let output_start = usize::try_from(
                            *selected_ring_offsets
                                .as_slice()
                                .get(output_ring)
                                .expect("selected ring has a start coordinate"),
                        )
                        .map_err(|_| {
                            row_err(logical, GeometryErrorKind::MalformedCsrOffsets.into())
                        })?;
                        let output_end = usize::try_from(
                            *selected_ring_offsets
                                .as_slice()
                                .get(output_ring.wrapping_add(1))
                                .expect("selected ring has an end coordinate"),
                        )
                        .map_err(|_| {
                            row_err(logical, GeometryErrorKind::MalformedCsrOffsets.into())
                        })?;
                        let output_window = coords.view(CoordWindow::from_range(
                            output_start..output_end,
                            coords.len(),
                        ));
                        let source_window = csr_value_window(
                            source_ring_offsets,
                            source_ring..source_ring.wrapping_add(1),
                        );
                        let source_window = polygons.coords().view(CoordWindow::from_range(
                            source_window,
                            polygons.coords().len(),
                        ));
                        let published = crate::geometry::publish_transformed_chain(
                            &source_window,
                            &output_window,
                            true,
                            source_frame,
                            frame,
                            &mut budget,
                            &mut metric,
                        )
                        .map_err(|error| row_err(logical, error))?;
                        if let Some(points) = published {
                            for point in points {
                                output.push(point);
                            }
                        } else {
                            output.extend_window(&coords, output_start..output_end);
                        }
                        output_rings
                            .push_end(output.len(), i32::MAX as usize)
                            .map_err(|error| row_err(logical, error))?;
                    }
                    output_polygons
                        .push_end(output_rings.row_count(), i32::MAX as usize)
                        .map_err(|error| row_err(logical, error))?;
                }
                let coords = output.finish().map_err(batch_err)?;
                let output_ring_count = output_rings.row_count();
                let ring_offsets = output_rings.finish(coords.len()).map_err(batch_err)?;
                let polygon_offsets = output_polygons
                    .finish(output_ring_count)
                    .map_err(batch_err)?;
                let polygon_offsets = polygon_offsets.cast_level();
                return Ok(PackedColumnOutput::Polygons {
                    coords,
                    ring_offsets: ring_offsets.cast_level(),
                    polygon_offsets,
                });
            }
            Ok(PackedColumnOutput::Polygons {
                coords,
                ring_offsets: selected_ring_offsets.cast_level(),
                polygon_offsets: selected_polygon_offsets.cast_level(),
            })
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PyGeometryArray;
    use crate::crs::frame::{Frame, crs_arc_static};
    use crate::geographic::AngularFactor;
    use crate::geometry::storage::{RowSelection, StorageLayout};
    use crate::geometry::{LineSeq, Polygon, Ring, Shape};
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

    fn packed_owner(
        storage: GeometryArrayStorage,
        missing: Option<ValidityMask>,
        layout: StorageLayout,
    ) -> ArrayRows {
        ArrayRows::admit(Arc::new(storage), missing, layout)
            .expect("test packed storage is admitted")
    }

    fn geographic_array(shapes: Vec<Shape>) -> PyGeometryArray {
        let frame = Frame::new(Some(crs_arc_static("EPSG:4326")), None)
            .expect("EPSG:4326 is a valid geographic frame");
        let array = PyGeometryArray::from_shapes(shapes, frame).unwrap();
        assert!(
            matches!(array.storage(), GeometryArrayStorage::Packed(_)),
            "homogeneous rows use packed storage",
        );
        array
    }

    #[test]
    fn packed_to_crs_lines_rebuild_offsets_after_edge_subdivision() {
        let source = PyGeometryArray::packed_lines(
            CoordSeq::try_from_vecs(
                vec![20_000_000.0, -20_000_000.0],
                vec![0.0, 0.0],
                None,
                None,
            )
            .unwrap(),
            CsrOffsetColumn::try_new(vec![0, 2], 2).unwrap(),
            Frame::Crs(crs_arc_static("EPSG:3857")),
        )
        .unwrap();
        let columns = source
            .packed_columns()
            .expect("packed line source resolves");
        let source_frame = Frame::Crs(crate::crs::frame::crs_arc_static("EPSG:3857"));
        let transform_frame = crate::crs::frame::GeometryTransformFrame::new(
            &source_frame,
            crate::crs::frame::crs_arc_static("EPSG:4326"),
            None,
            crate::crs::TransformOptions::default(),
        )
        .unwrap();
        let transformer = crate::crs::TransformPlan::from_geometry_frame(transform_frame).unwrap();
        let PackedColumnOutput::Lines { coords, offsets } =
            map_selected_to_crs(columns, &transformer).unwrap()
        else {
            panic!("line transforms remain packed lines");
        };
        assert_eq!(offsets.as_slice(), &[0, 3]);
        assert!(coords.xs()[0] > 170.0);
        assert_eq!(coords.xs()[1], 0.0);
        assert!(coords.xs()[2] < -170.0);
        let axis = crate::geographic::GeographicFrame::crs84().longitude();
        for pair in coords.xs().array_windows::<2>() {
            let step = axis.chart_step(pair[0], pair[1]).unwrap().delta(axis);
            assert!(step.abs() <= axis.half_period());
        }
    }

    fn ordinary_geographic(array: &PyGeometryArray, frame: GeographicFrame) -> bool {
        array
            .packed_columns()
            .expect("packed array resolves to column descriptors")
            .ordinary_geographic_predicates(frame)
    }

    fn geographic_ring(points: &[(f64, f64)]) -> Ring {
        Ring::closed(
            points
                .iter()
                .map(|&(x, y)| Point::new_raw_xy(x, y))
                .collect(),
        )
        .expect("test ring is closed")
    }

    fn geographic_polygon(west: f64, north: f64) -> Shape {
        Shape::Polygon(Polygon::new(
            geographic_ring(&[
                (west, 30.0),
                (west + 1.0, 30.0),
                (west + 1.0, north),
                (west, north),
                (west, 30.0),
            ]),
            Vec::new(),
        ))
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
            let owner = packed_owner(storage, None, StorageLayout::LogicalRows);
            let Some(PackedColumns::Points(points)) = owner.packed_columns() else {
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
            let columns = resolve_point_columns(&coords, selection, None);
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
        let storage = GeometryArrayStorage::Packed(PackedStorage::Points {
            coords: Arc::new(coords),
            row_map: gather,
        });
        let dense = packed_owner(storage.clone(), None, StorageLayout::LogicalRows);
        let nullable = packed_owner(storage, Some(missing), StorageLayout::LogicalRows);
        for (owner, expected) in [
            (&dense, [
                4.0, 4.0, 4.0, 4.0, 1.0, 1.0, 1.0, 1.0, 4.0, 4.0, 4.0, 4.0,
            ]),
            (&nullable, [
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
            ]),
        ] {
            let columns = owner.packed_columns().expect("admitted packed points");
            let (bounds, allocations) = allocation_count(|| {
                packed_column_kernels::geographic_bounds_values_from_columns_masked(
                    &columns,
                    crate::geographic::GeographicFrame::crs84(),
                    owner.missing(),
                )
            });
            let bounds = bounds.unwrap();
            assert_eq!(allocations, 1);
            assert_eq!(bounds.len(), expected.len());
            for (actual, expected) in bounds.iter().zip(expected) {
                if expected.is_nan() {
                    assert!(actual.is_nan());
                } else {
                    assert_eq!(*actual, expected);
                }
            }
        }

        let block_coords = CoordSeq::try_from_vecs(
            vec![-0.0, 1.0, 2.0, 3.0, 450.0, -270.0, 180.0, 540.0, 40.0],
            vec![-0.0, 11.0, 12.0, 13.0, 3.0, 4.0, 5.0, 6.0, 90.0],
            None,
            None,
        )
        .expect("nine CRS84 point rows have parallel XY columns");
        let dense_columns = PackedColumns::Points(resolve_point_columns(
            &block_coords,
            RowSelectionRef::Identity,
            None,
        ));
        let dense_bounds = packed_column_kernels::geographic_bounds_values_from_columns(
            &dense_columns,
            GeographicFrame::crs84(),
        )
        .expect("dense geographic point bounds");
        let expected_dense = [
            [-0.0, -0.0, -0.0, -0.0],
            [1.0, 11.0, 1.0, 11.0],
            [2.0, 12.0, 2.0, 12.0],
            [3.0, 13.0, 3.0, 13.0],
            [90.0, 3.0, 90.0, 3.0],
            [90.0, 4.0, 90.0, 4.0],
            [-180.0, 5.0, -180.0, 5.0],
            [-180.0, 6.0, -180.0, 6.0],
            [40.0, 90.0, 40.0, 90.0],
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        assert_eq!(dense_bounds, expected_dense);
        let (dense_rows, _) = dense_bounds.as_chunks::<4>();
        assert!(
            dense_rows
                .first()
                .expect("the first point has one bounds row")
                .iter()
                .all(|ordinate| ordinate.to_bits() == (-0.0_f64).to_bits())
        );

        let window_columns = PackedColumns::Points(resolve_point_columns(
            &block_coords,
            RowSelectionRef::Window { start: 1, len: 8 },
            None,
        ));
        let window_bounds = packed_column_kernels::geographic_bounds_values_from_columns(
            &window_columns,
            GeographicFrame::crs84(),
        )
        .expect("offset contiguous geographic point bounds");
        let expected_window = [
            [1.0, 11.0, 1.0, 11.0],
            [2.0, 12.0, 2.0, 12.0],
            [3.0, 13.0, 3.0, 13.0],
            [90.0, 3.0, 90.0, 3.0],
            [90.0, 4.0, 90.0, 4.0],
            [-180.0, 5.0, -180.0, 5.0],
            [-180.0, 6.0, -180.0, 6.0],
            [40.0, 90.0, 40.0, 90.0],
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        assert_eq!(window_bounds, expected_window);

        let selected = RowSelection::gather_checked_native(vec![3, 1, 3, 4], block_coords.len())
            .expect("selected point rows are in range");
        let selected_missing = ValidityMask::from_vec(4, vec![false, true, false, false]).unwrap();
        let selected_owner = packed_owner(
            GeometryArrayStorage::Packed(PackedStorage::Points {
                coords: Arc::new(block_coords.clone()),
                row_map: selected,
            }),
            Some(selected_missing),
            StorageLayout::LogicalRows,
        );
        let selected_columns = selected_owner
            .packed_columns()
            .expect("nullable selected points admit a packed view");
        let selected_bounds = packed_column_kernels::geographic_bounds_values_from_columns_masked(
            &selected_columns,
            GeographicFrame::crs84(),
            selected_owner.missing(),
        )
        .expect("gathered nullable geographic point bounds");
        let expected_selected = [
            [3.0, 13.0, 3.0, 13.0],
            [f64::NAN; 4],
            [3.0, 13.0, 3.0, 13.0],
            [90.0, 3.0, 90.0, 3.0],
        ]
        .into_iter()
        .flatten();
        assert_eq!(selected_bounds.len(), 16);
        for (actual, expected) in selected_bounds.iter().zip(expected_selected) {
            if expected.is_nan() {
                assert!(actual.is_nan());
            } else {
                assert_eq!(actual.to_bits(), expected.to_bits());
            }
        }
    }

    #[test]
    fn ordinary_geographic_screen_declines_a_seam_vertex_in_row_k() {
        let rows = geographic_array(vec![
            geographic_polygon(0.0, 31.0),
            geographic_polygon(179.0, 31.0),
            geographic_polygon(4.0, 31.0),
        ]);
        assert!(!ordinary_geographic(&rows, GeographicFrame::crs84()));

        let explicit_period = geographic_array(vec![Shape::Polygon(Polygon::new(
            geographic_ring(&[(0.0, -10.0), (360.0, -10.0), (300.0, 10.0), (0.0, -10.0)]),
            Vec::new(),
        ))]);
        assert!(!ordinary_geographic(
            &explicit_period,
            GeographicFrame::crs84()
        ));
    }

    #[test]
    fn faithful_geographic_screen_keeps_alias_rows_and_declines_mixed_charts() {
        let frame = GeographicFrame::crs84();
        let faithful = |array: &PyGeometryArray| {
            array
                .packed_columns()
                .unwrap()
                .faithful_geographic_predicates(frame)
        };
        let aliases = geographic_array(vec![
            geographic_polygon(200.0, 31.0),
            geographic_polygon(-210.0, 31.0),
            geographic_polygon(350.0, 31.0),
        ]);
        assert!(faithful(&aliases));
        assert!(faithful(&aliases.gather_logical_rows(&[2, 0, 2]).unwrap()));
        let seam = geographic_array(vec![geographic_polygon(179.0, 31.0)]);
        assert!(!faithful(&seam));
        let long_edge = geographic_array(vec![Shape::Polygon(Polygon::new(
            geographic_ring(&[
                (200.0, 0.0),
                (500.0, 0.0),
                (500.0, 5.0),
                (200.0, 5.0),
                (200.0, 0.0),
            ]),
            Vec::new(),
        ))]);
        assert!(!faithful(&long_edge));
        let alias_hole = geographic_array(vec![Shape::Polygon(Polygon::new(
            geographic_ring(&[
                (200.0, 0.0),
                (210.0, 0.0),
                (210.0, 10.0),
                (200.0, 10.0),
                (200.0, 0.0),
            ]),
            vec![geographic_ring(&[
                (-159.0, 1.0),
                (-158.0, 1.0),
                (-158.0, 2.0),
                (-159.0, 2.0),
                (-159.0, 1.0),
            ])],
        ))]);
        assert!(!faithful(&alias_hole));
        let invalid = geographic_array(vec![geographic_polygon(200.0, 91.0)]);
        assert!(!faithful(&invalid));
    }

    #[test]
    fn ordinary_geographic_screen_declines_pole_linework_but_keeps_point_rows() {
        let frame = GeographicFrame::crs84();
        for pole in [90.0_f64, -90.0] {
            let line = |latitude| {
                geographic_array(vec![Shape::LineString(
                    LineSeq::try_new(
                        CoordSeq::try_from_vecs(
                            vec![0.0, 20.0, 10.0],
                            vec![latitude; 3],
                            None,
                            None,
                        )
                        .unwrap(),
                    )
                    .unwrap(),
                )])
            };
            assert!(!ordinary_geographic(&line(pole), frame));
            assert!(ordinary_geographic(
                &line(90.0_f64.next_down().copysign(pole)),
                frame
            ));
            assert!(ordinary_geographic(
                &geographic_array(vec![Shape::Point(Point::new_raw_xy(0.0, pole))]),
                frame
            ));
        }
    }

    #[test]
    fn ordinary_geographic_screen_resets_at_each_row_and_obeys_frame_bounds() {
        let rows = geographic_array(vec![
            geographic_polygon(-160.0, 31.0),
            geographic_polygon(150.0, 31.0),
        ]);
        assert!(
            ordinary_geographic(&rows, GeographicFrame::crs84()),
            "the longitude jump between rows is not a source edge",
        );

        let invalid_latitude = geographic_array(vec![geographic_polygon(0.0, 91.0)]);
        assert!(!ordinary_geographic(
            &invalid_latitude,
            GeographicFrame::crs84()
        ));

        let factor = AngularFactor::try_new("unit", std::f64::consts::TAU / 400.0_f64.next_up())
            .expect("rounded-up grad period has a valid angular factor");
        let grads = GeographicFrame::try_new(factor, factor, 0.0)
            .expect("rounded-up grad period has a valid frame");
        let rounded_alias = geographic_array(vec![geographic_polygon(199.0, 31.0)]);
        assert!(!ordinary_geographic(&rounded_alias, grads));

        let exterior_pole_ulp = geographic_array(vec![geographic_polygon(0.0, 90.0_f64.next_up())]);
        assert!(!ordinary_geographic(
            &exterior_pole_ulp,
            GeographicFrame::crs84()
        ));
    }

    #[test]
    fn ordinary_geographic_screen_handles_points_lines_and_selected_rows() {
        let points = geographic_array(vec![
            Shape::Point(Point::new_raw_xy(1.0, 2.0)),
            Shape::Point(Point::new_raw_xy(180.0, 2.0)),
            Shape::Point(Point::new_raw_xy(4.0, 5.0)),
        ]);
        assert!(!ordinary_geographic(&points, GeographicFrame::crs84()));
        assert!(ordinary_geographic(
            &points.gather_logical_rows(&[2, 0, 2]).unwrap(),
            GeographicFrame::crs84()
        ));

        let present = geographic_array(vec![
            Shape::Point(Point::new_raw_xy(1.0, 2.0)),
            Shape::Point(Point::new_raw_xy(4.0, 5.0)),
        ]);
        let compact = PyGeometryArray::scatter_present_rows(
            &present,
            ValidityMask::from_vec(4, vec![true, false, true, false]).expect("compact point mask"),
        )
        .expect("present points scatter into compact nullable storage");
        assert!(ordinary_geographic(&compact, GeographicFrame::crs84()));

        let line = |xs| {
            Shape::LineString(
                LineSeq::try_new(
                    CoordSeq::try_from_vecs(xs, vec![30.0, 31.0], None, None).unwrap(),
                )
                .unwrap(),
            )
        };
        let lines = geographic_array(vec![
            line(vec![-160.0, -150.0]),
            line(vec![150.0, 160.0]),
            line(vec![-170.0, 170.0]),
        ]);
        assert!(!ordinary_geographic(&lines, GeographicFrame::crs84()));
        assert!(ordinary_geographic(
            &lines.gather_logical_row_range(0..2).unwrap(),
            GeographicFrame::crs84()
        ));
    }

    #[test]
    fn ordinary_geographic_screen_resets_at_each_hole_boundary() {
        let shell = geographic_ring(&[
            (-170.0, -20.0),
            (0.0, -20.0),
            (170.0, -20.0),
            (170.0, 20.0),
            (0.0, 20.0),
            (-170.0, 20.0),
            (-170.0, -20.0),
        ]);
        let hole = |west| {
            geographic_ring(&[
                (west, -10.0),
                (west + 10.0, -10.0),
                (west + 10.0, 10.0),
                (west, 10.0),
                (west, -10.0),
            ])
        };
        let polygon = geographic_array(vec![Shape::Polygon(Polygon::new(shell, vec![
            hole(-160.0),
            hole(150.0),
        ]))]);
        assert!(
            ordinary_geographic(&polygon, GeographicFrame::crs84()),
            "the longitude jump between holes is not a source edge",
        );
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

        let point_owner = packed_owner(point_storage, None, StorageLayout::LogicalRows);
        let Some(points) = point_owner.packed_columns() else {
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

        for storage in [line_storage, polygon_storage] {
            let owner = packed_owner(storage, None, StorageLayout::LogicalRows);
            let Some(columns) = owner.packed_columns() else {
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
    fn nullable_point_cursor_uses_only_admitted_present_ranks() {
        let compact_map = RowSelection::gather_checked_native(vec![5_usize, 2, 5], 6).unwrap();
        let compact_mask = ValidityMask::from_vec(6, vec![true, false, true, true, false, false])
            .expect("compact mask");
        let owner = packed_owner(
            GeometryArrayStorage::Packed(PackedStorage::Points {
                coords: Arc::new(coords()),
                row_map: compact_map,
            }),
            Some(compact_mask),
            StorageLayout::PresentOnly,
        );
        let Some(PackedColumns::Points(points)) = owner.packed_columns() else {
            panic!("nullable packed points resolve through owner admission");
        };
        assert_eq!(points.rows().present_count(), 3);
        assert_eq!(points.rows().cursor().collect::<Vec<_>>(), [
            (0, None),
            (1, Some(0)),
            (2, None),
            (3, None),
            (4, Some(1)),
            (5, Some(2))
        ]);
        assert_eq!(points.coords().xs().to_vec(), [5.0, 2.0, 5.0]);
    }

    #[test]
    fn nullable_point_cursor_decodes_non_byte_aligned_validity_windows() {
        let parent = ValidityMask::from_sparse(80, &[4, 7, 11, 19, 27, 32, 40, 63, 72]).unwrap();
        let window = parent.window(3, 65).expect("window contains missing rows");
        assert_eq!(window.arrow_bitmap().1, 3);
        let missing = [1_usize, 4, 8, 16, 24, 29, 37, 60];

        let owner = packed_owner(
            GeometryArrayStorage::Packed(PackedStorage::Points {
                coords: Arc::new(
                    CoordSeq::try_from_vecs(vec![0.0; 57], vec![1.0; 57], None, None)
                        .expect("57 point rows"),
                ),
                row_map: RowSelection::Identity,
            }),
            Some(window),
            StorageLayout::PresentOnly,
        );
        let Some(PackedColumns::Points(points)) = owner.packed_columns() else {
            panic!("nullable packed points resolve through owner admission");
        };
        let compact = points.rows();
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
    fn point_row_cursor_preserves_dense_selections_and_nullable_values() {
        let empty_owner = packed_owner(
            GeometryArrayStorage::Packed(PackedStorage::Points {
                coords: Arc::new(CoordSeq::empty(CoordinateAxes::XY)),
                row_map: RowSelection::Identity,
            }),
            None,
            StorageLayout::LogicalRows,
        );
        let Some(PackedColumns::Points(empty)) = empty_owner.packed_columns() else {
            panic!("empty packed points resolve");
        };
        assert_eq!(
            empty.rows().cursor().collect::<Vec<_>>(),
            Vec::<(usize, Option<usize>)>::new()
        );

        let all_missing = ValidityMask::from_vec(9, vec![true; 9]).unwrap();
        let missing_owner = packed_owner(
            GeometryArrayStorage::Packed(PackedStorage::Points {
                coords: Arc::new(CoordSeq::empty(CoordinateAxes::XY)),
                row_map: RowSelection::Identity,
            }),
            Some(all_missing),
            StorageLayout::PresentOnly,
        );
        let Some(PackedColumns::Points(compact_missing)) = missing_owner.packed_columns() else {
            panic!("all-missing packed points resolve");
        };
        assert_eq!(
            compact_missing.rows().cursor().collect::<Vec<_>>(),
            (0..9).map(|logical| (logical, None)).collect::<Vec<_>>()
        );

        let dense_map = RowSelection::gather_checked_native(vec![4_usize, 1, 4, 0], 5).unwrap();
        let dense_owner = packed_owner(
            GeometryArrayStorage::Packed(PackedStorage::Points {
                coords: Arc::new(
                    CoordSeq::try_from_vecs(
                        vec![10.0, 20.0, 30.0, 40.0, 50.0],
                        vec![1.0; 5],
                        None,
                        None,
                    )
                    .expect("five point rows"),
                ),
                row_map: dense_map,
            }),
            None,
            StorageLayout::LogicalRows,
        );
        let Some(PackedColumns::Points(dense_gather)) = dense_owner.packed_columns() else {
            panic!("dense gathered packed points resolve");
        };
        assert_eq!(dense_gather.rows().cursor().collect::<Vec<_>>(), [
            (0, Some(4)),
            (1, Some(1)),
            (2, Some(4)),
            (3, Some(0))
        ]);

        let compact_mask = ValidityMask::from_vec(4, vec![false, true, false, false]).unwrap();
        let compact_owner = packed_owner(
            GeometryArrayStorage::Packed(PackedStorage::Points {
                coords: Arc::new(
                    CoordSeq::try_from_vecs(vec![10.0, 20.0, 30.0], vec![1.0; 3], None, None)
                        .expect("present point rows"),
                ),
                row_map: RowSelection::gather_checked_native(vec![2_usize, 1, 2], 3).unwrap(),
            }),
            Some(compact_mask),
            StorageLayout::PresentOnly,
        );
        let Some(PackedColumns::Points(compact_gather)) = compact_owner.packed_columns() else {
            panic!("nullable gathered packed points resolve");
        };
        assert_eq!(compact_gather.rows().cursor().collect::<Vec<_>>(), [
            (0, Some(0)),
            (1, None),
            (2, Some(1)),
            (3, Some(2))
        ]);
        assert_eq!(compact_gather.coords().xs(), &[30.0, 20.0, 30.0]);

        let dense_window = packed_owner(
            GeometryArrayStorage::Packed(PackedStorage::Points {
                coords: Arc::new(
                    CoordSeq::try_from_vecs(vec![0.0; 8], vec![1.0; 8], None, None)
                        .expect("eight point rows"),
                ),
                row_map: storage::row_selection_from_logical_rows(
                    RowSelectionRef::Identity,
                    8,
                    3..8,
                ),
            }),
            None,
            StorageLayout::LogicalRows,
        );
        let Some(PackedColumns::Points(dense_window)) = dense_window.packed_columns() else {
            panic!("dense windowed packed points resolve");
        };
        assert_eq!(dense_window.rows().cursor().collect::<Vec<_>>(), [
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
            let owner = packed_owner(storage.clone(), mask.cloned(), StorageLayout::LogicalRows);
            let columns = owner.packed_columns().expect("packed columns");
            let (admission, admission_allocations) = allocation_count(|| {
                packed_column_kernels::ensure_geographic_columns_present(
                    &columns,
                    crate::geographic::GeographicFrame::crs84(),
                )
            });
            admission.unwrap();
            let (bounds, bounds_allocations) =
                allocation_count(|| packed_column_kernels::raw_total_bounds_from_columns(&columns));
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
        let owner = packed_owner(storage, None, StorageLayout::LogicalRows);
        let columns = owner.packed_columns().expect("packed columns");
        let (values, allocations) =
            allocation_count(|| packed_column_kernels::bounds_values_from_columns(&columns));
        assert_eq!(values.len(), 32 * 4);
        assert_eq!(allocations, 1);
    }

    #[test]
    fn selected_reverse_reserves_coordinate_and_offset_columns() {
        let counts = [2, 128].map(|rows| {
            let storage = gathered_lines(rows);
            let owner = packed_owner(storage, None, StorageLayout::LogicalRows);
            let Some(PackedColumns::Lines(columns)) = owner.packed_columns() else {
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
                    crate::geometry::OriginSpec::Center,
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
            let owner = packed_owner(storage, None, StorageLayout::LogicalRows);
            let columns = owner.packed_columns().expect("packed columns");
            let linear = crate::geometry::AffineLinear::new([2.0, 0.0, 0.0, 2.0]).unwrap();
            let (output, allocations) = allocation_count(|| {
                packed_column_kernels::packed_per_row_self_origin_affine_columns(
                    columns,
                    crate::geometry::OriginSpec::Center,
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
            let owner = packed_owner(storage, None, StorageLayout::LogicalRows);
            let columns = owner.packed_columns().expect("packed columns");
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
