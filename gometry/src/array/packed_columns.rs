#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Detached packed-column execution: resolve `row_map`, release the GIL, and hand logical contiguous columns to kernels in `packed_column_kernels.rs`.

use std::sync::Arc;

use crate::array::packed_gather::{
    gather_line_columns, gather_point_columns, gather_polygon_columns, normalized_gather_storage,
};
use crate::array::{
    CoordSeq, CoordinateReplacement, CsrOffsetColumn, Frame, GeometryArrayStorage, PolygonLevel,
    PyErr, PyGeometryArray, PyResult, Python, ReplacementAxis, Result, Ring, RingLevel,
    RowSelectionRef, rows_err,
};
use crate::geometry::{CoordSeqBuilder, CoordWindow};

pub(crate) enum PackedColumnError {
    Batch(crate::error::Error),
    Row {
        row: usize,
        error: crate::error::Error,
    },
}

pub(crate) type PackedColumnResult<T> = std::result::Result<T, PackedColumnError>;

pub(crate) enum ColumnSource<'a, T> {
    Borrowed(&'a T),
    Owned(T),
}

impl<T> ColumnSource<'_, T> {
    const fn as_ref(&self) -> &T {
        match self {
            Self::Borrowed(value) => value,
            Self::Owned(value) => value,
        }
    }
}

pub(crate) struct PointColumns<'a> {
    coords: ColumnSource<'a, CoordSeq>,
}

pub(crate) struct XyColumns<'a> {
    pub xs: &'a [f64],
    pub ys: &'a [f64],
}

impl PointColumns<'_> {
    pub(crate) const fn coords(&self) -> &CoordSeq {
        self.coords.as_ref()
    }

    /// Contiguous logical X/Y slices — row-map normalization already happened.
    pub(crate) fn xy(&self) -> XyColumns<'_> {
        let coords = self.coords();
        XyColumns {
            xs: coords.xs(),
            ys: coords.ys(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.coords().len()
    }
}

pub(crate) struct LineColumns<'a> {
    coords: ColumnSource<'a, CoordSeq>,
    offsets: ColumnSource<'a, CsrOffsetColumn>,
}

impl LineColumns<'_> {
    pub(crate) const fn coords(&self) -> &CoordSeq {
        self.coords.as_ref()
    }

    pub(crate) fn offsets(&self) -> &[i32] {
        self.offsets.as_ref().as_slice()
    }

    pub(crate) const fn offsets_column(&self) -> &CsrOffsetColumn {
        self.offsets.as_ref()
    }

    pub(crate) fn xy(&self) -> XyColumns<'_> {
        let coords = self.coords();
        XyColumns {
            xs: coords.xs(),
            ys: coords.ys(),
        }
    }

    pub(crate) fn rows(&self) -> usize {
        self.offsets().len().saturating_sub(1)
    }
}

pub(crate) struct PolygonColumns<'a> {
    coords: ColumnSource<'a, CoordSeq>,
    ring_offsets: ColumnSource<'a, CsrOffsetColumn<RingLevel>>,
    polygon_offsets: ColumnSource<'a, CsrOffsetColumn<PolygonLevel>>,
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
    pub group_missing: Option<&'a [bool]>,
}

impl SegmentedRuns<'_> {
    pub(crate) fn n_groups(&self) -> usize {
        self.group_offsets.map_or_else(
            || self.n_leaves(),
            |offsets| offsets.len().saturating_sub(1),
        )
    }

    pub(crate) const fn n_leaves(&self) -> usize {
        self.leaf_offsets.len().saturating_sub(1)
    }

    /// Leaf-index range owned by `group`.
    pub(crate) fn group_leaves(&self, group: usize) -> std::ops::Range<usize> {
        self.group_offsets.map_or_else(
            || group..group + 1,
            |offsets| offsets[group] as usize..offsets[group + 1] as usize,
        )
    }

    /// Vertex window for leaf `leaf` in the physical column.
    pub(crate) const fn leaf_vertices(&self, leaf: usize) -> std::ops::Range<usize> {
        self.leaf_offsets[leaf] as usize..self.leaf_offsets[leaf + 1] as usize
    }

    /// True when the group is marked missing.
    pub(crate) fn is_missing(&self, group: usize) -> bool {
        self.group_missing.is_some_and(|mask| mask[group])
    }
}

impl LineColumns<'_> {
    /// Topology descriptor for planar line-length dual reduction. Group space
    /// is identity over leaves (one line row = one leaf).
    pub(crate) fn segmented_runs<'a>(&'a self, missing: Option<&'a [bool]>) -> SegmentedRuns<'a> {
        SegmentedRuns {
            leaf_offsets: self.offsets(),
            group_offsets: None,
            group_missing: missing,
        }
    }
}

impl PolygonColumns<'_> {
    /// Topology descriptor for planar polygon perimeter dual reduction.
    pub(crate) fn segmented_runs<'a>(&'a self, missing: Option<&'a [bool]>) -> SegmentedRuns<'a> {
        SegmentedRuns {
            leaf_offsets: self.ring_offsets(),
            group_offsets: Some(self.polygon_offsets()),
            group_missing: missing,
        }
    }
}

impl PolygonColumns<'_> {
    pub(crate) const fn coords(&self) -> &CoordSeq {
        self.coords.as_ref()
    }

    pub(crate) fn ring_offsets(&self) -> &[i32] {
        self.ring_offsets.as_ref().as_slice()
    }

    pub(crate) fn polygon_offsets(&self) -> &[i32] {
        self.polygon_offsets.as_ref().as_slice()
    }

    pub(crate) const fn polygon_offsets_column(&self) -> &CsrOffsetColumn<PolygonLevel> {
        self.polygon_offsets.as_ref()
    }

    pub(crate) const fn ring_offsets_column(&self) -> &CsrOffsetColumn<RingLevel> {
        self.ring_offsets.as_ref()
    }

    pub(crate) fn xy(&self) -> XyColumns<'_> {
        let coords = self.coords();
        XyColumns {
            xs: coords.xs(),
            ys: coords.ys(),
        }
    }

    pub(crate) fn rows(&self) -> usize {
        self.polygon_offsets().len().saturating_sub(1)
    }
}

pub(crate) enum PackedColumns<'a> {
    Points(PointColumns<'a>),
    Lines(LineColumns<'a>),
    Polygons(PolygonColumns<'a>),
}

/// One logical row of packed homogeneous storage — encodes the per-variant
/// offset walk shared by z/m extremes, bounds, centroid, and point-on-surface.
pub(crate) enum PackedRow<'a> {
    Points {
        row: usize,
    },
    Lines {
        row: usize,
        offsets: &'a [i32],
    },
    Polygons {
        row: usize,
        ring_offsets: &'a [i32],
        polygon_offsets: &'a [i32],
    },
}

impl PackedRow<'_> {
    pub(crate) const fn index(&self) -> usize {
        match self {
            Self::Points { row } | Self::Lines { row, .. } | Self::Polygons { row, .. } => *row,
        }
    }

    /// Point: ``row..row+1``; line: CSR window; polygon: all-ring vertex span.
    pub(crate) fn vertex_window(&self) -> Option<std::ops::Range<usize>> {
        match *self {
            Self::Points { row } => Some(row..row + 1),
            Self::Lines { row, offsets } => Some(offsets[row] as usize..offsets[row + 1] as usize),
            Self::Polygons {
                row,
                ring_offsets,
                polygon_offsets,
            } => {
                let rings = polygon_offsets[row] as usize..polygon_offsets[row + 1] as usize;
                if rings.is_empty() {
                    None
                } else {
                    Some(ring_offsets[rings.start] as usize..ring_offsets[rings.end] as usize)
                }
            },
        }
    }

    /// Polygon rows only: ring-index range within ``ring_offsets``.
    pub(crate) const fn polygon_rings(&self) -> Option<std::ops::Range<usize>> {
        match *self {
            Self::Polygons {
                row,
                polygon_offsets,
                ..
            } => {
                let rings = polygon_offsets[row] as usize..polygon_offsets[row + 1] as usize;
                Some(rings)
            },
            Self::Points { .. } | Self::Lines { .. } => None,
        }
    }

    /// Polygon rows only: exterior-ring vertex window.
    pub(crate) fn polygon_shell_window(&self) -> Option<std::ops::Range<usize>> {
        match *self {
            Self::Polygons {
                row,
                ring_offsets,
                polygon_offsets,
            } => {
                let rings = polygon_offsets[row] as usize..polygon_offsets[row + 1] as usize;
                if rings.is_empty() {
                    None
                } else {
                    Some(ring_offsets[rings.start] as usize..ring_offsets[rings.start + 1] as usize)
                }
            },
            Self::Points { .. } | Self::Lines { .. } => None,
        }
    }
}

impl PackedColumns<'_> {
    pub(crate) fn xy(&self) -> XyColumns<'_> {
        match self {
            Self::Points(columns) => columns.xy(),
            Self::Lines(columns) => columns.xy(),
            Self::Polygons(columns) => columns.xy(),
        }
    }

    pub(crate) fn row_count(&self) -> usize {
        match self {
            Self::Points(columns) => columns.len(),
            Self::Lines(columns) => columns.rows(),
            Self::Polygons(columns) => columns.rows(),
        }
    }

    pub(crate) fn map_rows<T>(&self, mut f: impl FnMut(PackedRow<'_>) -> T) -> Vec<T> {
        match self {
            Self::Points(columns) => (0..columns.len())
                .map(|row| f(PackedRow::Points { row }))
                .collect(),
            Self::Lines(columns) => {
                let offsets = columns.offsets();
                (0..columns.rows())
                    .map(|row| f(PackedRow::Lines { row, offsets }))
                    .collect()
            },
            Self::Polygons(columns) => {
                let ring_offsets = columns.ring_offsets();
                let polygon_offsets = columns.polygon_offsets();
                (0..columns.rows())
                    .map(|row| {
                        f(PackedRow::Polygons {
                            row,
                            ring_offsets,
                            polygon_offsets,
                        })
                    })
                    .collect()
            },
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

fn rebase_csr_window(offsets: &[i32], start: usize, len: usize) -> Result<CsrOffsetColumn> {
    let window = &offsets[start..=(start + len)];
    let base = window[0] as usize;
    let rebased: Vec<usize> = window
        .iter()
        .map(|&offset| {
            (offset as usize)
                .checked_sub(base)
                .ok_or_else(|| crate::geometry::GeometryErrorKind::MalformedCsrOffsets.into())
        })
        .collect::<Result<_>>()?;
    let vertex_cap = *rebased.last().ok_or_else(|| -> crate::error::Error {
        crate::geometry::GeometryErrorKind::MalformedCsrOffsets.into()
    })?;
    CsrOffsetColumn::<()>::try_new(rebased, vertex_cap)
}

fn rebase_polygon_window(
    ring_offsets: &[i32],
    polygon_offsets: &[i32],
    start: usize,
    len: usize,
) -> Result<(CsrOffsetColumn<RingLevel>, CsrOffsetColumn<PolygonLevel>)> {
    let ring_start = polygon_offsets[start] as usize;
    let ring_end = polygon_offsets[start + len] as usize;
    let vertex_base = ring_offsets[ring_start] as usize;
    let rebased_rings: Vec<usize> = ring_offsets[ring_start..=ring_end]
        .iter()
        .map(|&offset| {
            (offset as usize)
                .checked_sub(vertex_base)
                .ok_or_else(|| crate::geometry::GeometryErrorKind::MalformedCsrOffsets.into())
        })
        .collect::<Result<_>>()?;
    let ring_cap = *rebased_rings.last().ok_or_else(|| -> crate::error::Error {
        crate::geometry::GeometryErrorKind::MalformedCsrOffsets.into()
    })?;
    let out_ring_offsets = CsrOffsetColumn::<RingLevel>::try_new(rebased_rings, ring_cap)?;
    let polygon_base = ring_start;
    let rebased_polygons: Vec<usize> = polygon_offsets[start..=start + len]
        .iter()
        .map(|&offset| {
            (offset as usize)
                .checked_sub(polygon_base)
                .ok_or_else(|| crate::geometry::GeometryErrorKind::MalformedCsrOffsets.into())
        })
        .collect::<Result<_>>()?;
    let polygon_cap = *rebased_polygons
        .last()
        .ok_or_else(|| -> crate::error::Error {
            crate::geometry::GeometryErrorKind::MalformedCsrOffsets.into()
        })?;
    let out_polygon_offsets =
        CsrOffsetColumn::<PolygonLevel>::try_new(rebased_polygons, polygon_cap)?;
    Ok((out_ring_offsets, out_polygon_offsets))
}

fn resolve_point_columns<'a>(
    coords: &'a CoordSeq,
    row_map: RowSelectionRef<'a>,
) -> PointColumns<'a> {
    match row_map {
        RowSelectionRef::Identity => PointColumns {
            coords: ColumnSource::Borrowed(coords),
        },
        RowSelectionRef::Window { start, len } => PointColumns {
            coords: ColumnSource::Owned(
                coords.view(CoordWindow::trusted(start..start + len, coords.len())),
            ),
        },
        RowSelectionRef::Gather(map) => PointColumns {
            coords: ColumnSource::Owned(gather_point_columns(coords, map)),
        },
    }
}

fn resolve_line_columns<'a>(
    coords: &'a CoordSeq,
    offsets: &'a CsrOffsetColumn,
    row_map: RowSelectionRef<'a>,
) -> Result<LineColumns<'a>> {
    let offset_slice = offsets.as_slice();
    match row_map {
        RowSelectionRef::Identity => Ok(LineColumns {
            coords: ColumnSource::Borrowed(coords),
            offsets: ColumnSource::Borrowed(offsets),
        }),
        RowSelectionRef::Window { start, len } => {
            let vertex_start = offset_slice[start] as usize;
            let vertex_end = offset_slice[start + len] as usize;
            Ok(LineColumns {
                coords: ColumnSource::Owned(
                    coords.view(CoordWindow::trusted(vertex_start..vertex_end, coords.len())),
                ),
                offsets: ColumnSource::Owned(rebase_csr_window(offset_slice, start, len)?),
            })
        },
        RowSelectionRef::Gather(map) => {
            let (out_coords, out_offsets) = gather_line_columns(coords, offset_slice, map)?;
            Ok(LineColumns {
                coords: ColumnSource::Owned(out_coords),
                offsets: ColumnSource::Owned(out_offsets),
            })
        },
    }
}

fn resolve_polygon_columns<'a>(
    coords: &'a CoordSeq,
    ring_offsets: &'a CsrOffsetColumn<RingLevel>,
    polygon_offsets: &'a CsrOffsetColumn<PolygonLevel>,
    row_map: RowSelectionRef<'a>,
) -> Result<PolygonColumns<'a>> {
    let ring_offset_slice = ring_offsets.as_slice();
    let polygon_offset_slice = polygon_offsets.as_slice();
    match row_map {
        RowSelectionRef::Identity => Ok(PolygonColumns {
            coords: ColumnSource::Borrowed(coords),
            ring_offsets: ColumnSource::Borrowed(ring_offsets),
            polygon_offsets: ColumnSource::Borrowed(polygon_offsets),
        }),
        RowSelectionRef::Window { start, len } => {
            let ring_start = polygon_offset_slice[start] as usize;
            let ring_end = polygon_offset_slice[start + len] as usize;
            let vertex_start = ring_offset_slice[ring_start] as usize;
            let vertex_end = ring_offset_slice[ring_end] as usize;
            let (out_ring_offsets, out_polygon_offsets) =
                rebase_polygon_window(ring_offset_slice, polygon_offset_slice, start, len)?;
            Ok(PolygonColumns {
                coords: ColumnSource::Owned(
                    coords.view(CoordWindow::trusted(vertex_start..vertex_end, coords.len())),
                ),
                ring_offsets: ColumnSource::Owned(out_ring_offsets),
                polygon_offsets: ColumnSource::Owned(out_polygon_offsets),
            })
        },
        RowSelectionRef::Gather(map) => {
            let (out_coords, out_ring_offsets, out_polygon_offsets) =
                gather_polygon_columns(coords, ring_offset_slice, polygon_offset_slice, map)?;
            Ok(PolygonColumns {
                coords: ColumnSource::Owned(out_coords),
                ring_offsets: ColumnSource::Owned(out_ring_offsets),
                polygon_offsets: ColumnSource::Owned(out_polygon_offsets),
            })
        },
    }
}

fn resolve_packed_columns(
    storage: &GeometryArrayStorage,
) -> PackedColumnResult<Option<PackedColumns<'_>>> {
    Ok(match storage {
        GeometryArrayStorage::Mixed(_) => None,
        GeometryArrayStorage::Points { coords, row_map } => Some(PackedColumns::Points(
            resolve_point_columns(coords, row_map.as_deref()),
        )),
        GeometryArrayStorage::Lines {
            coords,
            offsets,
            row_map,
        } => Some(PackedColumns::Lines(
            resolve_line_columns(coords, offsets, row_map.as_deref()).map_err(batch_err)?,
        )),
        GeometryArrayStorage::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
            row_map,
        } => Some(PackedColumns::Polygons(
            resolve_polygon_columns(coords, ring_offsets, polygon_offsets, row_map.as_deref())
                .map_err(batch_err)?,
        )),
    })
}

fn assemble_packed_output(output: PackedColumnOutput, frame: Frame) -> PyGeometryArray {
    match output {
        PackedColumnOutput::Points(coords) => PyGeometryArray::packed_points(coords, frame),
        PackedColumnOutput::Lines { coords, offsets } => {
            PyGeometryArray::packed_lines(coords, offsets, frame)
        },
        PackedColumnOutput::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
        } => PyGeometryArray::packed_polygons(coords, ring_offsets, polygon_offsets, frame),
    }
}

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
    // Same active-ordinate closure as pack_admission::ring_seq_is_packable /
    // dense identity validate_packed_replacement_rings / pickle (D05/N5):
    // XY-only same_point would admit Z/M-open rings on sliced/gathered paths.
    let len = window.end.saturating_sub(window.start);
    if len < Ring::MIN_VERTICES_CLOSED {
        return Err(PackedColumnError::Batch(
            crate::geometry::GeometryErrorKind::RingTooShort(len).into(),
        ));
    }
    let first = coords.point_at(window.start);
    let last = coords.point_at(window.end - 1);
    if !crate::geometry::same_active_position(first, last) {
        return Err(PackedColumnError::Batch(
            crate::geometry::GeometryErrorKind::message("polygon ring must be closed"),
        ));
    }
    Ok(())
}

fn finish_replacement_builder(
    builder: CoordSeqBuilder,
    allow_mask_placeholders: bool,
) -> PackedColumnResult<CoordSeq> {
    if allow_mask_placeholders {
        Ok(builder.finish_unchecked())
    } else {
        builder.finish().map_err(batch_err)
    }
}

fn packed_row_missing<const HAS_MISSING: bool>(missing: Option<&[bool]>, row: usize) -> bool {
    if HAS_MISSING {
        missing.expect("HAS_MISSING rows carry a mask")[row]
    } else {
        false
    }
}

fn replace_point_columns<const HAS_MISSING: bool>(
    point_columns: &PointColumns<'_>,
    replacement: &CoordinateReplacement,
    replacement_index: &mut usize,
    missing: Option<&[bool]>,
) -> PackedColumnResult<PackedColumnOutput> {
    let old = point_columns.coords();
    let mut builder = CoordSeqBuilder::with_capacity(old.axes(), old.len());
    for row in 0..old.len() {
        if packed_row_missing::<HAS_MISSING>(missing, row) {
            builder.push_at(old, row);
        } else {
            push_replacement_vertex(&mut builder, old, row, replacement, *replacement_index);
            *replacement_index += 1;
        }
    }
    Ok(PackedColumnOutput::Points(finish_replacement_builder(
        builder,
        HAS_MISSING,
    )?))
}

fn replace_line_columns<const HAS_MISSING: bool>(
    line_columns: &LineColumns<'_>,
    replacement: &CoordinateReplacement,
    replacement_index: &mut usize,
    missing: Option<&[bool]>,
) -> PackedColumnResult<PackedColumnOutput> {
    let old = line_columns.coords();
    let offsets = line_columns.offsets();
    let mut builder = CoordSeqBuilder::with_capacity(old.axes(), old.len());
    for row in 0..line_columns.rows() {
        let window = offsets[row] as usize..offsets[row + 1] as usize;
        if packed_row_missing::<HAS_MISSING>(missing, row) {
            for old_index in window {
                builder.push_at(old, old_index);
            }
        } else {
            for old_index in window {
                push_replacement_vertex(
                    &mut builder,
                    old,
                    old_index,
                    replacement,
                    *replacement_index,
                );
                *replacement_index += 1;
            }
        }
    }
    Ok(PackedColumnOutput::Lines {
        coords: finish_replacement_builder(builder, HAS_MISSING)?,
        offsets: line_columns.offsets_column().clone(),
    })
}

fn replace_polygon_columns<const HAS_MISSING: bool>(
    polygon_columns: &PolygonColumns<'_>,
    replacement: &CoordinateReplacement,
    replacement_index: &mut usize,
    missing: Option<&[bool]>,
) -> PackedColumnResult<PackedColumnOutput> {
    let old = polygon_columns.coords();
    let ring_offsets = polygon_columns.ring_offsets();
    let polygon_offsets = polygon_columns.polygon_offsets();
    let mut builder = CoordSeqBuilder::with_capacity(old.axes(), old.len());
    let mut validate_windows = Vec::new();
    for row in 0..polygon_columns.rows() {
        let rings = polygon_offsets[row] as usize..polygon_offsets[row + 1] as usize;
        for ring in rings {
            let window = ring_offsets[ring] as usize..ring_offsets[ring + 1] as usize;
            if packed_row_missing::<HAS_MISSING>(missing, row) {
                for old_index in window {
                    builder.push_at(old, old_index);
                }
            } else {
                validate_windows.push(window.clone());
                for old_index in window {
                    push_replacement_vertex(
                        &mut builder,
                        old,
                        old_index,
                        replacement,
                        *replacement_index,
                    );
                    *replacement_index += 1;
                }
            }
        }
    }
    let coords = finish_replacement_builder(builder, HAS_MISSING)?;
    for window in validate_windows {
        validate_replacement_ring(&coords, window)?;
    }
    Ok(PackedColumnOutput::Polygons {
        coords,
        ring_offsets: polygon_columns.ring_offsets_column().clone(),
        polygon_offsets: polygon_columns.polygon_offsets_column().clone(),
    })
}

fn replace_packed_columns_for_mask<const HAS_MISSING: bool>(
    columns: PackedColumns<'_>,
    replacement: &CoordinateReplacement,
    replacement_index: &mut usize,
    missing: Option<&[bool]>,
) -> PackedColumnResult<PackedColumnOutput> {
    match columns {
        PackedColumns::Points(point_columns) => replace_point_columns::<HAS_MISSING>(
            &point_columns,
            replacement,
            replacement_index,
            missing,
        ),
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
    missing: Option<&[bool]>,
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

impl PyGeometryArray {
    pub(crate) fn reduce_packed_columns_detached<R>(
        &self,
        py: Python<'_>,
        kernel: impl for<'a> FnOnce(PackedColumns<'a>) -> PackedColumnResult<R> + Send,
    ) -> PyResult<Option<R>>
    where
        R: Send,
    {
        if matches!(self.storage(), GeometryArrayStorage::Mixed(_)) {
            return Ok(None);
        }
        let storage = Arc::clone(self.storage_arc());
        let memo = Arc::clone(&self.gathered_memo);
        let result = py
            .detach(move || {
                let storage = normalized_gather_storage(&storage, &memo)?;
                let columns = resolve_packed_columns(storage.as_ref())?.expect("not mixed");
                kernel(columns)
            })
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
            .map(|output| assemble_packed_output(output, output_frame));
        Ok(output)
    }

    pub(crate) fn map_packed_coordseq_detached(
        &self,
        py: Python<'_>,
        output_frame: Frame,
        kernel: impl FnOnce(&CoordSeq) -> crate::error::Result<CoordSeq> + Send,
    ) -> PyResult<Option<Self>> {
        if matches!(self.storage(), GeometryArrayStorage::Mixed(_)) {
            return Ok(None);
        }
        let storage = Arc::clone(self.storage_arc());
        let memo = Arc::clone(&self.gathered_memo);
        let output = py
            .detach(move || -> PackedColumnResult<PackedColumnOutput> {
                let storage = normalized_gather_storage(&storage, &memo)?;
                let columns = resolve_packed_columns(storage.as_ref())?.expect("not mixed");
                match columns {
                    PackedColumns::Points(point_columns) => {
                        let out_coords = kernel(point_columns.coords()).map_err(batch_err)?;
                        Ok(PackedColumnOutput::Points(out_coords))
                    },
                    PackedColumns::Lines(line_columns) => {
                        let out_coords = kernel(line_columns.coords()).map_err(batch_err)?;
                        Ok(PackedColumnOutput::Lines {
                            coords: out_coords,
                            offsets: line_columns.offsets_column().clone(),
                        })
                    },
                    PackedColumns::Polygons(polygon_columns) => {
                        let out_coords = kernel(polygon_columns.coords()).map_err(batch_err)?;
                        Ok(PackedColumnOutput::Polygons {
                            coords: out_coords,
                            ring_offsets: polygon_columns.ring_offsets_column().clone(),
                            polygon_offsets: polygon_columns.polygon_offsets_column().clone(),
                        })
                    },
                }
            })
            .map_err(packed_columns_err)?;
        Ok(Some(
            assemble_packed_output(output, output_frame).with_missing_mask(self.missing().cloned()),
        ))
    }

    pub(crate) fn replace_packed_coords_detached(
        &self,
        py: Python<'_>,
        output_frame: Frame,
        replacement: CoordinateReplacement,
    ) -> PyResult<Option<Self>> {
        if matches!(self.storage(), GeometryArrayStorage::Mixed(_)) {
            return Ok(None);
        }
        let storage = Arc::clone(self.storage_arc());
        let memo = Arc::clone(&self.gathered_memo);
        let missing = self.missing().cloned();
        let output = py
            .detach(move || -> PackedColumnResult<PackedColumnOutput> {
                let storage = normalized_gather_storage(&storage, &memo)?;
                let columns = resolve_packed_columns(storage.as_ref())?.expect("not mixed");
                let mut replacement_index = 0;
                let output = replace_packed_columns_dispatch(
                    columns,
                    &replacement,
                    &mut replacement_index,
                    missing.as_deref(),
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
            assemble_packed_output(output, output_frame).with_missing_mask(self.missing().cloned()),
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
        if matches!(left.storage(), GeometryArrayStorage::Mixed(_))
            || matches!(right.storage(), GeometryArrayStorage::Mixed(_))
        {
            return Ok(None);
        }
        let left_storage = Arc::clone(left.storage_arc());
        let left_memo = Arc::clone(&left.gathered_memo);
        let right_storage = Arc::clone(right.storage_arc());
        let right_memo = Arc::clone(&right.gathered_memo);
        let result = py
            .detach(move || {
                let left_storage = normalized_gather_storage(&left_storage, &left_memo)?;
                let right_storage = normalized_gather_storage(&right_storage, &right_memo)?;
                let left_columns =
                    resolve_packed_columns(left_storage.as_ref())?.expect("not mixed");
                let right_columns =
                    resolve_packed_columns(right_storage.as_ref())?.expect("not mixed");
                kernel(left_columns, right_columns)
            })
            .map_err(packed_columns_err)?;
        Ok(Some(result))
    }

    /// Points-only reduce: returns `None` for mixed or line/polygon packed storage.
    pub(crate) fn reduce_packed_points_detached<R>(
        &self,
        py: Python<'_>,
        kernel: impl FnOnce(&[f64], &[f64]) -> PackedColumnResult<R> + Send,
    ) -> PyResult<Option<R>>
    where
        R: Send,
    {
        let GeometryArrayStorage::Points { .. } = self.storage() else {
            return Ok(None);
        };
        let storage = Arc::clone(self.storage_arc());
        let memo = Arc::clone(&self.gathered_memo);
        let result = py
            .detach(move || -> PackedColumnResult<R> {
                let storage = normalized_gather_storage(&storage, &memo)?;
                let columns = resolve_packed_columns(storage.as_ref())?.expect("points");
                let PackedColumns::Points(point_columns) = columns else {
                    unreachable!("reduce_packed_points_detached requires point storage");
                };
                let XyColumns { xs, ys } = point_columns.xy();
                kernel(xs, ys)
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
        let (GeometryArrayStorage::Points { .. }, GeometryArrayStorage::Points { .. }) =
            (self.storage(), right.storage())
        else {
            return Ok(None);
        };
        let left_storage = Arc::clone(self.storage_arc());
        let left_memo = Arc::clone(&self.gathered_memo);
        let right_storage = Arc::clone(right.storage_arc());
        let right_memo = Arc::clone(&right.gathered_memo);
        let values = py
            .detach(move || -> PackedColumnResult<Vec<T>> {
                let left_storage = normalized_gather_storage(&left_storage, &left_memo)?;
                let right_storage = normalized_gather_storage(&right_storage, &right_memo)?;
                let left_columns = resolve_packed_columns(left_storage.as_ref())?.expect("points");
                let right_columns =
                    resolve_packed_columns(right_storage.as_ref())?.expect("points");
                let PackedColumns::Points(left) = left_columns else {
                    unreachable!("pair_packed_points_detached requires point storage");
                };
                let PackedColumns::Points(right) = right_columns else {
                    unreachable!("pair_packed_points_detached requires point storage");
                };
                let XyColumns {
                    xs: left_xs,
                    ys: left_ys,
                } = left.xy();
                let XyColumns {
                    xs: right_xs,
                    ys: right_ys,
                } = right.xy();
                kernel(left_xs, left_ys, right_xs, right_ys).map_err(batch_err)
            })
            .map_err(packed_columns_err)?;
        Ok(Some(values))
    }

    /// O(1) packed-storage X/Y swap — narrow escape hatch, not a general scan.
    pub(crate) fn swap_xy_packed_storage(&self) -> Option<Self> {
        match self.storage() {
            GeometryArrayStorage::Points { coords, row_map } => Some(Self::packed_points_mapped(
                coords.swap_xy(),
                self.frame.clone(),
                row_map.clone(),
            )),
            GeometryArrayStorage::Lines {
                coords,
                offsets,
                row_map,
            } => Some(Self::packed_lines_mapped(
                coords.swap_xy(),
                offsets.clone(),
                self.frame.clone(),
                row_map.clone(),
            )),
            GeometryArrayStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            } => Some(Self::packed_polygons_mapped(
                coords.swap_xy(),
                ring_offsets.clone(),
                polygon_offsets.clone(),
                self.frame.clone(),
                row_map.clone(),
            )),
            GeometryArrayStorage::Mixed(_) => None,
        }
    }
}
