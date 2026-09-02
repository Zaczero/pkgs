#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Kernels that consume resolved packed columns: CRS transform, per-row
//! bounds, segmentize/densify, and per-row affine — plus the sanctioned
//! `line_measure`/`polygon_measure` lane over raw storage columns.
//!
//! Row-map normalization and GIL release happen in `packed_columns.rs` (the
//! detached execution seam); everything here operates on already-logical
//! contiguous columns.
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]

use std::num::NonZeroU32;

use crate::array::packed_columns::{
    LineColumns, PackedColumnError, PackedColumnResult, PackedColumns, PointColumns, PointRows,
    PolygonColumns, SegmentedRuns, XyColumns, batch_err, row_err,
};
use crate::array::storage_helpers::{
    packed_line_measure, packed_line_row_pivot, packed_polygon_measure, packed_polygon_row_pivot,
};
use crate::array::{
    Bounds, CoordSeq, CsrOffsetBuilder, GeometryArrayStorage, OriginSpec, PackedColumnBuilder,
    PackedColumnOutput, PointRowLayout, Result, RingLevel, RowSelectionRef, Shape, ValidityMask,
    affine_about, column_window, crs, line_logical_len, polygon_logical_len, polygon_rings_range,
    row_bounds_3d, row_bounds_values,
};
use crate::geometry::{CoordSeqBuilder, CoordWindow, LineSeq, XY, clip_segment, same_point};

/// Emit one round buffer polygon per packed point without per-row shapes.
///
/// The scalar point kernel builds the same inscribed circle, but repeats its
/// step-angle trigonometry and allocates a `Shape` graph for every row. All
/// rows here share the unit-circle table and are written into one XY column;
/// CSR offsets retain one polygon shell per present row and zero shells for
/// masked rows.
pub(crate) fn buffer_point_columns(
    points: &PointColumns<'_>,
    distance: f64,
    quadrant_segments: NonZeroU32,
    missing: Option<&ValidityMask>,
) -> PackedColumnResult<Option<PackedColumnOutput>> {
    let rows = points.len();
    if missing.is_some_and(|mask| mask.len() != rows) {
        return Ok(None);
    }
    let steps = crate::geometry::ExpansionBudget::product(
        "buffer",
        "quadrant_segments",
        4,
        quadrant_segments.get() as usize,
    )
    .map_err(batch_err)?;
    let vertices_per_row = steps.checked_add(1).ok_or_else(|| {
        batch_err(crate::geometry::GeometryErrorKind::OffsetCapacityExceeded.into())
    })?;
    let point_rows = points.rows();
    let present_rows = point_rows.present_count();
    let total = crate::geometry::ExpansionBudget::product(
        "buffer",
        "quadrant_segments",
        present_rows,
        vertices_per_row,
    )
    .map_err(batch_err)?;
    let mut budget = crate::geometry::ExpansionBudget::new("buffer", "quadrant_segments");
    budget.add(total).map_err(batch_err)?;

    let mut unit_x = Vec::new();
    let mut unit_y = Vec::new();
    crate::try_reserve_vec(&mut unit_x, steps).map_err(PackedColumnError::Batch)?;
    crate::try_reserve_vec(&mut unit_y, steps).map_err(PackedColumnError::Batch)?;
    let (sin_step, cos_step) = (std::f64::consts::TAU / steps as f64).sin_cos();
    let (mut sin_theta, mut cos_theta) = (0.0_f64, 1.0_f64);
    for step in 0..steps {
        if step > 0 {
            (cos_theta, sin_theta) = (
                cos_theta * cos_step - sin_theta * sin_step,
                sin_theta * cos_step + cos_theta * sin_step,
            );
        }
        crate::try_push_vec(&mut unit_x, cos_theta).map_err(PackedColumnError::Batch)?;
        crate::try_push_vec(&mut unit_y, sin_theta).map_err(PackedColumnError::Batch)?;
    }

    let XyColumns {
        xs: source_xs,
        ys: source_ys,
    } = points.physical_xy();
    let mut xs = Vec::new();
    let mut ys = Vec::new();
    crate::try_reserve_vec(&mut xs, total).map_err(PackedColumnError::Batch)?;
    crate::try_reserve_vec(&mut ys, total).map_err(PackedColumnError::Batch)?;
    let mut ring_offsets = CsrOffsetBuilder::new();
    let mut polygon_offsets = CsrOffsetBuilder::new();
    let mut ring_count = 0_usize;
    for (_row, physical) in point_rows.cursor() {
        if let Some(physical) = physical {
            let center_x = source_xs[physical];
            let center_y = source_ys[physical];
            for step in 0..steps {
                let x = center_x + distance * unit_x[step];
                let y = center_y + distance * unit_y[step];
                if !x.is_finite() || !y.is_finite() {
                    return Ok(None);
                }
                crate::try_push_vec(&mut xs, x).map_err(PackedColumnError::Batch)?;
                crate::try_push_vec(&mut ys, y).map_err(PackedColumnError::Batch)?;
            }
            let first = xs[xs.len() - steps];
            let first_y = ys[ys.len() - steps];
            crate::try_push_vec(&mut xs, first).map_err(PackedColumnError::Batch)?;
            crate::try_push_vec(&mut ys, first_y).map_err(PackedColumnError::Batch)?;
            ring_count += 1;
            ring_offsets
                .push_end(xs.len(), i32::MAX as usize)
                .map_err(batch_err)?;
        }
        polygon_offsets
            .push_end(ring_count, i32::MAX as usize)
            .map_err(batch_err)?;
    }
    let coords = CoordSeq::try_from_vecs(xs, ys, None, None).map_err(batch_err)?;
    let ring_offsets = ring_offsets.finish(total).map_err(batch_err)?.cast_level();
    let polygon_offsets = polygon_offsets
        .finish(ring_count)
        .map_err(batch_err)?
        .cast_level();
    Ok(Some(PackedColumnOutput::Polygons {
        coords,
        ring_offsets,
        polygon_offsets,
    }))
}

// `line_measure`/`polygon_measure` are the sanctioned indirection required by
// `tools/gates/_check_packed_execution.py`: array method surfaces may not call the
// raw `packed_*_measure` storage helpers directly.

/// Private crossover: a leaf/run with this many *segments* (verts − 1) or more
/// is "long" — always PerRun. Shorter runs take ColumnStream when a contiguous
/// short span is large enough that the flat map wins.
///
/// Measured on roads (≈9 segs) vs a single 100k-vert line: flat ColumnStream
/// alone regresses the long line ~1.37×, so long runs stay on PerRun.
const COLUMN_STREAM_MAX_SEGMENTS: usize = 24;

/// Minimum contiguous short groups before ColumnStream is preferred over PerRun.
/// Measured on the roads fixture (10k short lines); re-measure before changing.
const COLUMN_STREAM_MIN_GROUPS: usize = 64;

/// Clip packed XY line rows into packed output without materializing result shapes.
///
/// The result remains packed only while every row has at most one surviving
/// line part. A row that re-enters the rectangle declines the lane so the
/// caller can retain the general `MultiLineString` representation and its
/// ordinate/error policy.
pub(crate) fn clip_lines_packed_xy(
    line_columns: LineColumns<'_>,
    rect: Bounds,
    geographic_frame: Option<crate::geographic::GeographicFrame>,
) -> PackedColumnResult<Option<PackedColumnOutput>> {
    let coords = line_columns.coords();
    if coords.zs().is_some() || coords.ms().is_some() {
        return Ok(None);
    }
    let (xs, ys) = (coords.xs(), coords.ys());
    let mut builder = PackedColumnBuilder::try_like(coords, coords.len()).map_err(batch_err)?;

    for row in 0..line_columns.rows() {
        if line_columns.is_missing(row) {
            builder.close_row().map_err(batch_err)?;
            continue;
        }
        let window = line_columns.row_window(row);
        let (start, end) = (window.start, window.end);
        let row_bounds = row_bounds_values(xs, ys, window.clone());
        if row_bounds[0].is_nan() {
            return Ok(None);
        }
        let bounds = Bounds::new_raw(row_bounds[0], row_bounds[1], row_bounds[2], row_bounds[3]);
        if let Some(frame) = geographic_frame {
            let shape = Shape::LineString(
                LineSeq::try_new(
                    coords.view(CoordWindow::from_range(window.clone(), coords.len())),
                )
                .expect("packed line rows contain at least two vertices"),
            );
            if !crate::geometry::raw_coordinate_bounds_are_sound_in_frame(
                Some(&shape),
                bounds,
                frame,
            ) {
                return Ok(None);
            }
        }
        if !bounds.intersects(rect) {
            return Ok(None);
        }
        if rect.contains(bounds) {
            builder.push_window(coords, window).map_err(batch_err)?;
            continue;
        }

        let mut part_count = 0;
        let mut last_end = None;
        for index in start..end.saturating_sub(1) {
            let Some((clipped_start, clipped_end)) = clip_segment(
                XY::new(xs[index], ys[index]),
                XY::new(xs[index + 1], ys[index + 1]),
                rect,
            ) else {
                continue;
            };
            if same_point(clipped_start, clipped_end) {
                continue;
            }
            if last_end.is_some_and(|last| same_point(last, clipped_start)) {
                builder
                    .push_xy(clipped_end.x, clipped_end.y)
                    .map_err(batch_err)?;
            } else {
                part_count += 1;
                if part_count > 1 {
                    return Ok(None);
                }
                builder
                    .push_xy(clipped_start.x, clipped_start.y)
                    .map_err(batch_err)?;
                builder
                    .push_xy(clipped_end.x, clipped_end.y)
                    .map_err(batch_err)?;
            }
            last_end = Some(clipped_end);
        }
        if part_count == 0 {
            return Ok(None);
        }
        builder.close_row().map_err(batch_err)?;
    }

    let capacity = builder.vertex_len();
    let (coords, offsets) = builder.finish(capacity).map_err(batch_err)?;
    Ok(Some(PackedColumnOutput::Lines { coords, offsets }))
}

/// Dual-strategy planar line-length (or ring-perimeter leaf) reduction.
///
/// Two engines behind one entry, partitioned by **run groups**:
/// - **PerRun** — compact in-register reducer ([`line_length_columns`]) for
///   few/long runs.
/// - **ColumnStream** — per-leaf reducers over contiguous short-group spans
///   (identity leaves only): each leaf's valid adjacent-pair window is reduced
///   independently; cross-leaf boundary pairs are never entered.
///
/// No cross-group algebraic reassociation: each group's sum folds only its own
/// pair contributions.
pub(crate) fn segmented_planar_lengths(
    runs: SegmentedRuns<'_>,
    xs: &[f64],
    ys: &[f64],
    scale: f64,
) -> Result<Vec<f64>> {
    segmented_planar_lengths_dims::<false>(runs, xs, ys, &[], scale)
}

/// Dual-strategy 3-D length over the same topology.
pub(crate) fn segmented_planar_lengths_3d(
    runs: SegmentedRuns<'_>,
    xs: &[f64],
    ys: &[f64],
    zs: &[f64],
    scale: f64,
) -> Result<Vec<f64>> {
    segmented_planar_lengths_dims::<true>(runs, xs, ys, zs, scale)
}

fn leaf_segment_count(runs: &SegmentedRuns<'_>, leaf: usize) -> usize {
    runs.leaf_vertices(leaf).len().saturating_sub(1)
}

fn group_is_short(runs: &SegmentedRuns<'_>, group: usize) -> bool {
    let mut leaves = runs.group_leaves(group);
    leaves.is_empty()
        || leaves.all(|leaf| leaf_segment_count(runs, leaf) < COLUMN_STREAM_MAX_SEGMENTS)
}

fn segmented_planar_lengths_dims<const HAS_Z: bool>(
    runs: SegmentedRuns<'_>,
    xs: &[f64],
    ys: &[f64],
    zs: &[f64],
    scale: f64,
) -> Result<Vec<f64>> {
    let n_groups = runs.n_groups();
    if n_groups == 0 {
        return Ok(Vec::new());
    }
    debug_assert!(!HAS_Z || zs.len() == xs.len());
    debug_assert_eq!(xs.len(), ys.len());

    if !runs.row_map.is_identity() {
        let mut output = Vec::new();
        crate::boundary::try_reserve_vec(&mut output, n_groups)?;
        for group in 0..n_groups {
            crate::boundary::try_push_vec(
                &mut output,
                per_run_group_length::<HAS_Z>(&runs, group, xs, ys, zs, scale),
            )?;
        }
        return Ok(output);
    }

    if runs.group_offsets.is_none() {
        // Identity lines: prefer ColumnStream for large short spans.
        if n_groups >= COLUMN_STREAM_MIN_GROUPS {
            // Cheap sample: if the first/mid/last groups are short, stream the
            // whole array (roads is homogeneous). Mixed arrays fall to partition.
            let sample_short = group_is_short(&runs, 0)
                && group_is_short(&runs, n_groups / 2)
                && group_is_short(&runs, n_groups - 1);
            if sample_short {
                let mut out = Vec::new();
                crate::boundary::try_reserve_vec(&mut out, n_groups)?;
                out.resize(n_groups, 0.0);
                column_stream_identity_lengths::<HAS_Z>(
                    &runs,
                    0..n_groups,
                    xs,
                    ys,
                    zs,
                    scale,
                    &mut out,
                );
                return Ok(out);
            }
            return partitioned_identity_lengths::<HAS_Z>(&runs, xs, ys, zs, scale);
        }
        return tight_identity_per_run::<HAS_Z>(&runs, xs, ys, zs, scale);
    }

    // Multi-leaf groups (polygons): reinterpret ring offsets as identity leaf
    // runs, reduce rings unscaled through the short/long selector, then fold
    // ring results by polygon offsets (missing → NaN; scale once per polygon).
    let leaf_missing = runs.group_missing.and_then(|missing| {
        let mut mask = vec![false; runs.n_leaves()];
        for (group, is_missing) in missing.iter().enumerate() {
            if is_missing {
                mask[runs.group_leaves(group)].fill(true);
            }
        }
        ValidityMask::from_vec(runs.n_leaves(), mask)
    });
    let leaf_runs = SegmentedRuns {
        leaf_offsets: runs.leaf_offsets,
        group_offsets: None,
        group_missing: leaf_missing.as_ref(),
        row_map: RowSelectionRef::Identity,
    };
    let leaf_lengths = segmented_planar_lengths_dims::<HAS_Z>(leaf_runs, xs, ys, zs, 1.0)?;
    let scale_is_one = scale.to_bits() == 1.0_f64.to_bits();
    let mut output = Vec::new();
    crate::boundary::try_reserve_vec(&mut output, n_groups)?;
    for group in 0..n_groups {
        let value = if runs.is_missing(group) {
            f64::NAN
        } else {
            let total: f64 = leaf_lengths[runs.group_leaves(group)].iter().sum();
            if scale_is_one { total } else { total * scale }
        };
        crate::boundary::try_push_vec(&mut output, value)?;
    }
    Ok(output)
}

fn partitioned_identity_lengths<const HAS_Z: bool>(
    runs: &SegmentedRuns<'_>,
    xs: &[f64],
    ys: &[f64],
    zs: &[f64],
    scale: f64,
) -> Result<Vec<f64>> {
    let n_groups = runs.n_groups();
    let mut out = Vec::new();
    crate::boundary::try_reserve_vec(&mut out, n_groups)?;
    out.resize(n_groups, 0.0);
    let mut group = 0;
    while group < n_groups {
        let short = group_is_short(runs, group);
        let span_start = group;
        group += 1;
        while group < n_groups && group_is_short(runs, group) == short {
            group += 1;
        }
        if short && group - span_start >= COLUMN_STREAM_MIN_GROUPS {
            column_stream_identity_lengths::<HAS_Z>(
                runs,
                span_start..group,
                xs,
                ys,
                zs,
                scale,
                &mut out,
            );
        } else {
            fill_per_run_span::<HAS_Z>(runs, span_start..group, xs, ys, zs, scale, &mut out);
        }
    }
    Ok(out)
}

fn fill_per_run_span<const HAS_Z: bool>(
    runs: &SegmentedRuns<'_>,
    groups: std::ops::Range<usize>,
    xs: &[f64],
    ys: &[f64],
    zs: &[f64],
    scale: f64,
    out: &mut [f64],
) {
    for (g, slot) in out
        .iter_mut()
        .enumerate()
        .take(groups.end)
        .skip(groups.start)
    {
        *slot = per_run_group_length::<HAS_Z>(runs, g, xs, ys, zs, scale);
    }
}

/// Packed-line PerRun: identity groups, minimal overhead.
fn tight_identity_per_run<const HAS_Z: bool>(
    runs: &SegmentedRuns<'_>,
    xs: &[f64],
    ys: &[f64],
    zs: &[f64],
    scale: f64,
) -> Result<Vec<f64>> {
    let n = runs.n_groups();
    let offsets = runs.leaf_offsets;
    let missing = runs.group_missing;
    let scale_is_one = scale.to_bits() == 1.0_f64.to_bits();
    let mut out = Vec::new();
    crate::boundary::try_reserve_vec(&mut out, n)?;
    out.resize(n, 0.0);
    for (g, slot) in out.iter_mut().enumerate() {
        if missing.is_some_and(|mask| mask.is_missing(g)) {
            *slot = f64::NAN;
            continue;
        }
        let start = offsets[g] as usize;
        let end = offsets[g + 1] as usize;
        if end <= start + 1 {
            continue;
        }
        let total = if HAS_Z {
            crate::geometry::line_length_3d_columns(
                &xs[start..end],
                &ys[start..end],
                &zs[start..end],
            )
        } else {
            crate::geometry::line_length_columns(&xs[start..end], &ys[start..end])
        };
        *slot = if scale_is_one { total } else { total * scale };
    }
    Ok(out)
}

fn per_run_group_length<const HAS_Z: bool>(
    runs: &SegmentedRuns<'_>,
    group: usize,
    xs: &[f64],
    ys: &[f64],
    zs: &[f64],
    scale: f64,
) -> f64 {
    if runs.is_missing(group) {
        return f64::NAN;
    }
    let mut total = 0.0_f64;
    for leaf in runs.group_leaves(group) {
        let window = runs.leaf_vertices(leaf);
        if window.len() < 2 {
            continue;
        }
        let start = window.start;
        let end = window.end;
        total += if HAS_Z {
            crate::geometry::line_length_3d_columns(
                &xs[start..end],
                &ys[start..end],
                &zs[start..end],
            )
        } else {
            crate::geometry::line_length_columns(&xs[start..end], &ys[start..end])
        };
    }
    total * scale
}

/// Production ColumnStream for identity groups (packed lines / ring leaves).
///
/// Selects a per-leaf reducer over each contiguous short-group span: each
/// leaf's vertex window is an independent valid pair range. The single
/// cross-leaf boundary pair between adjacent leaves is **skipped before** any
/// squared-norm work (never classified then discarded) — ColumnStream does
/// **not** map pairs across leaves. Empty / duplicate-offset / missing groups
/// emit 0 / NaN without touching coordinates.
///
/// Within a valid leaf the compact-guard SIMD body is applied in full chunks,
/// then a scalar residual — no overlap-tail replay. Accumulators are
/// leaf-local (no cross-leaf reassociation).
/// File-local 4-lane SIMD for ColumnStream only. Broader reducers keep the
/// global eight-lane `REDUCE_LANES` / `ReduceSimd` (x86-64-v2 spills at 8 for
/// this dual-load length body; 4 beats 8 by ~1.09–1.19× and 2 by ~1.02–1.15×).
const STREAM_LANES: usize = 4;
type StreamSimd = std::simd::Simd<f64, STREAM_LANES>;
type StreamSimdU64 = std::simd::Simd<u64, STREAM_LANES>;

fn column_stream_identity_lengths<const HAS_Z: bool>(
    runs: &SegmentedRuns<'_>,
    groups: std::ops::Range<usize>,
    xs: &[f64],
    ys: &[f64],
    zs: &[f64],
    scale: f64,
    out: &mut [f64],
) {
    use std::ops::Not as _;
    use std::simd::StdFloat as _;
    use std::simd::cmp::SimdPartialEq as _;
    use std::simd::num::SimdFloat as _;

    let offsets = runs.leaf_offsets;
    let missing = runs.group_missing;
    let scale_is_one = scale.to_bits() == 1.0_f64.to_bits();

    for g in groups {
        if missing.is_some_and(|mask| mask.is_missing(g)) {
            out[g] = f64::NAN;
            continue;
        }
        let start = offsets[g] as usize;
        let end = offsets[g + 1] as usize;
        // Valid pairs: [start, end - 1). Boundary pair at end-1 → next leaf is
        // never entered here — mask-by-construction.
        if end <= start + 1 {
            out[g] = 0.0;
            continue;
        }
        let pair_end = end - 1; // exclusive absolute pair-start index
        let mut total = 0.0_f64;
        let mut pair = start;

        // Full SIMD chunks entirely inside this group's valid pair range.
        while pair + STREAM_LANES <= pair_end {
            let x0 = StreamSimd::from_slice(&xs[pair..]);
            let x1 = StreamSimd::from_slice(&xs[pair + 1..]);
            let y0 = StreamSimd::from_slice(&ys[pair..]);
            let y1 = StreamSimd::from_slice(&ys[pair + 1..]);
            let dx = x1 - x0;
            let dy = y1 - y0;
            let (squared, zero_delta) = if HAS_Z {
                let z0 = StreamSimd::from_slice(&zs[pair..]);
                let z1 = StreamSimd::from_slice(&zs[pair + 1..]);
                let dz = z1 - z0;
                (
                    dx * dx + dy * dy + dz * dz,
                    dx.simd_eq(StreamSimd::splat(0.0))
                        & dy.simd_eq(StreamSimd::splat(0.0))
                        & dz.simd_eq(StreamSimd::splat(0.0)),
                )
            } else {
                (
                    dx * dx + dy * dy,
                    dx.simd_eq(StreamSimd::splat(0.0)) & dy.simd_eq(StreamSimd::splat(0.0)),
                )
            };
            // Same squared-norm trust rule as scalar/SIMD length: normal, or
            // exact-zero with all-zero deltas. Positive subnormals are untrusted.
            let bits = squared.to_bits();
            let exp = bits & StreamSimdU64::splat(0x7FF0_0000_0000_0000);
            let is_normal = exp.simd_ne(StreamSimdU64::splat(0))
                & exp.simd_ne(StreamSimdU64::splat(0x7FF0_0000_0000_0000));
            let is_zero = squared.simd_eq(StreamSimd::splat(0.0));
            let bad = (is_normal | (is_zero & zero_delta)).not();
            if bad.any() {
                // Cold: one hypot-safe segment at a time for this chunk only.
                for p in pair..pair + STREAM_LANES {
                    total += stream_segment_length::<HAS_Z>(xs, ys, zs, p);
                }
            } else {
                total += squared.sqrt().reduce_sum();
            }
            pair += STREAM_LANES;
        }
        // Scalar residual — no overlap replay of the last full chunk.
        while pair < pair_end {
            total += stream_segment_length::<HAS_Z>(xs, ys, zs, pair);
            pair += 1;
        }
        out[g] = if scale_is_one { total } else { total * scale };
    }
}

fn stream_segment_length<const HAS_Z: bool>(
    xs: &[f64],
    ys: &[f64],
    zs: &[f64],
    pair: usize,
) -> f64 {
    let dx = xs[pair + 1] - xs[pair];
    let dy = ys[pair + 1] - ys[pair];
    if HAS_Z {
        let dz = zs[pair + 1] - zs[pair];
        let squared = dx * dx + dy * dy + dz * dz;
        if crate::geometry::squared_norm_is_trustworthy(
            squared,
            dx == 0.0 && dy == 0.0 && dz == 0.0,
        ) {
            squared.sqrt()
        } else {
            dx.hypot(dy).hypot(dz)
        }
    } else {
        let squared = dx * dx + dy * dy;
        if crate::geometry::squared_norm_is_trustworthy(squared, dx == 0.0 && dy == 0.0) {
            squared.sqrt()
        } else {
            dx.hypot(dy)
        }
    }
}

pub(crate) fn line_measure<'a, F>(
    xs: &[f64],
    ys: &[f64],
    offsets: &[i32],
    map: impl Into<RowSelectionRef<'a>>,
    measure: F,
) -> Result<Vec<f64>>
where
    F: Fn(&[f64], &[f64], std::ops::Range<usize>) -> f64,
{
    packed_line_measure(xs, ys, offsets, map, measure)
}

/// Mask-aware variant of [`line_measure`]. Missing rows retain their output
/// slot but never expose their rectangular NaN placeholder to a measure.
pub(crate) fn line_measure_masked<'a, F>(
    xs: &[f64],
    ys: &[f64],
    offsets: &[i32],
    map: impl Into<RowSelectionRef<'a>>,
    missing: Option<&ValidityMask>,
    measure: F,
) -> Result<Vec<f64>>
where
    F: Fn(&[f64], &[f64], std::ops::Range<usize>) -> f64,
{
    let Some(missing) = missing else {
        return line_measure(xs, ys, offsets, map, measure);
    };
    let map = map.into();
    let rows = line_logical_len(offsets, map);
    let mut output = Vec::new();
    crate::boundary::try_reserve_vec(&mut output, rows)?;
    for logical in 0..rows {
        let value = if missing.is_missing(logical) {
            f64::NAN
        } else {
            let window = map.csr_window(offsets, logical);
            measure(
                column_window(xs, &window),
                column_window(ys, &window),
                window,
            )
        };
        crate::boundary::try_push_vec(&mut output, value)?;
    }
    Ok(output)
}

pub(crate) fn polygon_measure<'a, F, C>(
    xs: &[f64],
    ys: &[f64],
    ring_offsets: &[i32],
    polygon_offsets: &[i32],
    map: impl Into<RowSelectionRef<'a>>,
    ring_measure: F,
    combine: C,
) -> Result<Vec<f64>>
where
    F: Fn(&[f64], &[f64], std::ops::Range<usize>) -> f64,
    C: Fn(std::ops::Range<usize>, &dyn Fn(usize) -> f64) -> f64,
{
    packed_polygon_measure(
        xs,
        ys,
        ring_offsets,
        polygon_offsets,
        map,
        ring_measure,
        combine,
    )
}

/// Mask-aware variant of [`polygon_measure`]. Missing polygon rows emit the
/// numeric sentinel without invoking ring measurement or combination.
pub(crate) fn polygon_measure_masked<'a, F, C>(
    xs: &[f64],
    ys: &[f64],
    ring_offsets: &[i32],
    polygon_offsets: &[i32],
    map: impl Into<RowSelectionRef<'a>>,
    missing: Option<&ValidityMask>,
    ring_measure: F,
    combine: C,
) -> Result<Vec<f64>>
where
    F: Fn(&[f64], &[f64], std::ops::Range<usize>) -> f64,
    C: Fn(std::ops::Range<usize>, &dyn Fn(usize) -> f64) -> f64,
{
    let Some(missing) = missing else {
        return polygon_measure(
            xs,
            ys,
            ring_offsets,
            polygon_offsets,
            map,
            ring_measure,
            combine,
        );
    };
    let map = map.into();
    let rows = polygon_logical_len(polygon_offsets, map);
    let mut output = Vec::new();
    crate::boundary::try_reserve_vec(&mut output, rows)?;
    for logical in 0..rows {
        let value = if missing.is_missing(logical) {
            f64::NAN
        } else {
            let rings = polygon_rings_range(polygon_offsets, map, logical);
            let ring = |index: usize| {
                let window = ring_offsets[index] as usize..ring_offsets[index + 1] as usize;
                ring_measure(
                    column_window(xs, &window),
                    column_window(ys, &window),
                    window,
                )
            };
            combine(rings, &ring)
        };
        crate::boundary::try_push_vec(&mut output, value)?;
    }
    Ok(output)
}

/// Validate geographic ordinates for present packed rows only. Nullable
/// storage deliberately uses NaN placeholder coordinates; the mask, rather
/// than the placeholder payload, is the trust boundary.
pub(crate) fn ensure_geographic_columns_present(
    columns: &PackedColumns<'_>,
    missing: Option<&ValidityMask>,
    frame: crate::geographic::GeographicFrame,
) -> crate::error::Result<()> {
    let XyColumns { xs, ys } = columns.physical_xy();
    for (row, window) in columns
        .map_rows(|row| row.vertex_window())
        .into_iter()
        .enumerate()
    {
        if missing.is_some_and(|mask| mask.is_missing(row)) {
            continue;
        }
        if let Some(window) = window {
            crate::crs::ensure_geographic_columns_in_frame(
                frame,
                column_window(xs, &window),
                column_window(ys, &window),
            )?;
        }
    }
    Ok(())
}

/// Transform present packed points once, then rebuild the selected point
/// column. Missing placeholders never cross the transform boundary; compact
/// inputs return present-only coordinates and row-aligned inputs are rebuilt
/// with their placeholders in one output allocation.
pub(crate) fn map_masked_point_columns_to_crs(
    transformer: &crs::TransformPlan,
    coords: &CoordSeq,
    selection: RowSelectionRef<'_>,
    missing: &ValidityMask,
    present_count: usize,
    layout: PointRowLayout,
) -> crate::error::Result<CoordSeq> {
    let rows = PointRows::new(coords.len(), selection, Some(missing), layout);
    debug_assert_eq!(rows.len(), missing.len());
    debug_assert_eq!(rows.present_count(), present_count);
    let compact = rows.is_compact();
    let mut xs = Vec::new();
    let mut ys = Vec::new();
    let mut zs = coords.zs().map(|_| Vec::new());
    let mut ms = coords.ms().map(|_| Vec::new());
    crate::try_reserve_vec(&mut xs, present_count)?;
    crate::try_reserve_vec(&mut ys, present_count)?;
    if let Some(zs) = zs.as_mut() {
        crate::try_reserve_vec(zs, present_count)?;
    }
    if let Some(ms) = ms.as_mut() {
        crate::try_reserve_vec(ms, present_count)?;
    }
    for (_, physical) in rows.cursor() {
        let Some(physical) = physical else {
            continue;
        };
        crate::try_push_vec(&mut xs, coords.xs()[physical])?;
        crate::try_push_vec(&mut ys, coords.ys()[physical])?;
        if let (Some(output), Some(source)) = (zs.as_mut(), coords.zs()) {
            crate::try_push_vec(output, source[physical])?;
        }
        if let (Some(output), Some(source)) = (ms.as_mut(), coords.ms()) {
            crate::try_push_vec(output, source[physical])?;
        }
    }
    let zt = zs.as_mut().map_or(crate::Zt::None, |values| {
        crate::Zt::Z(values.as_mut_slice())
    });
    if !xs.is_empty() {
        transformer.transform_coordinates(&mut xs, &mut ys, zt)?;
    }

    if compact {
        return CoordSeq::try_from_vecs(xs, ys, zs, ms);
    }

    let mut output = CoordSeqBuilder::try_with_capacity(coords.axes(), rows.len())?;
    let mut present_row = 0;
    for (_, physical) in rows.cursor() {
        if physical.is_some() {
            output.push_xyzm(
                xs[present_row],
                ys[present_row],
                zs.as_ref().map(|values| values[present_row]),
                ms.as_ref().map(|values| values[present_row]),
            );
            present_row += 1;
        } else {
            output.push_xyzm(
                f64::NAN,
                f64::NAN,
                coords.zs().map(|_| f64::NAN),
                coords.ms().map(|_| f64::NAN),
            );
        }
    }
    debug_assert_eq!(present_row, present_count);
    Ok(output.finish_infallible())
}

/// Per-row 3D bounds from packed homogeneous storage — the kernel behind
/// `GeometryArray.bounds_3d` (all rings for polygons, matching `Shape::bounds_3d`).
pub(crate) fn bounds_3d_values_from_columns(
    columns: &PackedColumns<'_>,
) -> Vec<Option<crate::geometry::Bounds3D>> {
    let XyColumns { xs, ys } = columns.physical_xy();
    let zs = match &columns {
        PackedColumns::Points(point_columns) => point_columns.coords().zs(),
        PackedColumns::Lines(line_columns) => line_columns.coords().zs(),
        PackedColumns::Polygons(polygon_columns) => polygon_columns.coords().zs(),
    }
    .expect("caller gates has_z");
    columns.map_rows(|row| {
        row.vertex_window()
            .and_then(|window| row_bounds_3d(xs, ys, zs, window))
    })
}

/// Dispatch a packed-column reduce over line or polygon storage only — the
/// shared shape behind length/length_3d packed lanes (points are handled
/// separately or excluded upstream).
pub(crate) fn reduce_lines_or_polygons<R>(
    columns: PackedColumns<'_>,
    lines: impl FnOnce(LineColumns<'_>) -> R,
    polygons: impl FnOnce(PolygonColumns<'_>) -> R,
) -> R {
    match columns {
        PackedColumns::Lines(line_columns) => lines(line_columns),
        PackedColumns::Polygons(polygon_columns) => polygons(polygon_columns),
        PackedColumns::Points(_) => unreachable!("lines/polygons packed lane excludes points"),
    }
}

fn bounds_values_from_columns_impl<const MASKED: bool>(
    columns: &PackedColumns<'_>,
    missing: Option<&ValidityMask>,
) -> Result<Vec<f64>> {
    let XyColumns { xs, ys } = columns.physical_xy();
    let rows = columns.row_count();
    // Keep the row result in its natural fixed-width shape while reducing;
    // flattening moves the same allocation into NumPy's one-dimensional lane.
    let mut values = Vec::new();
    crate::boundary::try_reserve_vec(&mut values, rows)?;
    match columns {
        PackedColumns::Points(point_columns) => {
            let rows = point_columns.rows();
            for (_, physical) in rows.cursor() {
                let Some(physical) = physical else {
                    crate::try_push_vec(&mut values, [f64::NAN; 4])?;
                    continue;
                };
                let (x, y) = (xs[physical], ys[physical]);
                crate::try_push_vec(&mut values, [x, y, x, y])?;
            }
        },
        PackedColumns::Lines(line_columns) => {
            for row in 0..line_columns.rows() {
                if MASKED && missing.is_some_and(|mask| mask.is_missing(row)) {
                    crate::try_push_vec(&mut values, [f64::NAN; 4])?;
                    continue;
                }
                let window = line_columns.row_window(row);
                crate::try_push_vec(&mut values, row_bounds_values(xs, ys, window))?;
            }
        },
        PackedColumns::Polygons(polygon_columns) => {
            let ring_offsets = polygon_columns.ring_offsets();
            for row in 0..polygon_columns.rows() {
                if MASKED && missing.is_some_and(|mask| mask.is_missing(row)) {
                    crate::try_push_vec(&mut values, [f64::NAN; 4])?;
                    continue;
                }
                let rings = polygon_columns.polygon_rings(row);
                let bounds = if rings.is_empty() {
                    [f64::NAN; 4]
                } else {
                    let shell =
                        ring_offsets[rings.start] as usize..ring_offsets[rings.start + 1] as usize;
                    row_bounds_values(xs, ys, shell)
                };
                crate::try_push_vec(&mut values, bounds)?;
            }
        },
    }
    Ok(values.into_flattened())
}

pub(crate) fn bounds_values_from_columns(columns: &PackedColumns<'_>) -> Result<Vec<f64>> {
    bounds_values_from_columns_impl::<false>(columns, None)
}

/// Packed bounds with nullable rows excluded before the per-row extrema walk.
pub(crate) fn bounds_values_from_columns_masked(
    columns: &PackedColumns<'_>,
    missing: Option<&ValidityMask>,
) -> Result<Vec<f64>> {
    missing.map_or_else(
        || bounds_values_from_columns(columns),
        |missing| bounds_values_from_columns_impl::<true>(columns, Some(missing)),
    )
}

pub(crate) fn geographic_bounds_values_from_columns(
    columns: &PackedColumns<'_>,
) -> Result<Vec<f64>> {
    geographic_bounds_values_from_columns_impl::<false>(columns, None)
}

pub(crate) fn geographic_bounds_values_from_columns_masked(
    columns: &PackedColumns<'_>,
    missing: Option<&ValidityMask>,
) -> Result<Vec<f64>> {
    missing.map_or_else(
        || geographic_bounds_values_from_columns_impl::<false>(columns, None),
        |missing| geographic_bounds_values_from_columns_impl::<true>(columns, Some(missing)),
    )
}

fn geographic_bounds_values_from_columns_impl<const MASKED: bool>(
    columns: &PackedColumns<'_>,
    missing: Option<&ValidityMask>,
) -> Result<Vec<f64>> {
    let mut values = Vec::new();
    let capacity = columns
        .row_count()
        .checked_mul(4)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    crate::boundary::try_reserve_vec(&mut values, capacity)?;
    match columns {
        PackedColumns::Points(_) => {
            return if MASKED {
                bounds_values_from_columns_masked(columns, missing)
            } else {
                bounds_values_from_columns(columns)
            };
        },
        PackedColumns::Lines(line_columns) => {
            let coords = line_columns.coords();
            let XyColumns { xs, ys } = line_columns.physical_xy();
            for row in 0..line_columns.rows() {
                if MASKED && missing.is_some_and(|mask| mask.is_missing(row)) {
                    values.extend_from_slice(&[f64::NAN; 4]);
                    continue;
                }
                let window = line_columns.row_window(row);
                let bounds = if window.is_empty() {
                    [f64::NAN; 4]
                } else {
                    let shape = Shape::LineString(
                        LineSeq::try_new(
                            coords.view(CoordWindow::from_range(window.clone(), coords.len())),
                        )
                        .expect("line coordinate sequence must contain at least two vertices"),
                    );
                    if shape.crosses_antimeridian() {
                        crate::geometry::geographic_crossing_bounds(&shape)
                            .map_or([f64::NAN; 4], Bounds::into_array)
                    } else {
                        row_bounds_values(xs, ys, window)
                    }
                };
                values.extend_from_slice(&bounds);
            }
        },
        PackedColumns::Polygons(polygon_columns) => {
            let coords = polygon_columns.coords();
            let XyColumns { xs, ys } = polygon_columns.physical_xy();
            let ring_offsets = polygon_columns.ring_offsets();
            for row in 0..polygon_columns.rows() {
                if MASKED && missing.is_some_and(|mask| mask.is_missing(row)) {
                    values.extend_from_slice(&[f64::NAN; 4]);
                    continue;
                }
                let rings = polygon_columns.polygon_rings(row);
                let bounds = if rings.is_empty() {
                    [f64::NAN; 4]
                } else {
                    let shell =
                        ring_offsets[rings.start] as usize..ring_offsets[rings.start + 1] as usize;
                    let shape = Shape::Polygon(GeometryArrayStorage::polygon_view(
                        coords,
                        ring_offsets,
                        rings,
                    ));
                    if shape.crosses_antimeridian() {
                        crate::geometry::geographic_crossing_bounds(&shape)
                            .map_or([f64::NAN; 4], Bounds::into_array)
                    } else {
                        row_bounds_values(xs, ys, shell)
                    }
                };
                values.extend_from_slice(&bounds);
            }
        },
    }
    Ok(values)
}

pub(crate) fn total_bounds_from_columns(columns: &PackedColumns<'_>) -> Option<Bounds> {
    if let PackedColumns::Polygons(polygons) = columns {
        // Polygon envelopes are defined by their shells. Invalid external
        // holes remain representable for validation/repair, but must not make
        // a packed array disagree with scalar Polygon::bounds().
        let XyColumns { xs, ys } = polygons.physical_xy();
        let ring_offsets = polygons.ring_offsets();
        let mut total: Option<Bounds> = None;
        for row in 0..polygons.rows() {
            if polygons.is_missing(row) {
                continue;
            }
            let rings = polygons.polygon_rings(row);
            let Some(shell_index) = (!rings.is_empty()).then_some(rings.start) else {
                continue;
            };
            let shell = ring_offsets[shell_index] as usize..ring_offsets[shell_index + 1] as usize;
            let (minx, maxx) = crate::geometry::column_minmax(&xs[shell.clone()])?;
            let (miny, maxy) = crate::geometry::column_minmax(&ys[shell])?;
            let bounds = Bounds::new_raw(minx, miny, maxx, maxy);
            match &mut total {
                Some(current) => current.include_bounds(bounds),
                None => total = Some(bounds),
            }
        }
        return total;
    }
    let XyColumns { xs, ys } = columns.physical_xy();
    let mut total: Option<Bounds> = None;
    for window in columns
        .map_rows(|row| row.vertex_window())
        .into_iter()
        .flatten()
    {
        let Some((minx, maxx)) = crate::geometry::column_minmax(&xs[window.clone()]) else {
            continue;
        };
        let Some((miny, maxy)) = crate::geometry::column_minmax(&ys[window]) else {
            continue;
        };
        let bounds = Bounds::new_raw(minx, miny, maxx, maxy);
        match &mut total {
            Some(current) => current.include_bounds(bounds),
            None => total = Some(bounds),
        }
    }
    total
}

pub(crate) fn subdivide_line_columns(
    columns: &LineColumns<'_>,
    operation: &'static str,
    parameter: &'static str,
    subdivide: impl Fn(&CoordSeq, &mut crate::geometry::ExpansionBudget) -> Result<CoordSeq>,
) -> Result<PackedColumnOutput> {
    let coords = columns.coords();
    let rows = columns.rows();
    let mut builder = PackedColumnBuilder::try_like(coords, coords.len())?;
    let mut budget = crate::geometry::ExpansionBudget::new(operation, parameter);
    for row in 0..rows {
        if columns.is_missing(row) {
            builder.close_row()?;
            continue;
        }
        let window = columns.row_window(row);
        builder.push_subdivided(coords, window, &subdivide, &mut budget)?;
    }
    let cap = builder.vertex_len();
    let (out_coords, out_offsets) = builder.finish(cap)?;
    Ok(PackedColumnOutput::Lines {
        coords: out_coords,
        offsets: out_offsets,
    })
}

pub(crate) fn subdivide_polygon_columns(
    columns: &PolygonColumns<'_>,
    operation: &'static str,
    parameter: &'static str,
    subdivide: impl Fn(&CoordSeq, &mut crate::geometry::ExpansionBudget) -> Result<CoordSeq>,
) -> Result<PackedColumnOutput> {
    let coords = columns.coords();
    let ring_offsets = columns.ring_offsets();
    let mut builder = PackedColumnBuilder::try_like(coords, coords.len())?;
    let mut budget = crate::geometry::ExpansionBudget::new(operation, parameter);
    let mut polygon_builder = CsrOffsetBuilder::with_capacity(columns.rows())?;
    let mut output_rings = 0;
    for row in 0..columns.rows() {
        if columns.is_missing(row) {
            polygon_builder.push_end(output_rings, i32::MAX as usize)?;
            continue;
        }
        for ring in columns.polygon_rings(row) {
            let ring_window = ring_offsets[ring] as usize..ring_offsets[ring + 1] as usize;
            builder.push_subdivided(coords, ring_window, &subdivide, &mut budget)?;
            output_rings += 1;
        }
        polygon_builder.push_end(output_rings, i32::MAX as usize)?;
    }
    let cap = builder.vertex_len();
    let (out_coords, out_ring_offsets) = builder.finish(cap)?;
    Ok(PackedColumnOutput::Polygons {
        coords: out_coords,
        ring_offsets: out_ring_offsets.cast_level::<RingLevel>(),
        polygon_offsets: polygon_builder.finish(output_rings)?.cast_level(),
    })
}

pub(crate) fn packed_per_row_self_origin_affine_columns(
    columns: PackedColumns<'_>,
    spec: OriginSpec,
    matrix: [f64; 6],
) -> PackedColumnResult<PackedColumnOutput> {
    let [a, b, d, e, _, _] = matrix;
    match columns {
        PackedColumns::Points(point_columns) => Ok(PackedColumnOutput::Points(
            point_columns.contiguous_coords().unwrap_or_else(|| {
                let mut builder = CoordSeqBuilder::with_capacity(
                    point_columns.coords().axes(),
                    point_columns.rows().present_count(),
                );
                for (_, physical) in point_columns.rows().cursor() {
                    if let Some(physical) = physical {
                        builder.push_at(point_columns.coords(), physical);
                    }
                }
                builder.finish_infallible()
            }),
        )),
        PackedColumns::Lines(line_columns) => {
            let coords = line_columns.coords();
            let XyColumns { xs, ys } = line_columns.physical_xy();
            let rows = line_columns.rows();
            let mut builder = PackedColumnBuilder::try_like(coords, xs.len()).map_err(batch_err)?;
            for row in 0..rows {
                if line_columns.is_missing(row) {
                    builder.close_row().map_err(batch_err)?;
                    continue;
                }
                let window = line_columns.row_window(row);
                if window.len() < 2 {
                    builder.close_row().map_err(batch_err)?;
                    continue;
                }
                let pivot = packed_line_row_pivot(spec, xs, ys, window.clone())
                    .map_err(|error| row_err(row, error))?;
                let row_matrix = affine_about(a, b, d, e, pivot);
                builder
                    .push_affine(coords, window, &row_matrix)
                    .map_err(|error| row_err(row, error))?;
            }
            let cap = builder.vertex_len();
            let (out_coords, out_offsets) = builder.finish(cap).map_err(batch_err)?;
            Ok(PackedColumnOutput::Lines {
                coords: out_coords,
                offsets: out_offsets,
            })
        },
        PackedColumns::Polygons(polygon_columns) => {
            let coords = polygon_columns.coords();
            let ring_offsets = polygon_columns.ring_offsets();
            let XyColumns { xs, ys } = polygon_columns.physical_xy();
            let rows = polygon_columns.rows();
            let mut builder = PackedColumnBuilder::try_like(coords, xs.len()).map_err(batch_err)?;
            let mut output_polygons = CsrOffsetBuilder::with_capacity(rows).map_err(batch_err)?;
            let mut output_rings = 0;
            for row in 0..rows {
                if polygon_columns.is_missing(row) {
                    output_polygons
                        .push_end(output_rings, i32::MAX as usize)
                        .map_err(batch_err)?;
                    continue;
                }
                let rings = polygon_columns.polygon_rings(row);
                let pivot = packed_polygon_row_pivot(spec, xs, ys, ring_offsets, rings.clone())
                    .map_err(|error| row_err(row, error))?;
                let row_matrix = affine_about(a, b, d, e, pivot);
                for ring in rings {
                    let window = ring_offsets[ring] as usize..ring_offsets[ring + 1] as usize;
                    builder
                        .push_affine(coords, window, &row_matrix)
                        .map_err(|error| row_err(row, error))?;
                    output_rings += 1;
                }
                output_polygons
                    .push_end(output_rings, i32::MAX as usize)
                    .map_err(batch_err)?;
            }
            let vertex_len = builder.vertex_len();
            let (out_coords, out_ring_offsets) = builder.finish(vertex_len).map_err(batch_err)?;
            Ok(PackedColumnOutput::Polygons {
                coords: out_coords,
                ring_offsets: out_ring_offsets.cast_level(),
                polygon_offsets: output_polygons
                    .finish(output_rings)
                    .map_err(batch_err)?
                    .cast_level(),
            })
        },
    }
}
