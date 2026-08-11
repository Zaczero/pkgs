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

use std::sync::Arc;

use crate::array::packed_columns::{
    LineColumns, PackedColumnResult, PackedColumns, PackedRow, PolygonColumns, SegmentedRuns,
    XyColumns, batch_err, row_err,
};
use crate::array::storage_helpers::{
    packed_line_measure, packed_line_row_pivot, packed_polygon_measure, packed_polygon_row_pivot,
};
use crate::array::{
    Bounds, CoordSeq, GeometryArrayStorage, OriginSpec, PackedColumnBuilder, PackedColumnOutput,
    Result, RingLevel, RowSelectionRef, Shape, affine_about, column_window, crs, line_logical_len,
    polygon_logical_len, polygon_rings_range, row_bounds_3d, row_bounds_values,
};
use crate::geometry::{CoordWindow, LineSeq};

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
) -> Vec<f64> {
    segmented_planar_lengths_dims::<false>(runs, xs, ys, &[], scale)
}

/// Dual-strategy 3-D length over the same topology.
pub(crate) fn segmented_planar_lengths_3d(
    runs: SegmentedRuns<'_>,
    xs: &[f64],
    ys: &[f64],
    zs: &[f64],
    scale: f64,
) -> Vec<f64> {
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
) -> Vec<f64> {
    let n_groups = runs.n_groups();
    if n_groups == 0 {
        return Vec::new();
    }
    debug_assert!(!HAS_Z || zs.len() == xs.len());
    debug_assert_eq!(xs.len(), ys.len());

    if runs.group_offsets.is_none() {
        // Identity lines: prefer ColumnStream for large short spans.
        if n_groups >= COLUMN_STREAM_MIN_GROUPS {
            // Cheap sample: if the first/mid/last groups are short, stream the
            // whole array (roads is homogeneous). Mixed arrays fall to partition.
            let sample_short = group_is_short(&runs, 0)
                && group_is_short(&runs, n_groups / 2)
                && group_is_short(&runs, n_groups - 1);
            if sample_short {
                let mut out = vec![0.0; n_groups];
                column_stream_identity_lengths::<HAS_Z>(
                    &runs,
                    0..n_groups,
                    xs,
                    ys,
                    zs,
                    scale,
                    &mut out,
                );
                return out;
            }
            return partitioned_identity_lengths::<HAS_Z>(&runs, xs, ys, zs, scale);
        }
        return tight_identity_per_run::<HAS_Z>(&runs, xs, ys, zs, scale);
    }

    // Multi-leaf groups (polygons): reinterpret ring offsets as identity leaf
    // runs, reduce rings unscaled through the short/long selector, then fold
    // ring results by polygon offsets (missing → NaN; scale once per polygon).
    let leaf_missing = runs.group_missing.map(|missing| {
        let mut mask = vec![false; runs.n_leaves()];
        for (group, &is_missing) in missing.iter().enumerate() {
            if is_missing {
                mask[runs.group_leaves(group)].fill(true);
            }
        }
        mask
    });
    let leaf_runs = SegmentedRuns {
        leaf_offsets: runs.leaf_offsets,
        group_offsets: None,
        group_missing: leaf_missing.as_deref(),
    };
    let leaf_lengths = segmented_planar_lengths_dims::<HAS_Z>(leaf_runs, xs, ys, zs, 1.0);
    let scale_is_one = scale.to_bits() == 1.0_f64.to_bits();
    (0..n_groups)
        .map(|group| {
            if runs.is_missing(group) {
                return f64::NAN;
            }
            let total: f64 = leaf_lengths[runs.group_leaves(group)].iter().sum();
            if scale_is_one { total } else { total * scale }
        })
        .collect()
}

fn partitioned_identity_lengths<const HAS_Z: bool>(
    runs: &SegmentedRuns<'_>,
    xs: &[f64],
    ys: &[f64],
    zs: &[f64],
    scale: f64,
) -> Vec<f64> {
    let n_groups = runs.n_groups();
    let mut out = vec![0.0; n_groups];
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
    out
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
) -> Vec<f64> {
    let n = runs.n_groups();
    let offsets = runs.leaf_offsets;
    let missing = runs.group_missing;
    let scale_is_one = scale.to_bits() == 1.0_f64.to_bits();
    let mut out = vec![0.0; n];
    for (g, slot) in out.iter_mut().enumerate() {
        if missing.is_some_and(|mask| mask[g]) {
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
    out
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
        if missing.is_some_and(|mask| mask[g]) {
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
) -> Vec<f64>
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
    missing: Option<&[bool]>,
    measure: F,
) -> Vec<f64>
where
    F: Fn(&[f64], &[f64], std::ops::Range<usize>) -> f64,
{
    let Some(missing) = missing else {
        return line_measure(xs, ys, offsets, map, measure);
    };
    let map = map.into();
    (0..line_logical_len(offsets, map))
        .map(|logical| {
            if missing[logical] {
                return f64::NAN;
            }
            let window = map.csr_window(offsets, logical);
            measure(
                column_window(xs, &window),
                column_window(ys, &window),
                window,
            )
        })
        .collect()
}

pub(crate) fn polygon_measure<'a, F, C>(
    xs: &[f64],
    ys: &[f64],
    ring_offsets: &[i32],
    polygon_offsets: &[i32],
    map: impl Into<RowSelectionRef<'a>>,
    ring_measure: F,
    combine: C,
) -> Vec<f64>
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
    missing: Option<&[bool]>,
    ring_measure: F,
    combine: C,
) -> Vec<f64>
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
    (0..polygon_logical_len(polygon_offsets, map))
        .map(|logical| {
            if missing[logical] {
                return f64::NAN;
            }
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
        })
        .collect()
}

/// Validate geographic ordinates for present packed rows only. Nullable
/// storage deliberately uses NaN placeholder coordinates; the mask, rather
/// than the placeholder payload, is the trust boundary.
pub(crate) fn ensure_geographic_columns_present(
    columns: &PackedColumns<'_>,
    missing: Option<&[bool]>,
) -> crate::error::Result<()> {
    let XyColumns { xs, ys } = columns.xy();
    for (row, window) in columns
        .map_rows(|row| row.vertex_window())
        .into_iter()
        .enumerate()
    {
        if missing.is_some_and(|mask| mask[row]) {
            continue;
        }
        if let Some(window) = window {
            crate::crs::ensure_geographic_columns(
                column_window(xs, &window),
                column_window(ys, &window),
            )?;
        }
    }
    Ok(())
}

pub(crate) fn map_coordseq_to_crs(
    transformer: &crs::Transformer,
    seq: &CoordSeq,
) -> crate::error::Result<CoordSeq> {
    let mut xs = Arc::<[f64]>::from(seq.xs());
    let mut ys = Arc::<[f64]>::from(seq.ys());
    let mut zs = seq.zs().map(Arc::<[f64]>::from);
    let zt = zs.as_mut().map_or(crate::Zt::None, |zs| {
        crate::Zt::Z(Arc::get_mut(zs).expect("unique Arc"))
    });
    transformer.transform_coordinates(
        Arc::get_mut(&mut xs).expect("unique Arc"),
        Arc::get_mut(&mut ys).expect("unique Arc"),
        zt,
    )?;
    Ok(CoordSeq::from_columns(xs, ys, zs, seq.carried_ms()))
}

/// Per-row 3D bounds from packed homogeneous storage — the kernel behind
/// `GeometryArray.bounds_3d` (all rings for polygons, matching `Shape::bounds_3d`).
pub(crate) fn bounds_3d_values_from_columns(
    columns: &PackedColumns<'_>,
) -> Vec<Option<crate::geometry::Bounds3D>> {
    let XyColumns { xs, ys } = columns.xy();
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

pub(crate) fn bounds_values_from_columns(columns: &PackedColumns<'_>) -> Vec<f64> {
    let XyColumns { xs, ys } = columns.xy();
    let mut values = Vec::with_capacity(columns.row_count() * 4);
    columns.map_rows(|row| {
        let bounds = match row {
            PackedRow::Points { row } => [xs[row], ys[row], xs[row], ys[row]],
            PackedRow::Lines { .. } => match row.vertex_window() {
                Some(window) if !window.is_empty() => row_bounds_values(xs, ys, window),
                _ => [f64::NAN; 4],
            },
            PackedRow::Polygons { .. } => row
                .polygon_shell_window()
                .map_or([f64::NAN; 4], |shell| row_bounds_values(xs, ys, shell)),
        };
        values.extend_from_slice(&bounds);
    });
    values
}

/// Packed bounds with nullable rows excluded before the per-row extrema walk.
pub(crate) fn bounds_values_from_columns_masked(
    columns: &PackedColumns<'_>,
    missing: Option<&[bool]>,
) -> Vec<f64> {
    let Some(missing) = missing else {
        return bounds_values_from_columns(columns);
    };
    let XyColumns { xs, ys } = columns.xy();
    let mut values = Vec::with_capacity(columns.row_count() * 4);
    let mut logical = 0;
    columns.map_rows(|row| {
        let row_index = logical;
        logical += 1;
        if missing[row_index] {
            values.extend_from_slice(&[f64::NAN; 4]);
            return;
        }
        let bounds = match row {
            PackedRow::Points { row } => [xs[row], ys[row], xs[row], ys[row]],
            PackedRow::Lines { .. } => match row.vertex_window() {
                Some(window) if !window.is_empty() => row_bounds_values(xs, ys, window),
                _ => [f64::NAN; 4],
            },
            PackedRow::Polygons { .. } => row
                .polygon_shell_window()
                .map_or([f64::NAN; 4], |shell| row_bounds_values(xs, ys, shell)),
        };
        values.extend_from_slice(&bounds);
    });
    values
}

pub(crate) fn geographic_bounds_values_from_columns(columns: &PackedColumns<'_>) -> Vec<f64> {
    geographic_bounds_values_from_columns_impl::<false>(columns, &[])
}

pub(crate) fn geographic_bounds_values_from_columns_masked(
    columns: &PackedColumns<'_>,
    missing: Option<&[bool]>,
) -> Vec<f64> {
    missing.map_or_else(
        || geographic_bounds_values_from_columns_impl::<false>(columns, &[]),
        |missing| geographic_bounds_values_from_columns_impl::<true>(columns, missing),
    )
}

fn geographic_bounds_values_from_columns_impl<const MASKED: bool>(
    columns: &PackedColumns<'_>,
    missing: &[bool],
) -> Vec<f64> {
    let mut values = Vec::with_capacity(columns.row_count() * 4);
    match columns {
        PackedColumns::Points(_) => {
            return if MASKED {
                bounds_values_from_columns_masked(columns, Some(missing))
            } else {
                bounds_values_from_columns(columns)
            };
        },
        PackedColumns::Lines(line_columns) => {
            let coords = line_columns.coords();
            let XyColumns { xs, ys } = line_columns.xy();
            let offsets = line_columns.offsets();
            for row in 0..line_columns.rows() {
                if MASKED && missing[row] {
                    values.extend_from_slice(&[f64::NAN; 4]);
                    continue;
                }
                let window = offsets[row] as usize..offsets[row + 1] as usize;
                let bounds = if window.is_empty() {
                    [f64::NAN; 4]
                } else {
                    let shape = Shape::LineString(LineSeq::from_trusted(
                        coords.view(CoordWindow::trusted(window.clone(), coords.len())),
                    ));
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
            let XyColumns { xs, ys } = polygon_columns.xy();
            let ring_offsets = polygon_columns.ring_offsets();
            let polygon_offsets = polygon_columns.polygon_offsets();
            for row in 0..polygon_columns.rows() {
                if MASKED && missing[row] {
                    values.extend_from_slice(&[f64::NAN; 4]);
                    continue;
                }
                let rings = polygon_offsets[row] as usize..polygon_offsets[row + 1] as usize;
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
    values
}

pub(crate) fn total_bounds_from_columns(columns: &PackedColumns<'_>) -> Option<Bounds> {
    if let PackedColumns::Polygons(polygons) = columns {
        // Polygon envelopes are defined by their shells. Invalid external
        // holes remain representable for validation/repair, but must not make
        // a packed array disagree with scalar Polygon::bounds().
        let XyColumns { xs, ys } = polygons.xy();
        let ring_offsets = polygons.ring_offsets();
        let polygon_offsets = polygons.polygon_offsets();
        let mut total: Option<Bounds> = None;
        for row in 0..polygons.rows() {
            let rings = polygon_offsets[row] as usize..polygon_offsets[row + 1] as usize;
            let Some(shell_index) = (!rings.is_empty()).then_some(rings.start) else {
                continue;
            };
            let shell = ring_offsets[shell_index] as usize..ring_offsets[shell_index + 1] as usize;
            let (minx, maxx) = crate::geometry::column_minmax(&xs[shell.clone()])?;
            let (miny, maxy) = crate::geometry::column_minmax(&ys[shell])?;
            let bounds = Bounds::new_unchecked(minx, miny, maxx, maxy);
            match &mut total {
                Some(current) => current.include_bounds(bounds),
                None => total = Some(bounds),
            }
        }
        return total;
    }
    let XyColumns { xs, ys } = columns.xy();
    let (minx, maxx) = crate::geometry::column_minmax(xs)?;
    let (miny, maxy) = crate::geometry::column_minmax(ys)?;
    Some(Bounds::new_unchecked(minx, miny, maxx, maxy))
}

pub(crate) fn subdivide_line_columns(
    columns: &LineColumns<'_>,
    operation: &'static str,
    parameter: &'static str,
    subdivide: impl Fn(&CoordSeq, &mut crate::geometry::ExpansionBudget) -> Result<CoordSeq>,
) -> Result<PackedColumnOutput> {
    let coords = columns.coords();
    let offsets = columns.offsets();
    let rows = columns.rows();
    let mut builder = PackedColumnBuilder::like(coords, coords.len());
    let mut budget = crate::geometry::ExpansionBudget::new(operation, parameter);
    for row in 0..rows {
        let window = offsets[row] as usize..offsets[row + 1] as usize;
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
    let mut builder = PackedColumnBuilder::like(coords, coords.len());
    let mut budget = crate::geometry::ExpansionBudget::new(operation, parameter);
    for &[start, end] in ring_offsets.array_windows::<2>() {
        let ring_window = start as usize..end as usize;
        builder.push_subdivided(coords, ring_window, &subdivide, &mut budget)?;
    }
    let cap = builder.vertex_len();
    let (out_coords, out_ring_offsets) = builder.finish(cap)?;
    Ok(PackedColumnOutput::Polygons {
        coords: out_coords,
        ring_offsets: out_ring_offsets.cast_level::<RingLevel>(),
        polygon_offsets: columns.polygon_offsets_column().clone(),
    })
}

pub(crate) fn packed_per_row_self_origin_affine_columns(
    columns: PackedColumns<'_>,
    spec: OriginSpec,
    matrix: [f64; 6],
) -> PackedColumnResult<PackedColumnOutput> {
    let [a, b, d, e, _, _] = matrix;
    match columns {
        PackedColumns::Points(point_columns) => {
            Ok(PackedColumnOutput::Points(point_columns.coords().clone()))
        },
        PackedColumns::Lines(line_columns) => {
            let coords = line_columns.coords();
            let offsets = line_columns.offsets();
            let XyColumns { xs, ys } = line_columns.xy();
            let rows = line_columns.rows();
            let mut builder = PackedColumnBuilder::like(coords, xs.len());
            for row in 0..rows {
                let window = offsets[row] as usize..offsets[row + 1] as usize;
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
            let polygon_offsets = polygon_columns.polygon_offsets();
            let XyColumns { xs, ys } = polygon_columns.xy();
            let rows = polygon_columns.rows();
            let mut builder = PackedColumnBuilder::like(coords, xs.len());
            for row in 0..rows {
                let rings = polygon_offsets[row] as usize..polygon_offsets[row + 1] as usize;
                let pivot = packed_polygon_row_pivot(spec, xs, ys, ring_offsets, rings.clone())
                    .map_err(|error| row_err(row, error))?;
                let row_matrix = affine_about(a, b, d, e, pivot);
                for ring in rings {
                    let window = ring_offsets[ring] as usize..ring_offsets[ring + 1] as usize;
                    builder
                        .extend_affine(coords, window, &row_matrix)
                        .map_err(|error| row_err(row, error))?;
                }
            }
            let out_coords = builder.finish_coords_only().map_err(batch_err)?;
            Ok(PackedColumnOutput::Polygons {
                coords: out_coords,
                ring_offsets: polygon_columns.ring_offsets_column().clone(),
                polygon_offsets: polygon_columns.polygon_offsets_column().clone(),
            })
        },
    }
}
