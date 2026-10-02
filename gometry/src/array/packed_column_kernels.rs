#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
//! Kernels that consume resolved packed columns: CRS transform, per-row
//! bounds, segmentize/densify, per-row affine, and packed length reduction.
//!
//! `PackedRow` owns selected CSR and coordinate windows; `SegmentedRuns`
//! retains its separate ordered length-reduction contract.
use std::num::NonZeroU32;

use crate::array::packed_columns::{
    LineColumns, PackedColumnResult, PackedColumns, PointColumns, PointRows, PolygonColumns,
    SegmentedRuns, XyColumns, batch_err, row_err,
};
use crate::array::{
    Bounds, CoordSeq, CsrOffsetBuilder, OriginSpec, PackedColumnBuilder, PackedColumnOutput,
    PointRowLayout, Result, RingLevel, Shape, crs,
};
use crate::geometry::storage::columns::{packed_line_row_pivot, packed_polygon_row_pivot};
use crate::geometry::storage::{
    PackedRow, RowSelectionRef, ValidityMask, column_window, packed_line_row,
    packed_polygon_bounds, packed_polygon_vertex_window, row_bounds, row_bounds_values,
};
use crate::geometry::{
    CoordSeqBuilder, CoordWindow, LineSeq, MOrdinate, Point, ZOrdinate, storage,
};

/// Expand coordinate validation into the packed row loop. Its `?` returns the
/// first invalid coordinate through the detached batch operation.
macro_rules! validate_geographic_columns {
    ($frame:expr, $longitude:expr, $xs:expr, $ys:expr $(,)?) => {{
        let __frame = $frame;
        let __longitude = $longitude;
        let __xs = $xs;
        let __ys = $ys;
        for (&__lon, &__lat) in std::iter::zip(__xs, __ys) {
            crate::crs::geographic_lonlat_result!(__frame, __longitude, __lon, __lat,)?;
        }
        Ok::<(), crate::error::Error>(())
    }};
}

/// Expand an admitted ring lookup inside the perimeter reduction. The caller
/// iterates the row's admitted ring range, so the hot loop needs no per-ring
/// range check or `Option` callback.
macro_rules! measure_admitted_polygon_ring {
    ($ring_offsets:expr, $ring:expr, $xs:expr, $ys:expr, $measure:expr $(,)?) => {{
        let __window = $crate::geometry::storage::admitted_csr_window!($ring_offsets, $ring);
        ($measure)(
            column_window($xs, &__window),
            column_window($ys, &__window),
            __window,
        )
    }};
}

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
    let vertices_per_row = steps
        .checked_add(1)
        .expect("buffer expansion steps are bounded before addition");
    let point_rows = points.rows();
    let present_rows = point_rows.present_count();
    let total = present_rows.checked_mul(vertices_per_row).ok_or_else(|| {
        batch_err(crate::geometry::GeometryErrorKind::OffsetCapacityExceeded.into())
    })?;
    storage::admit_packed_coord_count(total).map_err(batch_err)?;

    let mut unit_x = Vec::with_capacity(steps);
    let mut unit_y = Vec::with_capacity(steps);
    let (sin_step, cos_step) = (std::f64::consts::TAU / steps as f64).sin_cos();
    let (mut sin_theta, mut cos_theta) = (0.0_f64, 1.0_f64);
    for step in 0..steps {
        if step > 0 {
            (cos_theta, sin_theta) = (
                cos_theta * cos_step - sin_theta * sin_step,
                sin_theta * cos_step + cos_theta * sin_step,
            );
        }
        unit_x.push(cos_theta);
        unit_y.push(sin_theta);
    }

    let first_unit_x = *unit_x
        .first()
        .expect("nonzero expansion has a first vertex");
    let first_unit_y = *unit_y
        .first()
        .expect("nonzero expansion has a first vertex");
    let mut xs = Vec::with_capacity(total);
    let mut ys = Vec::with_capacity(total);
    let mut ring_offsets = CsrOffsetBuilder::with_capacity(present_rows);
    let mut polygon_offsets = CsrOffsetBuilder::with_capacity(rows);
    for (_row, physical) in point_rows.cursor() {
        if let Some(physical) = physical {
            let (center_x, center_y) = points.physical_xy_at(physical);
            let first = (
                center_x + distance * first_unit_x,
                center_y + distance * first_unit_y,
            );
            if !first.0.is_finite() || !first.1.is_finite() {
                return Ok(None);
            }
            xs.push(first.0);
            ys.push(first.1);
            for (&unit_x, &unit_y) in unit_x.iter().skip(1).zip(unit_y.iter().skip(1)) {
                let x = center_x + distance * unit_x;
                let y = center_y + distance * unit_y;
                if !x.is_finite() || !y.is_finite() {
                    return Ok(None);
                }
                xs.push(x);
                ys.push(y);
            }
            xs.push(first.0);
            ys.push(first.1);
            ring_offsets
                .push_end(xs.len(), i32::MAX as usize)
                .map_err(batch_err)?;
        }
        polygon_offsets
            .push_end(ring_offsets.row_count(), i32::MAX as usize)
            .map_err(batch_err)?;
    }
    let coords = CoordSeq::try_from_vecs(xs, ys, None, None).map_err(batch_err)?;
    let ring_count = ring_offsets.row_count();
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

    if !runs.row_map.is_identity() {
        let mut output = Vec::with_capacity(n_groups);
        for group in 0..n_groups {
            output.push(per_run_group_length::<HAS_Z>(
                &runs, group, xs, ys, zs, scale,
            ));
        }
        return output;
    }

    if runs.group_offsets.is_none() {
        // Identity lines: prefer ColumnStream for large short spans.
        if n_groups >= COLUMN_STREAM_MIN_GROUPS {
            // Cheap sample: if the first/mid/last groups are short, stream the
            // whole array (roads is homogeneous). Mixed arrays fall to partition.
            let sample_short = group_is_short(&runs, 0)
                && group_is_short(&runs, n_groups / 2)
                && group_is_short(&runs, n_groups.wrapping_sub(1));
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
    let leaf_missing = runs.group_missing.and_then(|missing| {
        let mut mask = vec![false; runs.n_leaves()];
        for (group, is_missing) in missing.iter().enumerate() {
            if is_missing {
                mask.get_mut(runs.group_leaves(group))
                    .expect("admitted polygon group leaves fit the ring mask")
                    .fill(true);
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
    let leaf_lengths = segmented_planar_lengths_dims::<HAS_Z>(leaf_runs, xs, ys, zs, 1.0);
    let scale_is_one = scale.to_bits() == 1.0_f64.to_bits();
    let mut output = Vec::with_capacity(n_groups);
    for group in 0..n_groups {
        let value = if runs.is_missing(group) {
            f64::NAN
        } else {
            let total: f64 = column_window(&leaf_lengths, &runs.group_leaves(group))
                .iter()
                .sum();
            if scale_is_one { total } else { total * scale }
        };
        output.push(value);
    }
    output
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
        group = group.wrapping_add(1);
        while group < n_groups && group_is_short(runs, group) == short {
            group = group.wrapping_add(1);
        }
        if short && group.wrapping_sub(span_start) >= COLUMN_STREAM_MIN_GROUPS {
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
        if missing.is_some_and(|mask| mask.is_missing(g)) {
            *slot = f64::NAN;
            continue;
        }
        let window = RowSelectionRef::Identity.csr_window(offsets, g);
        if window.len() < 2 {
            continue;
        }
        let total = if HAS_Z {
            crate::geometry::line_length_3d_columns(
                column_window(xs, &window),
                column_window(ys, &window),
                column_window(zs, &window),
            )
        } else {
            crate::geometry::line_length_columns(
                column_window(xs, &window),
                column_window(ys, &window),
            )
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
        total += if HAS_Z {
            crate::geometry::line_length_3d_columns(
                column_window(xs, &window),
                column_window(ys, &window),
                column_window(zs, &window),
            )
        } else {
            crate::geometry::line_length_columns(
                column_window(xs, &window),
                column_window(ys, &window),
            )
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

#[expect(
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    reason = "the measured ColumnStream keeps IEEE vector/scalar arithmetic and ordered accumulation over admitted row windows"
)]
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
        let window = RowSelectionRef::Identity.csr_window(offsets, g);
        let start = window.start;
        let end = window.end;
        // Valid pairs: [start, end - 1). Boundary pair at end-1 → next leaf is
        // never entered here — mask-by-construction.
        if end <= start.wrapping_add(1) {
            out[g] = 0.0;
            continue;
        }
        let pair_end = end.wrapping_sub(1); // exclusive absolute pair-start index
        let mut total = 0.0_f64;
        let mut pair = start;

        // Full SIMD chunks entirely inside this group's valid pair range.
        while pair.wrapping_add(STREAM_LANES) <= pair_end {
            let pair_window = pair..pair_end;
            let shifted_pair_window = pair.wrapping_add(1)..pair_end.wrapping_add(1);
            let x0 = StreamSimd::from_slice(column_window(xs, &pair_window));
            let x1 = StreamSimd::from_slice(column_window(xs, &shifted_pair_window));
            let y0 = StreamSimd::from_slice(column_window(ys, &pair_window));
            let y1 = StreamSimd::from_slice(column_window(ys, &shifted_pair_window));
            let dx = x1 - x0;
            let dy = y1 - y0;
            let (squared, zero_delta) = if HAS_Z {
                let z0 = StreamSimd::from_slice(column_window(zs, &pair_window));
                let z1 = StreamSimd::from_slice(column_window(zs, &shifted_pair_window));
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
                for p in pair..pair.wrapping_add(STREAM_LANES) {
                    total += stream_segment_length::<HAS_Z>(xs, ys, zs, p);
                }
            } else {
                total += squared.sqrt().reduce_sum();
            }
            pair = pair.wrapping_add(STREAM_LANES);
        }
        // Scalar residual — no overlap replay of the last full chunk.
        while pair < pair_end {
            total += stream_segment_length::<HAS_Z>(xs, ys, zs, pair);
            pair = pair.wrapping_add(1);
        }
        out[g] = if scale_is_one { total } else { total * scale };
    }
}

#[expect(
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    reason = "the scalar residual uses adjacent coordinates admitted by the ColumnStream row boundary and preserves its IEEE length arithmetic"
)]
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

pub(crate) fn geodesic_line_measure<F>(
    columns: &LineColumns<'_>,
    frame: crate::geographic::GeographicFrame,
    mut measure: F,
) -> PackedColumnResult<Vec<f64>>
where
    F: FnMut(&[f64], &[f64]) -> crate::error::Result<f64>,
{
    let coords = columns.coords();
    let xs = coords.xs();
    let ys = coords.ys();
    let rows = columns.rows();
    let row_map = columns.selection();
    let offsets = columns.offsets();
    let longitude = frame.longitude();
    let mut output = Vec::with_capacity(rows);
    for logical in 0..rows {
        if columns.is_missing(logical) {
            output.push(f64::NAN);
            continue;
        }
        let row = packed_line_row!(logical, coords, offsets, row_map);
        let PackedRow::Lines {
            vertices_start,
            vertices_end,
            ..
        } = row
        else {
            unreachable!("line columns produce packed line rows")
        };
        let window = vertices_start..vertices_end;
        let row_xs = column_window(xs, &window);
        let row_ys = column_window(ys, &window);
        validate_geographic_columns!(frame, longitude, row_xs, row_ys)?;
        output.push(measure(row_xs, row_ys).map_err(|error| row_err(logical, error))?);
    }
    Ok(output)
}

pub(crate) fn geodesic_polygon_measure<F>(
    columns: &PolygonColumns<'_>,
    frame: crate::geographic::GeographicFrame,
    ring_measure: F,
) -> PackedColumnResult<Vec<f64>>
where
    F: Fn(&[f64], &[f64], std::ops::Range<usize>) -> crate::error::Result<f64>,
{
    let coords = columns.coords();
    let xs = coords.xs();
    let ys = coords.ys();
    let rows = columns.rows();
    let longitude = frame.longitude();
    let mut output = Vec::with_capacity(rows);
    for logical in 0..rows {
        if columns.is_missing(logical) {
            output.push(f64::NAN);
            continue;
        }
        let row = columns.packed_row(logical);
        let PackedRow::Polygons {
            ring_offsets,
            rings_start,
            rings_end,
            ..
        } = row
        else {
            unreachable!("polygon columns produce packed polygon rows")
        };
        if rings_start == rings_end {
            output.push(0.0);
            continue;
        }
        let vertices =
            packed_polygon_vertex_window!(ring_offsets.as_slice(), rings_start..rings_end,);
        let row_xs = column_window(xs, &vertices);
        let row_ys = column_window(ys, &vertices);
        validate_geographic_columns!(frame, longitude, row_xs, row_ys)?;
        let shell = measure_admitted_polygon_ring!(
            ring_offsets.as_slice(),
            rings_start,
            xs,
            ys,
            ring_measure,
        )
        .map_err(|error| row_err(logical, error))?;
        let mut total = shell;
        for ring in rings_start.wrapping_add(1)..rings_end {
            total += measure_admitted_polygon_ring!(
                ring_offsets.as_slice(),
                ring,
                xs,
                ys,
                ring_measure,
            )
            .map_err(|error| row_err(logical, error))?;
        }
        output.push(total);
    }
    Ok(output)
}

/// Validate geographic ordinates for present packed rows only. Nullable
/// storage deliberately uses NaN placeholder coordinates; the mask, rather
/// than the placeholder payload, is the trust boundary.
#[cfg(test)]
pub(crate) fn ensure_geographic_columns_present(
    columns: &PackedColumns<'_>,
    frame: crate::geographic::GeographicFrame,
) -> crate::error::Result<()> {
    let XyColumns { xs, ys } = columns.physical_xy();
    let longitude = frame.longitude();
    columns.collect_rows(|row| {
        if let Some(window) = row.and_then(|row| row.vertex_window()) {
            validate_geographic_columns!(
                frame,
                longitude,
                column_window(xs, &window),
                column_window(ys, &window),
            )?;
        }
        Ok(())
    })
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
    layout: PointRowLayout,
) -> std::result::Result<CoordSeq, crate::crs::TransformFailure> {
    let rows = PointRows::new(coords.len(), selection, Some(missing), layout);
    let present_count = rows.present_count();
    let compact = rows.is_compact();
    storage::admit_packed_coord_count(if compact { present_count } else { rows.len() })
        .map_err(crate::crs::TransformFailure::from)?;
    let mut xs = Vec::with_capacity(present_count);
    let mut ys = Vec::with_capacity(present_count);
    let mut zs = coords.zs().map(|_| Vec::with_capacity(present_count));
    let mut ms = coords.ms().map(|_| Vec::with_capacity(present_count));
    for (_, physical) in rows.cursor() {
        let Some(physical) = physical else {
            continue;
        };
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
    if !xs.is_empty() {
        transformer.transform_coordinates_indexed(&mut xs, &mut ys, zt)?;
    }

    if compact {
        return Ok(CoordSeq::try_from_vecs(xs, ys, zs, ms)?);
    }

    let mut output = CoordSeqBuilder::with_capacity(coords.axes(), rows.len());
    let mut xs = xs.into_iter();
    let mut ys = ys.into_iter();
    let mut zs = zs.map(Vec::into_iter);
    let mut ms = ms.map(Vec::into_iter);
    for (_, physical) in rows.cursor() {
        if physical.is_some() {
            output.push_xyzm(
                xs.next().expect("present point has transformed X"),
                ys.next().expect("present point has transformed Y"),
                zs.as_mut()
                    .map(|values| values.next().expect("present point has transformed Z")),
                ms.as_mut()
                    .map(|values| values.next().expect("present point has transformed M")),
            );
        } else {
            output.push_xyzm(
                f64::NAN,
                f64::NAN,
                coords.zs().map(|_| f64::NAN),
                coords.ms().map(|_| f64::NAN),
            );
        }
    }
    Ok(output.finish()?)
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

/// Expand the shared polygon row-bounds body into masked and dense loops so
/// each loop can keep its mask decision outside per-row geometry resolution.
macro_rules! packed_polygon_bounds_values {
    ($columns:expr, $ring_offsets:expr, $xs:expr, $ys:expr, $row:expr $(,)?) => {{
        let __columns = $columns;
        let __ring_offsets = $ring_offsets;
        let __xs = $xs;
        let __ys = $ys;
        let __row = $row;
        let __rings = __columns.polygon_rings(__row);
        if __rings.is_empty() {
            [f64::NAN; 4]
        } else {
            let __vertices = packed_polygon_vertex_window!(__ring_offsets, __rings);
            row_bounds_values(__xs, __ys, __vertices)
        }
    }};
}

fn bounds_values_from_columns_impl<const MASKED: bool>(
    columns: &PackedColumns<'_>,
    missing: Option<&ValidityMask>,
) -> Vec<f64> {
    let XyColumns { xs, ys } = columns.physical_xy();
    let rows = columns.row_count();
    // Keep the row result in its natural fixed-width shape while reducing;
    // flattening moves the same allocation into NumPy's one-dimensional lane.
    let mut values = Vec::with_capacity(rows);
    match columns {
        PackedColumns::Points(point_columns) => {
            let rows = point_columns.rows();
            for (_, physical) in rows.cursor() {
                let Some(physical) = physical else {
                    values.push([f64::NAN; 4]);
                    continue;
                };
                let (x, y) = point_columns.physical_xy_at(physical);
                values.push([x, y, x, y]);
            }
        },
        PackedColumns::Lines(line_columns) => {
            for row in 0..line_columns.rows() {
                if MASKED && missing.is_some_and(|mask| mask.is_missing(row)) {
                    values.push([f64::NAN; 4]);
                    continue;
                }
                let window = line_columns.row_window(row);
                values.push(row_bounds_values(xs, ys, window));
            }
        },
        PackedColumns::Polygons(polygon_columns) => {
            let ring_offsets = polygon_columns.ring_offsets();
            let rows = polygon_columns.rows();
            let missing = if MASKED { missing } else { None };
            if let Some(mask) = missing {
                for (row, is_missing) in mask.iter().enumerate() {
                    if is_missing {
                        values.push([f64::NAN; 4]);
                        continue;
                    }
                    values.push(packed_polygon_bounds_values!(
                        polygon_columns,
                        ring_offsets,
                        xs,
                        ys,
                        row,
                    ));
                }
            } else {
                for row in 0..rows {
                    values.push(packed_polygon_bounds_values!(
                        polygon_columns,
                        ring_offsets,
                        xs,
                        ys,
                        row,
                    ));
                }
            }
        },
    }
    values.into_flattened()
}

pub(crate) fn bounds_values_from_columns(columns: &PackedColumns<'_>) -> Vec<f64> {
    bounds_values_from_columns_impl::<false>(columns, None)
}

/// Packed bounds with nullable rows excluded before the per-row extrema walk.
pub(crate) fn bounds_values_from_columns_masked(
    columns: &PackedColumns<'_>,
    missing: Option<&ValidityMask>,
) -> Vec<f64> {
    missing.map_or_else(
        || bounds_values_from_columns(columns),
        |missing| bounds_values_from_columns_impl::<true>(columns, Some(missing)),
    )
}

pub(crate) fn geographic_bounds_values_from_columns(
    columns: &PackedColumns<'_>,
    frame: crate::geographic::GeographicFrame,
) -> Result<Vec<f64>> {
    geographic_bounds_values_from_columns_impl::<false>(columns, frame, None)
}

pub(crate) fn geographic_bounds_values_from_columns_masked(
    columns: &PackedColumns<'_>,
    frame: crate::geographic::GeographicFrame,
    missing: Option<&ValidityMask>,
) -> Result<Vec<f64>> {
    missing.map_or_else(
        || geographic_bounds_values_from_columns_impl::<false>(columns, frame, None),
        |missing| geographic_bounds_values_from_columns_impl::<true>(columns, frame, Some(missing)),
    )
}

fn geographic_bounds_values_from_columns_impl<const MASKED: bool>(
    columns: &PackedColumns<'_>,
    frame: crate::geographic::GeographicFrame,
    missing: Option<&ValidityMask>,
) -> Result<Vec<f64>> {
    if let PackedColumns::Points(_) = columns {
        return Ok(if MASKED {
            bounds_values_from_columns_masked(columns, missing)
        } else {
            bounds_values_from_columns(columns)
        });
    }
    let mut values = Vec::with_capacity(columns.row_count());
    match columns {
        PackedColumns::Points(_) => unreachable!("point bounds use coordinate columns directly"),
        PackedColumns::Lines(line_columns) => {
            let coords = line_columns.coords();
            let XyColumns { xs, ys } = line_columns.physical_xy();
            for row in 0..line_columns.rows() {
                if MASKED && missing.is_some_and(|mask| mask.is_missing(row)) {
                    values.push([f64::NAN; 4]);
                    continue;
                }
                let window = line_columns.row_window(row);
                let bounds = if window.is_empty() {
                    [f64::NAN; 4]
                } else {
                    let shape = Shape::LineString(LineSeq::row_view(
                        coords,
                        CoordWindow::from_range(window.clone(), coords.len()),
                    ));
                    if crate::geometry::crosses_antimeridian_in_frame(&shape, frame) {
                        crate::geometry::geographic_crossing_bounds_in_frame(&shape, frame)
                            .map_or([f64::NAN; 4], Bounds::into_array)
                    } else {
                        row_bounds_values(xs, ys, window)
                    }
                };
                values.push(bounds);
            }
        },
        PackedColumns::Polygons(polygon_columns) => {
            let XyColumns { xs, ys } = polygon_columns.physical_xy();
            for row in 0..polygon_columns.rows() {
                if MASKED && missing.is_some_and(|mask| mask.is_missing(row)) {
                    values.push([f64::NAN; 4]);
                    continue;
                }
                let packed_row = polygon_columns.packed_row(row);
                let bounds = if let Some(vertices) = packed_row.vertex_window() {
                    let shape = packed_row.to_shape()?;
                    if crate::geometry::crosses_antimeridian_in_frame(&shape, frame) {
                        crate::geometry::geographic_crossing_bounds_in_frame(&shape, frame)
                            .map_or([f64::NAN; 4], Bounds::into_array)
                    } else {
                        row_bounds_values(xs, ys, vertices)
                    }
                } else {
                    [f64::NAN; 4]
                };
                values.push(bounds);
            }
        },
    }
    Ok(values.into_flattened())
}

pub(crate) fn total_bounds_from_columns(columns: &PackedColumns<'_>) -> Option<Bounds> {
    if let PackedColumns::Polygons(polygons) = columns {
        let coords = polygons.coords();
        let ring_offsets = polygons.ring_offsets();
        let mut total: Option<Bounds> = None;
        for row in 0..polygons.rows() {
            if polygons.is_missing(row) {
                continue;
            }
            let rings = polygons.polygon_rings(row);
            let Some(bounds) =
                packed_polygon_bounds!(coords, ring_offsets, rings.start, rings.end,)
            else {
                continue;
            };
            match &mut total {
                Some(current) => current.include_bounds(bounds),
                None => total = Some(bounds),
            }
        }
        return total;
    }
    let XyColumns { xs, ys } = columns.physical_xy();
    let mut total: Option<Bounds> = None;
    columns.collect_rows::<_, ()>(|row| {
        let Some(window) = row.and_then(|row| row.vertex_window()) else {
            return;
        };
        let Some(bounds) = row_bounds(column_window(xs, &window), column_window(ys, &window))
        else {
            return;
        };
        match &mut total {
            Some(current) => current.include_bounds(bounds),
            None => total = Some(bounds),
        }
    });
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
    let selected_len = columns.materialized_present_vertices(0)?;
    let mut builder = PackedColumnBuilder::like(coords, selected_len, rows);
    for row in 0..rows {
        if columns.is_missing(row) {
            builder.close_row()?;
            continue;
        }
        let window = columns.row_window(row);
        let mut budget = crate::geometry::ExpansionBudget::new(operation, parameter);
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
    let (selected_len, rings) = columns.materialized_present_sizes()?;
    let mut builder = PackedColumnBuilder::like(coords, selected_len, rings);
    let mut polygon_builder = CsrOffsetBuilder::with_capacity(columns.rows());
    let mut output_rings = 0;
    for row in 0..columns.rows() {
        if columns.is_missing(row) {
            polygon_builder.push_end(output_rings, i32::MAX as usize)?;
            continue;
        }
        let mut budget = crate::geometry::ExpansionBudget::new(operation, parameter);
        let packed_row = columns.packed_row(row);
        let row_rings = packed_row
            .polygon_rings()
            .expect("packed polygon row has a ring range");
        for ring in row_rings.clone() {
            let ring_window = packed_row
                .polygon_ring_window(ring)
                .expect("ring lies in its admitted polygon row");
            builder.push_subdivided(coords, ring_window, &subdivide, &mut budget)?;
        }
        output_rings = output_rings.wrapping_add(row_rings.len());
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
    linear: &crate::geometry::AffineLinear,
) -> PackedColumnResult<PackedColumnOutput> {
    match columns {
        PackedColumns::Points(point_columns) => match spec {
            OriginSpec::Centroid | OriginSpec::Center => {
                let coords = if let Some(coords) = point_columns.contiguous_coords() {
                    coords
                } else {
                    let rows = point_columns.rows();
                    storage::admit_packed_coord_count(rows.present_count()).map_err(batch_err)?;
                    let mut builder = CoordSeqBuilder::with_capacity(
                        point_columns.coords().axes(),
                        rows.present_count(),
                    );
                    for (_, physical) in rows.cursor() {
                        if let Some(physical) = physical {
                            builder.push_at(point_columns.coords(), physical);
                        }
                    }
                    builder.finish().map_err(batch_err)?
                };
                Ok(PackedColumnOutput::Points(coords))
            },
            OriginSpec::Fixed(pivot_x, pivot_y) => {
                let transform = linear.at((pivot_x, pivot_y)).map_err(batch_err)?;
                let rows = point_columns.rows();
                let axes = point_columns.coords().axes();
                let capacity = if rows.is_compact() {
                    rows.present_count()
                } else {
                    rows.len()
                };
                storage::admit_packed_coord_count(capacity).map_err(batch_err)?;
                let mut builder = CoordSeqBuilder::with_capacity(axes, capacity);
                let mut has_missing = false;
                for (logical, physical) in rows.cursor() {
                    let Some(physical) = physical else {
                        if rows.is_compact() {
                            continue;
                        }
                        has_missing = true;
                        builder.push(Point::new_raw_axes(
                            f64::NAN,
                            f64::NAN,
                            ZOrdinate(axes.has_z().then_some(f64::NAN)),
                            MOrdinate(axes.has_m().then_some(f64::NAN)),
                        ));
                        continue;
                    };
                    let point = point_columns.coords().point_at(physical);
                    let (x, y) = transform
                        .map_xy(point.x, point.y)
                        .map_err(|error| row_err(logical, error))?;
                    builder.push(
                        Point::new_axes(x, y, ZOrdinate(point.z()), MOrdinate(point.m()))
                            .map_err(|error| row_err(logical, error))?,
                    );
                }
                let coords = if rows.is_compact() || !has_missing {
                    builder.finish().map_err(batch_err)?
                } else {
                    builder
                        .finish_with_missing_placeholders()
                        .map_err(batch_err)?
                };
                Ok(PackedColumnOutput::Points(coords))
            },
        },
        PackedColumns::Lines(line_columns) => {
            let coords = line_columns.coords();
            let XyColumns { xs, ys } = line_columns.physical_xy();
            let rows = line_columns.rows();
            let capacity = line_columns.materialized_present_vertices(2)?;
            let mut builder = PackedColumnBuilder::like(coords, capacity, rows);
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
                let transform = linear.at(pivot).map_err(|error| row_err(row, error))?;
                builder
                    .push_affine_about_origin_admitted(coords, window, &transform)
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
            let rows = polygon_columns.rows();
            let (capacity, rings) = polygon_columns.materialized_present_sizes()?;
            let mut builder = PackedColumnBuilder::like(coords, capacity, rings);
            let mut output_polygons = CsrOffsetBuilder::with_capacity(rows);
            let mut output_rings = 0;
            for row in 0..rows {
                if polygon_columns.is_missing(row) {
                    output_polygons
                        .push_end(output_rings, i32::MAX as usize)
                        .map_err(batch_err)?;
                    continue;
                }
                let packed_row = polygon_columns.packed_row(row);
                let row_rings = packed_row
                    .polygon_rings()
                    .expect("packed polygon row has a ring range");
                let pivot = packed_polygon_row_pivot(spec, packed_row)
                    .map_err(|error| row_err(row, error))?;
                let transform = linear.at(pivot).map_err(|error| row_err(row, error))?;
                for ring in row_rings.clone() {
                    let window = packed_row
                        .polygon_ring_window(ring)
                        .expect("ring lies in its admitted polygon row");
                    builder
                        .push_affine_about_origin_admitted(coords, window, &transform)
                        .map_err(|error| row_err(row, error))?;
                }
                output_rings = output_rings.wrapping_add(row_rings.len());
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
