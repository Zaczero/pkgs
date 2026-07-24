#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Kernels that consume resolved packed columns: CRS transform, per-row
//! bounds, segmentize/densify, and per-row affine — plus the sanctioned
//! `line_measure`/`polygon_measure` lane over raw storage columns.
//!
//! Row-map normalization and GIL release happen in `packed_columns.rs` (the
//! detached execution seam); everything here operates on already-logical
//! contiguous columns.

use std::sync::Arc;

use super::packed_columns::{
    LineColumns, PackedColumnResult, PackedColumns, PackedRow, PolygonColumns, XyColumns,
    batch_err, row_err,
};
use super::storage_helpers::{
    packed_line_measure, packed_line_row_pivot, packed_polygon_measure, packed_polygon_row_pivot,
};
use super::*;
use crate::geometry::{CoordWindow, LineSeq};

// `line_measure`/`polygon_measure` are the sanctioned indirection required by
// `tools/gates/_check_packed_execution.py`: array method surfaces may not call the
// raw `packed_*_measure` storage helpers directly.

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
    transform_packed_columns(transformer, seq)
}

fn arc_copy_from_slice(source: &[f64]) -> Arc<[f64]> {
    Arc::from(source)
}

fn arc_get_mut_slice(arc: &mut Arc<[f64]>) -> &mut [f64] {
    Arc::get_mut(arc).expect("unique Arc")
}

pub(crate) fn transform_packed_columns(
    transformer: &crs::Transformer,
    seq: &CoordSeq,
) -> crate::error::Result<CoordSeq> {
    let mut xs = arc_copy_from_slice(seq.xs());
    let mut ys = arc_copy_from_slice(seq.ys());
    let mut zs = seq.zs().map(arc_copy_from_slice);
    let zt = zs
        .as_mut()
        .map_or(crate::Zt::None, |zs| crate::Zt::Z(arc_get_mut_slice(zs)));
    transformer.transform_coordinates(
        arc_get_mut_slice(&mut xs),
        arc_get_mut_slice(&mut ys),
        zt,
    )?;
    Ok(CoordSeq::from_columns(xs, ys, zs, seq.carried_ms()))
}

/// Per-row 3D bounds from packed homogeneous storage — the kernel behind
/// `GeometryArray.bounds_3d` (all rings for polygons, matching `Shape::bounds_3d`).
pub(crate) fn bounds_3d_values_from_columns(
    columns: &PackedColumns<'_>,
) -> Vec<Option<crate::py::support::Bounds3D>> {
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
    bounds_values_from_columns_masked(columns, None)
}

/// Packed bounds with nullable rows excluded before the per-row extrema walk.
pub(crate) fn bounds_values_from_columns_masked(
    columns: &PackedColumns<'_>,
    missing: Option<&[bool]>,
) -> Vec<f64> {
    let XyColumns { xs, ys } = columns.xy();
    let mut values = Vec::with_capacity(columns.row_count() * 4);
    let mut logical = 0;
    columns.map_rows(|row| {
        let row_index = logical;
        logical += 1;
        if missing.is_some_and(|mask| mask[row_index]) {
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
    let mut values = Vec::with_capacity(columns.row_count() * 4);
    match columns {
        PackedColumns::Points(_) => return bounds_values_from_columns(columns),
        PackedColumns::Lines(line_columns) => {
            let coords = line_columns.coords();
            let XyColumns { xs, ys } = line_columns.xy();
            let offsets = line_columns.offsets();
            for row in 0..line_columns.rows() {
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
    let XyColumns { xs, ys } = columns.xy();
    let (minx, maxx) = crate::geometry::column_minmax(xs)?;
    let (miny, maxy) = crate::geometry::column_minmax(ys)?;
    Some(Bounds::new_unchecked(minx, miny, maxx, maxy))
}

pub(crate) fn subdivide_line_columns(
    columns: &LineColumns<'_>,
    operation: &'static str,
    parameter: &'static str,
    subdivide: impl Fn(&CoordSeq) -> Result<CoordSeq>,
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
    subdivide: impl Fn(&CoordSeq) -> Result<CoordSeq>,
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
