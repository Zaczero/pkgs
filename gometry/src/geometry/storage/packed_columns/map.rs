use super::transform::TargetLongitudeOutput;
use super::{
    CoordSeq, CoordSeqBuilder, CoordinateAxes, CsrOffsetBuilder, GeographicFrame,
    PackedColumnOutput, PackedColumns, Point, RowSelectionRef, TransformFailure, batch_err,
    row_err, storage,
};
use crate::error::BatchResult;

fn finish_pointwise_coords(builder: CoordSeqBuilder, has_missing: bool) -> BatchResult<CoordSeq> {
    if has_missing {
        builder
            .finish_with_missing_placeholders()
            .map_err(batch_err)
    } else {
        builder.finish().map_err(batch_err)
    }
}

/// Every layout rounds through the column kernel. Contiguous dense points and
/// complete identity CSR payloads quantize their backing columns directly.
/// Other selections and nullable layouts first gather present coordinates with
/// the shared row traversal: backing columns may include missing placeholders,
/// unselected coordinates, or an unreferenced coordinate or ring tail.
///
/// Rounding fails only on overflow; that path reruns the per-point traversal so
/// the error names its logical row.
pub(crate) fn quantize_selected_columns(
    columns: PackedColumns<'_>,
    precision: i32,
) -> BatchResult<PackedColumnOutput> {
    let gathered = match whole_selected_columns(columns) {
        Some(output) => output,
        None => map_selected_points(columns, columns_axes(columns), Ok)?,
    };
    quantize_output(gathered, precision).or_else(|_| {
        map_selected_points(columns, columns_axes(columns), |point| {
            point.quantize(precision)
        })
    })
}

const fn columns_axes(columns: PackedColumns<'_>) -> CoordinateAxes {
    match columns {
        PackedColumns::Points(points) => points.coords().axes(),
        PackedColumns::Lines(lines) => lines.coords().axes(),
        PackedColumns::Polygons(polygons) => polygons.coords().axes(),
    }
}

/// The selected coordinates as the backing columns themselves, when the
/// selection covers them exactly.
fn whole_selected_columns(columns: PackedColumns<'_>) -> Option<PackedColumnOutput> {
    match columns {
        PackedColumns::Points(points) => points.contiguous_coords().map(PackedColumnOutput::Points),
        PackedColumns::Lines(lines) => (lines.missing.is_none()
            && matches!(lines.row_map, RowSelectionRef::Identity)
            && lines
                .offsets()
                .last()
                .is_some_and(|&end| end as usize == lines.coords().len()))
        .then(|| PackedColumnOutput::Lines {
            coords: lines.coords().clone(),
            offsets: lines.offsets.clone(),
        }),
        PackedColumns::Polygons(polygons) => (polygons.missing.is_none()
            && matches!(polygons.row_map, RowSelectionRef::Identity)
            && polygons.polygon_offsets().last().is_some_and(|&end| {
                end as usize == polygons.ring_offsets().len().saturating_sub(1)
            })
            && polygons
                .ring_offsets()
                .last()
                .is_some_and(|&end| end as usize == polygons.coords().len()))
        .then(|| PackedColumnOutput::Polygons {
            coords: polygons.coords().clone(),
            ring_offsets: polygons.ring_offsets.clone(),
            polygon_offsets: polygons.polygon_offsets.clone(),
        }),
    }
}

fn quantize_output(
    output: PackedColumnOutput,
    precision: i32,
) -> crate::error::Result<PackedColumnOutput> {
    Ok(match output {
        PackedColumnOutput::Points(coords) => {
            PackedColumnOutput::Points(coords.quantize(precision)?)
        },
        PackedColumnOutput::Lines { coords, offsets } => PackedColumnOutput::Lines {
            coords: coords.quantize(precision)?,
            offsets,
        },
        PackedColumnOutput::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
        } => PackedColumnOutput::Polygons {
            coords: coords.quantize(precision)?,
            ring_offsets,
            polygon_offsets,
        },
    })
}

pub(super) fn canonicalize_transformed_points(
    coordinates: &CoordSeq,
    frame: GeographicFrame,
) -> crate::error::Result<CoordSeq> {
    coordinates.try_map_points(|point| {
        frame
            .canonical_computed_point(point)
            .map_err(|error| crate::crs::CrsError::invalid(error.to_string()))
    })
}

pub(super) fn transform_selected_coords(
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

pub(crate) fn map_selected_points(
    columns: PackedColumns<'_>,
    output_axes: CoordinateAxes,
    mut map: impl FnMut(Point) -> crate::error::Result<Point>,
) -> BatchResult<PackedColumnOutput> {
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
