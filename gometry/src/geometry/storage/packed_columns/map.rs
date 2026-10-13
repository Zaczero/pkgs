use super::transform::TargetLongitudeOutput;
use super::{
    CoordSeq, CoordSeqBuilder, CoordinateAxes, CsrOffsetBuilder, GeographicFrame,
    PackedColumnOutput, PackedColumnResult, PackedColumns, Point, TransformFailure, batch_err,
    row_err, storage,
};

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
