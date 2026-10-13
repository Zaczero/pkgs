use super::*;

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
