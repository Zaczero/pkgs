#![expect(
    clippy::needless_pass_by_value,
    reason = "PyO3 special-method receivers must retain their binding-compatible ownership shape"
)]

use crate::array::packed_columns::{PackedColumnResult, XyColumns};
use crate::array::{
    Arc, ComputedCaches, CoordSeq, CoordinateAxes, Frame, GeometryArrayStorage, GeometryKind,
    PackedColumnError, PackedColumnOutput, PackedColumns, PackedRow, PackedStorage, Point,
    PointColumnBuilder, PolygonLevel, PyGeometryArray, PyResult, Python, Result, RingLevel,
    RowSelection, Shape, StorageLayout, column_window, packed_centroid_xy, packed_polygon_bounds,
    packed_surface_point,
};
use crate::geometry::{CoordWindow, DerivedPointStrategy, LineSeq};

fn point_from_unary_shape(shape: Shape) -> Point {
    match shape {
        Shape::Point(point) => point,
        Shape::Empty(GeometryKind::Point, _) => Point::new_raw_xy(f64::NAN, f64::NAN),
        _ => unreachable!("centroid/point_on_surface yield Point or POINT EMPTY"),
    }
}

fn packed_point_row(
    crosses_antimeridian: bool,
    shape: impl FnOnce() -> Result<Shape>,
    domain: crate::geometry::TopologyDomain,
    strategy: DerivedPointStrategy,
    op: fn(&Shape) -> Result<Shape>,
    column_point: impl FnOnce() -> Result<Point>,
) -> Result<Point> {
    if crosses_antimeridian {
        let shape = shape()?;
        Ok(point_from_unary_shape(
            crate::geometry::unary_antimeridian_derived(&shape, domain, strategy, op)?,
        ))
    } else {
        column_point()
    }
}

fn line_window_crosses_antimeridian(
    coords: &CoordSeq,
    window: std::ops::Range<usize>,
    domain: crate::geometry::TopologyDomain,
) -> bool {
    let view = coords.view(CoordWindow::from_range(window, coords.len()));
    match domain {
        crate::geometry::TopologyDomain::Cartesian => false,
        crate::geometry::TopologyDomain::Geographic(frame) => {
            crate::geometry::chain_crosses_antimeridian(&view, frame)
        },
    }
}

fn polygon_row_crosses_antimeridian(
    row: PackedRow<'_>,
    domain: crate::geometry::TopologyDomain,
) -> bool {
    let crate::array::PackedRow::Polygons { coords, .. } = row else {
        unreachable!("antimeridian ring traversal requires a packed polygon row")
    };
    row.polygon_rings()
        .expect("packed polygon row has a ring range")
        .any(|ring| {
            line_window_crosses_antimeridian(
                coords,
                row.polygon_ring_window(ring)
                    .expect("ring lies in its admitted polygon row"),
                domain,
            )
        })
}

fn longitude_span_cannot_cross_antimeridian(xs: &[f64]) -> bool {
    crate::geometry::column_minmax(xs).is_none_or(|(minx, maxx)| maxx - minx <= 180.0)
}

fn packed_point_unary<L, P>(
    array: &PyGeometryArray,
    py: Python<'_>,
    frame: Frame,
    domain: crate::geometry::TopologyDomain,
    strategy: DerivedPointStrategy,
    op: fn(&Shape) -> Result<Shape>,
    line_point: L,
    mut polygon_point: P,
) -> PyResult<Option<PyGeometryArray>>
where
    L: Fn(&CoordSeq, &[f64], &[f64], std::ops::Range<usize>) -> Result<Point> + Send + Sync,
    P: FnMut(&[f64], &[f64], PackedRow<'_>, &mut Vec<f64>) -> Result<Point> + Send + Sync,
{
    let has_missing = array.has_missing();
    array.map_packed_columns_detached(py, frame, |columns| match &columns {
        PackedColumns::Lines(line_columns) => {
            let coords = line_columns.coords();
            let XyColumns { xs, ys } = line_columns.physical_xy();
            let may_cross_antimeridian =
                domain.is_geographic() && !longitude_span_cannot_cross_antimeridian(xs);
            crate::array::admit_packed_coord_count(columns.row_count())
                .map_err(PackedColumnError::Batch)?;
            let mut builder =
                PointColumnBuilder::with_capacity(CoordinateAxes::XY, columns.row_count());
            columns.collect_rows::<_, PackedColumnResult<()>>(|row| {
                let Some(row) = row else {
                    builder.push(Point::new_raw_xy(f64::NAN, f64::NAN));
                    return Ok(());
                };
                let row_index = row.index();
                let window = row.vertex_window().expect("line rows have a vertex window");
                let crosses = may_cross_antimeridian
                    && line_window_crosses_antimeridian(coords, window.clone(), domain);
                let shape_window = window.clone();
                let point = packed_point_row(
                    crosses,
                    move || {
                        Ok(Shape::LineString(LineSeq::row_view(
                            coords,
                            CoordWindow::from_range(shape_window, coords.len()),
                        )))
                    },
                    domain,
                    strategy,
                    op,
                    || line_point(coords, xs, ys, window),
                )
                .map_err(|error| PackedColumnError::Row {
                    row: row_index,
                    error,
                })?;
                builder.push(point);
                Ok(())
            })?;
            Ok(PackedColumnOutput::Points(if has_missing {
                builder
                    .finish_with_missing_placeholders()
                    .map_err(PackedColumnError::Batch)?
            } else {
                builder.finish().map_err(PackedColumnError::Batch)?
            }))
        },
        PackedColumns::Polygons(polygon_columns) => {
            let XyColumns { xs, ys } = polygon_columns.physical_xy();
            let may_cross_antimeridian =
                domain.is_geographic() && !longitude_span_cannot_cross_antimeridian(xs);
            crate::array::admit_packed_coord_count(columns.row_count())
                .map_err(PackedColumnError::Batch)?;
            let mut builder =
                PointColumnBuilder::with_capacity(CoordinateAxes::XY, columns.row_count());
            let mut crossings = Vec::new();
            columns.collect_rows::<_, PackedColumnResult<()>>(|row| {
                let Some(row) = row else {
                    builder.push(Point::new_raw_xy(f64::NAN, f64::NAN));
                    return Ok(());
                };
                let row_index = row.index();
                let crosses =
                    may_cross_antimeridian && polygon_row_crosses_antimeridian(row, domain);
                let point = packed_point_row(
                    crosses,
                    move || row.to_shape(),
                    domain,
                    strategy,
                    op,
                    || polygon_point(xs, ys, row, &mut crossings),
                )
                .map_err(|error| PackedColumnError::Row {
                    row: row_index,
                    error,
                })?;
                builder.push(point);
                Ok(())
            })?;
            Ok(PackedColumnOutput::Points(if has_missing {
                builder
                    .finish_with_missing_placeholders()
                    .map_err(PackedColumnError::Batch)?
            } else {
                builder.finish().map_err(PackedColumnError::Batch)?
            }))
        },
        PackedColumns::Points(_) => unreachable!("points handled above"),
    })
}

impl PyGeometryArray {
    pub(crate) fn centroid_unary_packed(&self) -> PyResult<Self> {
        // A point is its own centroid; packed rows skip the per-row map.
        if let Some(points) = self.packed_points_xy()? {
            return Ok(points);
        }
        let frame = self.frame.clone();
        let domain = crate::geometry::topology_domain(&self.frame)?;
        if let Some(points) = Python::attach(|py| {
            packed_point_unary(
                self,
                py,
                frame,
                domain,
                DerivedPointStrategy::Centroid,
                Shape::centroid,
                |_, xs, ys, window| {
                    packed_centroid_xy(crate::geometry::centroid_line_row_columns(
                        column_window(xs, &window),
                        column_window(ys, &window),
                    ))
                },
                |xs, ys, row, _crossings| {
                    let crate::array::PackedRow::Polygons { ring_offsets, .. } = row else {
                        unreachable!("packed polygon centroid requires polygon storage")
                    };
                    packed_centroid_xy(crate::geometry::centroid_polygon_row_columns(
                        xs,
                        ys,
                        ring_offsets.as_slice(),
                        row.polygon_rings()
                            .expect("packed polygon row has a ring range"),
                    ))
                },
            )
        })? {
            return Ok(points);
        }
        self.map_shapes(|shape| {
            Ok(crate::geometry::unary_antimeridian_derived(
                shape,
                domain,
                DerivedPointStrategy::Centroid,
                Shape::centroid,
            )?)
        })
    }

    pub(crate) fn point_on_surface_unary_packed(&self) -> PyResult<Self> {
        if let Some(points) = self.packed_points_xy()? {
            return Ok(points);
        }
        let frame = self.frame.clone();
        let domain = crate::geometry::topology_domain(&self.frame)?;
        if let Some(points) = Python::attach(|py| {
            packed_point_unary(
                self,
                py,
                frame,
                domain,
                DerivedPointStrategy::Interior,
                |shape| Ok(shape.point_on_surface()),
                |_, xs, ys, window| {
                    packed_surface_point(crate::geometry::point_on_surface_line_columns(
                        column_window(xs, &window),
                        column_window(ys, &window),
                    ))
                },
                |xs, ys, row, crossings| {
                    let crate::array::PackedRow::Polygons {
                        coords,
                        ring_offsets,
                        rings_start,
                        rings_end,
                        ..
                    } = row
                    else {
                        unreachable!("packed polygon point-on-surface requires polygon storage")
                    };
                    packed_surface_point(crate::geometry::point_on_surface_polygon_row_columns(
                        xs,
                        ys,
                        ring_offsets.as_slice(),
                        rings_start..rings_end,
                        packed_polygon_bounds!(
                            coords,
                            ring_offsets.as_slice(),
                            rings_start,
                            rings_end,
                        ),
                        crossings,
                    ))
                },
            )
        })? {
            return Ok(points);
        }
        self.map_shapes(|shape| {
            Ok(crate::geometry::unary_antimeridian_derived(
                shape,
                domain,
                DerivedPointStrategy::Interior,
                |shape| Ok(shape.point_on_surface()),
            )?)
        })
    }

    pub(crate) fn envelope_unary_packed(&self) -> PyResult<Self> {
        if let Some(points) = self.packed_points_xy()? {
            return Ok(points);
        }
        // All-rectangular fast path: when every row's box is NON-degenerate
        // (`bounds_to_shape` would make a `Polygon`, not a `Point`/`LineString`),
        // write the 5 box vertices per row straight into packed `Polygon`
        // columns — no intermediate box `Shape`s and no `from_shapes` repack.
        // Matches `rect_polygon`'s vertex order exactly. The envelope of a box is
        // itself, so the per-element bounds carry over as the result's cache.
        if let Some(bounds) = self.existing_element_bounds()
            && bounds.iter().all(|b| {
                b.is_some_and(|bb| {
                    !crate::geometry::same_topological_coordinate(bb.minx(), bb.maxx())
                        && !crate::geometry::same_topological_coordinate(bb.miny(), bb.maxy())
                })
            })
        {
            let rows = bounds.len();
            let vertex_count = rows
                .checked_mul(5)
                .filter(|&count| i32::try_from(count).is_ok())
                .ok_or_else(|| {
                    crate::PyErr::from(crate::error::Error::from(
                        crate::geometry::GeometryErrorKind::OffsetCapacityExceeded,
                    ))
                })?;
            let mut xs = Vec::with_capacity(vertex_count);
            let mut ys = Vec::with_capacity(vertex_count);
            let mut ring_builder = crate::geometry::CsrOffsetBuilder::with_capacity(rows);
            let mut polygon_builder = crate::geometry::CsrOffsetBuilder::with_capacity(rows);
            for b in bounds.iter() {
                let bb = b.expect("all bounds present (checked)");
                xs.extend_from_slice(&[bb.minx(), bb.maxx(), bb.maxx(), bb.minx(), bb.minx()]);
                ys.extend_from_slice(&[bb.miny(), bb.miny(), bb.maxy(), bb.maxy(), bb.miny()]);
                ring_builder.push_end(xs.len(), vertex_count)?;
                polygon_builder.push_end(ring_builder.row_count(), rows)?;
            }
            let ring_offsets = ring_builder.finish(vertex_count)?.cast_level::<RingLevel>();
            let polygon_offsets = polygon_builder.finish(rows)?.cast_level::<PolygonLevel>();
            return Ok(Self::from_result_storage(
                Arc::new(GeometryArrayStorage::Packed(PackedStorage::Polygons {
                    coords: Arc::new(CoordSeq::try_from_vecs(xs, ys, None, None)?),
                    ring_offsets,
                    polygon_offsets,
                    row_map: RowSelection::Identity,
                })),
                self.frame.clone(),
                self.missing().cloned(),
                // The output rectangles have the same per-row bounds as the
                // input (envelope of a box is the box), so the bounds carry
                // over — but they are DIFFERENT geometry, so the prepared
                // handles must NOT alias the input's lazily-filled slots.
                StorageLayout::LogicalRows,
                ComputedCaches::carried(
                    Arc::clone(&self.bounds_cache),
                    Arc::clone(&self.total_bounds_cache),
                ),
            )?);
        }
        // Packed polygons/lines: build each box from its coordinate window
        // directly — no per-row Shape view (which clones the ring coords).
        if let Some(boxes) = self.storage().envelope_box_shapes()? {
            return Ok(Self::from_shapes(boxes, self.frame.clone())?);
        }
        self.map_shapes(|shape| Ok(shape.envelope()?))
    }

    pub(crate) fn convex_hull_unary_packed(&self, py: Python<'_>) -> PyResult<Self> {
        if let Some(identity) = self.packed_points_identity() {
            return Ok(identity);
        }
        self.map_shapes_detached(py, Shape::convex_hull)
    }
}
