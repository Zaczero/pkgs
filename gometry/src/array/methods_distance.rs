use crate::array::{
    Arc, BoundsSeed, ComputedCaches, CoordSeq, CoordinateAxes, Frame, GeometryKind, Point,
    PolygonLevel, PyGeometryArray, PyResult, Python, Result, RingLevel, Shape, StorageLayout,
};
use crate::error::{BatchError, BatchResult};
use crate::geographic::PointSelectionChart;
use crate::geographic::chart::point_set_convex_hull_shape;
use crate::geometry::storage::builder::PointColumnBuilder;
use crate::geometry::storage::packed_columns::{PackedColumnOutput, PackedColumns, XyColumns};
use crate::geometry::storage::{
    GeometryArrayStorage, PackedRow, PackedStorage, RowSelection, column_window,
    packed_centroid_xy, packed_polygon_bounds, packed_surface_point,
};
use crate::geometry::{CoordWindow, DerivedPointStrategy, LineSeq, storage};

fn point_from_unary_shape(shape: &Shape) -> Point {
    match shape {
        Shape::Point(point) => *point,
        Shape::Empty(GeometryKind::Point, _) => Point::new_raw_xy(f64::NAN, f64::NAN),
        Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..) => {
            unreachable!("centroid/point_on_surface yield Point or POINT EMPTY")
        },
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
            &crate::geometry::unary_antimeridian_derived(&shape, domain, strategy, op)?,
        ))
    } else {
        column_point()
    }
}

fn packed_polygon_crossing_point(
    row: PackedRow<'_>,
    domain: crate::geometry::TopologyDomain,
    strategy: DerivedPointStrategy,
    op: fn(&Shape) -> Result<Shape>,
) -> Result<Point> {
    let shape = row.to_shape()?;
    Ok(point_from_unary_shape(
        &crate::geometry::unary_antimeridian_derived(&shape, domain, strategy, op)?,
    ))
}

fn packed_bounds_certify_ordinary(
    bounds: crate::geometry::Bounds,
    domain: crate::geometry::TopologyDomain,
) -> bool {
    match domain {
        crate::geometry::TopologyDomain::Cartesian => true,
        crate::geometry::TopologyDomain::Geographic(frame) => {
            frame.bounds_certify_ordinary_support(bounds)
        },
    }
}

fn packed_polygon_row_point(
    row: PackedRow<'_>,
    cached_bounds: BoundsSeed,
    may_cross_antimeridian: bool,
    domain: crate::geometry::TopologyDomain,
    strategy: DerivedPointStrategy,
    op: fn(&Shape) -> Result<Shape>,
    polygon_point: impl FnOnce() -> Result<(Point, Option<crate::geometry::Bounds>)>,
) -> Result<Point> {
    if let BoundsSeed::Value(cached_bounds) = cached_bounds {
        if cached_bounds.is_none_or(|bounds| packed_bounds_certify_ordinary(bounds, domain))
            || !may_cross_antimeridian
        {
            return polygon_point().map(|(point, _)| point);
        }
        if polygon_row_crosses_antimeridian(row, domain) {
            return packed_polygon_crossing_point(row, domain, strategy, op);
        }
        return polygon_point().map(|(point, _)| point);
    }

    let (point, bounds) = polygon_point()?;
    if bounds.is_some_and(|bounds| packed_bounds_certify_ordinary(bounds, domain))
        || !may_cross_antimeridian
    {
        return Ok(point);
    }
    if polygon_row_crosses_antimeridian(row, domain) {
        packed_polygon_crossing_point(row, domain, strategy, op)
    } else {
        Ok(point)
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
    let crate::geometry::storage::PackedRow::Polygons { coords, .. } = row else {
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

fn longitude_span_cannot_cross_antimeridian(
    xs: &[f64],
    frame: crate::geographic::GeographicFrame,
) -> bool {
    crate::geometry::column_minmax(xs)
        .is_none_or(|(minx, maxx)| frame.longitude_bounds_certify_no_seam(minx, maxx))
}

fn packed_rows_may_cross_antimeridian(
    xs: &[f64],
    domain: crate::geometry::TopologyDomain,
    element_bounds: Option<&[Option<crate::geometry::Bounds>]>,
) -> bool {
    let crate::geometry::TopologyDomain::Geographic(frame) = domain else {
        return false;
    };
    element_bounds.map_or_else(
        || !longitude_span_cannot_cross_antimeridian(xs, frame),
        |bounds| {
            bounds
                .iter()
                .flatten()
                .any(|bounds| !frame.longitude_bounds_certify_no_seam(bounds.minx(), bounds.maxx()))
        },
    )
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
    L: Fn(&CoordSeq, &[f64], &[f64], std::ops::Range<usize>) -> Result<Point> + pyo3::marker::Ungil,
    P: FnMut(
            &[f64],
            &[f64],
            PackedRow<'_>,
            &mut Vec<f64>,
        ) -> Result<(Point, Option<crate::geometry::Bounds>)>
        + pyo3::marker::Ungil,
{
    let has_missing = array.has_missing();
    let element_bounds = array.existing_element_bounds();
    let element_bounds = element_bounds.as_deref().map(Vec::as_slice);
    array.map_packed_columns_detached(py, frame, |columns| match &columns {
        PackedColumns::Lines(line_columns) => {
            let coords = line_columns.coords();
            let XyColumns { xs, ys } = line_columns.physical_xy();
            let may_cross_antimeridian =
                packed_rows_may_cross_antimeridian(xs, domain, element_bounds);
            storage::admit_packed_coord_count(columns.row_count()).map_err(BatchError::Batch)?;
            let mut builder =
                PointColumnBuilder::with_capacity(CoordinateAxes::XY, columns.row_count());
            columns.collect_rows::<_, BatchResult<()>>(|row| {
                let Some(row) = row else {
                    builder.push(Point::new_raw_xy(f64::NAN, f64::NAN));
                    return Ok(());
                };
                let row_index = row.index();
                let window = row.vertex_window().expect("line rows have a vertex window");
                let cached_bounds = element_bounds.map_or(BoundsSeed::Unset, |bounds| {
                    BoundsSeed::Value(bounds.get(row_index).copied().flatten())
                });
                let row_certified = match (domain, cached_bounds) {
                    (
                        crate::geometry::TopologyDomain::Geographic(frame),
                        BoundsSeed::Value(Some(bounds)),
                    ) => frame.longitude_bounds_certify_no_seam(bounds.minx(), bounds.maxx()),
                    (_, BoundsSeed::Value(None))
                    | (crate::geometry::TopologyDomain::Cartesian, _) => true,
                    (_, BoundsSeed::Unset) => false,
                };
                let crosses = may_cross_antimeridian
                    && !row_certified
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
                .map_err(|error| BatchError::Row {
                    row: row_index,
                    error,
                })?;
                builder.push(point);
                Ok(())
            })?;
            Ok(PackedColumnOutput::Points(if has_missing {
                builder
                    .finish_with_missing_placeholders()
                    .map_err(BatchError::Batch)?
            } else {
                builder.finish().map_err(BatchError::Batch)?
            }))
        },
        PackedColumns::Polygons(polygon_columns) => {
            let XyColumns { xs, ys } = polygon_columns.physical_xy();
            let may_cross_antimeridian = match domain {
                crate::geometry::TopologyDomain::Cartesian => false,
                crate::geometry::TopologyDomain::Geographic(frame) => {
                    element_bounds.is_none_or(|bounds| {
                        bounds.iter().flatten().any(|bounds| {
                            !frame.longitude_bounds_certify_no_seam(bounds.minx(), bounds.maxx())
                        })
                    })
                },
            };
            storage::admit_packed_coord_count(columns.row_count()).map_err(BatchError::Batch)?;
            let mut builder =
                PointColumnBuilder::with_capacity(CoordinateAxes::XY, columns.row_count());
            let mut crossings = Vec::new();
            columns.collect_rows::<_, BatchResult<()>>(|row| {
                let Some(row) = row else {
                    builder.push(Point::new_raw_xy(f64::NAN, f64::NAN));
                    return Ok(());
                };
                let row_index = row.index();
                let cached_bounds = element_bounds.map_or(BoundsSeed::Unset, |bounds| {
                    BoundsSeed::Value(bounds.get(row_index).copied().flatten())
                });
                let point = packed_polygon_row_point(
                    row,
                    cached_bounds,
                    may_cross_antimeridian,
                    domain,
                    strategy,
                    op,
                    || polygon_point(xs, ys, row, &mut crossings),
                )
                .map_err(|error| BatchError::Row {
                    row: row_index,
                    error,
                })?;
                builder.push(point);
                Ok(())
            })?;
            Ok(PackedColumnOutput::Points(if has_missing {
                builder
                    .finish_with_missing_placeholders()
                    .map_err(BatchError::Batch)?
            } else {
                builder.finish().map_err(BatchError::Batch)?
            }))
        },
        PackedColumns::Points(_) => unreachable!("points handled above"),
    })
}

impl PyGeometryArray {
    pub(crate) fn centroid_unary_packed(&self, py: Python<'_>) -> PyResult<Self> {
        // A point is its own centroid; packed rows skip the per-row map.
        if let Some(points) = self.packed_points_xy()? {
            return Ok(points);
        }
        let frame = self.frame.clone();
        let domain = crate::geometry::topology_domain(&self.frame)?;
        if let Some(points) = packed_point_unary(
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
                let crate::geometry::storage::PackedRow::Polygons { ring_offsets, .. } = row else {
                    unreachable!("packed polygon centroid requires polygon storage")
                };
                let (centroid, bounds) = crate::geometry::centroid_polygon_row_columns_with_bounds(
                    xs,
                    ys,
                    ring_offsets.as_slice(),
                    row.polygon_rings()
                        .expect("packed polygon row has a ring range"),
                );
                Ok((packed_centroid_xy(centroid)?, bounds))
            },
        )? {
            return Ok(points);
        }
        self.map_shapes_detached(py, |shape| {
            crate::geometry::unary_antimeridian_centroid(shape, domain)
        })
    }

    pub(crate) fn point_on_surface_unary_packed(&self, py: Python<'_>) -> PyResult<Self> {
        if let Some(points) = self.packed_points_xy()? {
            return Ok(points);
        }
        let frame = self.frame.clone();
        let domain = crate::geometry::topology_domain(&self.frame)?;
        if let Some(points) = packed_point_unary(
            self,
            py,
            frame,
            domain,
            DerivedPointStrategy::Interior,
            Shape::point_on_surface,
            |_, xs, ys, window| {
                packed_surface_point(crate::geometry::point_on_surface_line_columns(
                    column_window(xs, &window),
                    column_window(ys, &window),
                ))
            },
            |xs, ys, row, crossings| {
                let crate::geometry::storage::PackedRow::Polygons {
                    coords,
                    ring_offsets,
                    rings_start,
                    rings_end,
                    ..
                } = row
                else {
                    unreachable!("packed polygon point-on-surface requires polygon storage")
                };
                let bounds = packed_polygon_bounds!(
                    coords,
                    ring_offsets.as_slice(),
                    rings_start,
                    rings_end,
                );
                let point =
                    packed_surface_point(crate::geometry::point_on_surface_polygon_row_columns(
                        xs,
                        ys,
                        ring_offsets.as_slice(),
                        rings_start..rings_end,
                        bounds,
                        crossings,
                    ))?;
                Ok((point, bounds))
            },
        )? {
            return Ok(points);
        }
        self.map_shapes_detached(py, |shape| {
            crate::geometry::unary_antimeridian_derived(
                shape,
                domain,
                DerivedPointStrategy::Interior,
                Shape::point_on_surface,
            )
        })
    }

    pub(crate) fn envelope_unary_packed(&self) -> crate::error::BatchResult<Self> {
        if let crate::geometry::TopologyDomain::Geographic(frame) =
            crate::geometry::topology_domain(&self.frame)?
        {
            return self.map_shapes_native_indexed(|shape, _| {
                crate::geographic::GeographicSupportEnvelope::try_from_shape_with_bounds(
                    frame,
                    shape,
                    shape.bounds(),
                )
                .map_err(crate::error::Error::from)?
                .into_shape(frame)});
        }
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
                    crate::error::Error::from(
                        crate::geometry::GeometryErrorKind::OffsetCapacityExceeded,
                    )
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
        self.map_shapes_native_indexed(|shape, _| shape.envelope())
    }

    pub(crate) fn convex_hull_unary_packed(
        &self,
        py: Python<'_>,
        resolver: crate::dispatch::MetricResolver,
    ) -> PyResult<Self> {
        if let Some(identity) = self.packed_points_identity() {
            return Ok(identity);
        }
        let mut metric_scratch = crate::dispatch::MetricScratch::default();
        let (domain, metric) = resolver.resolve_topology_ctx(&self.frame, &mut metric_scratch)?;
        let source_crs = self.frame.crs_owned();
        self.map_shapes_detached(py, move |shape| {
            let metric_access = || {
                metric
                    .source_chart_metric(source_crs.as_deref())?
                        .ok_or_else(|| {
                            crate::crs::CrsError::invalid(
                                "geographic computed-edge source ordinates require the operation's CRS metric",
                            )
                        })
            };
            let source_axes = shape.axes();
            let source_capture = if domain.is_geographic()
                && (source_axes.has_z() || source_axes.has_m())
            {
                #[cfg(test)]
                crate::geometry::record_root_work(crate::geometry::RootWorkEvent::SourceContextSetup);
                let source_context =
                    crate::geometry::SourceArcContext::new(domain, Some(&metric_access));
                crate::geometry::SourceArcCapture::with_context(&source_context)
            } else {
                crate::geometry::SourceArcCapture::none()
            };
            if let crate::geometry::TopologyDomain::Geographic(frame) = domain
                && !matches!(shape, Shape::Point(_) | Shape::Empty(..))
                && PointSelectionChart::accepts_point_set(shape)
                && let Some(hull) =
                    point_set_convex_hull_shape(shape, frame, shape.bounds(), &source_capture)?
            {
                return Ok(hull);
            }
            let hull = shape.convex_hull()?;
            match domain {
                crate::geometry::TopologyDomain::Cartesian => Ok(hull),
                crate::geometry::TopologyDomain::Geographic(frame) =>
                    crate::geometry::publish_raw_computed_shape_with_capture(
                        &hull,
                        frame,
                        "convex_hull",
                        &source_capture,
                    ),
            }
        })
    }
}

#[cfg(test)]
mod root_capture_tests {
    use super::*;
    use crate::crs::frame::crs_arc_static;
    use crate::geometry::Ring;

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn geographic_xy_hull_and_mrr_skip_source_root_capture() {
        let _guard = crate::crs::RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        Python::initialize();
        let frame = Frame::Crs(crs_arc_static("OGC:CRS84"));
        let line = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(0.0, 0.0),
                    Point::new_raw_xy(120.0, 60.0),
                    Point::new_raw_xy(360.0, 20.0),
                ])
                .expect("line coordinates"),
            )
            .expect("valid line"),
        );
        let polygon = Shape::Polygon(crate::geometry::Polygon::new(
            Ring::try_from_closed(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(-10.0, -5.0),
                    Point::new_raw_xy(10.0, -5.0),
                    Point::new_raw_xy(10.0, 5.0),
                    Point::new_raw_xy(-10.0, 5.0),
                    Point::new_raw_xy(-10.0, -5.0),
                ])
                .expect("polygon coordinates"),
            )
            .expect("closed ring"),
            Vec::new(),
        ));
        let chart_points = Shape::multi_point(
            CoordinateAxes::XY,
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(170.0, 0.0),
                Point::new_raw_xy(-170.0, 0.0),
                Point::new_raw_xy(180.0, 10.0),
            ])
            .expect("chart multipoint coordinates"),
        );
        let array = PyGeometryArray::from_shapes(vec![line, polygon, chart_points], frame).unwrap();

        crate::geometry::reset_root_work_counters();
        let hulls = Python::attach(|py| {
            array.convex_hull_unary_packed(py, crate::dispatch::MetricResolver::GeographicCarry)
        })
        .unwrap();

        assert_eq!(hulls.len(), 3);
        assert_eq!(hulls.uniform_axes(), Some(CoordinateAxes::XY));
        assert_eq!(
            crate::geometry::root_work_counters(),
            crate::geometry::RootWorkCounters::default()
        );

        crate::geometry::reset_root_work_counters();
        let rectangles = Python::attach(|py| array.minimum_rotated_rectangle(py)).unwrap();
        assert_eq!(rectangles.len(), 3);
        assert_eq!(rectangles.uniform_axes(), Some(CoordinateAxes::XY));
        assert_eq!(
            crate::geometry::root_work_counters(),
            crate::geometry::RootWorkCounters::default()
        );
    }
}
