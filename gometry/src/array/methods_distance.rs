#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::needless_pass_by_value,
    reason = "PyO3 special-method receivers must retain their binding-compatible ownership shape"
)]

use crate::array::packed_columns::XyColumns;
use crate::array::{
    Arc, Bounds, CoordSeq, CoordinateAxes, CsrOffsetColumn, EmptyKind, Frame, GeometryArrayStorage,
    PackedColumnError, PackedColumnOutput, PackedColumns, Point, PointColumnBuilder, PolygonLevel,
    PyGeometryArray, PyResult, Python, Result, RingLevel, RowSelection, Shape, column_window,
    line_crosses_antimeridian, packed_centroid_xy, packed_surface_point, row_bounds_values,
};
use crate::geometry::{CoordWindow, LineSeq};

fn point_from_unary_shape(shape: Shape) -> Point {
    match shape {
        Shape::Point(point) => point,
        Shape::Empty(EmptyKind::Point, _) => Point::new_unchecked_xy(f64::NAN, f64::NAN),
        _ => unreachable!("centroid/point_on_surface yield Point or POINT EMPTY"),
    }
}

fn packed_point_row(
    crosses_antimeridian: bool,
    shape: impl FnOnce() -> Shape,
    wrap_antimeridian: bool,
    op: fn(&Shape) -> Result<Shape>,
    column_point: impl FnOnce() -> Result<Point>,
) -> Result<Point> {
    if crosses_antimeridian {
        let shape = shape();
        Ok(point_from_unary_shape(
            crate::geometry::unary_antimeridian_derived(&shape, true, wrap_antimeridian, op)?,
        ))
    } else {
        column_point()
    }
}

fn line_window_crosses_antimeridian(coords: &CoordSeq, window: std::ops::Range<usize>) -> bool {
    line_crosses_antimeridian(&coords.view(CoordWindow::trusted(window, coords.len())))
}

fn polygon_row_crosses_antimeridian(
    coords: &CoordSeq,
    ring_offsets: &[i32],
    rings: std::ops::Range<usize>,
) -> bool {
    rings
        .map(|ring| ring_offsets[ring] as usize..ring_offsets[ring + 1] as usize)
        .any(|window| line_window_crosses_antimeridian(coords, window))
}

fn longitude_span_cannot_cross_antimeridian(xs: &[f64]) -> bool {
    crate::geometry::column_minmax(xs).is_none_or(|(minx, maxx)| maxx - minx <= 180.0)
}

fn packed_point_unary<L, P>(
    array: &PyGeometryArray,
    py: Python<'_>,
    frame: Frame,
    geographic: bool,
    wrap_antimeridian: bool,
    op: fn(&Shape) -> Result<Shape>,
    line_point: L,
    polygon_point: P,
) -> PyResult<Option<PyGeometryArray>>
where
    L: Fn(&CoordSeq, &[f64], &[f64], std::ops::Range<usize>) -> Result<Point> + Send + Sync,
    P: Fn(
            &CoordSeq,
            &[f64],
            &[f64],
            &[i32],
            std::ops::Range<usize>,
            Option<std::ops::Range<usize>>,
        ) -> Result<Point>
        + Send
        + Sync,
{
    array.map_packed_columns_detached(py, frame, |columns| match &columns {
        PackedColumns::Lines(line_columns) => {
            let coords = line_columns.coords();
            let XyColumns { xs, ys } = line_columns.xy();
            let row_crosses_antimeridian =
                (geographic && !longitude_span_cannot_cross_antimeridian(xs)).then(|| {
                    columns.map_rows(|row| {
                        line_window_crosses_antimeridian(
                            coords,
                            row.vertex_window().expect("line rows have a vertex window"),
                        )
                    })
                });
            let mut builder =
                PointColumnBuilder::with_capacity(CoordinateAxes::XY, columns.row_count());
            for result in columns.map_rows(|row| {
                let row_index = row.index();
                let window = row.vertex_window().expect("line rows have a vertex window");
                let crosses = row_crosses_antimeridian
                    .as_ref()
                    .is_some_and(|rows| rows[row_index]);
                let shape_window = window.clone();
                packed_point_row(
                    crosses,
                    move || {
                        Shape::LineString(LineSeq::from_trusted(
                            coords.view(CoordWindow::trusted(shape_window, coords.len())),
                        ))
                    },
                    wrap_antimeridian,
                    op,
                    || line_point(coords, xs, ys, window),
                )
                .map_err(|error| PackedColumnError::Row {
                    row: row_index,
                    error,
                })
            }) {
                builder.try_push(result)?;
            }
            Ok(PackedColumnOutput::Points(
                builder.finish().map_err(PackedColumnError::Batch)?,
            ))
        },
        PackedColumns::Polygons(polygon_columns) => {
            let coords = polygon_columns.coords();
            let XyColumns { xs, ys } = polygon_columns.xy();
            let ring_offsets = polygon_columns.ring_offsets();
            let row_crosses_antimeridian =
                (geographic && !longitude_span_cannot_cross_antimeridian(xs)).then(|| {
                    columns.map_rows(|row| {
                        polygon_row_crosses_antimeridian(
                            coords,
                            ring_offsets,
                            row.polygon_rings().expect("polygon rows have rings"),
                        )
                    })
                });
            let mut builder =
                PointColumnBuilder::with_capacity(CoordinateAxes::XY, columns.row_count());
            for result in columns.map_rows(|row| {
                let row_index = row.index();
                let rings = row.polygon_rings().expect("polygon rows have rings");
                let shell = row.polygon_shell_window();
                let crosses = row_crosses_antimeridian
                    .as_ref()
                    .is_some_and(|rows| rows[row_index]);
                let shape_rings = rings.clone();
                packed_point_row(
                    crosses,
                    move || {
                        Shape::Polygon(GeometryArrayStorage::polygon_view(
                            coords,
                            ring_offsets,
                            shape_rings,
                        ))
                    },
                    wrap_antimeridian,
                    op,
                    || polygon_point(coords, xs, ys, ring_offsets, rings, shell),
                )
                .map_err(|error| PackedColumnError::Row {
                    row: row_index,
                    error,
                })
            }) {
                builder.try_push(result)?;
            }
            Ok(PackedColumnOutput::Points(
                builder.finish().map_err(PackedColumnError::Batch)?,
            ))
        },
        PackedColumns::Points(_) => unreachable!("points handled above"),
    })
}

impl PyGeometryArray {
    pub(crate) fn centroid_unary_packed(&self) -> PyResult<Self> {
        // A point is its own centroid; packed rows skip the per-row map.
        if let Some(points) = self.packed_points_xy() {
            return Ok(points);
        }
        let frame = self.frame.clone();
        let geographic = crate::geometry::is_geographic_frame(&self.frame);
        if let Some(points) = Python::attach(|py| {
            packed_point_unary(
                self,
                py,
                frame,
                geographic,
                false,
                Shape::centroid,
                |_, xs, ys, window| {
                    packed_centroid_xy(crate::geometry::centroid_line_row_columns(
                        column_window(xs, &window),
                        column_window(ys, &window),
                    ))
                },
                |_, xs, ys, ring_offsets, rings, _| {
                    packed_centroid_xy(crate::geometry::centroid_polygon_row_columns(
                        xs,
                        ys,
                        ring_offsets,
                        rings,
                    ))
                },
            )
        })? {
            return Ok(points);
        }
        self.map_shapes(|shape| {
            Ok(crate::geometry::unary_antimeridian_derived(
                shape,
                geographic,
                false,
                Shape::centroid,
            )?)
        })
    }

    pub(crate) fn point_on_surface_unary_packed(&self) -> PyResult<Self> {
        if let Some(points) = self.packed_points_xy() {
            return Ok(points);
        }
        let frame = self.frame.clone();
        let geographic = crate::geometry::is_geographic_frame(&self.frame);
        if let Some(points) = Python::attach(|py| {
            packed_point_unary(
                self,
                py,
                frame,
                geographic,
                true,
                Shape::point_on_surface,
                |_, xs, ys, window| {
                    packed_surface_point(crate::geometry::point_on_surface_line_columns(
                        column_window(xs, &window),
                        column_window(ys, &window),
                    ))
                },
                |_, xs, ys, ring_offsets, rings, shell| {
                    let row_bounds = shell.map(|shell| {
                        let [minx, miny, maxx, maxy] = row_bounds_values(xs, ys, shell);
                        Bounds::new_unchecked(minx, miny, maxx, maxy)
                    });
                    packed_surface_point(crate::geometry::point_on_surface_polygon_row_columns(
                        xs,
                        ys,
                        ring_offsets,
                        rings,
                        row_bounds,
                    ))
                },
            )
        })? {
            return Ok(points);
        }
        self.map_shapes(|shape| {
            Ok(crate::geometry::unary_antimeridian_derived(
                shape,
                geographic,
                true,
                Shape::point_on_surface,
            )?)
        })
    }

    pub(crate) fn envelope_unary_packed(&self) -> PyResult<Self> {
        if let Some(points) = self.packed_points_xy() {
            return Ok(points);
        }
        // All-rectangular fast path: when every row's box is NON-degenerate
        // (`bounds_to_shape` would make a `Polygon`, not a `Point`/`LineString`),
        // write the 5 box vertices per row straight into packed `Polygon`
        // columns — no intermediate box `Shape`s and no `from_shapes` repack.
        // Matches `rect_polygon`'s vertex order exactly. The envelope of a box is
        // itself, so the per-element bounds carry over as the result's cache.
        if let Some(bounds) = self.cached_element_bounds()
            && bounds.iter().all(|b| {
                b.is_some_and(|bb| {
                    !crate::geometry::same_topological_coordinate(bb.minx(), bb.maxx())
                        && !crate::geometry::same_topological_coordinate(bb.miny(), bb.maxy())
                })
            })
        {
            let rows = bounds.len();
            let mut xs = Vec::with_capacity(rows * 5);
            let mut ys = Vec::with_capacity(rows * 5);
            for b in bounds.iter() {
                let bb = b.expect("all bounds present (checked)");
                xs.extend_from_slice(&[bb.minx(), bb.maxx(), bb.maxx(), bb.minx(), bb.minx()]);
                ys.extend_from_slice(&[bb.miny(), bb.miny(), bb.maxy(), bb.maxy(), bb.miny()]);
            }
            let ring_offsets = CsrOffsetColumn::<RingLevel>::try_new(
                (0..=rows).map(|row| row * 5).collect(),
                rows * 5,
            )?;
            let polygon_offsets =
                CsrOffsetColumn::<PolygonLevel>::try_new((0..=rows).collect(), rows)?;
            return Ok(Self::from_result_storage(
                Arc::new(GeometryArrayStorage::Polygons {
                    coords: Arc::new(CoordSeq::from_vecs(xs, ys, None, None)),
                    ring_offsets,
                    polygon_offsets,
                    row_map: RowSelection::Identity,
                }),
                self.frame.clone(),
                self.missing().cloned(),
                // The output rectangles have the same per-row bounds as the
                // input (envelope of a box is the box), so the bounds carry
                // over — but they are DIFFERENT geometry, so the prepared
                // handles must NOT alias the input's lazily-filled slots.
                Arc::clone(&self.bounds_cache),
                Arc::clone(&self.total_bounds_cache),
            ));
        }
        // Packed polygons/lines: build each box from its coordinate window
        // directly — no per-row Shape view (which clones the ring coords).
        if let Some(boxes) = self.storage().envelope_box_shapes() {
            return Ok(Self::from_shapes(boxes, self.frame.clone()));
        }
        self.map_shapes(|shape| Ok(shape.envelope()))
    }

    pub(crate) fn convex_hull_unary_packed(&self, py: Python<'_>) -> PyResult<Self> {
        if let Some(identity) = self.packed_points_identity() {
            return Ok(identity);
        }
        self.map_shapes_detached(py, Shape::convex_hull)
    }
}
