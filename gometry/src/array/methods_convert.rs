use crate::array::packed_columns::{LineColumns, PolygonColumns};
use crate::array::{
    Arc, Bounds, ComputedCaches, CoordSeq, F64Param, GeometryArrayStorage, OriginSpec,
    PackedColumnError, PackedColumns, PackedRow, PackedStorage, PyErr, PyGeometryArray, PyResult,
    Python, Result, RowSelection, Shape, StorageLayout, clip_lines_packed_xy, geometry,
    note_array_row, reverse_coord_windows, subdivide_line_columns, subdivide_polygon_columns,
    validate_densify_fraction, validate_max_segment_length,
};
use crate::geographic::GeographicFrame;
use crate::geometry::{ExpansionBudget, ShapeCountQuery, TopologyDomain};

fn verify_planar_segmentize_array(
    result: &PyGeometryArray,
    max_segment_length: &F64Param,
    to_metre: f64,
) -> PyResult<()> {
    match result.storage() {
        GeometryArrayStorage::Packed(PackedStorage::Points { .. }) => Ok(()),
        GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords,
            offsets,
            row_map,
        }) => {
            for row in 0..result.len() {
                if result.is_row_missing(row) {
                    continue;
                }
                let packed_row = PackedRow::line(row, coords, offsets, row_map.as_deref());
                let window = packed_row
                    .vertex_window()
                    .expect("packed line row has a coordinate window");
                verify_planar_segmentize_window(
                    coords,
                    window.start,
                    window.end,
                    max_segment_length.get(row) / to_metre,
                    row,
                )?;
            }
            Ok(())
        },
        GeometryArrayStorage::Packed(PackedStorage::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
            row_map,
        }) => {
            for row in 0..result.len() {
                if result.is_row_missing(row) {
                    continue;
                }
                let packed_row = PackedRow::polygon(
                    row,
                    coords,
                    ring_offsets,
                    polygon_offsets,
                    row_map.as_deref(),
                );
                for ring in packed_row
                    .polygon_rings()
                    .expect("packed polygon row has a ring range")
                {
                    let window = packed_row
                        .polygon_ring_window(ring)
                        .expect("ring lies in its admitted polygon row");
                    verify_planar_segmentize_window(
                        coords,
                        window.start,
                        window.end,
                        max_segment_length.get(row) / to_metre,
                        row,
                    )?;
                }
            }
            Ok(())
        },
        GeometryArrayStorage::Mixed(_) => {
            for row in result.row_operation().present_rows() {
                let logical = row.logical();
                row.row()
                    .with_shape(|shape| {
                        crate::geometry::verify_planar_segmentize_shape(
                            shape,
                            max_segment_length.get(logical) / to_metre,
                        )
                    })
                    .map_err(PyErr::from)?
                    .map_err(|error| note_array_row(error.into(), logical))?;
            }
            Ok(())
        },
    }
}

fn verify_planar_segmentize_window(
    coords: &CoordSeq,
    start: usize,
    end: usize,
    max_segment_length: f64,
    row: usize,
) -> PyResult<()> {
    let window = coords.view(crate::geometry::CoordWindow::from_range(
        start..end,
        coords.len(),
    ));
    crate::geometry::verify_planar_segmentize_output(&window, max_segment_length)
        .map_err(|error| note_array_row(error.into(), row))?;
    Ok(())
}

#[derive(Clone, Copy)]
pub(crate) struct GridSize {
    pub x: f64,
    pub y: f64,
}

#[derive(Clone, Copy)]
pub(crate) struct GridOrigin {
    pub x: f64,
    pub y: f64,
}

impl GridSize {
    const fn tuple(self) -> (f64, f64) {
        (self.x, self.y)
    }
}

impl GridOrigin {
    const fn tuple(self) -> (f64, f64) {
        (self.x, self.y)
    }
}

impl From<(f64, f64)> for GridSize {
    fn from((x, y): (f64, f64)) -> Self {
        Self { x, y }
    }
}

impl From<(f64, f64)> for GridOrigin {
    fn from((x, y): (f64, f64)) -> Self {
        Self { x, y }
    }
}

impl PyGeometryArray {
    /// Packed `segmentize` under a resolved placement.
    ///
    /// The placement is captured by VALUE (a `Geodesic` is a handful of `f64`
    /// ellipsoid constants, cheap to clone and trivially `Send`) because the
    /// packed lane hands its kernels to a detached worker: a borrowed
    /// placement could not satisfy the `Send + 'static` bound.
    #[expect(
        clippy::large_types_passed_by_value,
        reason = "the packed kernels are `Send + 'static` closures, so the ellipsoid must be \
                  owned rather than borrowed; it is `Copy` and resolved once per call"
    )]
    pub(crate) fn segmentize_unary_packed(
        &self,
        max_segment_length: &F64Param,
        geodesic: Option<(geographiclib_rs::Geodesic, GeographicFrame)>,
        to_metre: f64,
    ) -> PyResult<Self> {
        let for_columns = geodesic;
        let for_shapes = geodesic;
        let result = self.subdivide_unary_packed(
            "segmentize",
            "max_length",
            max_segment_length,
            |value| validate_max_segment_length(value).map(|_| ()),
            move |points, length, budget| {
                if let Some((_, frame)) = for_columns {
                    crate::crs::ensure_geographic_columns_in_frame(
                        frame,
                        points.xs(),
                        points.ys(),
                    )?;
                }
                crate::geometry::segmentize_points_budgeted(
                    points,
                    length / to_metre,
                    for_columns.as_ref().map_or(
                        crate::geometry::SegmentPlacement::Planar,
                        |(geodesic, frame)| {
                            crate::geometry::SegmentPlacement::Geodesic(geodesic, *frame)
                        },
                    ),
                    budget,
                )
            },
            move |shape, length, budget| {
                if let Some((_, frame)) = for_shapes {
                    crate::crs::ensure_geographic_domain_in_frame(frame, shape)?;
                }
                shape.segmentize_budgeted(
                    length / to_metre,
                    for_shapes.as_ref().map_or(
                        crate::geometry::SegmentPlacement::Planar,
                        |(geodesic, frame)| {
                            crate::geometry::SegmentPlacement::Geodesic(geodesic, *frame)
                        },
                    ),
                    budget,
                )
            },
        )?;
        if geodesic.is_none() {
            verify_planar_segmentize_array(&result, max_segment_length, to_metre)?;
        }
        Ok(result)
    }

    #[expect(
        clippy::large_types_passed_by_value,
        reason = "the packed kernels are detached, so the resolved ellipsoid is owned once per call"
    )]
    pub(crate) fn densify_unary_packed(
        &self,
        fraction: &F64Param,
        geodesic: Option<(geographiclib_rs::Geodesic, GeographicFrame)>,
    ) -> PyResult<Self> {
        let for_columns = geodesic;
        let for_shapes = geodesic;
        self.subdivide_unary_packed(
            "densify",
            "fraction",
            fraction,
            |value| validate_densify_fraction(value).map(|_| ()),
            move |points, fraction, budget| {
                if let Some((_, frame)) = for_columns {
                    crate::crs::ensure_geographic_columns_in_frame(
                        frame,
                        points.xs(),
                        points.ys(),
                    )?;
                }
                crate::geometry::densify_points_budgeted_with_placement(
                    points,
                    fraction,
                    for_columns.as_ref().map_or(
                        crate::geometry::SegmentPlacement::Planar,
                        |(geodesic, frame)| {
                            crate::geometry::SegmentPlacement::Geodesic(geodesic, *frame)
                        },
                    ),
                    budget,
                )
            },
            move |shape, fraction, budget| {
                if let Some((_, frame)) = for_shapes {
                    crate::crs::ensure_geographic_domain_in_frame(frame, shape)?;
                }
                shape.densified_budgeted_with_placement(
                    fraction,
                    for_shapes.as_ref().map_or(
                        crate::geometry::SegmentPlacement::Planar,
                        |(geodesic, frame)| {
                            crate::geometry::SegmentPlacement::Geodesic(geodesic, *frame)
                        },
                    ),
                    budget,
                )
            },
        )
    }

    fn subdivide_unary_packed(
        &self,
        operation: &'static str,
        parameter: &'static str,
        param: &F64Param,
        validate: impl Fn(f64) -> PyResult<()>,
        subdivide: impl Fn(&CoordSeq, f64, &mut ExpansionBudget) -> Result<CoordSeq>
        + Copy
        + Send
        + 'static,
        shape_fallback: impl Fn(&Shape, f64, &mut ExpansionBudget) -> Result<Shape>
        + Copy
        + Send
        + 'static,
    ) -> PyResult<Self> {
        param.try_validate(validate)?;
        if let Some(identity) = self.packed_points_identity() {
            return Ok(identity);
        }
        // Packed lines/polygons: one CSR window per row (or ring), columnar
        // subdivision, rebuilt offsets only when vertex counts change.
        // Per-element parameters keep the per-shape route.
        if !self.has_missing()
            && let Some(value) = param.as_scalar()
            && let Some(subdivided) = Python::attach(|py| {
                self.map_packed_columns_detached(py, self.frame.clone(), move |columns| {
                    match columns {
                        PackedColumns::Lines(line_columns) => subdivide_line_columns(
                            &line_columns,
                            operation,
                            parameter,
                            |points, budget| subdivide(points, value, budget),
                        )
                        .map_err(PackedColumnError::Batch),
                        PackedColumns::Polygons(polygon_columns) => subdivide_polygon_columns(
                            &polygon_columns,
                            operation,
                            parameter,
                            |points, budget| subdivide(points, value, budget),
                        )
                        .map_err(PackedColumnError::Batch),
                        PackedColumns::Points(_) => unreachable!("points identity above"),
                    }
                })
            })?
        {
            return Ok(subdivided);
        }
        Python::attach(|py| {
            self.map_shapes_detached_indexed_budgeted(
                py,
                operation,
                parameter,
                move |shape, row, budget| shape_fallback(shape, param.get(row), budget),
            )
        })
    }

    pub(crate) fn affine_transform_unary_packed(&self, matrix: [f64; 6]) -> Option<PyResult<Self>> {
        Python::attach(|py| self.packed_affine(py, &matrix).transpose())
    }

    pub(crate) fn rotate_unary_packed(
        &self,
        spec: OriginSpec,
        angle: f64,
    ) -> Option<PyResult<Self>> {
        Python::attach(|py| {
            let (sin, cos) = angle.sin_cos();
            self.packed_self_origin_affine(py, spec, [cos, -sin, sin, cos])
                .transpose()
        })
    }

    pub(crate) fn scale_unary_packed(
        &self,
        spec: OriginSpec,
        xfact: f64,
        yfact: f64,
    ) -> Option<PyResult<Self>> {
        Python::attach(|py| {
            self.packed_self_origin_affine(py, spec, [xfact, 0.0, 0.0, yfact])
                .transpose()
        })
    }

    pub(crate) fn skew_unary_packed(
        &self,
        spec: OriginSpec,
        tan_x: f64,
        tan_y: f64,
    ) -> Option<PyResult<Self>> {
        Python::attach(|py| {
            self.packed_self_origin_affine(py, spec, [1.0, tan_x, tan_y, 1.0])
                .transpose()
        })
    }

    pub(crate) fn reverse_unary_packed(&self) -> PyResult<Self> {
        if let Some(identity) = self.packed_points_identity() {
            return Ok(identity);
        }
        match self.storage() {
            GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            }) => {
                let (out_coords, out_offsets, out_map) = if row_map.is_identity() {
                    (
                        reverse_coord_windows(coords, offsets)?,
                        offsets.clone(),
                        row_map.clone(),
                    )
                } else {
                    let (out_coords, out_offsets) =
                        LineColumns::from_parts(coords, offsets, row_map.as_deref())
                            .reverse_selected()?;
                    (out_coords, out_offsets, RowSelection::Identity)
                };
                Ok(Self::from_result_storage(
                    Arc::new(GeometryArrayStorage::Packed(PackedStorage::Lines {
                        coords: Arc::new(out_coords),
                        offsets: out_offsets,
                        row_map: out_map,
                    })),
                    self.frame.clone(),
                    self.missing().cloned(),
                    StorageLayout::LogicalRows,
                    ComputedCaches::carried(
                        Arc::clone(&self.bounds_cache),
                        Arc::clone(&self.total_bounds_cache),
                    ),
                )?)
            },
            GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            }) => {
                let (out_coords, out_rings, out_polygons, out_map) = if row_map.is_identity() {
                    (
                        reverse_coord_windows(coords, ring_offsets)?,
                        ring_offsets.clone(),
                        polygon_offsets.clone(),
                        row_map.clone(),
                    )
                } else {
                    let (out_coords, out_rings, out_polygons) = PolygonColumns::from_parts(
                        coords,
                        ring_offsets,
                        polygon_offsets,
                        row_map.as_deref(),
                    )
                    .reverse_selected()?;
                    (out_coords, out_rings, out_polygons, RowSelection::Identity)
                };
                Ok(Self::from_result_storage(
                    Arc::new(GeometryArrayStorage::Packed(PackedStorage::Polygons {
                        coords: Arc::new(out_coords),
                        ring_offsets: out_rings,
                        polygon_offsets: out_polygons,
                        row_map: out_map,
                    })),
                    self.frame.clone(),
                    self.missing().cloned(),
                    StorageLayout::LogicalRows,
                    ComputedCaches::carried(
                        Arc::clone(&self.bounds_cache),
                        Arc::clone(&self.total_bounds_cache),
                    ),
                )?)
            },
            _ => self.map_shapes(|shape| Ok(shape.reverse()?)),
        }
    }

    pub(crate) fn swap_xy_unary_packed(&self) -> PyResult<Self> {
        self.swap_xy_packed_storage()?
            .map_or_else(|| self.map_shapes(|shape| Ok(shape.swap_xy()?)), Ok)
    }

    pub(crate) fn unique_points_unary_packed(&self, py: Python<'_>) -> PyResult<Self> {
        // A packed array can still contain a million-vertex row; walk the
        // pure-Rust deduplication detached just like the packed linref lanes.
        self.map_shapes_detached(py, |shape| shape.unique_points())
    }

    pub(crate) fn orient_polygons_unary_packed(&self, ccw: bool) -> PyResult<Self> {
        self.map_shapes(|shape| Ok(shape.orient_polygons(!ccw)?))
    }

    pub(crate) fn remove_repeated_points_unary_packed(
        &self,
        _py: Python<'_>,
        tolerance: &F64Param,
    ) -> PyResult<Self> {
        if let Some(identity) = self.packed_points_identity() {
            return Ok(identity);
        }
        Python::attach(|py| {
            self.map_shapes_shared_detached_indexed(py, move |shape, row| {
                let cleaned = shape.remove_repeated_points(tolerance.get(row))?;
                let mut counts = ShapeCountQuery::default();
                let cleaned_count = counts.coordinates(&cleaned)?;
                let source_count = counts.coordinates(shape)?;
                Ok((cleaned_count != source_count).then_some(cleaned))
            })
        })
    }

    pub(crate) fn snap_to_grid_unary_packed(
        &self,
        _py: Python<'_>,
        size: impl Into<GridSize>,
        origin: impl Into<GridOrigin>,
        repair: bool,
    ) -> Option<PyResult<Self>> {
        let size = size.into();
        let origin = origin.into();
        if repair {
            let domain = match geometry::topology_domain(&self.frame) {
                Ok(domain) => domain,
                Err(error) => return Some(Err(error.into())),
            };
            return Some(Python::attach(|py| {
                self.map_shapes_detached(py, move |shape| {
                    shape.snap_to_grid_repaired(size.tuple(), origin.tuple(), domain)
                })
            }));
        }
        let GeometryArrayStorage::Packed(packed) = self.storage() else {
            return None;
        };
        let axes = packed.axes();
        Python::attach(|py| {
            self.map_packed_points_detached(py, self.frame.clone(), axes, move |point| {
                let (sx, sy) = size.tuple();
                let (ox, oy) = origin.tuple();
                point.with_xy(
                    geometry::stable_snap_ordinate(point.x, ox, sx),
                    geometry::stable_snap_ordinate(point.y, oy, sy),
                )
            })
            .transpose()
        })
    }

    pub(crate) fn force_2d_unary_packed(&self, _py: Python<'_>) -> PyResult<Self> {
        if !self.has_z() && !self.has_m() {
            return Ok(self.clone());
        }
        Python::attach(|py| {
            if let Some(array) = self.map_packed_points_detached(
                py,
                self.frame.clone(),
                crate::CoordinateAxes::XY,
                |point| Ok(point.force_2d()),
            )? {
                return Ok(array);
            }
            self.map_shapes_shared_detached(py, |shape| {
                let forced = shape.force_2d()?;
                Ok((forced != *shape).then_some(forced))
            })
        })
    }

    pub(crate) fn normalize_unary_packed(&self, _py: Python<'_>) -> PyResult<Self> {
        if let Some(identity) = self.packed_points_identity() {
            return Ok(identity);
        }
        Python::attach(|py| {
            self.map_shapes_shared_detached(py, |shape| {
                let normalized = shape.normalize()?;
                Ok((normalized != *shape).then_some(normalized))
            })
        })
    }

    pub(crate) fn force_3d_unary_packed(&self, py: Python<'_>, z: f64) -> PyResult<Self> {
        self.set_z_unary_packed(py, Some(z), false)
    }

    pub(crate) fn set_z_unary_packed(
        &self,
        py: Python<'_>,
        value: Option<f64>,
        overwrite: bool,
    ) -> PyResult<Self> {
        self.set_z_impl(py, value, overwrite)
    }

    pub(crate) fn set_m_unary_packed(
        &self,
        py: Python<'_>,
        value: Option<f64>,
        overwrite: bool,
    ) -> PyResult<Self> {
        self.set_m_impl(py, value, overwrite)
    }

    pub(crate) fn quantize_unary_packed(&self, _py: Python<'_>, precision: i32) -> PyResult<Self> {
        if let Some(axes) = self.storage().packed_axes()
            && let Some(mapped) = Python::attach(|py| {
                self.map_packed_points_detached(py, self.frame.clone(), axes, move |point| {
                    Ok(point.quantize(precision))
                })
            })?
        {
            return Ok(mapped);
        }
        Python::attach(|py| {
            self.map_shapes_shared_detached(py, move |shape| {
                let quantized = shape.quantize(precision)?;
                Ok((quantized != *shape).then_some(quantized))
            })
        })
    }

    pub(crate) fn clip_by_rect_unary_packed(
        &self,
        py: Python<'_>,
        rect: Bounds,
        domain: TopologyDomain,
    ) -> PyResult<Self> {
        if matches!(
            self.storage(),
            GeometryArrayStorage::Packed(PackedStorage::Lines { .. })
        ) {
            let clipped =
                self.try_map_packed_columns_detached(py, self.frame.clone(), move |columns| {
                    match columns {
                        PackedColumns::Lines(lines) => clip_lines_packed_xy(&lines, rect, domain),
                        _ => Ok(None),
                    }
                })?;
            if let Some(result) = clipped {
                return Ok(result);
            }
        }
        self.map_shapes(|shape| {
            if let crate::geometry::TopologyDomain::Geographic(frame) = domain {
                Ok(shape.clip_by_rect_in_geographic_frame(rect, false, shape.bounds(), frame)?)
            } else {
                Ok(shape.clip_by_rect(rect, false)?)
            }
        })
    }
}
