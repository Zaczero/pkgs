use crate::array::{
    Arc, Bounds, CoordSeq, F64Param, GeometryArrayStorage, OriginSpec, PackedColumnError,
    PackedColumns, PyGeometryArray, PyResult, Python, Result, Shape, affine_about, geometry,
    reverse_coord_windows, subdivide_line_columns, subdivide_polygon_columns,
    validate_densify_fraction, validate_max_segment_length,
};
use crate::geometry::ExpansionBudget;

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
        geodesic: Option<geographiclib_rs::Geodesic>,
        to_metre: f64,
    ) -> PyResult<Self> {
        let for_columns = geodesic;
        let for_shapes = geodesic;
        self.subdivide_unary_packed(
            "segmentize",
            "max_segment_length",
            max_segment_length,
            |value| validate_max_segment_length(value).map(|_| ()),
            move |points, length, budget| {
                crate::geometry::segmentize_points_budgeted(
                    points,
                    length / to_metre,
                    for_columns.as_ref().map_or(
                        crate::geometry::SegmentPlacement::Planar,
                        crate::geometry::SegmentPlacement::Geodesic,
                    ),
                    budget,
                )
            },
            move |shape, length, budget| {
                shape.segmentize_budgeted(
                    length / to_metre,
                    for_shapes.as_ref().map_or(
                        crate::geometry::SegmentPlacement::Planar,
                        crate::geometry::SegmentPlacement::Geodesic,
                    ),
                    budget,
                )
            },
        )
    }

    pub(crate) fn densify_unary_packed(&self, fraction: &F64Param) -> PyResult<Self> {
        self.subdivide_unary_packed(
            "densify",
            "fraction",
            fraction,
            |value| validate_densify_fraction(value).map(|_| ()),
            crate::geometry::densify_points_budgeted,
            Shape::densified_budgeted,
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
            self.packed_self_origin_affine(py, spec, || {
                let (sin, cos) = angle.sin_cos();
                affine_about(cos, -sin, sin, cos, (0.0, 0.0))
            })
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
            self.packed_self_origin_affine(py, spec, || {
                affine_about(xfact, 0.0, 0.0, yfact, (0.0, 0.0))
            })
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
            self.packed_self_origin_affine(py, spec, || {
                affine_about(1.0, tan_x, tan_y, 1.0, (0.0, 0.0))
            })
            .transpose()
        })
    }

    pub(crate) fn reverse_unary_packed(&self) -> Self {
        if let Some(identity) = self.packed_points_identity() {
            return identity;
        }
        match self.storage() {
            GeometryArrayStorage::Lines {
                coords,
                offsets,
                row_map,
            } => Self::from_result_storage(
                Arc::new(GeometryArrayStorage::Lines {
                    coords: Arc::new(reverse_coord_windows(coords, offsets)),
                    offsets: offsets.clone(),
                    row_map: row_map.clone(),
                }),
                self.frame.clone(),
                self.missing().cloned(),
                Arc::clone(&self.bounds_cache),
                Arc::clone(&self.total_bounds_cache),
            ),
            GeometryArrayStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            } => Self::from_result_storage(
                Arc::new(GeometryArrayStorage::Polygons {
                    coords: Arc::new(reverse_coord_windows(coords, ring_offsets)),
                    ring_offsets: ring_offsets.clone(),
                    polygon_offsets: polygon_offsets.clone(),
                    row_map: row_map.clone(),
                }),
                self.frame.clone(),
                self.missing().cloned(),
                Arc::clone(&self.bounds_cache),
                Arc::clone(&self.total_bounds_cache),
            ),
            _ => self.map_shapes_infallible(Shape::reverse),
        }
    }

    pub(crate) fn swap_xy_unary_packed(&self) -> Self {
        self.swap_xy_packed_storage()
            .unwrap_or_else(|| self.map_shapes_infallible(Shape::swap_xy))
    }

    pub(crate) fn unique_points_unary_packed(&self, py: Python<'_>) -> PyResult<Self> {
        // A packed array can still contain a million-vertex row; walk the
        // pure-Rust deduplication detached just like the packed linref lanes.
        self.map_shapes_detached(py, |shape| Ok(shape.unique_points()))
    }

    pub(crate) fn orient_polygons_unary_packed(&self, ccw: bool) -> Self {
        self.map_shapes_infallible(|shape| shape.orient_polygons(!ccw))
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
                Ok((cleaned.coord_count() != shape.coord_count()).then_some(cleaned))
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
            let geographic = geometry::is_geographic_frame(&self.frame);
            return Some(Python::attach(|py| {
                self.map_shapes_detached(py, move |shape| {
                    shape.snap_to_grid_repaired(size.tuple(), origin.tuple(), geographic)
                })
            }));
        }
        Python::attach(|py| {
            self.map_packed_coordseq_detached(py, self.frame.clone(), move |coords| {
                coords.try_snap_to_grid(size.tuple(), origin.tuple())
            })
            .transpose()
        })
    }

    pub(crate) fn force_2d_unary_packed(&self, _py: Python<'_>) -> PyResult<Self> {
        if !self.has_z() && !self.has_m() {
            return Ok(self.clone());
        }
        Python::attach(|py| {
            if let Some(array) =
                self.map_packed_coordseq_detached(py, self.frame.clone(), |coords| {
                    Ok(coords.force_2d())
                })?
            {
                return Ok(array);
            }
            self.map_shapes_shared_detached(py, |shape| {
                let forced = shape.force_2d();
                Ok((forced != *shape).then_some(forced))
            })
        })
    }

    pub(crate) fn normalize_unary_packed(&self, _py: Python<'_>) -> Self {
        if let Some(identity) = self.packed_points_identity() {
            return identity;
        }
        Python::attach(|py| {
            self.map_shapes_shared_detached(py, |shape| {
                let normalized = shape.normalize();
                Ok((normalized != *shape).then_some(normalized))
            })
        })
        .expect("normalize is infallible")
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
        if let Some(mapped) = Python::attach(|py| {
            self.map_packed_coordseq_detached(py, self.frame.clone(), move |coords| {
                Ok(coords.quantize(precision))
            })
        })? {
            return Ok(mapped);
        }
        Python::attach(|py| {
            self.map_shapes_shared_detached(py, move |shape| {
                let quantized = shape.quantize(precision);
                Ok((quantized != *shape).then_some(quantized))
            })
        })
    }

    pub(crate) fn clip_by_rect_unary_packed(&self, rect: Bounds, drop: bool) -> PyResult<Self> {
        let geographic = crate::geometry::is_geographic_frame(&self.frame);
        self.map_shapes(|shape| {
            if geographic && shape.crosses_antimeridian() {
                Ok(shape.split_antimeridian()?.clip_by_rect(rect, drop)?)
            } else {
                Ok(shape.clip_by_rect(rect, drop)?)
            }
        })
    }
}
