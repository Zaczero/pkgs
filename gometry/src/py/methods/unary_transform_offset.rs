#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
use crate::dispatch::{MetricOp, kernels};
use crate::geometry::DEFAULT_MITER_LIMIT;
use crate::py::errors::GeometryError;
use crate::py::methods::unary_transform_methods::{
    Bound, Bounds, BufferCapStyle, BufferJoinStyle, BufferSide, DefaultedF64Input,
    DefaultedI64Input, DistanceUnit, F64Param, GridOrigin, NavigationPath, PyAny, PyGeometry,
    PyGeometryArray, PyResult, Python, QUADRANT_SEGMENTS_DEFAULT_I64, SimplifyMethod, SmoothMethod,
    finite_f64_required, parse_grid_size, pymethods, validate_buffer_miter_limit,
    validate_buffer_quadrant_segments, validate_densify_fraction, validate_max_segment_length,
    validate_smooth_iterations,
};
use crate::py::vectors::Groups;

fn validate_segmentize_units(
    fraction: bool,
    path: Option<NavigationPath>,
    unit: Option<DistanceUnit>,
) -> PyResult<()> {
    if path.is_some() && unit.is_some() {
        return Err(GeometryError::new_err(
            "segmentize does not accept unit when path is set; route distances are meters",
        ));
    }
    if fraction && unit.is_some() {
        return Err(GeometryError::new_err(
            "segmentize fraction= subdivides by a share of each segment and has no unit; \
             pass max_length= to use unit=",
        ));
    }
    Ok(())
}

#[pymethods]
impl PyGeometry {
    #[doc = doc_buffer!(scalar)]
    #[pyo3(
        signature = (distance, *, cap_style = BufferCapStyle::Round, join_style = BufferJoinStyle::Round, quadrant_segments = QUADRANT_SEGMENTS_DEFAULT_I64, miter_limit = DEFAULT_MITER_LIMIT, side = BufferSide::Both, unit = None),
        text_signature = "($self, distance, *, cap_style='round', join_style='round', quadrant_segments=8, miter_limit=5.0, side='both', unit=None)"
    )]
    pub fn buffer(
        &self,
        py: Python<'_>,
        distance: &Bound<'_, PyAny>,
        cap_style: BufferCapStyle,
        join_style: BufferJoinStyle,
        #[pyo3(from_py_with = crate::strict_i64)] quadrant_segments: i64,
        miter_limit: f64,
        side: BufferSide,
        unit: Option<DistanceUnit>,
    ) -> PyResult<crate::Typed> {
        let distance = F64Param::parse_raw(distance, "distance", unary_len!(scalar))?;
        distance.ensure_finite("distance")?;
        let quadrant_segments = validate_buffer_quadrant_segments(quadrant_segments)?;
        let miter_limit = validate_buffer_miter_limit(miter_limit)?;
        unary_spine_shapes!(
            scalar,
            py,
            self,
            crate::dispatch::Operation::Metric(MetricOp::Buffer),
            unit,
            default,
            move |data, ctx| {
                kernels::unary_buffer(
                    data,
                    ctx,
                    &distance,
                    cap_style,
                    join_style,
                    quadrant_segments,
                    miter_limit,
                    side,
                )
            }
        )
    }

    #[doc = doc_offset_curve!(scalar)]
    #[pyo3(
        signature = (distance, *, join_style = BufferJoinStyle::Round, quadrant_segments = QUADRANT_SEGMENTS_DEFAULT_I64, miter_limit = DEFAULT_MITER_LIMIT, unit = None),
        text_signature = "($self, distance, *, join_style='round', quadrant_segments=8, miter_limit=5.0, unit=None)"
    )]
    pub fn offset_curve(
        &self,
        py: Python<'_>,
        distance: &Bound<'_, PyAny>,
        join_style: BufferJoinStyle,
        #[pyo3(from_py_with = crate::strict_i64)] quadrant_segments: i64,
        miter_limit: f64,
        unit: Option<DistanceUnit>,
    ) -> PyResult<crate::Typed> {
        let distance = F64Param::parse_raw(distance, "distance", unary_len!(scalar))?;
        distance.ensure_finite("distance")?;
        let quadrant_segments = validate_buffer_quadrant_segments(quadrant_segments)?;
        let miter_limit = validate_buffer_miter_limit(miter_limit)?;
        unary_spine_shapes!(
            scalar,
            py,
            self,
            crate::dispatch::Operation::Metric(MetricOp::OffsetCurve),
            unit,
            default,
            move |data, ctx| {
                kernels::unary_offset_curve(
                    data,
                    ctx,
                    &distance,
                    join_style,
                    quadrant_segments,
                    miter_limit,
                )
            }
        )
    }

    #[doc = doc_simplify!(scalar)]
    #[pyo3(
        signature = (tolerance, *, method = SimplifyMethod::Vw, preserve_topology = true),
        text_signature = "($self, tolerance, *, method='vw', preserve_topology=True)"
    )]
    pub fn simplify(
        &self,
        py: Python<'_>,
        tolerance: &Bound<'_, PyAny>,
        method: SimplifyMethod,
        preserve_topology: bool,
    ) -> PyResult<crate::Typed> {
        let tolerance = F64Param::parse_raw(tolerance, "tolerance", unary_len!(scalar))?;
        tolerance.require_non_negative("tolerance")?;
        unary_spine_shapes!(
            scalar,
            py,
            self,
            crate::dispatch::Operation::Simplify,
            None,
            default,
            move |data, ctx| {
                kernels::unary_simplify(data, ctx, &tolerance, method, preserve_topology)
            }
        )
    }

    #[doc = doc_smooth!(scalar)]
    #[pyo3(
        signature = (*, iterations = DefaultedI64Input::Default(2), method = SmoothMethod::Chaikin, keep_endpoints = true),
        text_signature = "($self, *, iterations=2, method='chaikin', keep_endpoints=True)"
    )]
    pub fn smooth(
        &self,
        py: Python<'_>,
        iterations: DefaultedI64Input,
        method: SmoothMethod,
        keep_endpoints: bool,
    ) -> PyResult<crate::Typed> {
        if method == SmoothMethod::CatmullRom && !keep_endpoints {
            return Err(crate::GeometryError::new_err(
                "keep_endpoints=False is invalid with method='catmull_rom': Catmull-Rom always interpolates its endpoints",
            ));
        }
        let iterations = iterations.resolve(py, "iterations", unary_len!(scalar))?;
        iterations.try_validate(|value| validate_smooth_iterations(value).map(|_| ()))?;
        unary_spine_shapes!(
            scalar,
            py,
            self,
            crate::dispatch::Operation::Smooth,
            None,
            default,
            move |data, ctx| {
                kernels::unary_smooth(data, ctx, &iterations, method, keep_endpoints)
            }
        )
    }

    #[doc = doc_snap_to_grid!(scalar)]
    #[pyo3(
        signature = (grid_size, *, origin = GridOrigin::DEFAULT, repair = false),
        text_signature = "($self, grid_size, *, origin=(0.0, 0.0), repair=False)"
    )]
    pub fn snap_to_grid(
        &self,
        py: Python<'_>,
        grid_size: &Bound<'_, PyAny>,
        origin: GridOrigin,
        repair: bool,
    ) -> PyResult<crate::Typed> {
        let grid_size = parse_grid_size(grid_size)?;
        let origin = origin.as_pair();
        let op = if repair {
            crate::dispatch::Operation::Repair
        } else {
            crate::dispatch::Operation::SnapToGrid
        };
        unary_spine_shapes!(scalar, py, self, op, None, default, move |data, ctx| {
            kernels::unary_snap_to_grid(data, ctx, grid_size, origin, repair)
        })
    }

    #[doc = doc_remove_repeated_points!(scalar)]
    #[pyo3(
        signature = (*, tolerance = DefaultedF64Input::Default(0.0)),
        text_signature = "($self, *, tolerance=0.0)"
    )]
    pub fn remove_repeated_points(
        &self,
        py: Python<'_>,
        tolerance: DefaultedF64Input,
    ) -> PyResult<crate::Typed> {
        let tolerance = tolerance.resolve_raw(py, "tolerance", unary_len!(scalar))?;
        tolerance.require_non_negative("tolerance")?;
        unary_spine_shapes!(
            scalar,
            py,
            self,
            crate::dispatch::Operation::RemoveRepeatedPoints,
            None,
            default,
            move |data, ctx| { kernels::unary_remove_repeated_points(data, ctx, &tolerance) }
        )
    }

    #[doc = doc_segmentize!(scalar)]
    #[pyo3(
        signature = (max_length = None, /, *, fraction = None, path = None, unit = None),
        text_signature = "($self, max_length=None, /, *, fraction=None, path=None, unit=None)"
    )]
    pub fn segmentize(
        &self,
        py: Python<'_>,
        max_length: Option<&Bound<'_, PyAny>>,
        fraction: Option<&Bound<'_, PyAny>>,
        path: Option<NavigationPath>,
        unit: Option<DistanceUnit>,
    ) -> PyResult<crate::Typed> {
        validate_segmentize_units(fraction.is_some(), path, unit)?;
        match (max_length, fraction) {
            (Some(max_length), None) => {
                let max_length = F64Param::parse(max_length, "max_length", unary_len!(scalar))?;
                max_length.try_validate(|value| validate_max_segment_length(value).map(|_| ()))?;
                unary_spine_shapes!(
                    scalar,
                    py,
                    self,
                    crate::dispatch::Operation::Metric(MetricOp::Segmentize),
                    unit,
                    default,
                    move |data, ctx| { kernels::unary_segmentize(data, ctx, &max_length, path) }
                )
            },
            (None, Some(fraction)) => {
                let fraction = F64Param::parse_raw(fraction, "fraction", unary_len!(scalar))?;
                fraction.try_validate(|value| validate_densify_fraction(value).map(|_| ()))?;
                unary_spine_shapes!(
                    scalar,
                    py,
                    self,
                    crate::dispatch::Operation::Metric(MetricOp::Segmentize),
                    None,
                    default,
                    move |data, ctx| { kernels::unary_densify(data, ctx, &fraction, path) }
                )
            },
            _ => Err(GeometryError::new_err(
                "segmentize requires exactly one of max_length or fraction",
            )),
        }
    }

    #[doc = doc_clip_by_rect!(scalar)]
    #[pyo3(
        signature = (minx, miny, maxx, maxy)
    )]
    pub fn clip_by_rect(
        &self,
        py: Python<'_>,
        minx: &Bound<'_, PyAny>,
        miny: &Bound<'_, PyAny>,
        maxx: &Bound<'_, PyAny>,
        maxy: &Bound<'_, PyAny>,
    ) -> PyResult<crate::Typed> {
        let minx = finite_f64_required("minx", minx)?;
        let miny = finite_f64_required("miny", miny)?;
        let maxx = finite_f64_required("maxx", maxx)?;
        let maxy = finite_f64_required("maxy", maxy)?;
        unary_spine_shapes!(
            scalar,
            py,
            self,
            crate::dispatch::Operation::ClipByRect,
            None,
            default,
            move |data, ctx| {
                let rect = match ctx.domain {
                    crate::geometry::TopologyDomain::Geographic(_) => {
                        Bounds::new_geographic(minx, miny, maxx, maxy)?
                    },
                    crate::geometry::TopologyDomain::Cartesian => {
                        Bounds::new(minx, miny, maxx, maxy)?
                    },
                };
                kernels::unary_clip_by_rect(data, ctx, rect, false)
            }
        )
    }

    #[doc = doc_subdivide!(scalar)]
    #[pyo3(signature = (*, max_vertices = 256))]
    pub fn subdivide(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = crate::strict_i64)] max_vertices: i64,
    ) -> PyResult<PyGeometryArray> {
        self.subdivide_parts(py, max_vertices)
    }
}

#[pymethods]
impl PyGeometryArray {
    #[doc = doc_buffer!(array)]
    #[pyo3(
        signature = (distance, *, cap_style = BufferCapStyle::Round, join_style = BufferJoinStyle::Round, quadrant_segments = QUADRANT_SEGMENTS_DEFAULT_I64, miter_limit = DEFAULT_MITER_LIMIT, side = BufferSide::Both, unit = None),
        text_signature = "($self, distance, *, cap_style='round', join_style='round', quadrant_segments=8, miter_limit=5.0, side='both', unit=None)"
    )]
    pub fn buffer(
        &self,
        py: Python<'_>,
        distance: &Bound<'_, PyAny>,
        cap_style: BufferCapStyle,
        join_style: BufferJoinStyle,
        #[pyo3(from_py_with = crate::strict_i64)] quadrant_segments: i64,
        miter_limit: f64,
        side: BufferSide,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Self> {
        let distance = F64Param::parse_raw(distance, "distance", unary_len!(array, self))?;
        distance.ensure_finite("distance")?;
        let quadrant_segments = validate_buffer_quadrant_segments(quadrant_segments)?;
        let miter_limit = validate_buffer_miter_limit(miter_limit)?;
        if let Some(result) =
            self.buffer_points_unary_packed(py, &distance, quadrant_segments, side, unit)?
        {
            return Ok(result);
        }
        crate::dispatch::unary_array_shapes_budgeted(
            py,
            self,
            crate::dispatch::Operation::Metric(MetricOp::Buffer),
            unit,
            "quadrant_segments",
            move |data, ctx, budget| {
                kernels::unary_buffer_budgeted(
                    data,
                    ctx,
                    &distance,
                    cap_style,
                    join_style,
                    quadrant_segments,
                    miter_limit,
                    side,
                    budget,
                )
            },
        )
    }

    #[doc = doc_offset_curve!(array)]
    #[pyo3(
        signature = (distance, *, join_style = BufferJoinStyle::Round, quadrant_segments = QUADRANT_SEGMENTS_DEFAULT_I64, miter_limit = DEFAULT_MITER_LIMIT, unit = None),
        text_signature = "($self, distance, *, join_style='round', quadrant_segments=8, miter_limit=5.0, unit=None)"
    )]
    pub fn offset_curve(
        &self,
        py: Python<'_>,
        distance: &Bound<'_, PyAny>,
        join_style: BufferJoinStyle,
        #[pyo3(from_py_with = crate::strict_i64)] quadrant_segments: i64,
        miter_limit: f64,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Self> {
        let distance = F64Param::parse_raw(distance, "distance", unary_len!(array, self))?;
        distance.ensure_finite("distance")?;
        let quadrant_segments = validate_buffer_quadrant_segments(quadrant_segments)?;
        let miter_limit = validate_buffer_miter_limit(miter_limit)?;
        crate::dispatch::unary_array_shapes_budgeted(
            py,
            self,
            crate::dispatch::Operation::Metric(MetricOp::OffsetCurve),
            unit,
            "quadrant_segments",
            move |data, ctx, budget| {
                kernels::unary_offset_curve_budgeted(
                    data,
                    ctx,
                    &distance,
                    join_style,
                    quadrant_segments,
                    miter_limit,
                    budget,
                )
            },
        )
    }

    #[doc = doc_simplify!(array)]
    #[pyo3(
        signature = (tolerance, *, method = SimplifyMethod::Vw, preserve_topology = true),
        text_signature = "($self, tolerance, *, method='vw', preserve_topology=True)"
    )]
    pub fn simplify(
        &self,
        py: Python<'_>,
        tolerance: &Bound<'_, PyAny>,
        method: SimplifyMethod,
        preserve_topology: bool,
    ) -> PyResult<Self> {
        let tolerance = F64Param::parse_raw(tolerance, "tolerance", unary_len!(array, self))?;
        tolerance.require_non_negative("tolerance")?;
        unary_spine_shapes_extras!(
            array,
            py,
            self,
            crate::dispatch::Operation::Simplify,
            None,
            crate::dispatch::PackedUnary::Simplify {
                tolerance: tolerance.clone(),
                method,
                preserve_topology,
            },
            move |data, ctx| {
                kernels::unary_simplify(data, ctx, &tolerance, method, preserve_topology)
            }
        )
    }

    #[doc = doc_smooth!(array)]
    #[pyo3(
        signature = (*, iterations = DefaultedI64Input::Default(2), method = SmoothMethod::Chaikin, keep_endpoints = true),
        text_signature = "($self, *, iterations=2, method='chaikin', keep_endpoints=True)"
    )]
    pub fn smooth(
        &self,
        py: Python<'_>,
        iterations: DefaultedI64Input,
        method: SmoothMethod,
        keep_endpoints: bool,
    ) -> PyResult<Self> {
        if method == SmoothMethod::CatmullRom && !keep_endpoints {
            return Err(crate::GeometryError::new_err(
                "keep_endpoints=False is invalid with method='catmull_rom': Catmull-Rom always interpolates its endpoints",
            ));
        }
        let iterations = iterations.resolve(py, "iterations", unary_len!(array, self))?;
        iterations.try_validate(|value| validate_smooth_iterations(value).map(|_| ()))?;
        crate::dispatch::unary_array_shapes_budgeted(
            py,
            self,
            crate::dispatch::Operation::Smooth,
            None,
            "iterations",
            move |data, ctx, budget| {
                kernels::unary_smooth_budgeted(
                    data,
                    ctx,
                    &iterations,
                    method,
                    keep_endpoints,
                    budget,
                )
            },
        )
    }

    #[doc = doc_snap_to_grid!(array)]
    #[pyo3(
        signature = (grid_size, *, origin = GridOrigin::DEFAULT, repair = false),
        text_signature = "($self, grid_size, *, origin=(0.0, 0.0), repair=False)"
    )]
    pub fn snap_to_grid(
        &self,
        py: Python<'_>,
        grid_size: &Bound<'_, PyAny>,
        origin: GridOrigin,
        repair: bool,
    ) -> PyResult<Self> {
        let grid_size = parse_grid_size(grid_size)?;
        let origin = origin.as_pair();
        let op = if repair {
            crate::dispatch::Operation::Repair
        } else {
            crate::dispatch::Operation::SnapToGrid
        };
        unary_spine_shapes_extras!(
            array,
            py,
            self,
            op,
            None,
            crate::dispatch::PackedUnary::SnapToGrid {
                size: grid_size,
                origin,
                repair,
            },
            move |data, ctx| { kernels::unary_snap_to_grid(data, ctx, grid_size, origin, repair) }
        )
    }

    #[doc = doc_remove_repeated_points!(array)]
    #[pyo3(
        signature = (*, tolerance = DefaultedF64Input::Default(0.0)),
        text_signature = "($self, *, tolerance=0.0)"
    )]
    pub fn remove_repeated_points(
        &self,
        py: Python<'_>,
        tolerance: DefaultedF64Input,
    ) -> PyResult<Self> {
        let tolerance = tolerance.resolve_raw(py, "tolerance", unary_len!(array, self))?;
        tolerance.require_non_negative("tolerance")?;
        unary_spine_shapes_extras!(
            array,
            py,
            self,
            crate::dispatch::Operation::RemoveRepeatedPoints,
            None,
            crate::dispatch::PackedUnary::RemoveRepeatedPoints {
                tolerance: tolerance.clone(),
            },
            move |data, ctx| { kernels::unary_remove_repeated_points(data, ctx, &tolerance) }
        )
    }

    #[doc = doc_segmentize!(array)]
    #[pyo3(
        signature = (max_length = None, /, *, fraction = None, path = None, unit = None),
        text_signature = "($self, max_length=None, /, *, fraction=None, path=None, unit=None)"
    )]
    pub fn segmentize(
        &self,
        py: Python<'_>,
        max_length: Option<&Bound<'_, PyAny>>,
        fraction: Option<&Bound<'_, PyAny>>,
        path: Option<NavigationPath>,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Self> {
        validate_segmentize_units(fraction.is_some(), path, unit)?;
        match (max_length, fraction) {
            (Some(max_length), None) => {
                let max_length =
                    F64Param::parse(max_length, "max_length", unary_len!(array, self))?;
                max_length.try_validate(|value| validate_max_segment_length(value).map(|_| ()))?;
                unary_spine_shapes_extras!(
                    array,
                    py,
                    self,
                    crate::dispatch::Operation::Metric(MetricOp::Segmentize),
                    unit,
                    crate::dispatch::PackedUnary::Segmentize {
                        max_segment_length: max_length.clone(),
                        path,
                    },
                    move |data, ctx| kernels::unary_segmentize(data, ctx, &max_length, path)
                )
            },
            (None, Some(fraction)) => {
                let fraction = F64Param::parse_raw(fraction, "fraction", unary_len!(array, self))?;
                fraction.try_validate(|value| validate_densify_fraction(value).map(|_| ()))?;
                unary_spine_shapes_extras!(
                    array,
                    py,
                    self,
                    crate::dispatch::Operation::Metric(MetricOp::Segmentize),
                    None,
                    crate::dispatch::PackedUnary::Densify {
                        fraction: fraction.clone(),
                        path,
                    },
                    move |data, ctx| kernels::unary_densify(data, ctx, &fraction, path)
                )
            },
            _ => Err(GeometryError::new_err(
                "segmentize requires exactly one of max_length or fraction",
            )),
        }
    }

    #[doc = doc_clip_by_rect!(array)]
    #[pyo3(
        signature = (minx, miny, maxx, maxy)
    )]
    pub fn clip_by_rect(
        &self,
        py: Python<'_>,
        minx: &Bound<'_, PyAny>,
        miny: &Bound<'_, PyAny>,
        maxx: &Bound<'_, PyAny>,
        maxy: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let minx = finite_f64_required("minx", minx)?;
        let miny = finite_f64_required("miny", miny)?;
        let maxx = finite_f64_required("maxx", maxx)?;
        let maxy = finite_f64_required("maxy", maxy)?;
        unary_spine_shapes_extras!(
            array,
            py,
            self,
            crate::dispatch::Operation::ClipByRect,
            None,
            crate::dispatch::PackedUnary::ClipByRect {
                minx,
                miny,
                maxx,
                maxy,
            },
            move |data, ctx| {
                let rect = match ctx.domain {
                    crate::geometry::TopologyDomain::Geographic(_) => {
                        Bounds::new_geographic(minx, miny, maxx, maxy)?
                    },
                    crate::geometry::TopologyDomain::Cartesian => {
                        Bounds::new(minx, miny, maxx, maxy)?
                    },
                };
                kernels::unary_clip_by_rect(data, ctx, rect, false)
            }
        )
    }

    #[doc = doc_subdivide!(array)]
    #[pyo3(signature = (*, max_vertices = 256))]
    pub fn subdivide(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = crate::strict_i64)] max_vertices: i64,
    ) -> PyResult<Groups> {
        self.subdivide_rows(py, max_vertices)
    }
}
