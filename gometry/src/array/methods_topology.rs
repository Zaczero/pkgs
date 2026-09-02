#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::IntoPyObjectExt as _;
use pyo3::exceptions::PyOverflowError;
use pyo3::types::PyAnyMethods as _;

use crate::array::{
    Arc, Bound, CRSError, CoordinateAxes, FrameEdit, GeometryArrayStorage, GeometryTransformFrame,
    Point, Py, PyAny, PyBytes, PyErr, PyGeometryArray, PyResult, PyTuple, PyTupleMethods as _,
    Python, Result, Shape, coordinate_epoch_option, crs, f64_column_le_bytes, i32_column_le_bytes,
    parse_crs, parse_geometry_transform_options, pymethods, rebased_i32_offsets_le_bytes,
    selected_f64_column_le_bytes, selected_f64_windows_le_bytes,
};

#[pymethods]
impl PyGeometryArray {
    /// Relabel the CRS of all geometries without moving coordinates (see
    /// `Geometry.set_crs`; replacing a different declared CRS requires
    /// ``overwrite=True``).
    ///
    /// Parameters
    /// ----------
    /// crs : str or int
    ///     CRS as an EPSG code or authority/WKT string.
    ///
    /// overwrite : bool, default False
    ///     Allow replacing an existing, different CRS label.
    ///
    /// Returns
    /// -------
    /// GeometryArray
    ///
    /// Raises
    /// ------
    /// CRSError
    ///     If ``crs`` is not a recognized CRS, or it would silently replace a
    ///     different declared CRS without ``overwrite``.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.GeometryArray([gm.Point(1, 2)]).set_crs(4326).crs
    /// CRS("EPSG:4326")
    #[pyo3(signature = (crs, *, overwrite = false))]
    pub fn set_crs(&self, crs: &Bound<'_, PyAny>, overwrite: bool) -> PyResult<Self> {
        let frame = FrameEdit::SetCrs {
            crs: parse_crs(Some(crs))?,
            overwrite,
        }
        .apply(&self.frame)?;
        Ok(self.retag_frame(frame))
    }

    /// Declare (or clear) the array's coordinate epoch (see
    /// `Geometry.set_epoch`). ``None`` clears it; changing a present epoch
    /// needs ``overwrite=True``.
    ///
    /// Parameters
    /// ----------
    /// epoch : float or None
    ///     Decimal year, or ``None`` to clear.
    ///
    /// overwrite : bool, default False
    ///     Allow replacing an existing, different epoch.
    ///
    /// Returns
    /// -------
    /// GeometryArray
    ///     A copy carrying the new epoch.
    ///
    /// Raises
    /// ------
    /// CRSError
    ///     If a present epoch would change without ``overwrite=True``.
    /// GeometryError
    ///     If ``epoch`` is not a finite decimal year.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> arr = gm.GeometryArray([gm.Point(-122.4, 37.8, crs=4326)])
    /// >>> arr.set_epoch(2015.0).epoch
    /// 2015.0
    #[pyo3(signature = (epoch, *, overwrite = false))]
    pub fn set_epoch(&self, epoch: Option<&Bound<'_, PyAny>>, overwrite: bool) -> PyResult<Self> {
        let epoch = coordinate_epoch_option("epoch", epoch)?;
        let frame = FrameEdit::SetEpoch { epoch, overwrite }.apply(&self.frame)?;
        Ok(self.retag_frame(frame))
    }

    /// Reproject all geometries to a target CRS.
    /// The source coordinate epoch is the array's own ``epoch`` metadata
    /// (stamp it with ``set_epoch`` first to transform between dynamic
    /// frames);
    /// ``epoch`` here labels the *output* coordinate epoch.
    ///
    /// Parameters
    /// ----------
    /// crs : str or int
    ///     CRS as an EPSG code or authority/WKT string.
    ///
    /// area_of_interest : sequence of float, optional
    ///     Bounding ``(west, south, east, north)`` to pick the best transform.
    ///
    /// epoch : float, optional
    ///     Output coordinate epoch (decimal year) to tag on the result, for
    ///     dynamic frames. Omitted keeps the source epoch while it still
    ///     means something: the CRS is unchanged, or the target CRS is
    ///     dynamic (time-dependent). A static target clears it.
    ///
    /// authority : str, optional
    ///     Restrict candidate coordinate operations to this authority
    ///     (e.g. ``'EPSG'``).
    ///
    /// accuracy : float, optional
    ///     Maximum acceptable operation accuracy, in meters.
    ///
    /// allow_ballpark : bool, optional
    ///     Allow low-accuracy ballpark operations when no precise one exists.
    ///
    /// only_best : bool, optional
    ///     Use only the single best operation; no fallback.
    ///
    /// force_over : bool, optional
    ///     Keep coordinates on the source side of the antimeridian instead of
    ///     wrapping into ``[-180, 180]`` (PROJ ``FORCE_OVER=YES``). Like
    ///     ``only_best``, this also collapses operation selection to a single
    ///     candidate, so enumerating surfaces return exactly one operation.
    ///
    /// Returns
    /// -------
    /// GeometryArray
    ///
    /// Raises
    /// ------
    /// TransformError
    ///     If no transform exists between the frames or it fails to apply.
    /// CRSError
    ///     If a CRS is invalid or the source is missing.
    /// GeometryError
    ///     If ``epoch`` is not a finite decimal year.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> arr = gm.GeometryArray([gm.Point(1, 2, crs=4326)])
    /// >>> round(float(gm.get_coordinates(arr.to_crs(3857))[0][0][0]), 2)
    /// 111319.49
    #[pyo3(signature = (
        crs,
        *,
        area_of_interest = None,
        epoch = None,
        authority = None,
        accuracy = None,
        allow_ballpark = None,
        only_best = None,
        force_over = false
    ))]
    pub fn to_crs(
        &self,
        py: Python<'_>,
        crs: &Bound<'_, PyAny>,
        area_of_interest: Option<&Bound<'_, PyAny>>,
        epoch: Option<&Bound<'_, PyAny>>,
        authority: Option<String>,
        accuracy: Option<&Bound<'_, PyAny>>,
        allow_ballpark: Option<bool>,
        only_best: Option<bool>,
        force_over: bool,
    ) -> PyResult<Self> {
        if let Some(mask) = self.missing().cloned()
            && matches!(self.storage(), GeometryArrayStorage::Mixed(_))
        {
            let target =
                parse_crs(Some(crs))?.ok_or_else(|| CRSError::new_err("target CRS is required"))?;
            let options = parse_geometry_transform_options(
                area_of_interest,
                authority,
                accuracy,
                allow_ballpark,
                only_best,
                force_over,
            )?;
            let frame = GeometryTransformFrame::new(
                &self.frame,
                target,
                coordinate_epoch_option("epoch", epoch)?,
                options,
            )?;
            let output = frame.output;
            let transformer = crs::TransformPlan::new_with_options_at(
                &frame.source,
                &frame.target,
                frame.options,
                frame.state,
            )?;
            if transformer.is_identity() {
                return Ok(self.clone());
            }
            let execution = transformer.execution();
            let storage = Arc::clone(self.storage_arc());
            let transformed = py.detach(move || {
                let present: Vec<_> = storage
                    .iter_shapes()
                    .zip(mask.iter())
                    .filter_map(|(shape, missing)| (!missing).then(|| shape.into_owned()))
                    .collect();
                let borrowed: Vec<_> = present.iter().collect();
                let transformed = transformer.transform_shapes(&borrowed)?;
                let mut transformed = transformed.into_iter();
                let rows = mask
                    .iter()
                    .map(|missing| {
                        if missing {
                            Self::missing_placeholder()
                        } else {
                            transformed.next().expect("present row count is stable")
                        }
                    })
                    .collect();
                Ok::<_, crate::error::Error>((rows, mask))
            })?;
            crate::py::crs::drain_accuracy_warning(py, &execution)?;
            return Ok(
                Self::from_shapes(transformed.0, output).with_missing_mask(Some(transformed.1))
            );
        }
        let target =
            parse_crs(Some(crs))?.ok_or_else(|| CRSError::new_err("target CRS is required"))?;
        let options = parse_geometry_transform_options(
            area_of_interest,
            authority,
            accuracy,
            allow_ballpark,
            only_best,
            force_over,
        )?;
        let frame = GeometryTransformFrame::new(
            &self.frame,
            target,
            coordinate_epoch_option("epoch", epoch)?,
            options,
        )?;
        let output = frame.output;
        let transformer = crs::TransformPlan::new_with_options_at(
            &frame.source,
            &frame.target,
            frame.options,
            frame.state,
        )?;
        if transformer.is_identity() {
            return Ok(self.clone());
        }
        let execution = transformer.execution();
        if let GeometryArrayStorage::Mixed(_) = self.storage() {
            // Borrow array-owned shapes — no deep clone before the batched
            // PROJ transform; land outputs in array-owned mixed/packed.
            let storage = Arc::clone(self.storage_arc());
            let shapes = py.detach(move || {
                let GeometryArrayStorage::Mixed(shapes) = storage.as_ref() else {
                    unreachable!("matched Mixed");
                };
                let borrowed: Vec<_> = shapes.iter().collect();
                transformer.transform_shapes(&borrowed)
            })?;
            crate::py::crs::drain_accuracy_warning(py, &execution)?;
            Ok(Self::from_storage(
                GeometryArrayStorage::Mixed(shapes),
                output,
            ))
        } else {
            let result = self
                .map_packed_to_crs_detached(py, output, transformer)?
                .ok_or_else(|| {
                    PyErr::from(crate::error::Error::from(
                        crate::geometry::GeometryErrorKind::Projection(
                            "packed to_crs failed".into(),
                        ),
                    ))
                })?;
            crate::py::crs::drain_accuracy_warning(py, &execution)?;
            Ok(result)
        }
    }
    /// Per-element validity mask (see `Geometry.is_valid`).
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     ``True`` where the geometry is valid, one entry per row.
    #[getter]
    pub fn is_valid(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        crate::predicates::unary::is_valid_array(py, self)
    }

    #[expect(
        clippy::too_many_lines,
        reason = "packed vs mixed pickle lanes share one representation-safe entry"
    )]
    pub fn __reduce__(&self, py: Python<'_>) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        let module = crate::gometry_lib_module(py)?;
        macro_rules! unpickle {
            ($name:literal) => {
                module.getattr(pyo3::intern!(py, $name))?.unbind()
            };
        }
        // Mixed storage has no typed column lane. Packed storage serializes
        // only its finite present rows, then restores the logical mask below.
        if self.missing().is_some() && matches!(self.storage(), GeometryArrayStorage::Mixed(_)) {
            let rows = self.to_wkb_impl(py, false, None)?;
            let missing_bytes = self
                .missing()
                .map(|mask| PyBytes::new(py, &mask.to_missing_bytes()));
            let args = (rows, self.crs_str(), self.epoch(), missing_bytes).into_py_any(py)?;
            return Ok((unpickle!("_unpickle_geometry_array"), args));
        }
        if let Some(mask) = self.missing() {
            // Keep the declared homogeneous kind/axes for an all-missing
            // compact column; dropping its rows otherwise falls through the
            // empty Mixed representation before the reducer sees the schema.
            let present = if mask.present_count() == 0 && self.is_compact_nullable() {
                self.empty_homogeneous_like()
            } else {
                self.drop_missing()
            };
            let (callable, args) = present.__reduce__(py)?;
            let args = args.bind(py).cast::<PyTuple>()?;
            let missing = PyBytes::new(py, &mask.to_missing_bytes()).into_any();
            let mut values: Vec<Bound<'_, PyAny>> = args.iter().collect();
            *values
                .last_mut()
                .expect("typed geometry pickle always carries missing") = missing;
            let args = PyTuple::new(py, values)?.into_any().unbind();
            return Ok((callable, args));
        }
        match self.storage() {
            GeometryArrayStorage::Points { coords, row_map } => {
                let selection = row_map.as_deref();
                let selected_all_finite = |values: &[f64]| {
                    (0..selection.len(values.len()))
                        .all(|logical| values[selection.physical(logical)].is_finite())
                };
                if !selected_all_finite(coords.xs())
                    || !selected_all_finite(coords.ys())
                    || coords.zs().is_some_and(|z| !selected_all_finite(z))
                    || coords.ms().is_some_and(|m| !selected_all_finite(m))
                {
                    let rows = self.to_wkb_impl(py, false, None)?;
                    let args = (
                        rows,
                        self.crs_str(),
                        self.epoch(),
                        Option::<Bound<'_, PyBytes>>::None,
                    )
                        .into_py_any(py)?;
                    return Ok((unpickle!("_unpickle_geometry_array"), args));
                }
                let column = |values: &[f64]| -> PyResult<Bound<'_, PyBytes>> {
                    let bytes = selected_f64_column_le_bytes(values, selection)?;
                    Ok(PyBytes::new(py, &bytes))
                };
                let args = (
                    column(coords.xs())?,
                    column(coords.ys())?,
                    coords.zs().map(column).transpose()?,
                    coords.ms().map(column).transpose()?,
                    self.crs_str(),
                    self.epoch(),
                    Option::<Bound<'_, PyBytes>>::None,
                    Option::<Bound<'_, PyBytes>>::None,
                )
                    .into_py_any(py)?;
                Ok((unpickle!("_unpickle_point_array"), args))
            },
            GeometryArrayStorage::Lines {
                coords,
                offsets,
                row_map,
            } => {
                let selection = row_map.as_deref();
                let len = selection.len(offsets.len().saturating_sub(1));
                let windows = || (0..len).map(|logical| selection.csr_window(offsets, logical));
                let selected_all_finite = |values: &[f64]| {
                    windows().all(|window| values[window].iter().all(|value| value.is_finite()))
                };
                if selected_all_finite(coords.xs())
                    && selected_all_finite(coords.ys())
                    && coords.zs().is_none_or(selected_all_finite)
                    && coords.ms().is_none_or(selected_all_finite)
                {
                    let vertex_count = windows()
                        .try_fold(0_usize, |total, window| total.checked_add(window.len()))
                        .ok_or_else(|| {
                            PyOverflowError::new_err("coordinate count overflows usize")
                        })?;
                    let column = |values: &[f64]| -> PyResult<Bound<'_, PyBytes>> {
                        let bytes = selected_f64_windows_le_bytes(values, windows(), vertex_count)?;
                        Ok(PyBytes::new(py, &bytes))
                    };
                    let offset_bytes =
                        rebased_i32_offsets_le_bytes(windows().map(|window| window.len()), len)?;
                    let args = (
                        column(coords.xs())?,
                        column(coords.ys())?,
                        coords.zs().map(column).transpose()?,
                        coords.ms().map(column).transpose()?,
                        PyBytes::new(py, &offset_bytes),
                        self.crs_str(),
                        self.epoch(),
                        Option::<Bound<'_, PyBytes>>::None,
                        Option::<Bound<'_, PyBytes>>::None,
                    )
                        .into_py_any(py)?;
                    return Ok((unpickle!("_unpickle_line_array"), args));
                }
                // Same orphan-NaN densify path as packed points: drop_missing /
                // present-only gather leave NaN placeholders in physical columns.
                let physical_nonfinite = !crate::geometry::column_all_finite(coords.xs())
                    || !crate::geometry::column_all_finite(coords.ys())
                    || coords
                        .zs()
                        .is_some_and(|z| !crate::geometry::column_all_finite(z))
                    || coords
                        .ms()
                        .is_some_and(|m| !crate::geometry::column_all_finite(m));
                if physical_nonfinite {
                    let densified =
                        self.materialize_packed_lines_parts(coords, offsets, row_map.as_deref())?;
                    let densified_finite = matches!(densified.storage(), GeometryArrayStorage::Lines { coords, .. }
                        if crate::geometry::column_all_finite(coords.xs())
                            && crate::geometry::column_all_finite(coords.ys())
                            && coords.zs().is_none_or(crate::geometry::column_all_finite)
                            && coords.ms().is_none_or(crate::geometry::column_all_finite)
                    );
                    if densified_finite {
                        return densified.__reduce__(py);
                    }
                    let rows = self.to_wkb_impl(py, false, None)?;
                    let args = (
                        rows,
                        self.crs_str(),
                        self.epoch(),
                        Option::<Bound<'_, PyBytes>>::None,
                    )
                        .into_py_any(py)?;
                    return Ok((unpickle!("_unpickle_geometry_array"), args));
                }
                let column = |values: &[f64]| -> PyResult<Bound<'_, PyBytes>> {
                    let bytes = f64_column_le_bytes(values)?;
                    Ok(PyBytes::new(py, &bytes))
                };
                let offset_bytes = i32_column_le_bytes(offsets)?;
                let args = (
                    column(coords.xs())?,
                    column(coords.ys())?,
                    coords.zs().map(column).transpose()?,
                    coords.ms().map(column).transpose()?,
                    PyBytes::new(py, &offset_bytes),
                    self.crs_str(),
                    self.epoch(),
                    Option::<Bound<'_, PyBytes>>::None,
                    Option::<Bound<'_, PyBytes>>::None,
                )
                    .into_py_any(py)?;
                Ok((unpickle!("_unpickle_line_array"), args))
            },
            GeometryArrayStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            } => {
                let selection = row_map.as_deref();
                let len = selection.len(polygon_offsets.len().saturating_sub(1));
                let ring_windows = || {
                    (0..len).flat_map(|logical| {
                        selection.csr_window(polygon_offsets, logical).map(|ring| {
                            ring_offsets[ring] as usize..ring_offsets[ring + 1] as usize
                        })
                    })
                };
                let selected_all_finite = |values: &[f64]| {
                    ring_windows()
                        .all(|window| values[window].iter().all(|value| value.is_finite()))
                };
                if selected_all_finite(coords.xs())
                    && selected_all_finite(coords.ys())
                    && coords.zs().is_none_or(selected_all_finite)
                    && coords.ms().is_none_or(selected_all_finite)
                {
                    let ring_count = (0..len)
                        .try_fold(0_usize, |total, logical| {
                            total.checked_add(selection.csr_window(polygon_offsets, logical).len())
                        })
                        .ok_or_else(|| PyOverflowError::new_err("ring count overflows usize"))?;
                    let vertex_count = ring_windows()
                        .try_fold(0_usize, |total, window| total.checked_add(window.len()))
                        .ok_or_else(|| {
                            PyOverflowError::new_err("coordinate count overflows usize")
                        })?;
                    let column = |values: &[f64]| -> PyResult<Bound<'_, PyBytes>> {
                        let bytes =
                            selected_f64_windows_le_bytes(values, ring_windows(), vertex_count)?;
                        Ok(PyBytes::new(py, &bytes))
                    };
                    let ring_offset_bytes = rebased_i32_offsets_le_bytes(
                        ring_windows().map(|window| window.len()),
                        ring_count,
                    )?;
                    let polygon_offset_bytes = rebased_i32_offsets_le_bytes(
                        (0..len)
                            .map(|logical| selection.csr_window(polygon_offsets, logical).len()),
                        len,
                    )?;
                    let args = (
                        column(coords.xs())?,
                        column(coords.ys())?,
                        coords.zs().map(column).transpose()?,
                        coords.ms().map(column).transpose()?,
                        PyBytes::new(py, &ring_offset_bytes),
                        PyBytes::new(py, &polygon_offset_bytes),
                        self.crs_str(),
                        self.epoch(),
                        Option::<Bound<'_, PyBytes>>::None,
                        Option::<Bound<'_, PyBytes>>::None,
                    )
                        .into_py_any(py)?;
                    return Ok((unpickle!("_unpickle_polygon_array"), args));
                }
                let physical_nonfinite = !crate::geometry::column_all_finite(coords.xs())
                    || !crate::geometry::column_all_finite(coords.ys())
                    || coords
                        .zs()
                        .is_some_and(|z| !crate::geometry::column_all_finite(z))
                    || coords
                        .ms()
                        .is_some_and(|m| !crate::geometry::column_all_finite(m));
                if physical_nonfinite {
                    let densified = self.materialize_packed_polygons_parts(
                        coords,
                        ring_offsets,
                        polygon_offsets,
                        row_map.as_deref(),
                    )?;
                    let densified_finite = matches!(densified.storage(), GeometryArrayStorage::Polygons { coords, .. }
                        if crate::geometry::column_all_finite(coords.xs())
                            && crate::geometry::column_all_finite(coords.ys())
                            && coords.zs().is_none_or(crate::geometry::column_all_finite)
                            && coords.ms().is_none_or(crate::geometry::column_all_finite)
                    );
                    if densified_finite {
                        return densified.__reduce__(py);
                    }
                    let rows = self.to_wkb_impl(py, false, None)?;
                    let args = (
                        rows,
                        self.crs_str(),
                        self.epoch(),
                        Option::<Bound<'_, PyBytes>>::None,
                    )
                        .into_py_any(py)?;
                    return Ok((unpickle!("_unpickle_geometry_array"), args));
                }
                let column = |values: &[f64]| -> PyResult<Bound<'_, PyBytes>> {
                    let bytes = f64_column_le_bytes(values)?;
                    Ok(PyBytes::new(py, &bytes))
                };
                let ring_offset_bytes = i32_column_le_bytes(ring_offsets)?;
                let polygon_offset_bytes = i32_column_le_bytes(polygon_offsets)?;
                let args = (
                    column(coords.xs())?,
                    column(coords.ys())?,
                    coords.zs().map(column).transpose()?,
                    coords.ms().map(column).transpose()?,
                    PyBytes::new(py, &ring_offset_bytes),
                    PyBytes::new(py, &polygon_offset_bytes),
                    self.crs_str(),
                    self.epoch(),
                    Option::<Bound<'_, PyBytes>>::None,
                    Option::<Bound<'_, PyBytes>>::None,
                )
                    .into_py_any(py)?;
                Ok((unpickle!("_unpickle_polygon_array"), args))
            },
            GeometryArrayStorage::Mixed(_) => {
                let rows = self.to_wkb_impl(py, false, None)?;
                let args = (
                    rows,
                    self.crs_str(),
                    self.epoch(),
                    Option::<Bound<'_, PyBytes>>::None,
                )
                    .into_py_any(py)?;
                Ok((unpickle!("_unpickle_geometry_array"), args))
            },
        }
    }
}

impl PyGeometryArray {
    pub(crate) fn set_z_impl(
        &self,
        py: Python<'_>,
        value: Option<f64>,
        overwrite: bool,
    ) -> PyResult<Self> {
        self.set_ordinate_impl(py, Ordinate::Z, value, overwrite)
    }

    pub(crate) fn set_m_impl(
        &self,
        py: Python<'_>,
        value: Option<f64>,
        overwrite: bool,
    ) -> PyResult<Self> {
        self.set_ordinate_impl(py, Ordinate::M, value, overwrite)
    }

    fn set_ordinate_impl(
        &self,
        py: Python<'_>,
        ordinate: Ordinate,
        value: Option<f64>,
        overwrite: bool,
    ) -> PyResult<Self> {
        if value.is_none() && !ordinate.array_has(self) {
            // Clearing an ordinate no row carries: share the storage.
            return Ok(self.clone());
        }
        if value.is_some()
            && !overwrite
            && self.uniform_axes().is_some_and(|axes| ordinate.has(axes))
        {
            return Ok(self.clone());
        }
        if !self.is_compact_nullable() && !matches!(self.storage(), GeometryArrayStorage::Mixed(_))
        {
            let input_axes = self
                .uniform_axes()
                .expect("packed storage has uniform axes");
            let output_axes = ordinate.output_axes(input_axes, value);
            if let Some(mapped) = self.map_packed_points_detached(
                py,
                self.frame.clone(),
                output_axes,
                move |point| Ok(ordinate.set_point(point, value, overwrite)),
            )? {
                return Ok(mapped);
            }
        }
        self.map_shapes_detached(py, move |shape| ordinate.set_shape(shape, value, overwrite))
    }
}

#[derive(Clone, Copy)]
enum Ordinate {
    Z,
    M,
}

impl Ordinate {
    fn array_has(self, array: &PyGeometryArray) -> bool {
        match self {
            Self::Z => array.has_z(),
            Self::M => array.has_m(),
        }
    }

    const fn has(self, axes: CoordinateAxes) -> bool {
        match self {
            Self::Z => axes.has_z(),
            Self::M => axes.has_m(),
        }
    }

    const fn output_axes(self, axes: CoordinateAxes, value: Option<f64>) -> CoordinateAxes {
        let has = value.is_some();
        match self {
            Self::Z => CoordinateAxes::new(
                crate::geometry::HasZ(has),
                crate::geometry::HasM(axes.has_m()),
            ),
            Self::M => CoordinateAxes::new(
                crate::geometry::HasZ(axes.has_z()),
                crate::geometry::HasM(has),
            ),
        }
    }

    fn set_point(self, point: Point, value: Option<f64>, overwrite: bool) -> Point {
        let z = match self {
            Self::Z => value.or_else(|| (!overwrite).then(|| point.z()).flatten()),
            Self::M => point.z(),
        };
        let m = match self {
            Self::Z => point.m(),
            Self::M => value.or_else(|| (!overwrite).then(|| point.m()).flatten()),
        };
        Point::new_raw_axes(
            point.x,
            point.y,
            crate::geometry::ZOrdinate(z),
            crate::geometry::MOrdinate(m),
        )
    }

    fn set_shape(self, shape: &Shape, value: Option<f64>, overwrite: bool) -> Result<Shape> {
        match self {
            Self::Z => shape.set_z(value, overwrite),
            Self::M => shape.set_m(value, overwrite),
        }
    }
}
