#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::array::{
    Arc, Bound, CRSError, CoordinateAxes, FrameEdit, GeometryArrayStorage, GeometryTransformFrame,
    Point, Py, PyAny, PyErr, PyGeometryArray, PyResult, Python, Result, Shape, TransformOptionArgs,
    ValidityMask, coordinate_epoch_option, crs, note_array_row, parse_crs, pymethods,
};
use crate::geometry::{OrdinateAxis, set_point_ordinate};

fn transform_failure_pyerr(failure: crate::crs::TransformFailure) -> PyErr {
    let error: PyErr = failure.error.into();
    match failure.coordinate {
        Some(row) => note_array_row(error, row),
        None => error,
    }
}

fn present_rank_to_logical(mask: &ValidityMask, rank: usize) -> Option<usize> {
    mask.iter()
        .enumerate()
        .filter_map(|(logical, missing)| (!missing).then_some(logical))
        .nth(rank)
}

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
        Ok(self.retag_frame(frame)?)
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
        Ok(self.retag_frame(frame)?)
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
            let options = TransformOptionArgs {
                area_of_interest,
                source_epoch: None,
                target_epoch: None,
                authority,
                accuracy,
                allow_ballpark,
                only_best,
                force_over,
            }
            .parse()?;
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
            let converted_rows = py
                .detach(move || {
                    let present: Vec<_> = storage
                        .iter_shapes()
                        .zip(mask.iter())
                        .filter_map(|(shape, missing)| {
                            (!missing).then_some(shape.map(|shape| shape.into_owned()))
                        })
                        .collect::<crate::error::Result<Vec<_>>>()?;
                    let borrowed: Vec<_> = present.iter().collect();
                    let converted =
                        transformer
                            .transform_shapes_indexed(&borrowed)
                            .map_err(|failure| {
                                let coordinate = failure
                                    .coordinate
                                    .and_then(|rank| present_rank_to_logical(&mask, rank));
                                failure.with_coordinate(coordinate)
                            })?;
                    let mut converted = converted.into_iter();
                    let rows = mask
                        .iter()
                        .map(|missing| {
                            if missing {
                                Self::missing_placeholder()
                            } else {
                                converted.next().expect("present row count is stable")
                            }
                        })
                        .collect();
                    Ok::<_, crate::crs::TransformFailure>((rows, mask))
                })
                .map_err(transform_failure_pyerr)?;
            crate::py::crs::drain_accuracy_warning(py, &execution)?;
            return Ok(Self::from_shapes(converted_rows.0, output)?
                .with_missing_mask(Some(converted_rows.1))?);
        }
        let target =
            parse_crs(Some(crs))?.ok_or_else(|| CRSError::new_err("target CRS is required"))?;
        let options = TransformOptionArgs {
            area_of_interest,
            source_epoch: None,
            target_epoch: None,
            authority,
            accuracy,
            allow_ballpark,
            only_best,
            force_over,
        }
        .parse()?;
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
            let shapes = py
                .detach(move || {
                    let GeometryArrayStorage::Mixed(shapes) = storage.as_ref() else {
                        unreachable!("matched Mixed");
                    };
                    let borrowed: Vec<_> = shapes.iter().collect();
                    transformer.transform_shapes_indexed(&borrowed)
                })
                .map_err(transform_failure_pyerr)?;
            crate::py::crs::drain_accuracy_warning(py, &execution)?;
            Ok(Self::from_storage(
                GeometryArrayStorage::Mixed(shapes),
                output,
            )?)
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

    pub fn __reduce__(&self, py: Python<'_>) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        crate::py::functions::reduce_geometry_array(py, self)
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
        set_point_ordinate(
            point,
            match self {
                Self::Z => OrdinateAxis::Z,
                Self::M => OrdinateAxis::M,
            },
            value,
            overwrite,
        )
    }

    fn set_shape(self, shape: &Shape, value: Option<f64>, overwrite: bool) -> Result<Shape> {
        match self {
            Self::Z => shape.set_z(value, overwrite),
            Self::M => shape.set_m(value, overwrite),
        }
    }
}
