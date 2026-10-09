use crate::array::{
    Arc, ArrayRows, Bound, CRSError, CoordinateAxes, FrameEdit, GeometryTransformFrame, Py, PyAny,
    PyErr, PyGeometryArray, PyResult, Python, TransformOptionArgs, coordinate_epoch_option, crs,
    note_array_row, parse_crs, pymethods,
};
use crate::geometry::storage::{GeometryArrayStorage, ShapeRow};
use crate::geometry::{OrdinateAxis, set_point_ordinate};
use crate::predicates::unary;
use crate::py::crs::drain_accuracy_warning;
use crate::py::functions;

fn transform_failure_pyerr(failure: crate::crs::TransformFailure) -> PyErr {
    let error: PyErr = failure.error.into();
    match failure.coordinate {
        Some(row) => note_array_row(error, row),
        None => error,
    }
}

fn admit_identity_array_rows(
    transformer: &crs::TransformPlan,
    rows: &ArrayRows,
) -> std::result::Result<(), crate::crs::TransformFailure> {
    match rows.storage() {
        GeometryArrayStorage::Mixed(shapes) => {
            for (logical, shape) in shapes.iter().enumerate() {
                if rows.missing().is_none_or(|mask| !mask.is_missing(logical)) {
                    transformer.admit_identity_shape_at(logical, shape)?;
                }
            }
            Ok(())
        },
        GeometryArrayStorage::Packed(_) => {
            let columns = rows
                .packed_columns()
                .expect("packed storage resolves to packed columns");
            columns.try_for_each_present_row(|logical, row| {
                transformer.admit_identity_row_at(logical, ShapeRow::from_packed(row))
            })
        },
    }
}

#[pymethods]
impl PyGeometryArray {
    /// Relabel the CRS of all geometries without moving coordinates (see
    /// `Geometry.with_crs`; replacing a different declared CRS requires
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
    /// >>> gm.GeometryArray([gm.Point(1, 2)]).with_crs(4326).crs
    /// CRS("EPSG:4326")
    #[pyo3(signature = (crs, *, overwrite = false))]
    pub fn with_crs(&self, crs: &Bound<'_, PyAny>, overwrite: bool) -> PyResult<Self> {
        let frame = FrameEdit::SetCrs {
            crs: parse_crs(Some(crs))?,
            overwrite,
        }
        .apply(&self.frame)?;
        Ok(self.retag_frame(frame))
    }

    /// Declare (or clear) the array's coordinate epoch (see
    /// `Geometry.with_epoch`). The CRS frame must admit an epoch. ``None``
    /// clears it; changing a present epoch needs ``overwrite=True``.
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
    ///     If the CRS frame cannot carry an epoch or a present epoch would
    ///     change without ``overwrite=True``.
    /// GeometryError
    ///     If ``epoch`` is not a finite decimal year.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> arr = gm.GeometryArray([gm.Point(-122.4, 37.8, crs=4326)])
    /// >>> arr.with_epoch(2015.0).epoch
    /// 2015.0
    #[pyo3(signature = (epoch, *, overwrite = false))]
    pub fn with_epoch(&self, epoch: Option<&Bound<'_, PyAny>>, overwrite: bool) -> PyResult<Self> {
        let epoch = coordinate_epoch_option("epoch", epoch)?;
        let frame = FrameEdit::SetEpoch { epoch, overwrite }.apply(&self.frame)?;
        Ok(self.retag_frame(frame))
    }

    /// Reproject all geometries to a target CRS.
    /// The source coordinate epoch is the array's own ``epoch`` metadata
    /// (stamp it with ``with_epoch`` first when the operation needs one);
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
    ///     Output coordinate epoch (decimal year) to tag on the result. Omitted
    ///     keeps the source epoch when the CRS is unchanged or the target admits
    ///     geometry epochs; otherwise the output has no epoch.
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
    /// >>> round(float(arr.to_crs(3857).coords.x[0]), 2)
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
        if self.missing().is_some() && matches!(self.storage(), GeometryArrayStorage::Mixed(_)) {
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
            let transformer = crs::TransformPlan::from_geometry_frame(frame)?;
            let output = transformer.output_frame().clone();
            if transformer.is_identity() {
                let rows = self.rows.clone();
                py.detach(move || admit_identity_array_rows(&transformer, &rows))
                    .map_err(transform_failure_pyerr)?;
                return Ok(self.retag_frame(output));
            }
            let execution = transformer.execution();
            let rows = self.row_owner().clone();
            let converted_rows = py
                .detach(move || {
                    let missing = rows
                        .missing()
                        .expect("mixed nullable rows carry a validity mask");
                    let GeometryArrayStorage::Mixed(shapes) = rows.storage() else {
                        unreachable!("this transform path handles mixed nullable rows");
                    };
                    let present: Vec<_> = shapes
                        .iter()
                        .enumerate()
                        .filter_map(|(row, shape)| (!missing.is_missing(row)).then_some(shape))
                        .collect();
                    let converted = transformer
                        .transform_shapes_with_computed_edges_indexed(&present)
                        .map_err(|failure| {
                            let coordinate = failure
                                .coordinate
                                .and_then(|rank| rows.present_logical_indices().nth(rank));
                            failure.with_coordinate(coordinate)
                        })?;
                    let mut converted = converted.into_iter();
                    let output = rows
                        .logical_rows()
                        .map(|row| match row {
                            crate::geometry::storage::LogicalRow::Missing { .. } => {
                                ArrayRows::missing_placeholder()
                            },
                            crate::geometry::storage::LogicalRow::Present { .. } => {
                                converted.next().expect("present row count is stable")
                            },
                        })
                        .collect();
                    Ok::<_, crate::crs::TransformFailure>((output, missing.clone()))
                })
                .map_err(transform_failure_pyerr)?;
            drain_accuracy_warning(py, &execution)?;
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
        let transformer = crs::TransformPlan::from_geometry_frame(frame)?;
        let output = transformer.output_frame().clone();
        if transformer.is_identity() {
            let rows = self.rows.clone();
            py.detach(move || admit_identity_array_rows(&transformer, &rows))
                .map_err(transform_failure_pyerr)?;
            return Ok(self.retag_frame(output));
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
                    transformer.transform_shapes_with_computed_edges_indexed(&borrowed)
                })
                .map_err(transform_failure_pyerr)?;
            drain_accuracy_warning(py, &execution)?;
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
            drain_accuracy_warning(py, &execution)?;
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
        unary::is_valid_array(py, self)
    }

    pub fn __reduce__(&self, py: Python<'_>) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        functions::reduce_geometry_array(py, self)
    }
}

impl PyGeometryArray {
    pub(crate) fn replace_z_impl(
        &self,
        py: Python<'_>,
        value: Option<f64>,
        overwrite: bool,
    ) -> PyResult<Self> {
        if value.is_none() && !self.has_z() {
            // Clearing an ordinate no row carries: share the storage.
            return Ok(self.clone());
        }
        if value.is_some() && !overwrite && self.uniform_axes().is_some_and(CoordinateAxes::has_z) {
            return Ok(self.clone());
        }
        if !self.is_compact_nullable()
            && let GeometryArrayStorage::Packed(packed) = self.storage()
        {
            let input_axes = packed.axes();
            let output_axes = CoordinateAxes::new(
                crate::geometry::HasZ(value.is_some()),
                crate::geometry::HasM(input_axes.has_m()),
            );
            if let Some(mapped) = self.map_packed_points_detached(
                py,
                self.frame.clone(),
                output_axes,
                move |point| Ok(set_point_ordinate(point, OrdinateAxis::Z, value, overwrite)),
            )? {
                return Ok(mapped);
            }
        }
        self.map_shapes_detached(py, move |shape| shape.replace_z(value, overwrite))
    }
}
