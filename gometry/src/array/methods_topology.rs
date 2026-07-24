#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;

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
        // Packed storage carries no per-row frame: relabel by sharing
        // storage — the columns are untouched.
        if !matches!(self.storage(), GeometryArrayStorage::Mixed(_)) {
            return Ok(
                Self::from_storage_arc(Arc::clone(self.storage_arc()), frame)
                    .with_missing_mask(self.missing().cloned()),
            );
        }
        Ok(Self::mixed(
            self.items()
                .iter()
                .map(|item| PyGeometry::with_frame(item.shape.clone(), frame.clone()))
                .collect(),
            frame,
        )
        .with_missing_mask(self.missing().cloned()))
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
        if !matches!(self.storage(), GeometryArrayStorage::Mixed(_)) {
            // Packed storage carries no per-row frame: re-tagging is pure
            // metadata, the columns are shared untouched.
            return Ok(
                Self::from_storage_arc(Arc::clone(self.storage_arc()), frame)
                    .with_missing_mask(self.missing().cloned()),
            );
        }
        Ok(Self::mixed(
            self.items()
                .iter()
                .map(|item| PyGeometry::with_frame(item.shape.clone(), frame.clone()))
                .collect(),
            frame,
        )
        .with_missing_mask(self.missing().cloned()))
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
    ///     Restrict candidate transforms to this authority (e.g. ``'EPSG'``).
    ///
    /// accuracy : float, optional
    ///     Maximum acceptable transformation accuracy, in meters.
    ///
    /// allow_ballpark : bool, optional
    ///     Allow low-accuracy ballpark transforms when no precise one exists.
    ///
    /// only_best : bool, optional
    ///     Use only the single best transform; no fallback.
    ///
    /// force_over : bool, optional
    ///     Keep coordinates on the source side of the antimeridian (no wrap).
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
    /// >>> round(float(gm.get_coordinates(arr.to_crs(3857))[0][0]), 2)
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
        if let Some(mask) = self.missing().cloned() {
            // PROJ pipelines reject the NaN placeholder: reproject the present
            // rows densely, then scatter them back under the same mask.
            let dense = self.drop_missing().to_crs(
                py,
                crs,
                area_of_interest,
                epoch,
                authority,
                accuracy,
                allow_ballpark,
                only_best,
                force_over,
            )?;
            return Ok(Self::scatter_present_rows(&dense, mask));
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
        if frame.identity {
            return Ok(self.clone());
        }
        let output = frame.output;
        let transformer =
            crs::Transformer::new_with_options(&frame.source, &frame.target, frame.options);
        if let GeometryArrayStorage::Mixed(_) = self.storage() {
            // Borrow each element's shared shape — no deep clone of the
            // input coordinate tree before the batched PROJ transform.
            let storage = Arc::clone(self.storage_arc());
            let shapes = py.detach(move || {
                let GeometryArrayStorage::Mixed(items) = storage.as_ref() else {
                    unreachable!("matched Mixed");
                };
                let borrowed: Vec<_> = items.iter().map(|item| item.shape.shape()).collect();
                transformer.transform_shapes(&borrowed)
            })?;
            let items = shapes
                .into_iter()
                .map(|shape| PyGeometry::with_frame(shape, output.clone()))
                .collect();
            Ok(Self::pack_or_mixed(items, output))
        } else {
            self.map_packed_coordseq_detached(py, output, move |coords| {
                map_coordseq_to_crs(&transformer, coords)
            })?
            .ok_or_else(|| {
                PyErr::from(crate::error::Error::from(
                    crate::geometry::GeometryErrorKind::Projection("packed to_crs failed".into()),
                ))
            })
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
            let missing_bytes = self.missing().map(|mask| {
                PyBytes::new(py, &mask.iter().map(|&m| u8::from(m)).collect::<Vec<u8>>())
            });
            let args = (rows, self.crs_str(), self.epoch(), missing_bytes).into_py_any(py)?;
            return Ok((unpickle!("_unpickle_geometry_array"), args));
        }
        if let Some(mask) = self.missing() {
            let present = self.drop_missing();
            let (callable, args) = present.__reduce__(py)?;
            let args = args.bind(py).cast::<PyTuple>()?;
            let missing = PyBytes::new(
                py,
                &mask
                    .iter()
                    .map(|&missing| u8::from(missing))
                    .collect::<Vec<_>>(),
            )
            .into_any();
            let mut values: Vec<Bound<'_, PyAny>> = args.iter().collect();
            *values
                .last_mut()
                .expect("typed geometry pickle always carries missing") = missing;
            let args = PyTuple::new(py, values)?.into_any().unbind();
            return Ok((callable, args));
        }
        match self.storage() {
            GeometryArrayStorage::Points { coords, row_map } => {
                // Orphan NaN physical rows (drop_missing / present-only gather
                // clear the mask but leave missing placeholders in the full
                // column) must never be serialized. Densify to logical rows
                // only; if physical non-finite remains on the identity path,
                // fall through to mixed WKB.
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
                        self.materialize_packed_points_parts(coords, row_map.as_deref());
                    let densified_finite = matches!(densified.storage(), GeometryArrayStorage::Points { coords, .. }
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
                let column = |values: &[f64]| PyBytes::new(py, &f64_column_le_bytes(values));
                // Empty gather is a valid empty selection — encode as empty
                // bytes (not omitted Identity).
                let row_map_bytes = row_map
                    .pickle_row_map_indices()
                    .map(|map| PyBytes::new(py, &usize_row_map_le_bytes(&map)));
                let args = (
                    column(coords.xs()),
                    column(coords.ys()),
                    coords.zs().map(column),
                    coords.ms().map(column),
                    self.crs_str(),
                    self.epoch(),
                    row_map_bytes,
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
                let column = |values: &[f64]| PyBytes::new(py, &f64_column_le_bytes(values));
                let offset_bytes: Vec<u8> = offsets
                    .iter()
                    .flat_map(|offset| offset.to_le_bytes())
                    .collect();
                let row_map_bytes = row_map
                    .pickle_row_map_indices()
                    .map(|map| PyBytes::new(py, &usize_row_map_le_bytes(&map)));
                let args = (
                    column(coords.xs()),
                    column(coords.ys()),
                    coords.zs().map(column),
                    coords.ms().map(column),
                    PyBytes::new(py, &offset_bytes),
                    self.crs_str(),
                    self.epoch(),
                    row_map_bytes,
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
                let column = |values: &[f64]| PyBytes::new(py, &f64_column_le_bytes(values));
                let offset_bytes = |offsets: &[i32]| {
                    offsets
                        .iter()
                        .flat_map(|offset| offset.to_le_bytes())
                        .collect::<Vec<u8>>()
                };
                let row_map_bytes = row_map
                    .pickle_row_map_indices()
                    .map(|map| PyBytes::new(py, &usize_row_map_le_bytes(&map)));
                let args = (
                    column(coords.xs()),
                    column(coords.ys()),
                    coords.zs().map(column),
                    coords.ms().map(column),
                    PyBytes::new(py, &offset_bytes(ring_offsets)),
                    PyBytes::new(py, &offset_bytes(polygon_offsets)),
                    self.crs_str(),
                    self.epoch(),
                    row_map_bytes,
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
        if let Some(mapped) =
            self.map_packed_coordseq_detached(py, self.frame.clone(), move |coords| {
                Ok(ordinate.set_coords(coords, value, overwrite))
            })?
        {
            return Ok(mapped);
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

    fn set_coords(self, coords: &CoordSeq, value: Option<f64>, overwrite: bool) -> CoordSeq {
        match self {
            Self::Z => coords.set_z(value, overwrite),
            Self::M => coords.set_m(value, overwrite),
        }
    }

    fn set_shape(self, shape: &Shape, value: Option<f64>, overwrite: bool) -> Result<Shape> {
        match self {
            Self::Z => shape.set_z(value, overwrite),
            Self::M => shape.set_m(value, overwrite),
        }
    }
}
