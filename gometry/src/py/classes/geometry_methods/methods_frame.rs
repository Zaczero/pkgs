//! CRS and prepared-geometry methods on `PyGeometry`.

use pyo3::types::PyDict;

use crate::boundary::metadata::FrameEdit;
use crate::py::classes::geometry_methods::{
    Bound, CRSError, PyAny, PyErr, PyGeometry, PyResult, Python, Typed, pymethods,
};
use crate::py::crs::drain_accuracy_warning;
use crate::py::replace::{ReplacePresence, reject_unknown_kwargs, replace_crs, replace_epoch};
use crate::{
    Arc, GeometryTransformFrame, PyCrs, PyPreparedGeometry, coordinate_epoch_option, crs, crs_arc,
    parse_crs, parse_geometry_transform_options,
};

#[pymethods]
impl PyGeometry {
    /// Estimate a conformal metric CRS for this geometry.
    ///
    /// The complete geometry extent is evaluated against a fixed 0.1% linear
    /// scale-error ceiling. A datum-aware UTM or UPS CRS is preferred when it
    /// fits; otherwise a receiver-centered conformal CRS is considered.
    /// Empty, CRS-free, or geographically unsafe geometries raise ``CRSError``.
    ///
    /// Returns
    /// -------
    /// CRS
    ///
    /// Raises
    /// ------
    /// CRSError
    ///     If the geometry has no CRS, is empty, or one safe local frame
    ///     cannot represent its extent.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.Point(-122.4, 37.8, crs=4326).estimate_local_crs()
    /// CRS("EPSG:32610")
    pub fn estimate_local_crs(&self) -> PyResult<PyCrs> {
        let source = self.crs_str().ok_or_else(|| {
            CRSError::new_err("estimate_local_crs requires a CRS-tagged geometry")
        })?;
        Ok(PyCrs::from_canonical(crs_arc(crs::estimate_local_crs(
            &self.shape,
            source,
        )?)))
    }

    /// Create a `PreparedGeometry` predicate operand in O(1).
    /// The relevant spatial product is built lazily on first use.
    ///
    /// Returns
    /// -------
    /// PreparedGeometry
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> prep = gm.box(0, 0, 2, 2).prepare()
    /// >>> gm.contains(prep, gm.Point(1, 1))
    /// True
    pub fn prepare(&self) -> PyPreparedGeometry {
        // The prepared state IS the geometry's shared `ShapeData` handle: its
        // `OnceLock` caches (point tester, distance parts, convex-shell,
        // validity, bounds) build lazily on first use and amortize across every
        // later query — the native predicate engine reads them directly, so no
        // eager geo-rs prepared graph or `to_geo` conversion is built here.
        PyPreparedGeometry {
            geometry: self.clone(),
        }
    }

    /// Attach or relabel the CRS *without* moving coordinates.
    /// Declares what the coordinates already mean; to actually reproject them
    /// use `to_crs`. Attaching to a CRS-free geometry, clearing with
    /// ``None``, and identical re-tags are free; replacing one declared CRS
    /// with a *different* one requires ``overwrite=True`` (it is almost always
    /// a reprojection mistake).
    ///
    /// Parameters
    /// ----------
    /// crs : str or int
    ///     Target CRS (EPSG or authority/WKT), or ``None`` to clear.
    ///
    /// overwrite : bool, default False
    ///     Allow replacing an existing, different CRS label.
    ///
    /// Returns
    /// -------
    /// Geometry
    ///     A copy carrying the new CRS label.
    ///
    /// Raises
    /// ------
    /// CRSError
    ///     If ``crs`` is not a recognized CRS, or it would silently replace a
    ///     different declared CRS without ``overwrite``.
    ///
    /// See Also
    /// --------
    /// to_crs : Reproject coordinates to another CRS.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.Point(1, 2).set_crs(4326).crs
    /// CRS("EPSG:4326")
    #[pyo3(signature = (crs, *, overwrite = false))]
    pub fn set_crs(&self, crs: &Bound<'_, PyAny>, overwrite: bool) -> PyResult<Typed> {
        let frame = FrameEdit::SetCrs {
            crs: parse_crs(Some(crs))?,
            overwrite,
        }
        .apply(&self.frame)?;
        Ok(Typed(Self::with_frame(Arc::clone(&self.shape), frame)))
    }

    /// Declare (or clear) the coordinate epoch without moving coordinates.
    /// The epoch is the decimal year a dynamic-frame coordinate set was
    /// observed (e.g. ``2020.0``), metadata for transforms between dynamic and
    /// static datums. ``set_epoch(None)`` clears it. Changing a present epoch
    /// to a different value requires ``overwrite=True`` (the
    /// silent-frame-change guard, like ``set_crs``).
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
    /// Geometry
    ///     A copy carrying the new epoch (same coordinates and CRS).
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
    /// >>> gm.Point(-122.4, 37.8, crs=4326).set_epoch(2020.0).epoch
    /// 2020.0
    #[pyo3(signature = (epoch, *, overwrite = false))]
    pub fn set_epoch(&self, epoch: Option<&Bound<'_, PyAny>>, overwrite: bool) -> PyResult<Typed> {
        let epoch = coordinate_epoch_option("epoch", epoch)?;
        let frame = FrameEdit::SetEpoch { epoch, overwrite }.apply(&self.frame)?;
        Ok(Typed(Self::with_frame(Arc::clone(&self.shape), frame)))
    }

    /// Return a copy with the given CRS/epoch metadata replaced.
    ///
    /// Supports ``copy.replace`` on Python 3.13+; omitted keyword arguments
    /// keep the current value. ``crs=None`` / ``epoch=None`` clear metadata.
    ///
    /// Parameters
    /// ----------
    /// crs : str or int or None, optional
    ///     Replace or clear the CRS label.
    ///
    /// epoch : float or None, optional
    ///     Replace or clear the coordinate epoch.
    ///
    /// Returns
    /// -------
    /// Geometry
    #[pyo3(signature = (*, **kwargs), text_signature = "($self, /, *, crs=..., epoch=...)")]
    pub fn __replace__(&self, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Typed> {
        reject_unknown_kwargs(kwargs, &["crs", "epoch"], "Geometry.__replace__")?;
        let mut frame = self.frame.clone();
        if let ReplacePresence::Set(crs) = replace_crs(kwargs, "crs")? {
            frame = FrameEdit::SetCrs {
                crs,
                overwrite: true,
            }
            .apply(&frame)
            .map_err(PyErr::from)?;
        }
        if let ReplacePresence::Set(epoch) = replace_epoch(kwargs, "epoch")? {
            frame = FrameEdit::SetEpoch {
                epoch,
                overwrite: true,
            }
            .apply(&frame)
            .map_err(PyErr::from)?;
        }
        Ok(Typed(Self::with_frame(Arc::clone(&self.shape), frame)))
    }

    /// Reproject coordinates to a target CRS.
    /// The source coordinate epoch is this geometry's own ``epoch`` metadata;
    /// transform between dynamic frames by stamping the source with
    /// ``set_epoch`` first. ``epoch`` here labels the *output* coordinate
    /// epoch.
    ///
    /// Parameters
    /// ----------
    /// crs : str or int
    ///     Target CRS as an EPSG code or authority/WKT string.
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
    /// Geometry
    ///     The geometry reprojected to ``crs`` (same geometry type).
    ///
    /// Raises
    /// ------
    /// CRSError
    ///     If a CRS is invalid or the source is missing.
    /// TransformError
    ///     If no transform exists between the frames or it fails to apply.
    /// GeometryError
    ///     If ``epoch`` is not a finite decimal year.
    ///
    /// See Also
    /// --------
    /// set_crs : Declare/relabel the CRS *without* moving coordinates.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> round(gm.Point(1, 2, crs=4326).to_crs(3857).x, 2)
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
    ) -> PyResult<Typed> {
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
            return Ok(Typed(self.clone()));
        }
        let transformer =
            crs::Transformer::new_with_options(&frame.source, &frame.target, frame.options);
        let shape = transformer.transform_shape(&self.shape)?;
        drain_accuracy_warning(py)?;
        Ok(Typed(Self::with_frame(shape, frame.output)))
    }
}
