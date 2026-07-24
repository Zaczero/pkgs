use super::*;

#[pymethods]
impl PyCrs {
    /// Best coordinate operation from this CRS to ``target``.
    ///
    /// Parameters
    /// ----------
    /// target : str or int or CRS
    ///     Destination CRS.
    ///
    /// at : tuple of float, optional
    ///     Coordinate at which to select the best operation: ``(x, y)``,
    ///     ``(x, y, z)``, or ``(x, y, z, t)`` in the source CRS. This is an
    ///     alternative to a broader ``area_of_interest``.
    ///
    /// area_of_interest : sequence of float, optional
    ///     ``(west, south, east, north)`` area of interest.
    ///
    /// source_epoch, target_epoch : float, optional
    ///     Coordinate epochs for dynamic CRS.
    ///
    /// authority : str, optional
    ///     Restrict to operations from this authority.
    ///
    /// accuracy : float, optional
    ///     Maximum acceptable accuracy, in meters.
    ///
    /// allow_ballpark : bool, optional
    ///     Permit low-accuracy ballpark operations.
    ///
    /// only_best : bool, optional
    ///     Return only the single best operation.
    ///
    /// force_over : bool, optional
    ///     Prefer operations that keep coordinates in a continuous range.
    ///
    /// Returns
    /// -------
    /// CrsOperationInfo
    ///
    /// Raises
    /// ------
    /// CRSError
    ///     If the CRS arguments are unrecognized.
    #[pyo3(signature = (target, *, at = None, area_of_interest = None, source_epoch = None, target_epoch = None, authority = None, accuracy = None, allow_ballpark = None, only_best = None, force_over = false))]
    #[expect(clippy::too_many_arguments)]
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.CRS(4326).operation(3857)['name']
    /// 'pipeline'
    fn operation(
        slf: &Bound<'_, Self>,
        target: &Bound<'_, PyAny>,
        at: Option<&Bound<'_, PyAny>>,
        area_of_interest: Option<&Bound<'_, PyAny>>,
        source_epoch: Option<&Bound<'_, PyAny>>,
        target_epoch: Option<&Bound<'_, PyAny>>,
        authority: Option<String>,
        accuracy: Option<&Bound<'_, PyAny>>,
        allow_ballpark: Option<bool>,
        only_best: Option<bool>,
        force_over: bool,
    ) -> PyResult<crs::OperationInfo> {
        let options = TransformOptionArgs {
            area_of_interest,
            source_epoch,
            target_epoch,
            authority,
            accuracy,
            allow_ballpark,
            only_best,
            force_over,
        };
        let Some(at) = at else {
            return crs_operation(slf.as_any(), target, options);
        };
        let at = at.cast::<PyTuple>().map_err(|_| {
            GeometryError::new_err("at must be a (x, y), (x, y, z), or (x, y, z, t) tuple")
        })?;
        if !(2..=4).contains(&at.len()) {
            return Err(GeometryError::new_err(
                "at must be a (x, y), (x, y, z), or (x, y, z, t) tuple",
            ));
        }
        let x = at.get_item(0)?;
        let y = at.get_item(1)?;
        let z = (at.len() >= 3).then(|| at.get_item(2)).transpose()?;
        let t = (at.len() == 4).then(|| at.get_item(3)).transpose()?;
        crs_operation_at(
            slf.as_any(),
            target,
            &x,
            &y,
            z.as_ref(),
            t.as_ref(),
            options,
        )
    }

    /// All candidate operations from this CRS to ``target``, best first.
    ///
    /// Parameters
    /// ----------
    /// target : str or int or CRS
    ///     Destination CRS.
    ///
    /// area_of_interest : sequence of float, optional
    ///     ``(west, south, east, north)`` area of interest.
    ///
    /// source_epoch, target_epoch : float, optional
    ///     Coordinate epochs for dynamic CRS.
    ///
    /// authority : str, optional
    ///     Restrict to operations from this authority.
    ///
    /// accuracy : float, optional
    ///     Maximum acceptable accuracy, in meters.
    ///
    /// allow_ballpark : bool, optional
    ///     Permit low-accuracy ballpark operations.
    ///
    /// only_best : bool, optional
    ///     Return only the single best operation.
    ///
    /// force_over : bool, optional
    ///     Prefer operations that keep coordinates in a continuous range.
    ///
    /// Returns
    /// -------
    /// list of CrsOperationInfo
    #[pyo3(signature = (target, *, area_of_interest = None, source_epoch = None, target_epoch = None, authority = None, accuracy = None, allow_ballpark = None, only_best = None, force_over = false))]
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> len(gm.CRS(4326).operations(3857)) >= 1
    /// True
    fn operations(
        slf: &Bound<'_, Self>,
        target: &Bound<'_, PyAny>,
        area_of_interest: Option<&Bound<'_, PyAny>>,
        source_epoch: Option<&Bound<'_, PyAny>>,
        target_epoch: Option<&Bound<'_, PyAny>>,
        authority: Option<String>,
        accuracy: Option<&Bound<'_, PyAny>>,
        allow_ballpark: Option<bool>,
        only_best: Option<bool>,
        force_over: bool,
    ) -> PyResult<Vec<crs::OperationInfo>> {
        crs_operations(slf.as_any(), target, TransformOptionArgs {
            area_of_interest,
            source_epoch,
            target_epoch,
            authority,
            accuracy,
            allow_ballpark,
            only_best,
            force_over,
        })
    }
}
