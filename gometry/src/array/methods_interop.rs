//! Explicit optional-framework boundaries for `GeometryArray`.

use crate::array::{
    Bound, Py, PyAny, PyAnyMethods as _, PyDict, PyDictMethods as _, PyGeometryArray, PyResult,
    Python, pymethods,
};

#[pymethods]
impl PyGeometryArray {
    /// Build a pandas Series backed by gometry's concrete extension dtype.
    ///
    /// Parameters
    /// ----------
    /// index : sequence or pandas.Index, optional
    ///     Forwarded to ``pandas.Series``.
    /// name : hashable, optional
    ///     Series name.
    ///
    /// Returns
    /// -------
    /// object
    ///     A pandas Series sharing this geometry array.
    #[pyo3(signature = (*, index = None, name = None), text_signature = "($self, *, index=None, name=None)")]
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> type(gm.GeometryArray([gm.Point(1, 2, crs=4326)]).to_pandas()).__name__
    /// 'Series'
    pub fn to_pandas(
        &self,
        py: Python<'_>,
        index: Option<&Bound<'_, PyAny>>,
        name: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let module = py.import("gometry._pandas")?;
        let kwargs = PyDict::new(py);
        match index {
            Some(value) => kwargs.set_item("index", value)?,
            None => kwargs.set_item("index", py.None())?,
        }
        match name {
            Some(value) => kwargs.set_item("name", value)?,
            None => kwargs.set_item("name", py.None())?,
        }
        Ok(module
            .getattr("to_pandas")?
            .call((self.clone(),), Some(&kwargs))?
            .unbind())
    }

    /// Encode this array as a Polars binary (E)WKB Series.
    ///
    /// Parameters
    /// ----------
    /// name : str, default "geometry"
    ///     Output Series name.
    /// drop_epoch : bool, default False
    ///     Permit losing coordinate-epoch metadata, which WKB cannot encode.
    /// drop_crs : bool, default False
    ///     Permit losing a CRS that EWKB cannot embed (no EPSG authority
    ///     code); restore it via ``from_polars(..., crs=...)``. EPSG SRIDs
    ///     are always embedded when available.
    ///
    /// Returns
    /// -------
    /// object
    ///     A Polars Series containing WKB or EWKB values.
    #[pyo3(signature = (*, name = "geometry", drop_epoch = false, drop_crs = false))]
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> type(gm.GeometryArray([gm.Point(1, 2, crs=4326)]).to_polars()).__name__
    /// 'Series'
    pub fn to_polars(
        &self,
        py: Python<'_>,
        name: &str,
        drop_epoch: bool,
        drop_crs: bool,
    ) -> PyResult<Py<PyAny>> {
        let module = py.import("gometry._polars")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("name", name)?;
        kwargs.set_item("drop_epoch", drop_epoch)?;
        kwargs.set_item("drop_crs", drop_crs)?;
        Ok(module
            .getattr("to_polars")?
            .call((self.clone(),), Some(&kwargs))?
            .unbind())
    }

    /// Convert this array to a GeoPandas GeoSeries through vectorized WKB.
    ///
    /// Parameters
    /// ----------
    /// drop_epoch : bool, default False
    ///     Permit losing coordinate-epoch metadata, which GeoPandas cannot
    ///     represent.
    ///
    /// Returns
    /// -------
    /// object
    ///     A GeoPandas GeoSeries carrying this array's CRS.
    #[pyo3(signature = (*, drop_epoch = false))]
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> type(gm.GeometryArray([gm.Point(1, 2, crs=4326)]).to_geopandas()).__name__
    /// 'GeoSeries'
    pub fn to_geopandas(&self, py: Python<'_>, drop_epoch: bool) -> PyResult<Py<PyAny>> {
        let module = py.import("gometry._geopandas")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("drop_epoch", drop_epoch)?;
        Ok(module
            .getattr("to_geopandas")?
            .call((self.clone(),), Some(&kwargs))?
            .unbind())
    }

    /// Write this array to a GeoParquet file, optionally as a feature table.
    ///
    /// Parameters
    /// ----------
    /// path : path-like
    ///     Output Parquet path.
    /// attributes : pyarrow.Table or mapping, optional
    ///     Per-row attribute columns written beside the geometry column
    ///     (lengths must match).
    /// encoding : str, default "wkb"
    ///     Geometry encoding: ``'wkb'`` (portable default) or ``'native'``
    ///     (GeoArrow separated coordinates for homogeneous arrays).
    /// kwargs : mapping, optional
    ///     Additional options forwarded to ``pyarrow.parquet.write_table``.
    ///
    /// Returns
    /// -------
    /// None
    ///
    /// Raises
    /// ------
    /// GeometryError
    ///     If encoding is unknown, or attributes clash with the geometry
    ///     column or mismatch the row count.
    #[pyo3(signature = (path, *, attributes = None, encoding = "wkb", **kwargs))]
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> import tempfile, os
    /// >>> path = tempfile.mktemp(suffix='.parquet')
    /// >>> gm.GeometryArray([gm.Point(1, 2, crs=4326)]).to_geoparquet(path)
    /// >>> os.path.getsize(path) > 0
    /// True
    pub fn to_geoparquet(
        &self,
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        attributes: Option<&Bound<'_, PyAny>>,
        encoding: &str,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        let forwarded = match kwargs {
            Some(existing) => existing.copy()?,
            None => PyDict::new(py),
        };
        forwarded.set_item("attributes", attributes)?;
        forwarded.set_item("encoding", encoding)?;
        py.import("gometry._geoparquet")?
            .getattr("to_geoparquet")?
            .call((self.clone(), path), Some(&forwarded))?;
        Ok(())
    }
}
