use crate::py::crs::{
    Bound, Py, PyAny, PyBool, PyCrs, PyResult, Python, crs_html_escape, parse_crs,
};
frozen_pymethods! {
impl PyCrs {
    /// Build a CRS from any accepted input.
    ///
    /// Parameters
    /// ----------
    /// value : CRS-like or CRS
    ///     Authority string, EPSG code, authority tuple, PROJJSON/CF mapping,
    ///     WKT/PROJ string, CRS-holder object, or another ``CRS``.
    ///
    /// Returns
    /// -------
    /// CRS
    ///
    /// Raises
    /// ------
    /// CRSError
    ///     If the value is not a recognized CRS.
    #[new]
    fn new(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        Self::resolve(value)
    }

    /// Equality against anything CRS-like (a `CRS`, authority string, EPSG
    /// code, or ``to_wkt()``-bearing object) by canonical identity;
    /// non-CRS operands defer with ``NotImplemented``.
    ///
    /// A ``CRS`` is unhashable because equal raw strings and integers have
    /// unrelated hashes. Key mappings by ``crs.canonical`` instead.
    fn __eq__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> Py<PyAny> {
        match parse_crs(Some(other)) {
            Ok(Some(other)) => PyBool::new(py, *other == *self.canonical)
                .to_owned()
                .unbind()
                .into(),
            _ => py.NotImplemented(),
        }
    }

    #[expect(non_upper_case_globals, reason = "Python dunder name")]
    const __hash__: Option<Py<PyAny>> = None;

    fn __str__(&self) -> String {
        self.canonical.to_string()
    }

    fn __repr__(&self) -> String {
        format!("CRS({:?})", &*self.canonical)
    }

    /// HTML preview for notebooks: a compact table of `info` fields.
    fn _repr_html_(&self) -> PyResult<String> {
        let info = self.cached_info()?;
        let mut rows = Vec::new();
        rows.push(("CRS", info.crs.clone()));
        if let Some(name) = &info.name {
            rows.push(("Name", name.clone()));
        }
        if let (Some(authority), Some(code)) = (&info.authority, &info.code) {
            rows.push(("Authority", format!("{authority}:{code}")));
        }
        rows.push(("Type", info.kind.to_owned()));
        if let Some(name) = info.datum.as_ref().and_then(|datum| datum.name.clone()) {
            rows.push(("Datum", name));
        }
        if let Some(area) = &info.area_of_use {
            let label = area
                .name
                .clone()
                .unwrap_or_else(|| "area of use".to_owned());
            rows.push((
                "Area of use",
                format!(
                    "{} ({:.4}, {:.4}, {:.4}, {:.4})",
                    label, area.west, area.south, area.east, area.north
                ),
            ));
        }
        if !info.axes.is_empty() {
            let axes = info
                .axes
                .iter()
                .map(|axis| {
                    let name = axis
                        .abbreviation
                        .as_ref()
                        .or(axis.name.as_ref())
                        .cloned()
                        .unwrap_or_else(|| "axis".to_owned());
                    let direction = axis.direction.as_deref().unwrap_or("?").to_owned();
                    let unit = axis.unit_name.as_deref().unwrap_or("?").to_owned();
                    format!("{name} ({direction}, {unit})")
                })
                .collect::<Vec<_>>()
                .join(", ");
            rows.push(("Axes", axes));
        }
        let mut html = String::from("<table class=\"gometry-crs-html\">");
        for (label, value) in rows {
            html.push_str("<tr><th>");
            html.push_str(&crs_html_escape(label));
            html.push_str("</th><td>");
            html.push_str(&crs_html_escape(&value));
            html.push_str("</td></tr>");
        }
        html.push_str("</table>");
        Ok(html)
    }

    /// Pickle support: a CRS is its canonical string; the constructor
    /// rebuilds everything else lazily.
    fn __reduce__(&self, py: Python<'_>) -> (Py<PyAny>, (String,)) {
        (
            py.get_type::<PyCrs>().into_any().unbind(),
            (self.canonical.to_string(),),
        )
    }
}
}
