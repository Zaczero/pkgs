#![expect(
    clippy::used_underscore_items,
    reason = "`_repr_html_` is IPython's display protocol name"
)]
use pyo3::PyRef;
use pyo3::types::PyAnyMethods as _;

use crate::html_text_escape;
use crate::py::crs::{Bound, Py, PyAny, PyBool, PyCrs, PyResult, Python};
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

    /// Equality against another ``CRS`` by exact stored definition. Other
    /// operand types defer with ``NotImplemented``; construct a ``CRS`` first
    /// when stored identity is intended.
    ///
    /// Operational equivalence across different definitions belongs to
    /// ``same_as``. Equal ``CRS`` instances have equal hashes.
    fn __eq__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> Py<PyAny> {
        if let Ok(other) = other.extract::<PyRef<'_, Self>>() {
            return PyBool::new(
                py,
                self.definition.as_str() == other.definition.as_str(),
            )
            .to_owned()
            .unbind()
            .into();
        }
        py.NotImplemented()
    }

    fn __hash__(&self, py: Python<'_>) -> PyResult<isize> {
        py.import("builtins")?
            .getattr("hash")?
            .call1((self.definition.as_str(),))?
            .extract()
    }

    fn __str__(&self) -> String {
        self.definition.to_string()
    }

    fn __repr__(&self) -> String {
        format!("CRS({:?})", &*self.definition)
    }

    /// HTML preview for notebooks: a compact table of `info` fields.
    fn _repr_html_(&self) -> PyResult<String> {
        let info = self.cached_info()?;
        let mut rows: Vec<(&str, String)> = Vec::new();
        rows.push(("CRS", info.definition.to_string()));
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
            html.push_str(&html_text_escape(label));
            html.push_str("</th><td>");
            html.push_str(&html_text_escape(&value));
            html.push_str("</td></tr>");
        }
        html.push_str("</table>");
        Ok(html)
    }

    /// Pickle support: retain the definition and rebuild derived metadata lazily.
    fn __reduce__(&self, py: Python<'_>) -> PyResult<(Py<PyAny>, (String,))> {
        Ok((
            crate::gometry_lib_module(py)?
                .getattr(pyo3::intern!(py, "_unpickle_crs_v1"))?
                .unbind(),
            (self.definition.to_string(),),
        ))
    }
}
}
