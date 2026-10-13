use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::py::errors::require_epoch_drop;
use crate::{PyGeometry, PyGeometryArray, PyValidationReport, RepairMethod};

method_doc! {
    doc_to_wkt;
    body: concat!(r"Serialize to Well-Known Text.

", crate::writer_ordinate_policy!(), r"

Parameters
----------
include_srid : bool, default False
    Embed the EPSG code as an EWKT ``SRID=<code>;`` prefix. The PostGIS wire
    aliases ``OGC:CRS84`` to SRID 4326 and ``OGC:CRS84h`` to SRID 4979;
    decoding either alias yields that EPSG identity.

precision : int, optional
    Decimal places to round coordinates to (omit for full precision).

drop_epoch : bool, default False
    Permit losing coordinate-epoch metadata, which WKT cannot encode.");
    scalar: r"

Returns
-------
str
    The WKT string.

";
    array: r"

Returns
-------
list of str
    One WKT string per row; missing rows yield ``None``.

";
    tail: r"
Raises
------
GeometryError
    If ``precision`` is not between 0 and 15, or the geometry carries a coordinate epoch and
    ``drop_epoch`` is false.
CRSError
    If ``include_srid`` is set and the CRS has no EPSG code.

See Also
--------
from_wkt : Parse WKT back into a geometry.

Examples
--------
>>> import gometry as gm
>>> gm.Point(1.5, 2.5).to_wkt()
'POINT (1.5 2.5)'
>>> gm.GeometryArray([gm.Point(1.5, 2.5)]).to_wkt()
['POINT (1.5 2.5)']";
}

method_doc! {
    doc_to_wkb;
    body: concat!(r"Serialize to Well-Known Binary.

", crate::writer_ordinate_policy!(), r"

Parameters
----------
include_srid : bool, default False
    Embed the EPSG code as an EWKB SRID. The PostGIS wire aliases
    ``OGC:CRS84`` to SRID 4326 and ``OGC:CRS84h`` to SRID 4979; decoding
    either alias yields that EPSG identity.

precision : int, optional
    Decimal places to round coordinates to (omit for full precision).

drop_epoch : bool, default False
    Permit losing coordinate-epoch metadata, which (E)WKB cannot encode.");
    scalar: r"

Returns
-------
bytes
    The WKB payload.

";
    array: r"

Returns
-------
list of bytes
    One WKB payload per row; missing rows yield ``None``.

";
    tail: r"Notes
-----
The coordinate epoch is not representable in (E)WKB and does not survive a
round-trip; use Arrow interchange when the epoch matters.

Raises
------
GeometryError
    If ``precision`` is not between 0 and 15, or the geometry carries a
    coordinate epoch and ``drop_epoch`` is false.
CRSError
    If ``include_srid`` is set and the CRS has no EPSG code.

See Also
--------
from_wkb : Parse WKB/EWKB back into a geometry.

Examples
--------
>>> import gometry as gm
>>> pt = gm.Point(1, 2)
>>> pt.to_wkt() == gm.from_wkb(pt.to_wkb()).to_wkt()
True";
}

method_doc! {
    doc_to_geojson;
    body: concat!(r"Serialize to `GeoJSON` text. GeoJSON uses OGC:CRS84 longitude/latitude
order under RFC 7946 and carries no source CRS label. CRS-tagged input must use
the 2D WGS 84 geographic family
(``EPSG:4326`` or ``OGC:CRS84``) or the 3D longitude/latitude/height family
(``EPSG:4979`` or ``OGC:CRS84h``); reproject other CRS values first. CRS-free
input is explicitly assumed to already be WGS84 longitude/latitude:
coordinates outside longitude ``[-180, 180]`` or latitude ``[-90, 90]`` are
rejected, and antimeridian-crossing geometry is split according to RFC 7946.
If CRS-free coordinates have projected or other planar semantics, transform
them or choose another format.

", crate::writer_ordinate_policy!(), r"

GeoJSON cannot represent M; drop it with ``with_coordinates(m=None)`` (or ``force_2d()``).

Parameters
----------
drop_epoch : bool, default False
    Permit losing coordinate-epoch metadata, which GeoJSON cannot encode.");
    scalar: r"

Returns
-------
str
    The `GeoJSON` geometry string.

";
    array: r"

Returns
-------
list of str
    One `GeoJSON` geometry string per row; missing rows yield ``None``.

";
    tail: r#"Raises
------
CRSError
    If the input carries a CRS outside the 2D/3D WGS84 longitude/latitude family.
InvalidGeometryError
    If input carries M ordinates; drop them with ``with_coordinates(m=None)`` (or ``force_2d()``).
GeometryError
    If input carries a coordinate epoch and ``drop_epoch`` is false.

See Also
--------
from_geojson : Parse `GeoJSON` back into a geometry.

Examples
--------
>>> import gometry as gm
>>> gm.Point(1, 2).to_geojson()
'{"type":"Point","coordinates":[1.0,2.0]}'"#;
}

method_doc! {
    doc_validate;
    body: r"Structured validity report in the geometry's coordinate frame.
Geographic validity reads exact quotient topology, so period aliases and seam
cuts do not depend on their stored longitude spelling. An out-of-domain
latitude is reported as ``latitude_out_of_range``; geographic operations that
require coordinate admission still raise. For quotient-topology issues, the
location is a canonical coordinate of the issue's contact set, chosen
deterministically by minimum latitude then longitude. Projected and CRS-free geometry uses ordinary
planar OGC validity. A nonempty LineString with fewer than two distinct XY
positions is invalid and reports ``line_collapsed``.";
    scalar: r"

Returns
-------
ValidationReport
    Truthy when valid.

";
    array: r"

Returns
-------
list of ValidationReport
    One report per row; missing rows are ``None``.

";
    tail: r"See Also
--------
is_valid : Boolean-only test.
repair : Fix what the report diagnoses.

Examples
--------
>>> import gometry as gm
>>> bowtie = gm.from_wkt('POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))')
>>> report = bowtie.validate()
>>> (report.valid, report.reason)
(False, 'exterior ring has a self-intersection')";
}

method_doc! {
    doc_repair;
    body: r"Repair invalid geometry, returning corrected result(s) (OGC). Already-valid
input is returned unchanged at validation cost. Geographic repair checks exact
quotient validity before rebuilding; when output construction needs seam
vertices it splits the source while preserving traceable Z/M ordinates.
Projected and CRS-free geometry remains planar. A collapsed LineString becomes
a Point at its first coordinate. Collapsed MultiLineString members become a
Point or MultiPoint, combined with surviving linework in a GeometryCollection
when both dimensions remain.

Parameters
----------
method : {'linework', 'structure'}, default linework
    Repair strategy: ``linework`` nodes all boundary linework and selects
    regions from each original ring occurrence's winding modulo two before
    recombining each polygon's shell minus its holes; distinct polygon members
    are unioned. ``structure`` rebuilds each ring's nonzero-winding enclosed
    area and recombines it as shells minus holes.

    Z/M are carried at vertices traceable to the input; a rebuild that needs
    unsourceable vertices returns the mathematically natural XY result.";
    scalar: r"

Returns
-------
Geometry
    A valid geometry.

";
    array: r"

Returns
-------
GeometryArray
    One valid geometry per row.

";
    tail: r"
Raises
------
GeometryError
    If no adjacent binary64 assignment preserves the exact repaired geometry
    or the bounded embedding search is exhausted.
InvalidGeometryError
    If the geometry cannot be repaired.

See Also
--------
validate : Structured validity report.
is_valid : Boolean-only test.

Examples
--------
>>> import gometry as gm
>>> bowtie = gm.from_wkt('POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))')
>>> fixed = bowtie.repair()
>>> (fixed.is_valid, fixed.geometry_type)
(True, 'MultiPolygon')";
}

#[pymethods]
impl PyGeometry {
    #[doc = doc_to_wkt!(scalar)]
    #[pyo3(signature = (*, include_srid = false, precision = None, drop_epoch = false))]
    pub fn to_wkt(
        &self,
        py: Python<'_>,
        include_srid: bool,
        precision: Option<&Bound<'_, PyAny>>,
        drop_epoch: bool,
    ) -> PyResult<String> {
        require_epoch_drop(self.epoch(), drop_epoch, "to_wkt")?;
        self.to_wkt_impl(py, include_srid, precision)
    }

    #[doc = doc_to_wkb!(scalar)]
    #[pyo3(signature = (*, include_srid = false, precision = None, drop_epoch = false))]
    pub fn to_wkb<'py>(
        &self,
        py: Python<'py>,
        include_srid: bool,
        precision: Option<&Bound<'py, PyAny>>,
        drop_epoch: bool,
    ) -> PyResult<Bound<'py, PyBytes>> {
        require_epoch_drop(self.epoch(), drop_epoch, "to_wkb")?;
        self.to_wkb_impl(py, include_srid, precision)
    }

    #[doc = doc_to_geojson!(scalar)]
    #[pyo3(signature = (*, drop_epoch = false))]
    pub fn to_geojson(&self, py: Python<'_>, drop_epoch: bool) -> PyResult<String> {
        require_epoch_drop(self.epoch(), drop_epoch, "to_geojson")?;
        self.to_geojson_impl(py)
    }

    #[doc = doc_validate!(scalar)]
    pub fn validate(&self, py: Python<'_>) -> PyResult<PyValidationReport> {
        self.validate_impl(py)
    }

    #[doc = doc_repair!(scalar)]
    #[pyo3(
        signature = (*, method = RepairMethod::Linework),
        text_signature = "($self, *, method='linework')"
    )]
    pub fn repair(&self, py: Python<'_>, method: RepairMethod) -> PyResult<crate::Typed> {
        self.repair_impl(py, method)
    }
}

#[pymethods]
impl PyGeometryArray {
    #[doc = doc_to_wkt!(array)]
    #[pyo3(signature = (*, include_srid = false, precision = None, drop_epoch = false))]
    pub fn to_wkt(
        &self,
        py: Python<'_>,
        include_srid: bool,
        precision: Option<&Bound<'_, PyAny>>,
        drop_epoch: bool,
    ) -> PyResult<Py<PyAny>> {
        require_epoch_drop(self.epoch(), drop_epoch, "to_wkt")?;
        let rows = self.to_wkt_impl(py, include_srid, precision)?;
        self.masked_row_list(py, rows)
    }

    #[doc = doc_to_wkb!(array)]
    #[pyo3(signature = (*, include_srid = false, precision = None, drop_epoch = false))]
    pub fn to_wkb(
        &self,
        py: Python<'_>,
        include_srid: bool,
        precision: Option<&Bound<'_, PyAny>>,
        drop_epoch: bool,
    ) -> PyResult<Py<PyAny>> {
        require_epoch_drop(self.epoch(), drop_epoch, "to_wkb")?;
        let rows = self.to_wkb_impl(py, include_srid, precision)?;
        self.masked_row_list(py, rows)
    }

    #[doc = doc_to_geojson!(array)]
    #[pyo3(signature = (*, drop_epoch = false))]
    pub fn to_geojson(&self, py: Python<'_>, drop_epoch: bool) -> PyResult<Py<PyAny>> {
        require_epoch_drop(self.epoch(), drop_epoch, "to_geojson")?;
        let rows = self.to_geojson_impl(py)?;
        self.masked_row_list(py, rows)
    }

    #[doc = doc_validate!(array)]
    pub fn validate(&self, py: Python<'_>) -> PyResult<Vec<Option<PyValidationReport>>> {
        self.validate_impl(py)
    }

    #[doc = doc_repair!(array)]
    #[pyo3(
        signature = (*, method = RepairMethod::Linework),
        text_signature = "($self, *, method='linework')"
    )]
    pub fn repair(&self, py: Python<'_>, method: RepairMethod) -> PyResult<Self> {
        self.repair_impl(py, method)
    }
}
