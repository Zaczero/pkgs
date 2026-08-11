use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes};

use crate::py::errors::require_epoch_drop;
use crate::{PyGeometry, PyGeometryArray, PyValidationReport, RepairMethod};

macro_rules! doc_to_wkt {
    (scalar) => {
        concat!(doc_to_wkt!(@body), r"

Returns
-------
str
    The WKT string.

", doc_to_wkt!(@tail))
    };
    (array) => {
        concat!(doc_to_wkt!(@body), r"

Returns
-------
list of str
    One WKT string per row.

", doc_to_wkt!(@tail))
    };
    (@body) => {
        r"Serialize to Well-Known Text.

Parameters
----------
output_dimension : int, optional
    Cap the written ordinate count (2, 3, or 4) to at most the
    geometry's own dimensionality; defaults to writing all present
    ordinates. Cannot invent Z/M that the geometry does not carry.

include_srid : bool, default False
    Embed the EPSG code as an EWKT ``SRID=<code>;`` prefix. The PostGIS wire
    aliases ``OGC:CRS84`` to SRID 4326 and ``OGC:CRS84h`` to SRID 4979;
    decoding either alias yields that EPSG identity.

precision : int, optional
    Decimal places to round coordinates to (omit for full precision).

drop_epoch : bool, default False
    Permit losing coordinate-epoch metadata, which WKT cannot encode."
    };
    (@tail) => {
        r"
Raises
------
GeometryError
    If ``output_dimension`` is not 2, 3, or 4, or ``precision`` is not
    between 0 and 15, or the geometry carries a coordinate epoch and
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
['POINT (1.5 2.5)']"
    };
}

macro_rules! doc_to_wkb {
    (scalar) => {
        concat!(doc_to_wkb!(@body), r"

Returns
-------
bytes
    The WKB payload.

", doc_to_wkb!(@tail))
    };
    (array) => {
        concat!(doc_to_wkb!(@body), r"

Returns
-------
list of bytes
    One WKB payload per row.

", doc_to_wkb!(@tail))
    };
    (@body) => {
        r"Serialize to Well-Known Binary.

Parameters
----------
include_srid : bool, default False
    Embed the EPSG code as an EWKB SRID. The PostGIS wire aliases
    ``OGC:CRS84`` to SRID 4326 and ``OGC:CRS84h`` to SRID 4979; decoding
    either alias yields that EPSG identity.

precision : int, optional
    Decimal places to round coordinates to (omit for full precision).

drop_epoch : bool, default False
    Permit losing coordinate-epoch metadata, which (E)WKB cannot encode."
    };
    (@tail) => {
        r"Notes
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
True"
    };
}

macro_rules! doc_to_geojson {
    (scalar) => {
        concat!(doc_to_geojson!(@body), r"

Returns
-------
str
    The `GeoJSON` geometry string.

", doc_to_geojson!(@tail))
    };
    (array) => {
        concat!(doc_to_geojson!(@body), r"

Returns
-------
list of str
    One `GeoJSON` geometry string per row.

", doc_to_geojson!(@tail))
    };
    (@body) => {
        r"Serialize to `GeoJSON` text. `GeoJSON` is WGS84 by specification (RFC 7946):
CRS-tagged input must be ``EPSG:4326`` (or ``OGC:CRS84``) — reproject with
``to_crs(4326)`` first. CRS-free input is serialized as-is.

Parameters
----------
include_z : bool, default True
    Write Z ordinates when present.

    `GeoJSON` cannot represent M; remove it explicitly with ``set_m(None)``.

drop_epoch : bool, default False
    Permit losing coordinate-epoch metadata, which GeoJSON cannot encode."
    };
    (@tail) => {
        r#"Raises
------
CRSError
    If the input carries a CRS other than WGS84.
InvalidGeometryError
    If input carries M ordinates.
GeometryError
    If input carries a coordinate epoch and ``drop_epoch`` is false.

See Also
--------
from_geojson : Parse `GeoJSON` back into a geometry.

Examples
--------
>>> import gometry as gm
>>> gm.Point(1, 2).to_geojson()
'{"type":"Point","coordinates":[1.0,2.0]}'"#
    };
}

macro_rules! doc_validate {
    (scalar) => {
        concat!(doc_validate!(@body), r"

Returns
-------
ValidationReport
    Truthy when valid.

", doc_validate!(@tail))
    };
    (array) => {
        concat!(doc_validate!(@body), r"

Returns
-------
list of ValidationReport
    One report per row; missing rows are ``None``.

", doc_validate!(@tail))
    };
    (@body) => {
        r"Structured validity report in the geometry's coordinate frame.
Geographic antimeridian crossings are normalized before validation;
projected and CRS-free geometry uses ordinary planar OGC validity."
    };
    (@tail) => {
        r"See Also
--------
is_valid : Boolean-only test.
repair : Fix what the report diagnoses.

Examples
--------
>>> import gometry as gm
>>> bowtie = gm.from_wkt('POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))')
>>> report = bowtie.validate()
>>> (report.valid, report.reason)
(False, 'exterior ring has a self-intersection')"
    };
}

macro_rules! doc_repair {
    (scalar) => {
        concat!(doc_repair!(@body), r"

Returns
-------
Geometry
    A valid geometry.

", doc_repair!(@tail))
    };
    (array) => {
        concat!(doc_repair!(@body), r"

Returns
-------
GeometryArray
    One valid geometry per row.

", doc_repair!(@tail))
    };
    (@body) => {
        r"Repair invalid geometry, returning corrected result(s) (OGC). Already-valid
input is returned unchanged at validation cost. Geographic antimeridian
crossings are normalized before validity is decided, so a valid seam-crossing
geometry is never destructively repaired; an invalid crossing repairs from its
seam-split form. Projected and CRS-free geometry remains planar. Z/M ordinates
are carried through the rebuild.

Parameters
----------
method : {'linework', 'structure'}, default linework
    Repair strategy: ``linework`` nodes all boundary linework and
    reassembles regions by even-odd parity, keeping every input edge;
    ``structure`` rebuilds each ring's enclosed area and recombines them
    as shells minus holes, discarding collapsed components.

    Z/M are carried at vertices traceable to the input; a rebuild that needs
    unsourceable vertices returns the mathematically natural XY result."
    };
    (@tail) => {
        r"
Raises
------
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
(True, 'MultiPolygon')"
    };
}

#[pymethods]
impl PyGeometry {
    #[doc = doc_to_wkt!(scalar)]
    #[pyo3(signature = (*, output_dimension = None, include_srid = false, precision = None, drop_epoch = false))]
    pub fn to_wkt(
        &self,
        output_dimension: Option<&Bound<'_, PyAny>>,
        include_srid: bool,
        precision: Option<&Bound<'_, PyAny>>,
        drop_epoch: bool,
    ) -> PyResult<String> {
        require_epoch_drop(self.epoch(), drop_epoch, "to_wkt")?;
        self.to_wkt_impl(output_dimension, include_srid, precision)
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
    #[pyo3(signature = (*, include_z = true, drop_epoch = false))]
    pub fn to_geojson(&self, include_z: bool, drop_epoch: bool) -> PyResult<String> {
        require_epoch_drop(self.epoch(), drop_epoch, "to_geojson")?;
        self.to_geojson_impl(include_z)
    }

    #[doc = doc_validate!(scalar)]
    pub fn validate(&self) -> PyValidationReport {
        self.validate_impl()
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
    #[pyo3(signature = (*, output_dimension = None, include_srid = false, precision = None, drop_epoch = false))]
    pub fn to_wkt(
        &self,
        py: Python<'_>,
        output_dimension: Option<&Bound<'_, PyAny>>,
        include_srid: bool,
        precision: Option<&Bound<'_, PyAny>>,
        drop_epoch: bool,
    ) -> PyResult<Py<PyAny>> {
        require_epoch_drop(self.epoch(), drop_epoch, "to_wkt")?;
        let rows = self.to_wkt_impl(py, output_dimension, include_srid, precision)?;
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
    #[pyo3(signature = (*, include_z = true, drop_epoch = false))]
    pub fn to_geojson(
        &self,
        py: Python<'_>,
        include_z: bool,
        drop_epoch: bool,
    ) -> PyResult<Py<PyAny>> {
        require_epoch_drop(self.epoch(), drop_epoch, "to_geojson")?;
        let rows = self.to_geojson_impl(py, include_z)?;
        self.masked_row_list(py, rows)
    }

    #[doc = doc_validate!(array)]
    pub fn validate(&self) -> Vec<Option<PyValidationReport>> {
        self.validate_impl()
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
