//! Public CRS data-transfer objects: `CrsInfo` and the metadata/options/
//! catalog structs returned across the CRS API. Pure data — no logic, FFI, or
//! caches; the engine in the sibling modules produces and consumes them.
//!
//! Result DTOs mostly derive [`IntoPyObject`], which converts a named struct
//! into a `PyDict` keyed by field names — the field names and order ARE the
//! Python dict contract, so renames here are API changes.

use std::borrow::Cow;
use std::num::NonZeroUsize;

use pyo3::IntoPyObject;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::crs::Confidence;

crate::tokens::token_enum! {
    /// How `CRS.same_as` compares two CRS — one mutually-exclusive choice
    /// instead of an `(exact, ignore_axis_order)` bool pair whose
    /// `(true, true)` state was illegal and only rejected at runtime.
    pub(crate) enum CrsComparison("CRS comparison mode", token = none, param = "mode") {
        /// Equivalent ignoring geographic axis order (lon/lat vs lat/lon).
        IgnoreAxisOrder = "ignore_axis_order",
        /// Strict, exact match (every detail identical).
        Exact = "exact",
    }
}

crate::tokens::token_enum! {
    /// WKT dialect/version for CRS serialization, parsed once at the boundary
    /// instead of re-matching the version string on every export.
    pub(crate) enum WktVersion("WKT version", normalize = export_token, token = none) {
        /// WKT2:2019 (the default).
        Wkt2_2019 = "WKT2_2019",
        /// WKT2:2019, simplified profile.
        Wkt2_2019Simplified = "WKT2_2019_SIMPLIFIED",
        /// WKT2:2015.
        Wkt2_2015 = "WKT2_2015",
        /// WKT2:2015, simplified profile.
        Wkt2_2015Simplified = "WKT2_2015_SIMPLIFIED",
        /// WKT1, GDAL dialect.
        Wkt1Gdal = "WKT1_GDAL",
        /// WKT1, ESRI dialect.
        Wkt1Esri = "WKT1_ESRI",
    }
}

impl std::hash::Hash for WktVersion {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        (*self as u8).hash(state);
    }
}

crate::tokens::token_enum! {
    /// WKT `OUTPUT_AXIS` rule: whether to emit `AXIS[]` nodes. One enum
    /// instead of the `"auto"`/`"yes"`/`"no"` string re-parsed in both
    /// validation and export.
    pub(crate) enum WktAxisRule("WKT output_axis", normalize = lowercase_token, token = none, param = "output_axis") {
        /// Let PROJ decide based on the WKT version (the default).
        Auto = "auto",
        /// Always emit axis nodes.
        Yes = "yes",
        /// Never emit axis nodes.
        No = "no",
    }
}

crate::tokens::token_from_pyobject!(WktVersion, WktAxisRule, CrsComparison, ProjDirection);

/// PROJ database object type — one enum behind the `kind` string.
///
/// Used by the CRS catalog/search/codes APIs, replacing the
/// string↔`PJ_TYPE`↔`PJ_CATEGORY` re-matching that was split across three free
/// functions and re-run on every catalog query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum CrsObjectKind {
    /// Any CRS (the default).
    Crs,
    GeodeticCrs,
    GeographicCrs,
    Geographic2dCrs,
    Geographic3dCrs,
    GeocentricCrs,
    ProjectedCrs,
    VerticalCrs,
    CompoundCrs,
    TemporalCrs,
    EngineeringCrs,
    BoundCrs,
    OtherCrs,
    DerivedProjectedCrs,
    Ellipsoid,
    PrimeMeridian,
    GeodeticReferenceFrame,
    DynamicGeodeticReferenceFrame,
    VerticalReferenceFrame,
    DynamicVerticalReferenceFrame,
    DatumEnsemble,
    TemporalDatum,
    EngineeringDatum,
    ParametricDatum,
    Conversion,
    Transformation,
    ConcatenatedOperation,
    OtherCoordinateOperation,
}

#[derive(Debug, Clone)]
pub(crate) struct CrsInfo {
    pub crs: String,
    pub name: Option<String>,
    pub authority: Option<String>,
    pub code: Option<String>,
    pub kind: &'static str,
    pub is_derived: bool,
    pub deprecated: bool,
    pub remarks: Option<String>,
    pub scope: Option<String>,
    pub coordinate_system: Option<&'static str>,
    pub axis_order: Vec<&'static str>,
    pub celestial_body: Option<String>,
    pub has_point_motion_operation: bool,
    pub area_of_use: Option<AreaOfUse>,
    pub axes: Vec<AxisInfo>,
    pub domains: Vec<DomainInfo>,
    pub sub_crs: Vec<AuthorityObjectInfo>,
    pub source_crs: Option<AuthorityObjectInfo>,
    pub target_crs: Option<AuthorityObjectInfo>,
    pub coordinate_operation: Option<CrsCoordinateOperationInfo>,
    pub geodetic_crs: Option<AuthorityObjectInfo>,
    pub horizontal_datum: Option<AuthorityObjectInfo>,
    pub datum: Option<DatumInfo>,
    pub ellipsoid: Option<EllipsoidInfo>,
    pub prime_meridian: Option<PrimeMeridianInfo>,
}

impl CrsInfo {
    /// Whether this CRS — or any component of a compound CRS — has a kind
    /// matching `predicate`.
    pub(crate) fn has_kind(&self, predicate: impl Fn(&str) -> bool) -> bool {
        predicate(self.kind) || self.sub_crs.iter().any(|item| predicate(item.kind))
    }

    pub(crate) fn is_geographic(&self) -> bool {
        self.has_kind(|kind| kind.starts_with("geographic"))
    }

    pub(crate) fn is_projected(&self) -> bool {
        self.has_kind(|kind| kind == "projected")
    }

    pub(crate) fn is_vertical(&self) -> bool {
        self.has_kind(|kind| kind == "vertical")
    }

    pub(crate) fn is_geocentric(&self) -> bool {
        self.has_kind(|kind| kind == "geocentric")
    }

    pub(crate) fn is_compound(&self) -> bool {
        self.kind == "compound"
    }

    pub(crate) fn is_engineering(&self) -> bool {
        self.has_kind(|kind| kind == "engineering")
    }

    pub(crate) fn is_bound(&self) -> bool {
        self.kind == "bound"
    }

    pub(crate) const fn is_deprecated(&self) -> bool {
        self.deprecated
    }
}

impl<'py> IntoPyObject<'py> for CrsInfo {
    type Target = PyDict;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let is_geographic = self.is_geographic();
        let is_projected = self.is_projected();
        let is_vertical = self.is_vertical();
        let is_geocentric = self.is_geocentric();
        let is_compound = self.is_compound();
        let is_engineering = self.is_engineering();
        let is_bound = self.is_bound();
        let Self {
            crs,
            name,
            authority,
            code,
            kind,
            is_derived,
            deprecated,
            remarks,
            scope,
            coordinate_system,
            axis_order,
            celestial_body,
            has_point_motion_operation,
            area_of_use,
            axes,
            domains,
            sub_crs,
            source_crs,
            target_crs,
            coordinate_operation,
            geodetic_crs,
            horizontal_datum,
            datum,
            ellipsoid,
            prime_meridian,
        } = self;
        let dict = PyDict::new(py);
        dict.set_item("crs", crs)?;
        dict.set_item("name", name)?;
        dict.set_item("authority", authority)?;
        dict.set_item("code", code)?;
        dict.set_item("kind", kind)?;
        dict.set_item("is_derived", is_derived)?;
        dict.set_item("deprecated", deprecated)?;
        dict.set_item("remarks", remarks)?;
        dict.set_item("scope", scope)?;
        dict.set_item("coordinate_system", coordinate_system)?;
        dict.set_item("axis_order", axis_order)?;
        dict.set_item("celestial_body", celestial_body)?;
        dict.set_item("has_point_motion_operation", has_point_motion_operation)?;
        dict.set_item("area_of_use", area_of_use)?;
        dict.set_item("axes", axes)?;
        dict.set_item("domains", domains)?;
        dict.set_item("sub_crs", sub_crs)?;
        dict.set_item("source_crs", source_crs)?;
        dict.set_item("target_crs", target_crs)?;
        dict.set_item("coordinate_operation", coordinate_operation)?;
        dict.set_item("geodetic_crs", geodetic_crs)?;
        dict.set_item("horizontal_datum", horizontal_datum)?;
        dict.set_item("datum", datum)?;
        dict.set_item("ellipsoid", ellipsoid)?;
        dict.set_item("prime_meridian", prime_meridian)?;
        dict.set_item("is_geographic", is_geographic)?;
        dict.set_item("is_projected", is_projected)?;
        dict.set_item("is_vertical", is_vertical)?;
        dict.set_item("is_geocentric", is_geocentric)?;
        dict.set_item("is_compound", is_compound)?;
        dict.set_item("is_engineering", is_engineering)?;
        dict.set_item("is_bound", is_bound)?;
        Ok(dict)
    }
}

#[derive(Debug, Clone, IntoPyObject)]
pub(crate) struct DomainInfo {
    pub scope: Option<String>,
    pub area_of_use: Option<AreaOfUse>,
}

#[derive(Debug, Clone, IntoPyObject)]
pub(crate) struct DatumInfo {
    pub name: Option<String>,
    pub authority: Option<String>,
    pub code: Option<String>,
    pub kind: &'static str,
    pub frame_reference_epoch: Option<f64>,
    pub ensemble_accuracy: Option<f64>,
    pub ensemble_members: Vec<Self>,
}

#[derive(Debug, Clone, IntoPyObject)]
pub(crate) struct EllipsoidInfo {
    pub name: Option<String>,
    pub semi_major_metre: f64,
    pub semi_minor_metre: f64,
    pub inverse_flattening: f64,
    pub is_semi_minor_computed: bool,
}

#[derive(Debug, Clone, IntoPyObject)]
pub(crate) struct PrimeMeridianInfo {
    pub name: Option<String>,
    pub longitude: f64,
    pub unit_name: Option<String>,
    pub unit_conversion_factor: f64,
}

#[derive(Debug, Clone, IntoPyObject)]
pub(crate) struct AxisInfo {
    pub name: Option<String>,
    pub abbreviation: Option<String>,
    pub direction: Option<String>,
    pub unit_name: Option<String>,
    pub unit_conversion_factor: f64,
}

#[derive(Debug, Clone, IntoPyObject)]
pub(crate) struct AreaOfUse {
    pub west: f64,
    pub south: f64,
    pub east: f64,
    pub north: f64,
    pub name: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct AreaOfInterest {
    pub west: f64,
    pub south: f64,
    pub east: f64,
    pub north: f64,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct TransformOptions {
    pub area_of_interest: Option<AreaOfInterest>,
    pub source_epoch: Option<f64>,
    pub target_epoch: Option<f64>,
    pub authority: Option<String>,
    pub(crate) accuracy: Option<crate::NonNegative>,
    pub allow_ballpark: Option<bool>,
    pub only_best: Option<bool>,
    pub force_over: bool,
}

crate::tokens::token_enum! {
    /// Direction of a PROJ pipeline application.
    pub(crate) enum ProjDirection("direction", normalize = lowercase_token, token = none, param = "direction") {
        Forward = "forward",
        Inverse = "inverse",
    }
}

#[derive(Debug, Clone, IntoPyObject)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "mirrors the Python operation-dict contract: independent PROJ capability flags"
)]
pub(crate) struct OperationInfo {
    pub name: Option<String>,
    pub definition: Option<String>,
    pub description: Option<String>,
    pub accuracy: Option<f64>,
    pub has_inverse: bool,
    pub has_ballpark_transformation: bool,
    pub requires_coordinate_epoch: bool,
    pub instantiable: bool,
    pub method: Option<MethodInfo>,
    pub parameters: Vec<OperationParameterInfo>,
    pub grids: Vec<GridInfo>,
    pub steps: Vec<CrsCoordinateOperationInfo>,
    pub area_of_use: Option<AreaOfUse>,
    pub source: String,
    pub target: String,
    pub source_epoch: Option<f64>,
    pub target_epoch: Option<f64>,
}

#[derive(Debug, Clone, Copy, IntoPyObject)]
pub(crate) struct ProjectionFactors {
    pub meridional_scale: f64,
    pub parallel_scale: f64,
    pub areal_scale: f64,
    pub angular_distortion: f64,
    pub meridian_parallel_angle: f64,
    pub meridian_convergence: f64,
    pub tissot_semimajor: f64,
    pub tissot_semiminor: f64,
    pub dx_dlam: f64,
    pub dx_dphi: f64,
    pub dy_dlam: f64,
    pub dy_dphi: f64,
}

#[derive(Debug, Clone)]
pub(crate) struct ProjectionFactorColumns {
    pub meridional_scale: Vec<f64>,
    pub parallel_scale: Vec<f64>,
    pub areal_scale: Vec<f64>,
    pub angular_distortion: Vec<f64>,
    pub meridian_parallel_angle: Vec<f64>,
    pub meridian_convergence: Vec<f64>,
    pub tissot_semimajor: Vec<f64>,
    pub tissot_semiminor: Vec<f64>,
    pub dx_dlam: Vec<f64>,
    pub dx_dphi: Vec<f64>,
    pub dy_dlam: Vec<f64>,
    pub dy_dphi: Vec<f64>,
}

#[derive(Debug, Clone)]
pub(crate) struct GeodesicInverseInfo {
    pub distance: f64,
    pub distance_3d: Option<f64>,
    pub forward_azimuth: f64,
    pub reverse_azimuth: f64,
}

#[derive(Debug, Clone)]
pub(crate) struct GeodesicDirectInfo {
    pub longitude: f64,
    pub latitude: f64,
    pub final_azimuth: f64,
}

#[derive(Debug, Clone)]
pub(crate) struct GeodesicDirectColumns {
    pub longitude: Vec<f64>,
    pub latitude: Vec<f64>,
    pub final_azimuth: Vec<f64>,
}

#[derive(Debug, Clone)]
pub(crate) struct GeodesicInterpolateInfo {
    pub longitude: f64,
    pub latitude: f64,
    pub final_azimuth: f64,
    pub distance: f64,
}

#[derive(Debug, Clone)]
pub(crate) enum CfValue {
    String(String),
    Float(f64),
    FloatList(Vec<f64>),
}

#[derive(Debug, Clone, IntoPyObject)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "mirrors the Python operation-dict contract: independent PROJ capability flags"
)]
pub(crate) struct CrsCoordinateOperationInfo {
    pub name: Option<String>,
    pub definition: Option<String>,
    pub description: Option<String>,
    pub accuracy: Option<f64>,
    pub has_inverse: bool,
    pub has_ballpark_transformation: bool,
    pub requires_coordinate_epoch: bool,
    pub instantiable: bool,
    pub method: Option<MethodInfo>,
    pub parameters: Vec<OperationParameterInfo>,
    pub grids: Vec<GridInfo>,
    pub steps: Vec<Self>,
    pub area_of_use: Option<AreaOfUse>,
}

#[derive(Debug, Clone, IntoPyObject)]
pub(crate) struct OperationParameterInfo {
    pub name: Option<String>,
    pub authority: Option<String>,
    pub code: Option<String>,
    pub value: f64,
    pub value_string: Option<String>,
    pub unit_conversion_factor: f64,
    pub unit_name: Option<String>,
    pub unit_authority: Option<String>,
    pub unit_code: Option<String>,
    pub unit_category: Option<String>,
}

#[derive(Debug, Clone, IntoPyObject)]
pub(crate) struct MethodInfo {
    pub name: Option<String>,
    pub authority: Option<String>,
    pub code: Option<String>,
}

#[derive(Debug, Clone, IntoPyObject)]
pub(crate) struct GridInfo {
    pub short_name: Option<String>,
    pub full_name: Option<String>,
    pub package_name: Option<String>,
    pub available: bool,
}

#[derive(Debug, Clone, IntoPyObject)]
pub(crate) struct GridDatabaseInfo {
    pub name: String,
    pub full_name: Option<String>,
    pub package_name: Option<String>,
    pub url: Option<String>,
    pub direct_download: bool,
    pub available: bool,
}

#[derive(Debug, Clone, IntoPyObject)]
pub(crate) struct UnitInfo {
    pub authority: Option<String>,
    pub code: Option<String>,
    pub name: Option<String>,
    pub category: Option<String>,
    pub conversion_factor: f64,
    pub proj_short_name: Option<String>,
}

#[derive(Debug, Clone, IntoPyObject)]
pub(crate) struct ProjOperationCatalogInfo {
    pub id: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, IntoPyObject)]
pub(crate) struct EllipsoidCatalogInfo {
    pub id: String,
    pub semi_major: Option<String>,
    pub definition: Option<String>,
    pub name: Option<String>,
}

#[derive(Debug, Clone, IntoPyObject)]
pub(crate) struct PrimeMeridianCatalogInfo {
    pub id: String,
    pub definition: Option<String>,
}

#[derive(Debug, Clone, IntoPyObject)]
pub(crate) struct CrsCatalogInfo {
    pub crs: String,
    pub authority: Option<String>,
    pub code: Option<String>,
    pub name: Option<String>,
    pub kind: &'static str,
    pub deprecated: bool,
    pub area_of_use: Option<AreaOfUse>,
    pub projection_method_name: Option<String>,
    pub celestial_body: Option<String>,
}

#[derive(Debug, Clone, IntoPyObject)]
pub(crate) struct CelestialBodyInfo {
    pub authority: Option<String>,
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CrsCatalogOptions {
    pub kind: Option<CrsObjectKind>,
    pub area: Option<AreaOfInterest>,
    pub contains_area: bool,
    pub allow_deprecated: bool,
    pub celestial_body: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct UtmCatalogOptions {
    pub datum_name: Option<String>,
    pub area: Option<AreaOfInterest>,
    pub contains_area: bool,
    pub allow_deprecated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CrsSearchOptions {
    pub authority: Option<String>,
    pub kind: Option<CrsObjectKind>,
    pub approximate: bool,
    pub limit: NonZeroUsize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CrsWktOptions {
    pub pretty: bool,
    pub indentation_width: u8,
    pub output_axis: Option<WktAxisRule>,
    pub strict: bool,
}

impl Default for CrsWktOptions {
    fn default() -> Self {
        Self {
            pretty: false,
            indentation_width: 4,
            output_axis: None,
            strict: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CrsProjOptions {
    pub pretty: bool,
    pub indentation_width: u8,
    pub max_line_length: usize,
    pub approximate_tmerc: bool,
}

impl Default for CrsProjOptions {
    fn default() -> Self {
        Self {
            pretty: false,
            indentation_width: 2,
            max_line_length: 80,
            approximate_tmerc: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CrsProjJsonOptions {
    pub pretty: bool,
    pub indentation_width: u8,
}

impl Default for CrsProjJsonOptions {
    fn default() -> Self {
        Self {
            pretty: false,
            indentation_width: 2,
        }
    }
}

#[derive(Debug, Clone, IntoPyObject)]
pub(crate) struct EngineInfo {
    pub backend: &'static str,
    pub bundled_proj: bool,
    pub version: Option<String>,
    pub release: Option<String>,
    pub major: i32,
    pub minor: i32,
    pub patch: i32,
    pub search_path: Option<String>,
    pub paths: Vec<String>,
    pub database_path: Option<String>,
    #[pyo3(into_py_with = metadata_dict)]
    pub database_metadata: Vec<(String, String)>,
    pub user_writable_directory: Option<String>,
}

/// Render `(key, value)` metadata pairs as a Python dict (PROJ database
/// metadata is a keyed record, not a sequence).
#[expect(
    clippy::needless_pass_by_value,
    reason = "PyO3 IntoPyObject conversion callbacks receive Cow values by value"
)]
fn metadata_dict<'py>(
    items: Cow<'_, [(String, String)]>,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyAny>> {
    let dict = pyo3::types::PyDict::new(py);
    for (key, value) in items.as_ref() {
        dict.set_item(key, value)?;
    }
    Ok(dict.into_any())
}

#[derive(Debug, Clone, IntoPyObject)]
pub(crate) struct CacheBucketInfo {
    pub name: &'static str,
    pub entries: usize,
    pub capacity: usize,
}

#[derive(Debug, Clone, IntoPyObject)]
pub(crate) struct CacheInfo {
    pub generation: u64,
    pub total_entries: usize,
    pub total_capacity: usize,
    pub buckets: Vec<CacheBucketInfo>,
    /// Engine selected by the most recent transform on this thread.
    pub last_transform_engine: Option<&'static str>,
    /// Actual in-core batches and PROJ calls since the current cache generation.
    pub transform_invocations: usize,
}

#[derive(Debug, Clone, Default, IntoPyObject)]
pub(crate) struct RuntimeConfig {
    pub search_paths: Option<Vec<String>>,
    pub user_writable_directory: Option<String>,
}

#[derive(Debug, Clone, IntoPyObject)]
pub(crate) struct IdentifyCandidate {
    pub crs: String,
    pub name: Option<String>,
    pub authority: Option<String>,
    pub code: Option<String>,
    pub confidence: Confidence,
}

#[derive(Debug, Clone, IntoPyObject)]
pub(crate) struct AuthorityObjectInfo {
    pub crs: String,
    pub authority: Option<String>,
    pub code: Option<String>,
    pub name: Option<String>,
    pub kind: &'static str,
    pub deprecated: bool,
    pub area_of_use: Option<AreaOfUse>,
}
