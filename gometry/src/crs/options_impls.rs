//! Inherent impls for the CRS option/DTO types (`TransformOptions`,
//! `Crs*Options`, `ProjDirection`, `AreaOfInterest`).
//!
//! The data types live in `types`; their validation/builder methods reach
//! parent-`crs` helpers via `use super::*`.

use crate::crs::{
    AreaOfInterest, CString, CrsCatalogOptions, CrsComparison, CrsError, CrsObjectKind,
    CrsProjJsonOptions, CrsProjOptions, CrsSearchOptions, CrsWktOptions, ProjDirection,
    ProjStringVersion, ProjectionFactorColumns, ProjectionFactors, TransformOptions,
    UtmCatalogOptions, WktAxisRule, WktVersion, c_char, cstring, ptr, validate_search_limit,
};
use crate::error::Result;

impl TransformOptions {
    pub(crate) fn validate(&self) -> Result<()> {
        // Epoch finiteness is validated at the Python boundary (`coordinate_epoch_option`).
        if let Some(area) = self.area_of_interest {
            area.validate()?;
        }
        if let Some(authority) = &self.authority {
            if authority.is_empty() {
                return Err(CrsError::invalid(
                    "authority must be a non-empty string".to_owned(),
                ));
            }
            cstring(authority.as_str())?;
        }
        Ok(())
    }

    pub(crate) fn allows_in_core(&self) -> bool {
        *self == Self::default()
    }
}

impl ProjDirection {
    pub(crate) const fn to_proj(self) -> proj_sys::PJ_DIRECTION {
        match self {
            Self::Forward => proj_sys::PJ_DIRECTION_PJ_FWD,
            Self::Inverse => proj_sys::PJ_DIRECTION_PJ_INV,
        }
    }

    pub(crate) const fn reversed(self) -> Self {
        match self {
            Self::Forward => Self::Inverse,
            Self::Inverse => Self::Forward,
        }
    }
}

impl CrsCatalogOptions {
    pub(crate) fn validate(&self) -> Result<()> {
        if let Some(area) = self.area {
            area.validate()?;
        }
        if let Some(kind) = self.kind
            && !kind.is_crs()
        {
            return Err(CrsError::invalid(
                "CRS catalog kind must be a CRS type".to_owned(),
            ));
        }
        if let Some(celestial_body) = &self.celestial_body
            && celestial_body.is_empty()
        {
            return Err(CrsError::invalid(
                "celestial_body must be a non-empty string".to_owned(),
            ));
        }
        Ok(())
    }
}

impl UtmCatalogOptions {
    pub(crate) fn validate(&self) -> Result<()> {
        if let Some(area) = self.area {
            area.validate()?;
        }
        if let Some(datum_name) = &self.datum_name
            && datum_name.is_empty()
        {
            return Err(CrsError::invalid(
                "datum_name must be a non-empty string".to_owned(),
            ));
        }
        Ok(())
    }
}

impl CrsSearchOptions {
    pub(crate) fn validate(&self) -> Result<()> {
        if let Some(authority) = &self.authority
            && authority.is_empty()
        {
            return Err(CrsError::invalid(
                "CRS search authority must be a non-empty string".to_owned(),
            ));
        }
        if let Some(kind) = self.kind
            && !kind.is_crs()
        {
            return Err(CrsError::invalid(
                "CRS search kind must be a CRS type".to_owned(),
            ));
        }
        validate_search_limit(self.limit.get() as i64)?;
        Ok(())
    }
}

impl CrsWktOptions {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.indentation_width == 0 {
            return Err(CrsError::invalid(
                "WKT indentation_width must be at least 1".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) fn to_c_options(&self) -> Result<Vec<CString>> {
        self.validate()?;
        let mut options = Vec::with_capacity(4);
        options.push(format!(
            "MULTILINE={}",
            if self.pretty { "YES" } else { "NO" }
        ));
        options.push(format!("INDENTATION_WIDTH={}", self.indentation_width));
        if let Some(output_axis) = self.output_axis {
            options.push(format!("OUTPUT_AXIS={}", output_axis.as_proj_str()));
        }
        options.push(format!("STRICT={}", if self.strict { "YES" } else { "NO" }));
        options.into_iter().map(cstring).collect()
    }
}

impl CrsProjOptions {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.indentation_width == 0 {
            return Err(CrsError::invalid(
                "PROJ indentation_width must be at least 1".to_owned(),
            ));
        }
        if self.max_line_length == 0 {
            return Err(CrsError::invalid(
                "PROJ max_line_length must be at least 1".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) fn to_c_options(&self) -> Result<Vec<CString>> {
        self.validate()?;
        let mut options = Vec::with_capacity(4);
        options.push(format!(
            "MULTILINE={}",
            if self.pretty { "YES" } else { "NO" }
        ));
        options.push(format!("INDENTATION_WIDTH={}", self.indentation_width));
        options.push(format!("MAX_LINE_LENGTH={}", self.max_line_length));
        if self.approximate_tmerc {
            options.push("USE_APPROX_TMERC=YES".to_owned());
        }
        options.into_iter().map(cstring).collect()
    }
}

impl CrsProjJsonOptions {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.indentation_width == 0 {
            return Err(CrsError::invalid(
                "PROJJSON indentation_width must be at least 1".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) fn to_c_options(&self) -> Result<Vec<CString>> {
        self.validate()?;
        [
            format!("MULTILINE={}", if self.pretty { "YES" } else { "NO" }),
            format!("INDENTATION_WIDTH={}", self.indentation_width),
        ]
        .into_iter()
        .map(cstring)
        .collect()
    }
}

pub(crate) fn c_option_ptrs(options: &[CString]) -> Vec<*const c_char> {
    options
        .iter()
        .map(|option| option.as_ptr())
        .chain(std::iter::once(ptr::null()))
        .collect()
}

impl WktVersion {
    pub(crate) const fn to_proj(self) -> proj_sys::PJ_WKT_TYPE {
        match self {
            Self::Wkt2_2019 => proj_sys::PJ_WKT_TYPE_PJ_WKT2_2019,
            Self::Wkt2_2019Simplified => proj_sys::PJ_WKT_TYPE_PJ_WKT2_2019_SIMPLIFIED,
            Self::Wkt2_2015 => proj_sys::PJ_WKT_TYPE_PJ_WKT2_2015,
            Self::Wkt2_2015Simplified => proj_sys::PJ_WKT_TYPE_PJ_WKT2_2015_SIMPLIFIED,
            Self::Wkt1Gdal => proj_sys::PJ_WKT_TYPE_PJ_WKT1_GDAL,
            Self::Wkt1Esri => proj_sys::PJ_WKT_TYPE_PJ_WKT1_ESRI,
        }
    }
}

impl WktAxisRule {
    pub(crate) const fn as_proj_str(self) -> &'static str {
        match self {
            Self::Auto => "AUTO",
            Self::Yes => "YES",
            Self::No => "NO",
        }
    }
}

impl ProjStringVersion {
    pub(crate) const fn to_proj(self) -> proj_sys::PJ_PROJ_STRING_TYPE {
        match self {
            Self::V5 => proj_sys::PJ_PROJ_STRING_TYPE_PJ_PROJ_5,
            Self::V4 => proj_sys::PJ_PROJ_STRING_TYPE_PJ_PROJ_4,
        }
    }
}

impl CrsComparison {
    pub(crate) const fn criterion(self) -> proj_sys::PJ_COMPARISON_CRITERION {
        match self {
            Self::IgnoreAxisOrder => {
                proj_sys::PJ_COMPARISON_CRITERION_PJ_COMP_EQUIVALENT_EXCEPT_AXIS_ORDER_GEOGCRS
            },
            Self::Exact => proj_sys::PJ_COMPARISON_CRITERION_PJ_COMP_STRICT,
        }
    }
}
impl AreaOfInterest {
    pub(crate) fn new(west: f64, south: f64, east: f64, north: f64) -> Result<Self> {
        let area = Self {
            west,
            south,
            east,
            north,
        };
        area.validate()?;
        Ok(area)
    }

    pub(crate) fn validate(self) -> Result<()> {
        if [self.west, self.south, self.east, self.north]
            .iter()
            .all(|value| value.is_finite())
            && (-180.0..=180.0).contains(&self.west)
            && (-180.0..=180.0).contains(&self.east)
            && (-90.0..=90.0).contains(&self.south)
            && (-90.0..=90.0).contains(&self.north)
            && self.south <= self.north
        {
            return Ok(());
        }
        Err(CrsError::invalid(
            "area must be finite (west, south, east, north) degrees with west/east in [-180, 180], south/north in [-90, 90], and south <= north".to_owned(),
        ))
    }
}

impl ProjectionFactors {
    #[expect(
        clippy::large_types_passed_by_value,
        reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
    )]
    pub(crate) fn from_raw(raw: proj_sys::PJ_FACTORS, radians: bool) -> Self {
        let angle = |value: f64| if radians { value } else { value.to_degrees() };
        Self {
            meridional_scale: raw.meridional_scale,
            parallel_scale: raw.parallel_scale,
            areal_scale: raw.areal_scale,
            angular_distortion: angle(raw.angular_distortion),
            meridian_parallel_angle: angle(raw.meridian_parallel_angle),
            meridian_convergence: angle(raw.meridian_convergence),
            tissot_semimajor: raw.tissot_semimajor,
            tissot_semiminor: raw.tissot_semiminor,
            dx_dlam: raw.dx_dlam,
            dx_dphi: raw.dx_dphi,
            dy_dlam: raw.dy_dlam,
            dy_dphi: raw.dy_dphi,
        }
    }

    pub(crate) const fn is_finite(&self) -> bool {
        self.meridional_scale.is_finite()
            && self.parallel_scale.is_finite()
            && self.areal_scale.is_finite()
            && self.angular_distortion.is_finite()
            && self.meridian_parallel_angle.is_finite()
            && self.meridian_convergence.is_finite()
            && self.tissot_semimajor.is_finite()
            && self.tissot_semiminor.is_finite()
            && self.dx_dlam.is_finite()
            && self.dx_dphi.is_finite()
            && self.dy_dlam.is_finite()
            && self.dy_dphi.is_finite()
    }
}

impl ProjectionFactorColumns {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            meridional_scale: Vec::with_capacity(capacity),
            parallel_scale: Vec::with_capacity(capacity),
            areal_scale: Vec::with_capacity(capacity),
            angular_distortion: Vec::with_capacity(capacity),
            meridian_parallel_angle: Vec::with_capacity(capacity),
            meridian_convergence: Vec::with_capacity(capacity),
            tissot_semimajor: Vec::with_capacity(capacity),
            tissot_semiminor: Vec::with_capacity(capacity),
            dx_dlam: Vec::with_capacity(capacity),
            dx_dphi: Vec::with_capacity(capacity),
            dy_dlam: Vec::with_capacity(capacity),
            dy_dphi: Vec::with_capacity(capacity),
        }
    }

    #[expect(
        clippy::large_types_passed_by_value,
        reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
    )]
    pub(crate) fn push(&mut self, factors: ProjectionFactors) {
        self.meridional_scale.push(factors.meridional_scale);
        self.parallel_scale.push(factors.parallel_scale);
        self.areal_scale.push(factors.areal_scale);
        self.angular_distortion.push(factors.angular_distortion);
        self.meridian_parallel_angle
            .push(factors.meridian_parallel_angle);
        self.meridian_convergence.push(factors.meridian_convergence);
        self.tissot_semimajor.push(factors.tissot_semimajor);
        self.tissot_semiminor.push(factors.tissot_semiminor);
        self.dx_dlam.push(factors.dx_dlam);
        self.dx_dphi.push(factors.dx_dphi);
        self.dy_dlam.push(factors.dy_dlam);
        self.dy_dphi.push(factors.dy_dphi);
    }
}

impl CrsObjectKind {
    /// Parse the canonical `kind` token, defaulting to
    /// [`CrsObjectKind::Crs`] when unset.
    pub(crate) fn parse(kind: Option<&str>) -> Result<Self> {
        Ok(match kind.unwrap_or("crs") {
            "crs" => Self::Crs,
            "geodetic" => Self::GeodeticCrs,
            "geographic" => Self::GeographicCrs,
            "geographic_2d" => Self::Geographic2dCrs,
            "geographic_3d" => Self::Geographic3dCrs,
            "geocentric" => Self::GeocentricCrs,
            "projected" => Self::ProjectedCrs,
            "vertical" => Self::VerticalCrs,
            "compound" => Self::CompoundCrs,
            "temporal" => Self::TemporalCrs,
            "engineering" => Self::EngineeringCrs,
            "bound" => Self::BoundCrs,
            "other" => Self::OtherCrs,
            "derived_projected_crs" => Self::DerivedProjectedCrs,
            "ellipsoid" => Self::Ellipsoid,
            "prime_meridian" => Self::PrimeMeridian,
            "geodetic_reference_frame" => Self::GeodeticReferenceFrame,
            "dynamic_geodetic_reference_frame" => Self::DynamicGeodeticReferenceFrame,
            "vertical_reference_frame" => Self::VerticalReferenceFrame,
            "dynamic_vertical_reference_frame" => Self::DynamicVerticalReferenceFrame,
            "datum_ensemble" => Self::DatumEnsemble,
            "temporal_datum" => Self::TemporalDatum,
            "engineering_datum" => Self::EngineeringDatum,
            "parametric_datum" => Self::ParametricDatum,
            "conversion" => Self::Conversion,
            "transformation" => Self::Transformation,
            "concatenated_operation" => Self::ConcatenatedOperation,
            "other_coordinate_operation" => Self::OtherCoordinateOperation,
            other => {
                return Err(CrsError::invalid(format!(
                    "unknown PROJ database kind {other:?}"
                )));
            },
        })
    }

    /// Parse a `kind` restricted to CRS types (the catalog/search contract).
    pub(crate) fn parse_crs(kind: Option<&str>) -> Result<Self> {
        let parsed = Self::parse(kind)?;
        if parsed.is_crs() {
            Ok(parsed)
        } else {
            Err(CrsError::invalid(format!(
                "CRS catalog kind must be a CRS type, got {:?}",
                kind.unwrap_or("crs")
            )))
        }
    }

    pub(crate) const fn to_proj(self) -> proj_sys::PJ_TYPE {
        match self {
            Self::Crs => proj_sys::PJ_TYPE_PJ_TYPE_CRS,
            Self::GeodeticCrs => proj_sys::PJ_TYPE_PJ_TYPE_GEODETIC_CRS,
            Self::GeographicCrs => proj_sys::PJ_TYPE_PJ_TYPE_GEOGRAPHIC_CRS,
            Self::Geographic2dCrs => proj_sys::PJ_TYPE_PJ_TYPE_GEOGRAPHIC_2D_CRS,
            Self::Geographic3dCrs => proj_sys::PJ_TYPE_PJ_TYPE_GEOGRAPHIC_3D_CRS,
            Self::GeocentricCrs => proj_sys::PJ_TYPE_PJ_TYPE_GEOCENTRIC_CRS,
            Self::ProjectedCrs => proj_sys::PJ_TYPE_PJ_TYPE_PROJECTED_CRS,
            Self::VerticalCrs => proj_sys::PJ_TYPE_PJ_TYPE_VERTICAL_CRS,
            Self::CompoundCrs => proj_sys::PJ_TYPE_PJ_TYPE_COMPOUND_CRS,
            Self::TemporalCrs => proj_sys::PJ_TYPE_PJ_TYPE_TEMPORAL_CRS,
            Self::EngineeringCrs => proj_sys::PJ_TYPE_PJ_TYPE_ENGINEERING_CRS,
            Self::BoundCrs => proj_sys::PJ_TYPE_PJ_TYPE_BOUND_CRS,
            Self::OtherCrs => proj_sys::PJ_TYPE_PJ_TYPE_OTHER_CRS,
            Self::DerivedProjectedCrs => proj_sys::PJ_TYPE_PJ_TYPE_DERIVED_PROJECTED_CRS,
            Self::Ellipsoid => proj_sys::PJ_TYPE_PJ_TYPE_ELLIPSOID,
            Self::PrimeMeridian => proj_sys::PJ_TYPE_PJ_TYPE_PRIME_MERIDIAN,
            Self::GeodeticReferenceFrame => proj_sys::PJ_TYPE_PJ_TYPE_GEODETIC_REFERENCE_FRAME,
            Self::DynamicGeodeticReferenceFrame => {
                proj_sys::PJ_TYPE_PJ_TYPE_DYNAMIC_GEODETIC_REFERENCE_FRAME
            },
            Self::VerticalReferenceFrame => proj_sys::PJ_TYPE_PJ_TYPE_VERTICAL_REFERENCE_FRAME,
            Self::DynamicVerticalReferenceFrame => {
                proj_sys::PJ_TYPE_PJ_TYPE_DYNAMIC_VERTICAL_REFERENCE_FRAME
            },
            Self::DatumEnsemble => proj_sys::PJ_TYPE_PJ_TYPE_DATUM_ENSEMBLE,
            Self::TemporalDatum => proj_sys::PJ_TYPE_PJ_TYPE_TEMPORAL_DATUM,
            Self::EngineeringDatum => proj_sys::PJ_TYPE_PJ_TYPE_ENGINEERING_DATUM,
            Self::ParametricDatum => proj_sys::PJ_TYPE_PJ_TYPE_PARAMETRIC_DATUM,
            Self::Conversion => proj_sys::PJ_TYPE_PJ_TYPE_CONVERSION,
            Self::Transformation => proj_sys::PJ_TYPE_PJ_TYPE_TRANSFORMATION,
            Self::ConcatenatedOperation => proj_sys::PJ_TYPE_PJ_TYPE_CONCATENATED_OPERATION,
            Self::OtherCoordinateOperation => proj_sys::PJ_TYPE_PJ_TYPE_OTHER_COORDINATE_OPERATION,
        }
    }

    pub(crate) const fn is_crs(self) -> bool {
        matches!(
            self,
            Self::Crs
                | Self::GeodeticCrs
                | Self::GeographicCrs
                | Self::Geographic2dCrs
                | Self::Geographic3dCrs
                | Self::GeocentricCrs
                | Self::ProjectedCrs
                | Self::VerticalCrs
                | Self::CompoundCrs
                | Self::TemporalCrs
                | Self::EngineeringCrs
                | Self::BoundCrs
                | Self::OtherCrs
        )
    }
}
