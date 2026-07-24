#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use geographiclib_rs::Geodesic;

use super::*;
use crate::text::str_contains_ignore_ascii_case;

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct PositiveFiniteScale(f64);

impl PositiveFiniteScale {
    const ONE: Self = Self(1.0);

    fn try_new(unit: f64, error: impl FnOnce(&str) -> crate::error::Error) -> Result<Self> {
        if unit.is_finite() && unit > 0.0 {
            Ok(Self(unit))
        } else {
            Err(error(
                "axis linear-unit factor is not a positive finite value",
            ))
        }
    }

    pub(crate) const fn get(self) -> f64 {
        self.0
    }
}

fn ensure_positive_finite_linear_unit(
    unit: f64,
    error: impl FnOnce(&str) -> crate::error::Error,
) -> Result<PositiveFiniteScale> {
    PositiveFiniteScale::try_new(unit, error)
}

/// The single source of truth for CRS-aware metrics
/// (`area`/`length`/`distance`/ `buffer`/…): geographic CRS measure
/// geodesically on their own ellipsoid in meters; everything else (projected,
/// engineering, CRS-free) is planar in the unit scale selected by the caller
/// (`1.0` for native/coordinate units, CRS linear factor for explicit meters).
#[derive(Debug, Clone)]
pub(crate) enum MetricModel {
    /// Planar/Cartesian. `to_metre` is the active metric scale: `1.0` when the
    /// public unit is native coordinate units, or the projected CRS's linear
    /// axis factor for explicit `unit='meters'` (e.g. `0.3048…` for a
    /// US-survey-foot CRS). Scale outputs by `to_metre` (`to_metre²` for area)
    /// and divide inputs in the active metric unit by `to_metre`.
    Planar { to_metre: PositiveFiniteScale },
    /// Geodesic on the named (normalized) CRS's ellipsoid; results in meters.
    Geodesic(String),
}

impl MetricModel {
    /// Planar coordinate units (CRS-free): a meter is one coordinate unit.
    pub(crate) const COORDINATE: Self = Self::Planar {
        to_metre: PositiveFiniteScale::ONE,
    };

    /// A meter-denominated `distance` expressed in coordinate units, for the
    /// envelope/R-tree prefilters that walk coordinate space directly. `None`
    /// for a geodesic model, where a meter radius has no fixed coordinate-space
    /// size and the caller must fall back to a per-pair geodesic test.
    pub(crate) fn coordinate_radius(&self, distance: f64) -> Option<f64> {
        match self {
            Self::Planar { to_metre } => Some(distance / to_metre.get()),
            Self::Geodesic(_) => None,
        }
    }

    /// Meters per coordinate unit for coordinate-ordered metrics (`1.0` for raw
    /// coordinate units or a geodesic model), so envelope bounds compare
    /// against metric distances.
    pub(crate) const fn coordinate_scale(&self) -> f64 {
        match self {
            Self::Geodesic(_) => 1.0,
            Self::Planar { to_metre } => to_metre.get(),
        }
    }

    /// Whether distances under this model order monotonically with
    /// coordinate-space distance (every planar model does; a geodesic one does
    /// not, so R-tree distance pruning must use a geodesic lower bound).
    pub(crate) const fn has_coordinate_order(&self) -> bool {
        matches!(self, Self::Planar { .. })
    }

    /// One-word label for an operator-facing `explain` of the metric in force.
    pub(crate) const fn explain_label(&self) -> &'static str {
        match self {
            Self::Planar { .. } => "planar",
            Self::Geodesic(_) => "CRS-aware",
        }
    }
}

/// A [`MetricModel`] with its batch-invariant CRS resolution done ONCE: the
/// geodesic ellipsoid object is resolved (CRS-kind check, thread-cache refresh,
/// normalization, geodesic-cache lookup) a single time and carried by value, so
/// a per-row/per-pair loop pays zero CRS-resolution cost (the per-pair
/// `with_resolved_ellipsoid_metric` re-ran all of that for every element). The
/// owned [`Geodesic`] is a handful of `f64` ellipsoid constants — cheap to clone
/// once per batch and trivially `Send`, sidestepping the thread-local cache
/// borrow that a borrowed metric would hold across a GIL-released loop.
#[derive(Clone)]
pub(crate) enum ResolvedMetric {
    Planar {
        to_metre: PositiveFiniteScale,
    },
    Geodesic {
        crs: String,
        geodesic: Box<Geodesic>,
    },
}

impl ResolvedMetric {
    /// Resolve a model once. The geodesic arm runs the batch-invariant CRS
    /// resolution a single time; per-shape lon/lat domain validation is NOT done
    /// here (the per-pair kernel validates each operand it actually measures).
    pub(crate) fn from_model(model: &MetricModel) -> Result<Self> {
        Ok(match model {
            MetricModel::Planar { to_metre } => Self::Planar {
                to_metre: *to_metre,
            },
            MetricModel::Geodesic(crs) => {
                ensure_geodesic_lonlat_crs(crs)?;
                // Normalize ONCE per batch (was once per pair), keeping the
                // geodesic-cache key identical to the per-pair path.
                let crs = normalize(crs)?;
                let geodesic = with_geodesic_cache(&crs, |error| error, |g| Ok(Box::new(*g)))?;
                Self::Geodesic { crs, geodesic }
            },
        })
    }
}

/// Resolve the default/native [`MetricModel`] for a CRS (a compound CRS
/// delegates to its horizontal component; `None` is planar coordinate units).
///
/// Defaults are CRS-natural: geographic CRS use geodesic meters, projected CRS
/// validate their horizontal axes and then report/accept their native linear
/// coordinate units, and CRS-free geometries use raw coordinate units.
pub(crate) fn metric_model(crs: Option<&str>) -> Result<MetricModel> {
    crs.map_or(Ok(MetricModel::COORDINATE), metric_model_for)
}

pub(crate) fn metric_model_for(crs: &str) -> Result<MetricModel> {
    let info = info(crs)?;
    if info.kind.starts_with("geographic") {
        ensure_geographic_degree_units(crs, &info)?;
        return Ok(MetricModel::Geodesic(info.crs.clone()));
    }
    if info.kind == "compound"
        && let Some(sub) = info.sub_crs.first().filter(|sub| !sub.crs.is_empty())
    {
        return metric_model_for(&sub.crs);
    }
    planar_to_metre(crs, &info)?;
    Ok(MetricModel::COORDINATE)
}

/// Resolve an explicit meter-denominated [`MetricModel`] for a CRS.
///
/// Geographic CRS stay geodesic in meters; projected/engineering CRS scale
/// native coordinates through their horizontal linear-axis factor. CRS-free
/// callers are rejected before reaching this helper because they have no meter
/// scale.
pub(crate) fn metric_model_meters(crs: &str) -> Result<MetricModel> {
    let info = info(crs)?;
    if info.kind.starts_with("geographic") {
        ensure_geographic_degree_units(crs, &info)?;
        return Ok(MetricModel::Geodesic(info.crs.clone()));
    }
    if info.kind == "compound"
        && let Some(sub) = info.sub_crs.first().filter(|sub| !sub.crs.is_empty())
    {
        return metric_model_meters(&sub.crs);
    }
    Ok(MetricModel::Planar {
        to_metre: planar_to_metre(crs, &info)?,
    })
}

/// Meters per coordinate unit for a projected/engineering CRS: its horizontal
/// linear axis factor. The X and Y axes must share one linear (length) unit;
/// angular or non-uniform axes are rejected so explicit meter metrics are
/// honestly SI.
pub(crate) fn planar_to_metre(crs: &str, info: &CrsInfo) -> Result<PositiveFiniteScale> {
    let fail = |message: &str| Err(CrsError::metric_units(crs, message));
    if info.axes.len() < 2 {
        return fail("planar metrics require two horizontal axes");
    }
    // The first two axes must actually be horizontal: a vertical or temporal
    // CRS also carries linear units, but its factor says nothing about X/Y.
    if info
        .axes
        .iter()
        .take(2)
        .any(|axis| !is_horizontal_direction(axis.direction.as_deref()))
    {
        return fail(
            "planar metrics require two horizontal axes, but an axis is vertical or temporal",
        );
    }
    let Some(factors) = metric_axis_factors(info) else {
        return fail("planar metrics require linear (length) axis units, but the axes are angular");
    };
    let horizontal = &factors[..factors.len().min(2)];
    let unit = ensure_positive_finite_linear_unit(horizontal[0], |message| {
        CrsError::metric_units(crs, message)
    })?;
    let unit_value = unit.get();
    if horizontal
        .iter()
        .any(|factor| (factor - unit_value).abs() > 1e-9)
    {
        return fail("X and Y axes use different linear units; results would not be uniform");
    }
    Ok(unit)
}

pub(crate) fn ensure_geographic_degree_units(crs: &str, info: &CrsInfo) -> Result<()> {
    let fail = |message: String| Err(CrsError::geodesic_units(crs, message));
    if info.axes.len() < 2 {
        return fail("geographic CRS does not expose two horizontal axes".to_owned());
    }
    for (index, axis) in info.axes.iter().take(2).enumerate() {
        if !axis.unit_conversion_factor.is_finite()
            || (axis.unit_conversion_factor - DEGREE_TO_RADIAN).abs() > 1e-12
        {
            return fail(format!(
                "axis {index} uses {}, but geometry coordinates are interpreted as longitude/latitude degrees",
                axis.unit_name
                    .as_deref()
                    .unwrap_or("an unknown angular unit"),
            ));
        }
    }
    Ok(())
}

/// Validate that 3D Euclidean metrics (`distance_3d`/`length_3d`) are
/// meaningful on `crs`.
///
/// Every axis must share one linear (length) unit, so the
/// Z ordinate can be combined with X/Y under a Euclidean norm. Geographic
/// (angular horizontal) CRS are rejected — degrees cannot mix with meter
/// heights — as is any meter-vs-foot axis-unit mix in a 3D/compound CRS. A
/// projected 2D CRS is accepted: its Z ordinate inherits the horizontal linear
/// unit. A CRS-free geometry (`crs` is `None`) is accepted, with ordinates in
/// bare coordinate units (caveat emptor).
pub(crate) fn ensure_3d_metric(crs: Option<&str>) -> Result<f64> {
    let Some(crs) = crs else {
        return Ok(1.0);
    };
    let fail = |message: &str| CrsError::vertical_units(crs, message);
    let crs_info = info(crs)?;
    let Some(factors) = metric_axis_factors(&crs_info) else {
        return Err(fail(
            "axes are angular (geographic); reproject to a projected CRS with to_crs",
        ));
    };
    let unit = ensure_positive_finite_linear_unit(factors[0], fail)?;
    let unit_value = unit.get();
    if factors
        .iter()
        .any(|factor| (factor - unit_value).abs() > 1e-9)
    {
        return Err(fail(
            "axis units differ (e.g. meter vs foot); use one consistent linear unit",
        ));
    }
    // Meters per coordinate unit: callers scale 3D outputs so they are SI like
    // the 2D metrics (`1.0` for a CRS-free or meter CRS, `0.3048…` for feet).
    Ok(unit_value)
}

/// Linear (length) unit conversion factors of every axis, recursing into the
/// sub-CRS of a compound CRS. `None` if any axis is angular (degree/radian) or
/// has no usable unit — i.e. the CRS is not a pure linear/metric system.
pub(crate) fn metric_axis_factors(crs_info: &CrsInfo) -> Option<Vec<f64>> {
    if crs_info.kind.starts_with("geographic") {
        return None;
    }
    if crs_info.kind == "compound" {
        let mut factors = Vec::new();
        for sub in &crs_info.sub_crs {
            if sub.crs.is_empty() {
                return None;
            }
            let sub_info = info(&sub.crs).ok()?;
            factors.extend(metric_axis_factors(&sub_info)?);
        }
        return (!factors.is_empty()).then_some(factors);
    }
    let mut factors = Vec::with_capacity(crs_info.axes.len());
    for axis in &crs_info.axes {
        let unit = axis.unit_name.as_deref()?;
        if is_angular_unit(unit) || axis.unit_conversion_factor <= 0.0 {
            return None;
        }
        factors.push(axis.unit_conversion_factor);
    }
    (!factors.is_empty()).then_some(factors)
}

/// Whether an axis direction is horizontal (part of the X/Y plane). Unknown
/// directions count as horizontal — PROJ uses many compass spellings (e.g.
/// "South along 90°E" on polar grids) — so only the explicit vertical and
/// temporal directions are excluded.
pub(crate) const fn is_horizontal_direction(direction: Option<&str>) -> bool {
    let Some(direction) = direction else {
        return true;
    };
    !direction.eq_ignore_ascii_case("up")
        && !direction.eq_ignore_ascii_case("down")
        && !direction.eq_ignore_ascii_case("future")
        && !direction.eq_ignore_ascii_case("past")
        && !direction.eq_ignore_ascii_case("geocentricz")
}

pub(crate) fn is_angular_unit(unit: &str) -> bool {
    const ANGULAR: [&str; 6] = [
        "degree",
        "radian",
        "grad",
        "gon",
        "arc-second",
        "arc-minute",
    ];
    ANGULAR
        .iter()
        .any(|angular| str_contains_ignore_ascii_case(unit, angular))
}

/// Geodesic distance (meters) between two lon/lat points on `crs`'s ellipsoid.
pub(crate) fn geodesic_distance_crs(
    crs: &str,
    lon1: f64,
    lat1: f64,
    lon2: f64,
    lat2: f64,
) -> Result<f64> {
    ensure_geographic_lonlat(lon1, lat1)?;
    ensure_geographic_lonlat(lon2, lat2)?;
    with_geodesic(crs, |geodesic| {
        // Distance-only capability (the 4-tuple impl also grades azimuths).
        let distance: f64 = geodesic.inverse(lat1, lon1, lat2, lon2);
        finite(distance, "geodesic distance")
    })
}
