use std::sync::Arc;

use geographiclib_rs::Geodesic;

use crate::crs::geodesic::{
    CrsError, CrsInfo, Result, ensure_geodesic_lonlat_crs_at, with_geodesic_cache_at,
};
use crate::crs::info_with_state;
use crate::geographic::GeographicFrame;
use crate::text::str_contains_ignore_ascii_case;

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct PositiveFiniteScale(f64);

impl PositiveFiniteScale {
    pub(crate) const ONE: Self = Self(1.0);

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
    Geodesic {
        crs: Arc<String>,
        frame: GeographicFrame,
        state: Arc<crate::crs::RuntimeState>,
    },
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
            Self::Geodesic { .. } => None,
        }
    }

    /// Meters per coordinate unit for coordinate-ordered metrics (`1.0` for raw
    /// coordinate units or a geodesic model), so envelope bounds compare
    /// against metric distances.
    pub(crate) const fn coordinate_scale(&self) -> f64 {
        match self {
            Self::Geodesic { .. } => 1.0,
            Self::Planar { to_metre } => to_metre.get(),
        }
    }
}

/// Batch-resolved geodesic payload owned by [`ResolvedMetric`]. The geodesic
/// ellipsoid object is resolved (CRS-kind check, thread-cache refresh,
/// normalization, geodesic-cache lookup) once and carried by value, so a
/// per-row/per-pair loop pays zero CRS-resolution cost. The owned [`Geodesic`]
/// is a handful of `f64` ellipsoid constants, cheap to clone once per batch and
/// trivially `Send`, sidestepping the thread-local cache borrow that a borrowed
/// metric would hold across a GIL-released loop.
#[derive(Clone)]
pub(crate) struct GeodesicMetric {
    pub(crate) crs: Arc<String>,
    pub(crate) geodesic: Box<Geodesic>,
    pub(crate) frame: GeographicFrame,
    pub(crate) state: Arc<crate::crs::RuntimeState>,
}

impl GeodesicMetric {
    pub(crate) fn from_model(model: &MetricModel) -> Result<Self> {
        let MetricModel::Geodesic { crs, frame, state } = model else {
            return Err(CrsError::invalid("expected a geodesic metric model"));
        };
        ensure_geodesic_lonlat_crs_at(crs, state)?;
        let geodesic = with_geodesic_cache_at(
            crs,
            state,
            |error| error,
            |g| Box::try_new(*g).map_err(crate::error::Error::from),
        )?;
        Ok(Self {
            crs: Arc::clone(crs),
            geodesic,
            frame: *frame,
            state: Arc::clone(state),
        })
    }
}

/// A [`MetricModel`] with its batch-invariant CRS resolution done once.
#[derive(Clone)]
pub(crate) enum ResolvedMetric {
    Planar { to_metre: PositiveFiniteScale },
    Geodesic(GeodesicMetric),
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
            MetricModel::Geodesic { .. } => Self::Geodesic(GeodesicMetric::from_model(model)?),
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
    let state = crate::crs::runtime_state_snapshot()?;
    metric_model_at(crs, &state)
}

pub(crate) fn metric_model_at(
    crs: Option<&str>,
    state: &crate::crs::RuntimeState,
) -> Result<MetricModel> {
    crs.map_or(Ok(MetricModel::COORDINATE), |crs| {
        metric_model_for_at(crs, state)
    })
}

/// The horizontal component a metric resolves against.
///
/// Both `metric_model_for` and `metric_model_meters` walk compound CRS to the
/// same horizontal component by the same rules; they differ only in what they
/// build once they arrive. Keeping the walk here means a fix to the compound
/// traversal cannot be applied to one and forgotten on the other — they were
/// previously verbatim copies diverging in two lines.
enum HorizontalComponent {
    /// A geographic CRS: measured geodesically on its own ellipsoid.
    Geodesic {
        crs: Arc<String>,
        frame: GeographicFrame,
    },
    /// A projected/engineering CRS, with the info its axis validation needs.
    ///
    /// The CRS definition comes from `info.definition`, so the metric model
    /// retains the same normalized authored label used by the CRS object.
    Planar { info: std::sync::Arc<CrsInfo> },
}

fn horizontal_component(
    crs: &str,
    state: &crate::crs::RuntimeState,
) -> Result<HorizontalComponent> {
    let info = info_with_state(crs, state)?;
    // Bound geographic CRSs expose their source frame through the typed
    // metadata even though PROJ reports the root kind as `bound` and does not
    // list that source in `sub_crs`.
    if info.kind == "bound"
        && let Some(frame) = info.geographic_frame()
    {
        return Ok(HorizontalComponent::Geodesic {
            crs: Arc::try_new(
                crate::boundary::try_string_from_str(&info.definition)
                    .map_err(|()| crate::error::ResourceError::Allocation)?,
            )
            .map_err(crate::error::Error::from)?,
            frame,
        });
    }
    // Recursive geographic: compound horizontal components included.
    if info.is_geographic() {
        if info.kind.starts_with("geographic") {
            let frame = info.geographic_frame().ok_or_else(|| {
                CrsError::crs_create(crs, "geographic CRS has no typed native frame")
            })?;
            return Ok(HorizontalComponent::Geodesic {
                crs: Arc::try_new(
                    crate::boundary::try_string_from_str(&info.definition)
                        .map_err(|()| crate::error::ResourceError::Allocation)?,
                )
                .map_err(crate::error::Error::from)?,
                frame,
            });
        }
        if let Some(sub) = info.sub_crs.iter().find(|sub| {
            sub.kind.starts_with("geographic") && (!sub.crs.is_empty() || sub.definition.is_some())
        }) {
            return horizontal_component(
                sub.definition.as_deref().unwrap_or(sub.crs.as_str()),
                state,
            );
        }
        if let Some(sub) = info
            .sub_crs
            .first()
            .filter(|sub| !sub.crs.is_empty() || sub.definition.is_some())
        {
            return horizontal_component(
                sub.definition.as_deref().unwrap_or(sub.crs.as_str()),
                state,
            );
        }
    }
    if info.kind == "compound"
        && let Some(sub) = info
            .sub_crs
            .first()
            .filter(|sub| !sub.crs.is_empty() || sub.definition.is_some())
    {
        return horizontal_component(sub.definition.as_deref().unwrap_or(sub.crs.as_str()), state);
    }
    Ok(HorizontalComponent::Planar { info })
}

pub(crate) fn metric_model_for_at(
    crs: &str,
    state: &crate::crs::RuntimeState,
) -> Result<MetricModel> {
    Ok(match horizontal_component(crs, state)? {
        HorizontalComponent::Geodesic { crs, frame } => MetricModel::Geodesic {
            crs,
            frame,
            state: Arc::try_new(state.clone()).map_err(crate::error::Error::from)?,
        },
        // Validate the axes, then discard the scale: the CRS-natural default
        // reports native coordinate units, not metres.
        HorizontalComponent::Planar { info } => {
            planar_to_metre(&info.definition, &info, state)?;
            MetricModel::COORDINATE
        },
    })
}

/// Resolve an explicit meter-denominated [`MetricModel`] for a CRS.
///
/// Geographic CRS stay geodesic in meters; projected/engineering CRS scale
/// native coordinates through their horizontal linear-axis factor. CRS-free
/// callers are rejected before reaching this helper because they have no meter
/// scale.
pub(crate) fn metric_model_meters(crs: &str) -> Result<MetricModel> {
    let state = crate::crs::runtime_state_snapshot()?;
    metric_model_meters_at(crs, &state)
}

pub(crate) fn metric_model_meters_at(
    crs: &str,
    state: &crate::crs::RuntimeState,
) -> Result<MetricModel> {
    Ok(match horizontal_component(crs, state)? {
        HorizontalComponent::Geodesic { crs, frame } => MetricModel::Geodesic {
            crs,
            frame,
            state: Arc::try_new(state.clone()).map_err(crate::error::Error::from)?,
        },
        // Same walk as the CRS-natural resolver; here the axis factor is kept
        // so outputs scale to SI metres.
        HorizontalComponent::Planar { info } => MetricModel::Planar {
            to_metre: planar_to_metre(&info.definition, &info, state)?,
        },
    })
}

/// Meters per coordinate unit for a projected/engineering CRS: its horizontal
/// linear axis factor. The X and Y axes must share one linear (length) unit;
/// angular or non-uniform axes are rejected so explicit meter metrics are
/// honestly SI.
#[expect(
    clippy::float_cmp,
    reason = "unit conversion factors are exact CRS declarations, not approximate measurements"
)]
pub(crate) fn planar_to_metre(
    crs: &str,
    info: &CrsInfo,
    state: &crate::crs::RuntimeState,
) -> Result<PositiveFiniteScale> {
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
    let Some(factors) = metric_axis_factors_at(info, state)? else {
        return fail("planar metrics require linear (length) axis units, but the axes are angular");
    };
    let horizontal = &factors[..factors.len().min(2)];
    let unit = ensure_positive_finite_linear_unit(horizontal[0], |message| {
        CrsError::metric_units(crs, message)
    })?;
    let unit_value = unit.get();
    // Axis factors are declarations, so approximate equality would silently
    // substitute the first axis's factor for a distinct one.
    if horizontal.iter().any(|factor| *factor != unit_value) {
        return fail("X and Y axes use different linear units; results would not be uniform");
    }
    Ok(unit)
}

pub(crate) fn geographic_metric_frame_at(
    crs: &str,
    state: &crate::crs::RuntimeState,
) -> Result<GeographicFrame> {
    let info = info_with_state(crs, state)?;
    info.geographic_frame().ok_or_else(|| {
        CrsError::invalid(format!(
            "{} geodesic geometry measurements require a geographic longitude-latitude CRS",
            info.definition
        ))
    })
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
pub(crate) fn ensure_3d_metric(crs: Option<&str>) -> Result<PositiveFiniteScale> {
    let Some(crs) = crs else {
        return Ok(PositiveFiniteScale::ONE);
    };
    let state = crate::crs::runtime_state_snapshot()?;
    ensure_3d_metric_at(Some(crs), &state)
}

#[expect(
    clippy::float_cmp,
    reason = "unit conversion factors are exact CRS declarations, not approximate measurements"
)]
pub(crate) fn ensure_3d_metric_at(
    crs: Option<&str>,
    state: &crate::crs::RuntimeState,
) -> Result<PositiveFiniteScale> {
    let Some(crs) = crs else {
        return Ok(PositiveFiniteScale::ONE);
    };
    let fail = |message: &str| CrsError::vertical_units(crs, message);
    let crs_info = info_with_state(crs, state)?;
    let Some(factors) = metric_axis_factors_at(&crs_info, state)? else {
        return Err(fail(
            "axes are angular (geographic); reproject to a projected CRS with to_crs",
        ));
    };
    let unit = ensure_positive_finite_linear_unit(factors[0], fail)?;
    let unit_value = unit.get();
    if factors.iter().any(|factor| *factor != unit_value) {
        return Err(fail(
            "axis units differ (e.g. meter vs foot); use one consistent linear unit",
        ));
    }
    // Meters per coordinate unit (`1.0` for a CRS-free or meter CRS, `0.3048…`
    // for feet). This is the factor an explicit `unit='meters'` scales by — it
    // is NOT applied by default. 3D metrics report the CRS's own linear unit,
    // exactly like their 2D siblings; scaling unconditionally used to make
    // `line.length_3d` come back in metres while `line.length` came back in
    // feet, for the very same line.
    Ok(unit)
}

/// Linear (length) unit conversion factors of every axis, recursing into the
/// sub-CRS of a compound CRS. `Ok(None)` if any axis is angular (degree/radian)
/// or has no usable unit — i.e. the CRS is not a pure linear/metric system.
/// Resource failures from recursive CRS lookup or factor storage are returned
/// unchanged.
pub(crate) fn metric_axis_factors_at(
    crs_info: &CrsInfo,
    state: &crate::crs::RuntimeState,
) -> Result<Option<Vec<f64>>> {
    if crs_info.kind.starts_with("geographic") {
        return Ok(None);
    }
    if crs_info.kind == "compound" {
        let mut factors = Vec::new();
        for sub in &crs_info.sub_crs {
            let definition = sub.definition.as_deref().unwrap_or(sub.crs.as_str());
            if definition.is_empty() {
                return Ok(None);
            }
            let sub_info = info_with_state(definition, state)?;
            let Some(sub_factors) = metric_axis_factors_at(&sub_info, state)? else {
                return Ok(None);
            };
            if let Err(error) = crate::boundary::try_reserve_vec(&mut factors, sub_factors.len()) {
                drop(sub_factors);
                return Err(error);
            }
            // The exact reservation above covers every element moved from the
            // child, so extending the aggregate cannot grow it.
            factors.extend(sub_factors);
        }
        return Ok((!factors.is_empty()).then_some(factors));
    }
    for axis in &crs_info.axes {
        let Some(unit) = axis.unit_name.as_deref() else {
            return Ok(None);
        };
        if is_angular_unit(unit) || axis.unit_conversion_factor <= 0.0 {
            return Ok(None);
        }
    }
    let mut factors = Vec::new();
    crate::boundary::try_reserve_vec(&mut factors, crs_info.axes.len())?;
    // The validated axis count is the exact append count.
    factors.extend(crs_info.axes.iter().map(|axis| axis.unit_conversion_factor));
    Ok((!factors.is_empty()).then_some(factors))
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

#[cfg(test)]
mod tests {
    use super::metric_axis_factors_at;
    use crate::crs::{RUNTIME_STATE_TEST_LOCK, info_with_state, runtime_state_snapshot};
    use crate::error::{ErrorKind, ResourceError, Result};
    use crate::test_support::{allocation_count, fail_allocations_after};

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        clippy::needless_borrows_for_generic_args,
        reason = "the test retries compound factor construction across every allocator prefix"
    )]
    fn compound_metric_factors_propagate_allocation_failures_and_retry() -> Result<()> {
        let _guard = RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        let state = runtime_state_snapshot()?;
        let info = info_with_state("EPSG:7405", &state)?;
        let expected =
            metric_axis_factors_at(&info, &state)?.expect("EPSG:7405 has linear compound axes");
        assert!(expected.len() >= 3);
        let expected_scale = super::ensure_3d_metric_at(Some("EPSG:7405"), &state)?;

        let run = || super::ensure_3d_metric_at(Some("EPSG:7405"), &state);
        let (actual, allocations) = allocation_count(&run);
        assert_eq!(actual?.get(), expected_scale.get());
        assert!(allocations > 0, "compound factors did not allocate");
        for allowed in 0..allocations {
            let result = fail_allocations_after(allowed, &run);
            let error = result
                .err()
                .unwrap_or_else(|| panic!("compound prefix {allowed} unexpectedly succeeded"));
            assert!(matches!(
                error.kind(),
                ErrorKind::Resource(ResourceError::Allocation)
            ));
            assert_eq!(
                run()?.get(),
                expected_scale.get(),
                "compound retry at {allowed}"
            );
        }
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "the test uses assertions to distinguish a failed child CRS from a nonmetric result"
    )]
    fn compound_factor_lookup_does_not_hide_a_failed_child_crs() -> Result<()> {
        let _guard = RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        let state = runtime_state_snapshot()?;
        let mut info = (*info_with_state("EPSG:7405", &state)?).clone();
        info.sub_crs[0].crs = "EPSG:0".to_owned();
        let error = metric_axis_factors_at(&info, &state)
            .expect_err("a failed compound child must not become a nonmetric result");
        assert!(matches!(error.kind(), ErrorKind::Crs(_)));
        Ok(())
    }
}
