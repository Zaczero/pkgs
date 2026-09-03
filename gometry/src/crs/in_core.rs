#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! In-core projection formulas — WGS84 Web Mercator (EPSG:3857) and UTM — used
//! by the transform fast paths that avoid a PROJ pipeline. Pure closed-form
//! math (no PROJ FFI, no caches), kept isolated from the transform plumbing.

mod admission;
mod aea;
mod kernel;
mod laea;
mod lcc;
mod params;
mod stere;
mod sterea;
mod tmerc;
mod webmerc;

pub(in crate::crs) use admission::CachedInCoreDescriptor;
pub(crate) use admission::try_in_core_transform_with_state;
use params::Projected;
pub(in crate::crs) use stere::Pole;
use webmerc::WGS84_WEB_MERCATOR;

use crate::crs::in_core::kernel::{DEG_TO_RAD, ProjectionKernel, RAD_TO_DEG};
use crate::crs::local::UtmZone as LocalUtmZone;
use crate::crs::parse_epsg;
use crate::error::Result;

pub(super) fn lonlat_to_web_mercator_xy(lon: f64, lat: f64) -> Result<(f64, f64)> {
    webmerc::lonlat_to_web_mercator_xy(lon, lat)
}

pub(super) fn web_mercator_to_lonlat_xy(x: f64, y: f64) -> (f64, f64) {
    webmerc::web_mercator_to_lonlat_xy(x, y)
}

#[derive(Clone, Copy, Debug)]
pub(super) enum ProjSetup {
    WebMercator(Projected<webmerc::WebMerc>),
    Tmerc(Projected<tmerc::Tmerc>),
    Lcc(Projected<lcc::Lcc>),
    Laea(Projected<laea::Laea>),
    Aea(Projected<aea::Aea>),
    PolarStereo(Projected<stere::PolarStereo>),
    Sterea(Projected<sterea::Sterea>),
}

#[expect(
    clippy::large_enum_variant,
    reason = "in-core transforms stay Copy; boxing would add indirection to hot paths"
)]
#[derive(Clone, Copy, Debug)]
pub(super) enum InCoreTransform {
    RawWebMercatorForward,
    RawWebMercatorInverse,
    One(InCoreStep),
    Two {
        inverse: ProjSetup,
        forward: ProjSetup,
    },
}

#[derive(Clone, Copy, Debug)]
pub(super) enum InCoreStep {
    Forward(ProjSetup),
    Inverse(ProjSetup),
}

impl InCoreStep {
    const fn reversed(self) -> Self {
        match self {
            Self::Forward(setup) => Self::Inverse(setup),
            Self::Inverse(setup) => Self::Forward(setup),
        }
    }
}

impl InCoreTransform {
    pub(super) const fn lon_lat_to_web_mercator() -> Self {
        Self::RawWebMercatorForward
    }

    pub(super) const fn web_mercator_to_lon_lat() -> Self {
        Self::One(InCoreStep::Inverse(ProjSetup::WebMercator(
            WGS84_WEB_MERCATOR,
        )))
    }

    pub(super) const fn reversed(self) -> Self {
        match self {
            Self::RawWebMercatorForward => Self::RawWebMercatorInverse,
            Self::RawWebMercatorInverse => Self::RawWebMercatorForward,
            Self::One(step) => Self::One(step.reversed()),
            Self::Two { inverse, forward } => Self::Two {
                inverse: forward,
                forward: inverse,
            },
        }
    }

    pub(super) fn lon_lat_to_utm(zone: UtmZone) -> Self {
        Self::One(InCoreStep::Forward(ProjSetup::Tmerc(
            tmerc::wgs84_utm_projected(zone),
        )))
    }

    pub(super) fn utm_to_lon_lat(zone: UtmZone) -> Self {
        Self::One(InCoreStep::Inverse(ProjSetup::Tmerc(
            tmerc::wgs84_utm_projected(zone),
        )))
    }

    pub(super) fn web_mercator_to_utm(zone: UtmZone) -> Self {
        Self::Two {
            inverse: ProjSetup::WebMercator(WGS84_WEB_MERCATOR),
            forward: ProjSetup::Tmerc(tmerc::wgs84_utm_projected(zone)),
        }
    }

    pub(super) fn utm_to_web_mercator(zone: UtmZone) -> Self {
        Self::Two {
            inverse: ProjSetup::Tmerc(tmerc::wgs84_utm_projected(zone)),
            forward: ProjSetup::WebMercator(WGS84_WEB_MERCATOR),
        }
    }
}

fn forward_setup(setup: &ProjSetup, x: f64, y: f64) -> Result<(f64, f64)> {
    match setup {
        ProjSetup::WebMercator(p) => p.forward_xy(x, y),
        ProjSetup::Tmerc(p) => p.forward_xy(x, y),
        ProjSetup::Lcc(p) => p.forward_xy(x, y),
        ProjSetup::Laea(p) => p.forward_xy(x, y),
        ProjSetup::Aea(p) => p.forward_xy(x, y),
        ProjSetup::PolarStereo(p) => p.forward_xy(x, y),
        ProjSetup::Sterea(p) => p.forward_xy(x, y),
    }
}

fn inverse_setup(setup: &ProjSetup, x: f64, y: f64) -> Result<(f64, f64)> {
    match setup {
        ProjSetup::WebMercator(p) => p.inverse_xy(x, y),
        ProjSetup::Tmerc(p) => p.inverse_xy(x, y),
        ProjSetup::Lcc(p) => p.inverse_xy(x, y),
        ProjSetup::Laea(p) => p.inverse_xy(x, y),
        ProjSetup::Aea(p) => p.inverse_xy(x, y),
        ProjSetup::PolarStereo(p) => p.inverse_xy(x, y),
        ProjSetup::Sterea(p) => p.inverse_xy(x, y),
    }
}

/// Batch XY transform — resolves the `One`/`Two` + `Forward`/`Inverse` +
/// `ProjSetup` matches once per lane instead of per coordinate.
pub(super) fn transform_in_core_xy_batch(
    operation: &InCoreTransform,
    x: &mut [f64],
    y: &mut [f64],
) -> Result<()> {
    resolve_in_core_xy(operation).apply_batch(x, y)
}

/// Resolved in-core XY kernel — the `One`/`Two` + `Forward`/`Inverse` +
/// `ProjSetup` matches happen once per batch; [`apply`](Self::apply) is a
/// single flat dispatch.
#[derive(Clone, Copy)]
enum ResolvedInCoreXy<'a> {
    RawWebMercFwd,
    RawWebMercInv,
    WebMercFwd(&'a Projected<webmerc::WebMerc>),
    WebMercInv(&'a Projected<webmerc::WebMerc>),
    TmercFwd(&'a Projected<tmerc::Tmerc>),
    TmercInv(&'a Projected<tmerc::Tmerc>),
    LccFwd(&'a Projected<lcc::Lcc>),
    LccInv(&'a Projected<lcc::Lcc>),
    LaeaFwd(&'a Projected<laea::Laea>),
    LaeaInv(&'a Projected<laea::Laea>),
    AeaFwd(&'a Projected<aea::Aea>),
    AeaInv(&'a Projected<aea::Aea>),
    PolarStereoFwd(&'a Projected<stere::PolarStereo>),
    PolarStereoInv(&'a Projected<stere::PolarStereo>),
    StereaFwd(&'a Projected<sterea::Sterea>),
    StereaInv(&'a Projected<sterea::Sterea>),
    WebMercToUtm {
        webmerc: &'a Projected<webmerc::WebMerc>,
        tmerc: &'a Projected<tmerc::Tmerc>,
    },
    UtmToWebMerc {
        tmerc: &'a Projected<tmerc::Tmerc>,
        webmerc: &'a Projected<webmerc::WebMerc>,
    },
    TwoGeneric {
        inverse: &'a ProjSetup,
        forward: &'a ProjSetup,
    },
}

impl ResolvedInCoreXy<'_> {
    fn apply_batch(self, x: &mut [f64], y: &mut [f64]) -> Result<()> {
        match self {
            Self::RawWebMercFwd => webmerc::raw_forward_batch(x, y),
            Self::RawWebMercInv => webmerc::raw_inverse_batch(x, y),
            Self::WebMercFwd(p) => projected_batch::<webmerc::WebMerc, true>(p, x, y),
            Self::WebMercInv(p) => projected_batch::<webmerc::WebMerc, false>(p, x, y),
            Self::TmercFwd(p) => projected_batch::<tmerc::Tmerc, true>(p, x, y),
            Self::TmercInv(p) => projected_batch::<tmerc::Tmerc, false>(p, x, y),
            Self::LccFwd(p) => projected_batch::<lcc::Lcc, true>(p, x, y),
            Self::LccInv(p) => projected_batch::<lcc::Lcc, false>(p, x, y),
            Self::LaeaFwd(p) => projected_batch::<laea::Laea, true>(p, x, y),
            Self::LaeaInv(p) => projected_batch::<laea::Laea, false>(p, x, y),
            Self::AeaFwd(p) => projected_batch::<aea::Aea, true>(p, x, y),
            Self::AeaInv(p) => projected_batch::<aea::Aea, false>(p, x, y),
            Self::PolarStereoFwd(p) => projected_batch::<stere::PolarStereo, true>(p, x, y),
            Self::PolarStereoInv(p) => projected_batch::<stere::PolarStereo, false>(p, x, y),
            Self::StereaFwd(p) => projected_batch::<sterea::Sterea, true>(p, x, y),
            Self::StereaInv(p) => projected_batch::<sterea::Sterea, false>(p, x, y),
            Self::WebMercToUtm { webmerc, tmerc } => webmerc_to_utm_batch(webmerc, tmerc, x, y),
            Self::UtmToWebMerc { tmerc, webmerc } => utm_to_webmerc_batch(tmerc, webmerc, x, y),
            Self::TwoGeneric { inverse, forward } => two_generic_batch(inverse, forward, x, y),
        }
    }
}

fn projected_batch<K: ProjectionKernel, const FORWARD: bool>(
    projected: &Projected<K>,
    x: &mut [f64],
    y: &mut [f64],
) -> Result<()> {
    for (x, y) in std::iter::zip(x, y) {
        (*x, *y) = if FORWARD {
            validate_geographic_coordinate(*x, *y)?;
            let lam_rel = *x * DEG_TO_RAD - projected.frame.lon_0_rad;
            let (out_x, out_y) = K::forward_unframed(&projected.setup, lam_rel, *y * DEG_TO_RAD)?;
            (
                params::apply_false_origin(out_x, projected.frame.x_0)
                    * projected.frame.axis_scale.inverse(),
                params::apply_false_origin(out_y, projected.frame.y_0)
                    * projected.frame.axis_scale.inverse(),
            )
        } else {
            let (lam_rel, lat_rad) = K::inverse_unframed(
                &projected.setup,
                projected.frame.axis_scale.unframe(*x, projected.frame.x_0),
                projected.frame.axis_scale.unframe(*y, projected.frame.y_0),
            )?;
            (
                adjlon(projected.frame.lon_0_rad + lam_rel) * RAD_TO_DEG,
                lat_rad * RAD_TO_DEG,
            )
        };
    }
    Ok(())
}

fn validate_geographic_coordinate(lon_deg: f64, lat_deg: f64) -> Result<()> {
    if !lon_deg.is_finite() || !lat_deg.is_finite() {
        return Err(crate::geometry::GeometryErrorKind::NonFiniteCoordinate.into());
    }
    // The poles are valid geographic coordinates. Longitude is deliberately
    // unbounded: PROJ normalizes every finite longitude around the central
    // meridian, while latitude has a closed physical domain.
    if lat_deg.abs() > 90.0 {
        return Err(crate::geometry::GeometryErrorKind::projection_domain(
            format!(
                "latitude {lat_deg:e} is outside the geographic coordinate domain required by Web Mercator and other projections"
            ),
        ));
    }
    Ok(())
}

fn webmerc_to_utm_batch(
    webmerc: &Projected<webmerc::WebMerc>,
    tmerc: &Projected<tmerc::Tmerc>,
    x: &mut [f64],
    y: &mut [f64],
) -> Result<()> {
    for (x, y) in std::iter::zip(x, y) {
        let (lon, lat) = webmerc.inverse_xy(*x, *y)?;
        let lat = snap_webmerc_to_utm_latitude(lat);
        (*x, *y) = tmerc.forward_xy(lon, lat)?;
    }
    Ok(())
}

fn utm_to_webmerc_batch(
    tmerc: &Projected<tmerc::Tmerc>,
    webmerc: &Projected<webmerc::WebMerc>,
    x: &mut [f64],
    y: &mut [f64],
) -> Result<()> {
    for (x, y) in std::iter::zip(x, y) {
        let (lon, lat) = tmerc.inverse_xy(*x, *y)?;
        (*x, *y) = webmerc.forward_xy(lon, lat)?;
    }
    Ok(())
}

fn two_generic_batch(
    inverse: &ProjSetup,
    forward: &ProjSetup,
    x: &mut [f64],
    y: &mut [f64],
) -> Result<()> {
    let snap_latitude = is_webmerc_to_utm_composition(inverse, forward);
    for (x, y) in std::iter::zip(x, y) {
        let (out_x, mut out_y) = inverse_setup(inverse, *x, *y)?;
        if snap_latitude {
            out_y = snap_webmerc_to_utm_latitude(out_y);
        }
        (*x, *y) = forward_setup(forward, out_x, out_y)?;
    }
    Ok(())
}

const fn resolve_in_core_xy(operation: &InCoreTransform) -> ResolvedInCoreXy<'_> {
    match operation {
        InCoreTransform::RawWebMercatorForward => ResolvedInCoreXy::RawWebMercFwd,
        InCoreTransform::RawWebMercatorInverse => ResolvedInCoreXy::RawWebMercInv,
        InCoreTransform::One(InCoreStep::Forward(ProjSetup::WebMercator(p))) => {
            ResolvedInCoreXy::WebMercFwd(p)
        },
        InCoreTransform::One(InCoreStep::Inverse(ProjSetup::WebMercator(p))) => {
            ResolvedInCoreXy::WebMercInv(p)
        },
        InCoreTransform::One(InCoreStep::Forward(ProjSetup::Tmerc(p))) => {
            ResolvedInCoreXy::TmercFwd(p)
        },
        InCoreTransform::One(InCoreStep::Inverse(ProjSetup::Tmerc(p))) => {
            ResolvedInCoreXy::TmercInv(p)
        },
        InCoreTransform::One(InCoreStep::Forward(ProjSetup::Lcc(p))) => ResolvedInCoreXy::LccFwd(p),
        InCoreTransform::One(InCoreStep::Inverse(ProjSetup::Lcc(p))) => ResolvedInCoreXy::LccInv(p),
        InCoreTransform::One(InCoreStep::Forward(ProjSetup::Laea(p))) => {
            ResolvedInCoreXy::LaeaFwd(p)
        },
        InCoreTransform::One(InCoreStep::Inverse(ProjSetup::Laea(p))) => {
            ResolvedInCoreXy::LaeaInv(p)
        },
        InCoreTransform::One(InCoreStep::Forward(ProjSetup::Aea(p))) => ResolvedInCoreXy::AeaFwd(p),
        InCoreTransform::One(InCoreStep::Inverse(ProjSetup::Aea(p))) => ResolvedInCoreXy::AeaInv(p),
        InCoreTransform::One(InCoreStep::Forward(ProjSetup::PolarStereo(p))) => {
            ResolvedInCoreXy::PolarStereoFwd(p)
        },
        InCoreTransform::One(InCoreStep::Inverse(ProjSetup::PolarStereo(p))) => {
            ResolvedInCoreXy::PolarStereoInv(p)
        },
        InCoreTransform::One(InCoreStep::Forward(ProjSetup::Sterea(p))) => {
            ResolvedInCoreXy::StereaFwd(p)
        },
        InCoreTransform::One(InCoreStep::Inverse(ProjSetup::Sterea(p))) => {
            ResolvedInCoreXy::StereaInv(p)
        },
        InCoreTransform::Two {
            inverse: ProjSetup::WebMercator(webmerc),
            forward: ProjSetup::Tmerc(tmerc),
        } => ResolvedInCoreXy::WebMercToUtm { webmerc, tmerc },
        InCoreTransform::Two {
            inverse: ProjSetup::Tmerc(tmerc),
            forward: ProjSetup::WebMercator(webmerc),
        } => ResolvedInCoreXy::UtmToWebMerc { tmerc, webmerc },
        InCoreTransform::Two { inverse, forward } => {
            ResolvedInCoreXy::TwoGeneric { inverse, forward }
        },
    }
}

/// Snap composition-boundary latitudes onto the UTM domain edge after a Web
/// Mercator inverse (the inverse carries ~1-ulp roundoff at ±84/±80).
fn snap_webmerc_to_utm_latitude(lat: f64) -> f64 {
    if (lat - 84.0).abs() <= 1e-9 {
        84.0
    } else if (lat + 80.0).abs() <= 1e-9 {
        -80.0
    } else {
        lat
    }
}

const fn is_webmerc_to_utm_composition(inverse: &ProjSetup, forward: &ProjSetup) -> bool {
    matches!(
        (inverse, forward),
        (ProjSetup::WebMercator(_), ProjSetup::Tmerc(_))
    )
}

#[expect(
    clippy::decimal_literal_representation,
    reason = "EPSG authority codes are decimal identifiers"
)]
pub(super) fn utm_crs(crs: &str) -> Option<(LocalUtmZone, Pole)> {
    let code = parse_epsg(crs)?;
    match code {
        32601..=32660 => Some((LocalUtmZone::try_new(code - 32600).ok()?, Pole::North)),
        32701..=32760 => Some((LocalUtmZone::try_new(code - 32700).ok()?, Pole::South)),
        _ => None,
    }
}

#[expect(clippy::suboptimal_flops, reason = "standard projection formula")]
fn utm_central_meridian(zone: LocalUtmZone) -> f64 {
    f64::from(zone.get()) * 6.0 - 183.0
}

/// `PROJ`'s `adjlon` (`adjlon.cpp` verbatim): normalize a longitude
/// (radians) into `±π`, passthrough — with `PROJ`'s slight overshoot
/// tolerance against spurious sign switching at the date line — for
/// in-range values. `PROJ` applies this to the central-meridian delta on
/// every projection forward and to every inverse output — without it, an
/// in-domain longitude on the far side of the antimeridian from the zone's
/// central meridian (Δλ ≈ ±357°) reads as a far-out-of-domain input instead
/// of the true ±3° offset.
pub(super) fn adjlon(mut lon: f64) -> f64 {
    if lon.abs() < std::f64::consts::PI + 1e-12 {
        return lon;
    }
    lon += std::f64::consts::PI;
    lon -= std::f64::consts::TAU * (lon / std::f64::consts::TAU).floor();
    lon - std::f64::consts::PI
}

/// Parsed UTM zone metadata — stored once at transform construction so the
/// per-coordinate kernels never re-parse EPSG or decode zone/north.
#[derive(Clone, Copy, Debug)]
pub(super) struct UtmZone {
    pub(crate) pole: Pole,
    pub(crate) central_meridian: f64,
}

impl UtmZone {
    pub(crate) fn from_crs(crs: &str) -> Option<Self> {
        let (zone, pole) = utm_crs(crs)?;
        Some(Self {
            pole,
            central_meridian: utm_central_meridian(zone),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::kernel::Ellipsoid;
    use super::params::{OperationSpec, ProjectionFrame};
    use super::*;
    use crate::numeric::AxisScale;

    #[derive(Clone, Copy)]
    struct IdentityKernel;

    impl ProjectionKernel for IdentityKernel {
        type Setup = ();
        type MethodParams = ();

        const SPECS: &'static [OperationSpec<Self::MethodParams>] = &[];

        fn setup(_ellipsoid: Ellipsoid, _params: ()) -> Result<()> {
            Ok(())
        }

        fn forward_unframed(_setup: &(), lon: f64, lat: f64) -> Result<(f64, f64)> {
            Ok((lon, lat))
        }

        fn inverse_unframed(_setup: &(), x: f64, y: f64) -> Result<(f64, f64)> {
            Ok((x, y))
        }
    }

    #[test]
    fn projected_batch_preserves_negative_zero_through_zero_false_origin() {
        let projected = identity_projected();
        let mut x = [-0.0];
        let mut y = [-0.0];
        projected_batch::<IdentityKernel, true>(&projected, &mut x, &mut y).unwrap();
        assert_eq!(x[0].to_bits(), (-0.0_f64).to_bits());
        assert_eq!(y[0].to_bits(), (-0.0_f64).to_bits());
    }

    fn identity_projected() -> Projected<IdentityKernel> {
        Projected::<IdentityKernel> {
            frame: ProjectionFrame::ZERO,
            setup: (),
        }
        .with_axis_scale(AxisScale::from_unit_to_metre(0.304_800_609_601_219_24).unwrap())
    }

    #[test]
    fn scalar_forward_rejects_latitude_outside_closed_domain() {
        let error = identity_projected().forward_xy(0.0, 100.0).unwrap_err();
        assert!(error.to_string().contains("latitude"));
    }

    #[test]
    fn batch_forward_rejects_latitude_outside_closed_domain() {
        let mut x = [0.0];
        let mut y = [100.0];
        let error = projected_batch::<IdentityKernel, true>(&identity_projected(), &mut x, &mut y)
            .unwrap_err();
        assert!(error.to_string().contains("latitude"));
    }

    #[test]
    fn reversed_raw_one_and_two_step_operations_restore_coordinates() {
        let mut raw_x = [12.0];
        let mut raw_y = [45.0];
        let raw = InCoreTransform::lon_lat_to_web_mercator();
        transform_in_core_xy_batch(&raw, &mut raw_x, &mut raw_y).unwrap();
        transform_in_core_xy_batch(&raw.reversed(), &mut raw_x, &mut raw_y).unwrap();
        assert!((raw_x[0] - 12.0).abs() < 1e-12);
        assert!((raw_y[0] - 45.0).abs() < 1e-12);

        let zone = UtmZone {
            pole: Pole::North,
            central_meridian: 15.0,
        };
        let one = InCoreTransform::lon_lat_to_utm(zone);
        let mut one_x = [12.0];
        let mut one_y = [45.0];
        transform_in_core_xy_batch(&one, &mut one_x, &mut one_y).unwrap();
        transform_in_core_xy_batch(&one.reversed(), &mut one_x, &mut one_y).unwrap();
        assert!((one_x[0] - 12.0).abs() < 1e-12);
        assert!((one_y[0] - 45.0).abs() < 1e-12);

        let two = InCoreTransform::web_mercator_to_utm(zone);
        let mut two_x = [12.0];
        let mut two_y = [45.0];
        let (web_x, web_y) = WGS84_WEB_MERCATOR.forward_xy(two_x[0], two_y[0]).unwrap();
        two_x[0] = web_x;
        two_y[0] = web_y;
        transform_in_core_xy_batch(&two, &mut two_x, &mut two_y).unwrap();
        transform_in_core_xy_batch(&two.reversed(), &mut two_x, &mut two_y).unwrap();
        assert!((two_x[0] - web_x).abs() < 1e-9);
        assert!((two_y[0] - web_y).abs() < 1e-9);
    }
}
