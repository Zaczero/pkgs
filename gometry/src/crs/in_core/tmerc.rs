#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Poder/Engsager exact transverse Mercator — the algorithm behind PROJ's
//! `utm`/`etmerc` (ported from PROJ 9.8 `src/projections/tmerc.cpp`).

use super::kernel::{DEG_TO_RAD, Ellipsoid, ProjectionKernel};
use super::params::{
    FrameSpec, MethodSpec, OperationSpec, Projected, ProjectionFrame, Requirement,
    admit_projection, epsg, param_field,
};
use super::{Pole, UtmZone, adjlon};
use crate::error::Result;
use crate::geometry::GeometryErrorKind;

/// `PROJ`'s transverse-Mercator domain bound on the normalized easting
/// (`tmerc.cpp`: 150° from the central meridian).
const ETMERC_DOMAIN: f64 = 2.623_395_162_778;

/// The Poder/Engsager series constants for one ellipsoid + scale.
#[derive(Clone, Copy, Debug)]
pub(super) struct PoderEngsager {
    /// Gaussian -> geodetic latitude series (KW p190-191 (61)-(62)).
    cgb: [f64; 6],
    /// Geodetic -> Gaussian latitude series (KW p186-187 (51)-(52)).
    cbg: [f64; 6],
    /// Ellipsoidal N/E -> spherical N/E series (KW p194 (65)).
    utg: [f64; 6],
    /// Spherical N/E -> ellipsoidal N/E series (KW p196 (69)).
    gtu: [f64; 6],
    /// Normalized meridian quadrant, scaled by `k0` (KW p.50 (96)).
    qn: f64,
}

pub(super) const WGS84_ELLIPSOID: Ellipsoid =
    Ellipsoid::from_a_f(super::super::WGS84_A, super::super::WGS84_F);

#[derive(Clone, Copy, Debug, Default)]
pub(in crate::crs) struct TmercParams {
    lat_0_deg: f64,
    k0: f64,
}

#[derive(Clone, Copy, Debug, Default)]
pub(in crate::crs) struct Tmerc;

#[derive(Clone, Copy, Debug)]
pub(in crate::crs) struct Setup {
    pub(super) series: PoderEngsager,
    pub(super) y_origin: f64,
    pub(super) a_qn: f64,
    pub(super) inv_a_qn: f64,
}

pub(super) fn tmerc_setup(ellipsoid: Ellipsoid, params: TmercParams) -> Setup {
    let series = poder_engsager(ellipsoid, params.k0);
    let zb = origin_northing_offset(&series, params.lat_0_deg);
    let a_qn = ellipsoid.a * series.qn;
    Setup {
        series,
        y_origin: ellipsoid.a * zb,
        a_qn,
        inv_a_qn: 1.0 / a_qn,
    }
}

pub(super) fn wgs84_utm_projected(zone: UtmZone) -> Projected<Tmerc> {
    let setup = tmerc_setup(WGS84_ELLIPSOID, TmercParams {
        lat_0_deg: 0.0,
        k0: super::super::UTM_K0,
    });
    Projected {
        frame: ProjectionFrame {
            lon_0_rad: zone.central_meridian * DEG_TO_RAD,
            x_0: 500_000.0,
            y_0: match zone.pole {
                Pole::North => 0.0,
                Pole::South => 10_000_000.0,
            },
        },
        setup,
    }
}

const fn set_lat_0(params: &mut TmercParams, value: f64) -> bool {
    if value.is_finite() {
        params.lat_0_deg = value;
        true
    } else {
        false
    }
}

fn set_k0(params: &mut TmercParams, value: f64) -> bool {
    if value.is_finite() && value > 0.0 {
        params.k0 = value;
        true
    } else {
        false
    }
}

fn finalize_tmerc(params: TmercParams) -> Option<TmercParams> {
    (params.k0 > 0.0).then_some(params)
}

fn origin_northing_offset(series: &PoderEngsager, lat_0_deg: f64) -> f64 {
    if lat_0_deg == 0.0 {
        return 0.0;
    }
    let lat_0 = lat_0_deg.to_radians();
    let (sin_2, cos_2) = (2.0 * lat_0).sin_cos();
    let z = gatg(&series.cbg, lat_0, cos_2, sin_2);
    -series.qn * (z + clens(&series.gtu, 2.0 * z))
}

pub(super) const fn poder_engsager(ellipsoid: Ellipsoid, k0: f64) -> PoderEngsager {
    let n = ellipsoid.n;
    let mut cgb = [0.0_f64; 6];
    let mut cbg = [0.0_f64; 6];
    let mut utg = [0.0_f64; 6];
    let mut gtu = [0.0_f64; 6];

    let mut np = n;
    cgb[0] = n
        * (2.0
            + n * (-2.0 / 3.0
                + n * (-2.0 + n * (116.0 / 45.0 + n * (26.0 / 45.0 + n * (-2854.0 / 675.0))))));
    cbg[0] = n
        * (-2.0
            + n * (2.0 / 3.0
                + n * (4.0 / 3.0
                    + n * (-82.0 / 45.0 + n * (32.0 / 45.0 + n * (4642.0 / 4725.0))))));
    np *= n;
    cgb[1] = np
        * (7.0 / 3.0
            + n * (-8.0 / 5.0 + n * (-227.0 / 45.0 + n * (2704.0 / 315.0 + n * (2323.0 / 945.0)))));
    cbg[1] = np
        * (5.0 / 3.0
            + n * (-16.0 / 15.0 + n * (-13.0 / 9.0 + n * (904.0 / 315.0 + n * (-1522.0 / 945.0)))));
    np *= n;
    cgb[2] =
        np * (56.0 / 15.0 + n * (-136.0 / 35.0 + n * (-1262.0 / 105.0 + n * (73814.0 / 2835.0))));
    cbg[2] = np * (-26.0 / 15.0 + n * (34.0 / 21.0 + n * (8.0 / 5.0 + n * (-12686.0 / 2835.0))));
    np *= n;
    cgb[3] = np * (4279.0 / 630.0 + n * (-332.0 / 35.0 + n * (-399_572.0 / 14175.0)));
    cbg[3] = np * (1237.0 / 630.0 + n * (-12.0 / 5.0 + n * (-24832.0 / 14175.0)));
    np *= n;
    cgb[4] = np * (4174.0 / 315.0 + n * (-144_838.0 / 6237.0));
    cbg[4] = np * (-734.0 / 315.0 + n * (109_598.0 / 31185.0));
    np *= n;
    cgb[5] = np * (601_676.0 / 22275.0);
    cbg[5] = np * (444_337.0 / 155_925.0);

    let n2 = n * n;
    let qn = k0 / (1.0 + n) * (1.0 + n2 * (1.0 / 4.0 + n2 * (1.0 / 64.0 + n2 / 256.0)));

    let mut np = n2;
    utg[0] = n
        * (-0.5
            + n * (2.0 / 3.0
                + n * (-37.0 / 96.0
                    + n * (1.0 / 360.0 + n * (81.0 / 512.0 + n * (-96199.0 / 604_800.0))))));
    gtu[0] = n
        * (0.5
            + n * (-2.0 / 3.0
                + n * (5.0 / 16.0
                    + n * (41.0 / 180.0 + n * (-127.0 / 288.0 + n * (7891.0 / 37800.0))))));
    utg[1] = np
        * (-1.0 / 48.0
            + n * (-1.0 / 15.0
                + n * (437.0 / 1440.0 + n * (-46.0 / 105.0 + n * (1_118_711.0 / 3_870_720.0)))));
    gtu[1] = np
        * (13.0 / 48.0
            + n * (-3.0 / 5.0
                + n * (557.0 / 1440.0 + n * (281.0 / 630.0 + n * (-1_983_433.0 / 1_935_360.0)))));
    np *= n;
    utg[2] =
        np * (-17.0 / 480.0 + n * (37.0 / 840.0 + n * (209.0 / 4480.0 + n * (-5569.0 / 90720.0))));
    gtu[2] = np
        * (61.0 / 240.0
            + n * (-103.0 / 140.0 + n * (15061.0 / 26880.0 + n * (167_603.0 / 181_440.0))));
    np *= n;
    utg[3] = np * (-4397.0 / 161_280.0 + n * (11.0 / 504.0 + n * (830_251.0 / 7_257_600.0)));
    gtu[3] = np * (49561.0 / 161_280.0 + n * (-179.0 / 168.0 + n * (6_601_661.0 / 7_257_600.0)));
    np *= n;
    utg[4] = np * (-4583.0 / 161_280.0 + n * (108_847.0 / 3_991_680.0));
    gtu[4] = np * (34729.0 / 80640.0 + n * (-3_418_889.0 / 1_995_840.0));
    np *= n;
    utg[5] = np * (-20_648_693.0 / 638_668_800.0);
    gtu[5] = np * (212_378_941.0 / 319_334_400.0);

    PoderEngsager {
        cgb,
        cbg,
        utg,
        gtu,
        qn,
    }
}

pub(super) fn geographic_to_tmerc_unframed(
    lam_rad: f64,
    lat_rad: f64,
    setup: &Setup,
) -> Result<(f64, f64)> {
    let lat = lat_rad;
    let lam = adjlon(lam_rad);

    let (sin_2lat, cos_2lat) = (2.0 * lat).sin_cos();
    let cn = gatg(&setup.series.cbg, lat, cos_2lat, sin_2lat);
    let (sin_cn, cos_cn) = cn.sin_cos();
    let (sin_ce, cos_ce) = lam.sin_cos();
    let cos_cn_cos_ce = cos_cn * cos_ce;
    let cn = sin_cn.atan2(cos_cn_cos_ce);
    let inv_denom_tan_ce = 1.0 / (sin_cn * sin_cn + cos_cn_cos_ce * cos_cn_cos_ce).sqrt();
    let tan_ce = sin_ce * cos_cn * inv_denom_tan_ce;
    let ce = tan_ce.asinh();
    let two_inv_denom_tan_ce = 2.0 * inv_denom_tan_ce;
    let two_inv_denom_tan_ce_square = two_inv_denom_tan_ce * inv_denom_tan_ce;
    let tmp_r = cos_cn_cos_ce * two_inv_denom_tan_ce_square;
    let sin_arg_r = sin_cn * tmp_r;
    let cos_arg_r = cos_cn_cos_ce * tmp_r - 1.0;
    let sinh_arg_i = tan_ce * two_inv_denom_tan_ce;
    let cosh_arg_i = two_inv_denom_tan_ce_square - 1.0;
    let (d_cn, d_ce) = clen_s(
        &setup.series.gtu,
        sin_arg_r,
        cos_arg_r,
        sinh_arg_i,
        cosh_arg_i,
    );
    let cn = cn + d_cn;
    let ce = ce + d_ce;
    if ce.abs() > ETMERC_DOMAIN {
        return Err(GeometryErrorKind::projection(
            "coordinate is outside the transverse Mercator domain (more than 150 degrees from \
             the central meridian)",
        ));
    }
    let easting = setup.a_qn * ce;
    let northing = setup.a_qn * cn + setup.y_origin;
    if !easting.is_finite() || !northing.is_finite() {
        return Err(GeometryErrorKind::NonFiniteCoordinate.into());
    }
    Ok((easting, northing))
}

pub(super) fn tmerc_to_geographic_unframed(
    easting: f64,
    northing: f64,
    setup: &Setup,
) -> Result<(f64, f64)> {
    let cn = (northing - setup.y_origin) * setup.inv_a_qn;
    let ce = easting * setup.inv_a_qn;
    if ce.abs() > ETMERC_DOMAIN {
        return Err(GeometryErrorKind::projection(
            "coordinate is outside the transverse Mercator domain (more than 150 degrees from \
             the central meridian)",
        ));
    }
    let (sin_arg_r, cos_arg_r) = (2.0 * cn).sin_cos();
    let exp_2_ce = (2.0 * ce).exp();
    let half_inv_exp_2_ce = 0.5 / exp_2_ce;
    let sinh_arg_i = 0.5 * exp_2_ce - half_inv_exp_2_ce;
    let cosh_arg_i = 0.5 * exp_2_ce + half_inv_exp_2_ce;
    let (d_cn, d_ce) = clen_s(
        &setup.series.utg,
        sin_arg_r,
        cos_arg_r,
        sinh_arg_i,
        cosh_arg_i,
    );
    let cn = cn + d_cn;
    let ce = ce + d_ce;
    let (sin_cn, cos_cn) = cn.sin_cos();
    let sinh_ce = ce.sinh();
    let ce = sinh_ce.atan2(cos_cn);
    let modulus_ce = (sinh_ce * sinh_ce + cos_cn * cos_cn).sqrt();
    let cn = sin_cn.atan2(modulus_ce);
    let tmp = 2.0 * modulus_ce / (sinh_ce * sinh_ce + 1.0);
    let sin_2_cn = sin_cn * tmp;
    let cos_2_cn = tmp * modulus_ce - 1.0;
    let lat = gatg(&setup.series.cgb, cn, cos_2_cn, sin_2_cn);
    let lam = adjlon(ce);
    if !lam.is_finite() || !lat.is_finite() {
        return Err(GeometryErrorKind::NonFiniteCoordinate.into());
    }
    Ok((lam, lat))
}

fn gatg(p: &[f64; 6], b: f64, cos_2b: f64, sin_2b: f64) -> f64 {
    let two_cos_2b = 2.0 * cos_2b;
    let mut h2 = 0.0_f64;
    let mut h1 = p[5];
    let mut h = h1;
    for &coefficient in p[..5].iter().rev() {
        h = -h2 + two_cos_2b * h1 + coefficient;
        h2 = h1;
        h1 = h;
    }
    b + h * sin_2b
}

fn clens(p: &[f64; 6], arg: f64) -> f64 {
    let cos_arg = arg.cos();
    let sin_arg = arg.sin();
    let two_cos_arg = 2.0 * cos_arg;
    let mut h2 = 0.0_f64;
    let mut h1 = p[5];
    let mut h = h1;
    for &coefficient in p[..5].iter().rev() {
        h = -h2 + two_cos_arg * h1 + coefficient;
        h2 = h1;
        h1 = h;
    }
    sin_arg * h
}

fn clen_s(
    a: &[f64; 6],
    sin_arg_r: f64,
    cos_arg_r: f64,
    sinh_arg_i: f64,
    cosh_arg_i: f64,
) -> (f64, f64) {
    let r = 2.0 * cos_arg_r * cosh_arg_i;
    let i = -2.0 * sin_arg_r * sinh_arg_i;
    let mut hr1 = 0.0_f64;
    let mut hi1 = 0.0_f64;
    let mut hi = 0.0_f64;
    let mut hr = a[5];
    for &coefficient in a[..5].iter().rev() {
        let hr2 = hr1;
        let hi2 = hi1;
        hr1 = hr;
        hi1 = hi;
        hr = -hr2 + r * hr1 - i * hi1 + coefficient;
        hi = -hi2 + i * hr1 + r * hi1;
    }
    let r = sin_arg_r * cosh_arg_i;
    let i = cos_arg_r * sinh_arg_i;
    (r * hr - i * hi, r * hi + i * hr)
}

impl ProjectionKernel for Tmerc {
    type Setup = Setup;
    type MethodParams = TmercParams;

    const SPECS: &'static [OperationSpec<TmercParams>] = &[OperationSpec {
        method: MethodSpec { code: "9807" },
        frame: FrameSpec {
            lon_0: epsg("8802"),
            x_0: epsg("8806"),
            y_0: epsg("8807"),
        },
        defaults: TmercParams {
            lat_0_deg: 0.0,
            k0: 1.0,
        },
        params: &[
            param_field(
                epsg("8801"),
                super::params::ParamUnit::Degrees,
                set_lat_0,
                Requirement::Required,
            ),
            param_field(
                epsg("8805"),
                super::params::ParamUnit::Unity,
                set_k0,
                Requirement::Required,
            ),
        ],
        finalize: finalize_tmerc,
    }];

    fn setup(ellipsoid: Ellipsoid, params: TmercParams) -> Result<Self::Setup> {
        Ok(tmerc_setup(ellipsoid, params))
    }

    fn forward_unframed(setup: &Self::Setup, lam_rad: f64, lat_rad: f64) -> Result<(f64, f64)> {
        geographic_to_tmerc_unframed(lam_rad, lat_rad, setup)
    }

    fn inverse_unframed(setup: &Self::Setup, x: f64, y: f64) -> Result<(f64, f64)> {
        tmerc_to_geographic_unframed(x, y, setup)
    }
}

pub(super) fn admit(
    operation: &super::super::CrsCoordinateOperationInfo,
    ellipsoid: Ellipsoid,
) -> Option<Projected<Tmerc>> {
    admit_projection::<Tmerc>(operation, ellipsoid)
}

#[cfg(test)]
mod tests {
    use super::super::params::ProjectionFrame;
    use super::*;

    fn projected_fixture(lon_0: f64, lat_0: f64, k0: f64, x_0: f64, y_0: f64) -> Projected<Tmerc> {
        Projected {
            frame: ProjectionFrame {
                lon_0_rad: lon_0 * DEG_TO_RAD,
                x_0,
                y_0,
            },
            setup: tmerc_setup(
                Ellipsoid::from_a_f(6_378_137.0, 1.0 / 298.257_222_101),
                TmercParams {
                    lat_0_deg: lat_0,
                    k0,
                },
            ),
        }
    }

    #[test]
    fn dopw22_tm_inverse_matches_proj_reference() {
        let projected = projected_fixture(-4.15, 52.7, 1.0, 64_859.655_7, 122_266.527_7);
        let (x, y) = projected.forward_xy(-4.51, 52.45).unwrap();
        assert!((x - 40_384.287_775_010_59).abs() < 1e-3, "x={x}");
        assert!((y - 94_507.926_710_246_96).abs() < 1e-3, "y={y}");
        let (lon, lat) = projected.inverse_xy(x, y).unwrap();
        assert!((lon - (-4.51)).abs() < 1e-6, "lon={lon}");
        assert!((lat - 52.45).abs() < 1e-6, "lat={lat}");
    }

    #[test]
    fn nad83_spcs_tm_forward_matches_proj_reference() {
        let projected = projected_fixture(-72.5, 42.5, 0.999_964_286, 500_000.0, 0.0);
        let (x, y) = projected.forward_xy(-72.5, 43.0).unwrap();
        assert!((x - 500_000.0).abs() < 1e-3);
        assert!(
            (y - 55_541.950_175_844_83).abs() < 1e-3,
            "y={y} y_origin={}",
            projected.setup.y_origin
        );
        let (lon, lat) = projected.inverse_xy(x, y).unwrap();
        assert!((lon - (-72.5)).abs() < 1e-6, "lon={lon}");
        assert!((lat - 43.0).abs() < 1e-6, "lat={lat}");
    }
}
