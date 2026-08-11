use crate::crs::geodesic::{Geodesic, Point, REDUCED_LAT_MIN_ONE_MINUS_F, REDUCED_LAT_TABLE_CELLS};

pub(crate) enum LowerBoundKernel {
    Disabled,
    ExactOblate,
    Tabulated(ReducedLatitudeTable),
}

/// A per-thread cache of the most recent lower-bound kernel keyed by the
/// ellipsoid `(a, f)`. Foot-finding builds one `EllipsoidMetric` per call; the
/// kernel depends only on the ellipsoid, and real workloads use one CRS, so a
/// single-slot cache turns the ~150 µs rebuild into an `Rc` clone after the
/// first build on each ellipsoid.
type CachedKernelSlot = Option<((f64, f64), std::rc::Rc<LowerBoundKernel>)>;

pub(crate) fn cached_lower_bound_kernel(geodesic: &Geodesic) -> std::rc::Rc<LowerBoundKernel> {
    thread_local! {
        static LAST_KERNEL: std::cell::RefCell<CachedKernelSlot> =
            const { std::cell::RefCell::new(None) };
    }
    let key = (geodesic.a, geodesic.f);
    LAST_KERNEL.with(|cell| {
        let mut slot = cell.borrow_mut();
        if let Some((cached_key, kernel)) = slot.as_ref()
            && *cached_key == key
        {
            return std::rc::Rc::clone(kernel);
        }
        let kernel = std::rc::Rc::new(LowerBoundKernel::for_geodesic(geodesic));
        *slot = Some((key, std::rc::Rc::clone(&kernel)));
        kernel
    })
}

impl LowerBoundKernel {
    pub(crate) fn for_geodesic(geodesic: &Geodesic) -> Self {
        let one_minus_f = 1.0 - geodesic.f;
        if geodesic.f < 0.0 || geodesic.f >= 1.0 {
            Self::Disabled
        } else if one_minus_f < REDUCED_LAT_MIN_ONE_MINUS_F {
            Self::ExactOblate
        } else {
            Self::Tabulated(ReducedLatitudeTable::new(one_minus_f))
        }
    }

    pub(crate) fn bound(&self, geodesic: &Geodesic, a: Point, b: Point) -> f64 {
        match self {
            Self::Disabled => 0.0,
            Self::ExactOblate => exact_auxiliary_sphere_bound(geodesic, a, b),
            Self::Tabulated(table) => table.bound(geodesic, a, b),
        }
    }
}

pub(crate) struct ReducedLatitudeTable {
    one_minus_f: f64,
    inv_step: f64,
    max_interp_error: f64,
    beta: Box<[f64]>,
}

impl ReducedLatitudeTable {
    fn new(one_minus_f: f64) -> Self {
        let step = std::f64::consts::PI / REDUCED_LAT_TABLE_CELLS as f64;
        let inv_step = 1.0 / step;
        let mut beta = Vec::with_capacity(REDUCED_LAT_TABLE_CELLS + 1);
        for index in 0..=REDUCED_LAT_TABLE_CELLS {
            let value = if index == 0 {
                -std::f64::consts::FRAC_PI_2
            } else if index == REDUCED_LAT_TABLE_CELLS {
                std::f64::consts::FRAC_PI_2
            } else {
                let phi = -std::f64::consts::FRAC_PI_2 + index as f64 * step;
                (one_minus_f * phi.tan()).atan()
            };
            beta.push(value);
        }
        let k = one_minus_f;
        let m = if k.to_bits() == 1.0_f64.to_bits() {
            0.0
        } else {
            (1.0 - k * k) / (k * k * k)
        };
        let max_interp_error = m * step * step / 8.0 + 64.0 * f64::EPSILON;
        Self {
            one_minus_f,
            inv_step,
            max_interp_error,
            beta: beta.into_boxed_slice(),
        }
    }

    fn beta_interval(&self, lat_degrees: f64) -> Option<(f64, f64)> {
        if !lat_degrees.is_finite() || !(-90.0..=90.0).contains(&lat_degrees) {
            return None;
        }
        let phi = lat_degrees.to_radians();
        let u = ((phi + std::f64::consts::FRAC_PI_2) * self.inv_step)
            .clamp(0.0, REDUCED_LAT_TABLE_CELLS as f64);
        let index = (u.floor() as usize).min(REDUCED_LAT_TABLE_CELLS - 1);
        let t = u - index as f64;
        let linear = self.beta[index] + t * (self.beta[index + 1] - self.beta[index]);
        Some((
            (linear - self.max_interp_error).next_down(),
            (linear + self.max_interp_error).next_up(),
        ))
    }

    fn bound(&self, geodesic: &Geodesic, a: Point, b: Point) -> f64 {
        let Some((lo1, hi1)) = self.beta_interval(a.y) else {
            return 0.0;
        };
        let Some((lo2, hi2)) = self.beta_interval(b.y) else {
            return 0.0;
        };
        let gap = if hi1 < lo2 {
            lo2 - hi1
        } else if hi2 < lo1 {
            lo1 - hi2
        } else {
            0.0
        };
        let delta_lon = (b.x - a.x).to_radians();
        if !delta_lon.is_finite() {
            return 0.0;
        }
        let cos1 = cos_lower_interval(lo1, hi1);
        let cos2 = cos_lower_interval(lo2, hi2);
        let sin_lat = round_down_nonnegative((gap / 2.0).sin().abs());
        let sin_lon = round_down_nonnegative((delta_lon / 2.0).sin().abs());
        if sin_lon > 0.99 {
            return exact_auxiliary_sphere_bound(geodesic, a, b);
        }
        let h_lower = add_down(
            square_down(sin_lat),
            mul_down(mul_down(square_down(sin_lon), cos1), cos2),
        )
        .clamp(0.0, 1.0);
        if h_lower > 0.9 {
            return exact_auxiliary_sphere_bound(geodesic, a, b);
        }
        let angle_lower = mul_down(
            2.0,
            round_down_nonnegative(round_down_nonnegative(h_lower.sqrt()).asin()),
        );
        round_down_nonnegative(geodesic.a * self.one_minus_f * angle_lower)
    }
}

pub(crate) fn exact_auxiliary_sphere_bound(geodesic: &Geodesic, a: Point, b: Point) -> f64 {
    if geodesic.f < 0.0 || geodesic.f >= 1.0 {
        return 0.0;
    }
    // Auxiliary-sphere bound: the geodesic is at least the semi-minor
    // axis times the central angle between the reduced latitudes (raw
    // Δλ ≤ the auxiliary longitude difference) — the same verified bound
    // the spatial index's geodesic pruner uses.
    let one_minus_f = 1.0 - geodesic.f;
    let semi_minor = geodesic.a * one_minus_f;
    let reduced = |lat_degrees: f64| {
        f64::atan(one_minus_f * lat_degrees.to_radians().tan())
            .clamp(-std::f64::consts::FRAC_PI_2, std::f64::consts::FRAC_PI_2)
    };
    let (beta1, beta2) = (reduced(a.y), reduced(b.y));
    let delta_lon = (b.x - a.x).to_radians();
    let sin_lat = ((beta2 - beta1) / 2.0).sin();
    let sin_lon = (delta_lon / 2.0).sin();
    // Plain ops: scalar `mul_add` is a libm call below x86-64-v3 (see
    // the index pruner's twin); the clamped bound needs no fused round.
    let h = (sin_lon * sin_lon * beta1.cos() * beta2.cos() + sin_lat * sin_lat).clamp(0.0, 1.0);
    semi_minor * 2.0 * h.sqrt().asin()
}

pub(crate) fn round_down_nonnegative(value: f64) -> f64 {
    if value <= 0.0 {
        0.0
    } else if value.is_finite() {
        value.next_down().max(0.0)
    } else {
        0.0
    }
}

pub(crate) fn square_down(value: f64) -> f64 {
    if value == 0.0 {
        0.0
    } else {
        round_down_nonnegative(value * value)
    }
}

pub(crate) fn mul_down(left: f64, right: f64) -> f64 {
    if left == 0.0 || right == 0.0 {
        0.0
    } else {
        round_down_nonnegative(left * right)
    }
}

pub(crate) fn add_down(left: f64, right: f64) -> f64 {
    if left == 0.0 {
        right
    } else if right == 0.0 {
        left
    } else {
        round_down_nonnegative(left + right)
    }
}

pub(crate) fn cos_lower_interval(lo: f64, hi: f64) -> f64 {
    let max_abs = lo.abs().max(hi.abs()).min(std::f64::consts::FRAC_PI_2);
    round_down_nonnegative(max_abs.cos())
}

/// Linearly interpolate a Z or M ordinate by `fraction`, present only when both
/// endpoints carry it. Uses the convex form `a*(1-fraction) + b*fraction`
/// (mirroring `geometry::interpolate_f64`): for finite endpoints and a
/// `fraction` in `[0, 1]` the result stays bounded by `max(|a|, |b|)`, avoiding
/// the `b - a` overflow class that would otherwise produce a non-finite
/// ordinate and snap the whole interpolated point back to an endpoint.
pub(crate) fn interpolate_optional_ordinate(
    a: Option<f64>,
    b: Option<f64>,
    fraction: f64,
) -> Option<f64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a * (1.0 - fraction) + b * fraction),
        _ => None,
    }
}
