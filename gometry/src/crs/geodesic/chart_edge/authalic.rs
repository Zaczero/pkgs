use geographiclib_rs::Geodesic;

use crate::geometry::{GeographicAreaDensity, MAX_AUTHALIC_HARMONICS};

const AUTHALIC_TAIL_RELATIVE: f64 = 1.0e-17;

#[derive(Clone, Copy, Debug)]
/// Truncated authalic-q sine series cached with its ellipsoid's geodesic.
pub(crate) struct AuthalicSeries {
    coefficients: [f64; MAX_AUTHALIC_HARMONICS],
    harmonics: usize,
}

impl AuthalicSeries {
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "the validated flattening domain bounds harmonics to nine and the largest binomial row to sixty-nine"
    )]
    pub(crate) fn new(geodesic: &Geodesic) -> Self {
        let e2 = geodesic.f * (2.0 - geodesic.f);
        let ratio = (e2 / 4.0).abs();
        let mut harmonics: usize = 1;
        #[expect(
            clippy::while_float,
            reason = "the retained sine-series order ends at the specified relative tail threshold"
        )]
        while ratio.powi(harmonics as i32) >= AUTHALIC_TAIL_RELATIVE {
            harmonics = harmonics.wrapping_add(1);
        }

        let mut coefficients = [0.0; MAX_AUTHALIC_HARMONICS];
        for (k, coefficient) in coefficients.iter_mut().enumerate().take(harmonics) {
            let mut sum = 0.0;
            let mut scaled_power = (e2 / 4.0).powi(k as i32);
            for j in k..3 * harmonics + 8 {
                let ratio = (2 * j + 2) as f64 / (2 * j + 1) as f64;
                sum += scaled_power * ratio * binomial(2 * j + 1, j - k);
                scaled_power *= e2 / 4.0;
            }
            *coefficient = (1.0 - e2) * sum * if k % 2 == 0 { 1.0 } else { -1.0 };
        }

        Self {
            coefficients,
            harmonics,
        }
    }

    pub(crate) fn area_density(
        self,
        geodesic: &Geodesic,
        reference_colatitude: f64,
        reference_pole: crate::geographic::GeodeticPole,
    ) -> GeographicAreaDensity {
        GeographicAreaDensity::new(
            self.coefficients,
            self.harmonics,
            geodesic.a,
            reference_colatitude,
            reference_pole,
        )
    }
}

fn binomial(n: usize, k: usize) -> f64 {
    let k = k.min(n.wrapping_sub(k));
    let mut coefficient = 1.0;
    for index in 1..=k {
        coefficient *= n.wrapping_add(1).wrapping_sub(index) as f64 / index as f64;
    }
    coefficient
}
