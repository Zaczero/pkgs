use geographiclib_rs::Geodesic;

use super::bounds::{lower_product, upper_hypot, upper_product, upper_ratio, upper_sum};
use super::{ChartEdge, ChartEdgeDerivatives};
use crate::geographic::RetainedChartDisplacement;
use crate::geographic::retained_exact::sites::retained_delta_scaled;
use crate::geometry::interval::Interval as CertifiedInterval;
use crate::geometry::{ChartEdgeDifferential, ChartEdgeDifferentialBounds};

pub(in crate::crs::geodesic) fn chart_edge_differential(
    edge: &ChartEdge,
    geodesic: &Geodesic,
    parameter: f64,
) -> ChartEdgeDifferential {
    let differential = edge.derivatives(parameter, geodesic);
    ChartEdgeDifferential {
        speed: differential.speed,
        azimuth: differential.azimuth,
    }
}

pub(in crate::crs::geodesic) fn chart_edge_differential_bounds(
    edge: &ChartEdge,
    geodesic: &Geodesic,
) -> ChartEdgeDifferentialBounds {
    edge.differential_bounds(geodesic)
}

pub(super) fn retained_longitude_radians(
    longitude: RetainedChartDisplacement,
    axis: crate::geographic::PeriodicAxis,
    orientation: f64,
    native_delta: f64,
) -> f64 {
    let radians_per_unit = axis.radians_per_unit();
    let ordinary = native_delta * radians_per_unit * orientation;
    if ordinary.is_finite() {
        return ordinary;
    }
    retained_delta_scaled(
        longitude.endpoints()[0].get(),
        longitude.endpoints()[1].get(),
        axis.period(),
        longitude.periods(),
        radians_per_unit * orientation,
    )
}

impl ChartEdge {
    pub(in crate::crs::geodesic) fn derivatives(
        self,
        parameter: f64,
        geodesic: &Geodesic,
    ) -> ChartEdgeDerivatives {
        let e2 = geodesic.f * (2.0 - geodesic.f);
        let dphi = self.latitude_delta * self.latitude_unit;
        let dlambda = self.longitude_delta;
        let (sin_latitude, cos_latitude) = self.latitude_sincos(parameter);
        let w2 = 1.0 - e2 * sin_latitude * sin_latitude;
        let w = w2.sqrt();
        let prime_vertical = geodesic.a / w;
        let meridian = geodesic.a * (1.0 - e2) / (w2 * w);
        let east = prime_vertical * cos_latitude * dlambda;
        let north = meridian * dphi;
        let speed = east.hypot(north);
        if speed == 0.0 {
            return ChartEdgeDerivatives {
                speed,
                #[cfg(test)]
                acceleration: 0.0,
                azimuth: 0.0,
            };
        }
        #[cfg(test)]
        let east_derivative = -meridian * sin_latitude * dphi * dlambda;
        #[cfg(test)]
        let north_derivative = dphi * dphi * 3.0 * e2 * sin_latitude * cos_latitude * meridian / w2;
        #[cfg(test)]
        let acceleration = east_derivative.hypot(north_derivative);
        let azimuth = east.atan2(north);
        ChartEdgeDerivatives {
            speed,
            #[cfg(test)]
            acceleration,
            azimuth,
        }
    }

    pub(in crate::crs::geodesic) fn differential_bounds(
        self,
        geodesic: &Geodesic,
    ) -> ChartEdgeDifferentialBounds {
        let one = CertifiedInterval::exact(1.0);
        let flattening = CertifiedInterval::exact(geodesic.f);
        let q = flattening.mul(CertifiedInterval::exact(2.0).sub(flattening));
        let c = one.sub(q);
        let d_min = 1.0_f64.min(c.lower());
        let d_min_root = CertifiedInterval::exact(d_min).sqrt();
        let semi_major = CertifiedInterval::exact(geodesic.a);
        let n_max = semi_major.div(d_min_root).upper();
        let m_max = semi_major
            .mul(CertifiedInterval::exact(c.upper()))
            .div(CertifiedInterval::exact(d_min).mul(d_min_root))
            .upper();
        let q_abs = q.max_abs_upper();
        let latitude_delta = CertifiedInterval::exact(self.end_latitude)
            .sub(CertifiedInterval::exact(self.start_latitude))
            .abs_upper();
        let latitude_rate = CertifiedInterval::exact(latitude_delta)
            .mul(CertifiedInterval::exact(self.latitude_unit.abs()))
            .upper();
        let longitude_rate = if self.longitude_delta_native.is_finite() {
            CertifiedInterval::from_bounds(
                self.longitude_delta_native.next_down(),
                self.longitude_delta_native.next_up(),
            )
            .mul(CertifiedInterval::exact(
                self.frame.longitude().radians_per_unit(),
            ))
            .max_abs_upper()
        } else {
            self.longitude_delta.abs().next_up()
        };
        let east_speed = self.east_speed_upper(n_max, longitude_rate);
        let speed = if self.is_parallel() {
            east_speed
        } else {
            upper_hypot(east_speed, upper_product(m_max, latitude_rate))
        };
        let east_north_acceleration = upper_hypot(
            upper_product(latitude_rate, longitude_rate),
            upper_ratio(
                upper_product(
                    upper_product(3.0, q_abs),
                    upper_product(latitude_rate, latitude_rate),
                ),
                lower_product(2.0, d_min),
            ),
        );
        let acceleration = upper_product(m_max, east_north_acceleration);
        let covariant_acceleration = upper_sum(acceleration, upper_product(speed, longitude_rate));
        ChartEdgeDifferentialBounds {
            speed,
            covariant_acceleration,
        }
    }
}
