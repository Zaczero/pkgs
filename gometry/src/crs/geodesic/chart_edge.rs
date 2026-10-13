//! Shared chart-edge geometry, measures, and arclength parametrization.
//!
//! Longitude direction and explicit-period circles come from
//! `PeriodicAxis::chart_step`; latitude stays in native units until
//! pole-referenced colatitude is formed.

use super::{GeodesicMetric, edge_distance, ellipsoid_metric, envelope, inverse_distance};

pub(super) mod arclength;
pub(super) mod authalic;
mod bounds;
pub(super) mod derivatives;
mod metric;
mod source_interval;
#[cfg(test)]
mod tests;

use derivatives::retained_longitude_radians;
use geographiclib_rs::Geodesic;

use crate::error::Result;
use crate::geographic::{
    ChartStep, CircularInterval, EdgeDirection, GeographicFrame, Latitude, LongitudeWindows,
    RetainedChartDisplacement,
};
use crate::geometry::{
    ChartBox, CoordinateAxes, GeometryErrorKind, HasM, HasZ, MOrdinate, Point, ZOrdinate,
};
use crate::work::{Unmetered, Work};

#[derive(Clone, Copy, Debug)]
pub(crate) struct ChartEdge {
    frame: GeographicFrame,
    meridian: Option<MeridianCoordinates>,
    longitude: RetainedChartDisplacement,
    start_latitude: f64,
    end_latitude: f64,
    latitude_delta: f64,
    longitude_delta_native: f64,
    longitude_delta: f64,
    latitude_unit: f64,
    /// Native chart-pole magnitude; geodetic north/south are frame conversions.
    chart_pole: f64,
}

#[derive(Clone, Copy, Debug)]
struct MeridianCoordinates {
    longitude: f64,
    start_latitude: f64,
    end_latitude: f64,
}

#[derive(Clone, Copy)]
pub(super) struct ChartEdgeDerivatives {
    pub(super) speed: f64,
    #[cfg(test)]
    pub(super) acceleration: f64,
    pub(super) azimuth: f64,
}

impl ChartEdge {
    #[expect(
        clippy::float_cmp,
        reason = "exact chart deltas and the native chart-pole magnitude determine physical degeneracy"
    )]
    pub(crate) fn is_degenerate(&self) -> bool {
        self.latitude_delta == 0.0
            && (self.longitude_delta_native == 0.0 || self.start_latitude.abs() == self.chart_pole)
    }

    pub(crate) fn witness_role(&self, parameter: f64) -> crate::geometry::WitnessRole {
        if !self.is_degenerate() && parameter > 0.0 && parameter < 1.0 {
            crate::geometry::WitnessRole::Interior
        } else {
            crate::geometry::WitnessRole::Point
        }
    }

    pub(crate) fn new(start: Point, end: Point, frame: GeographicFrame) -> Result<Self> {
        let axis = frame.longitude();
        let connection = axis
            .chart_connection(start.x, end.x)
            .expect("chart-edge coordinates were admitted before measurement");
        if let ChartStep::Shortest(EdgeDirection::HalfTurn(direction)) = connection.step {
            return Err(GeometryErrorKind::parameter(
                "longitude",
                EdgeDirection::HalfTurn(direction).delta(axis),
                "edge spans exactly half the longitude period; add a vertex",
            ));
        }
        Ok(Self::from_retained_admitted(
            start,
            end,
            frame,
            connection.retained(),
        ))
    }

    /// Build a metric edge from an already selected source or producer branch.
    /// Unlike `new`, this does not classify the branch from rounded endpoints.
    pub(crate) fn from_retained(
        start: Point,
        end: Point,
        frame: GeographicFrame,
        longitude: RetainedChartDisplacement,
    ) -> Result<Self> {
        let axis = frame.longitude();
        let admitted_start = axis
            .canonicalize(start.x)
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        let admitted_end = axis
            .canonicalize(end.x)
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        if admitted_start != longitude.endpoints()[0] || admitted_end != longitude.endpoints()[1] {
            return Err(GeometryErrorKind::Invalid(
                "retained chart edge endpoints do not match its source records".into(),
            )
            .into());
        }
        Ok(Self::from_retained_admitted(start, end, frame, longitude))
    }

    fn from_retained_admitted(
        start: Point,
        end: Point,
        frame: GeographicFrame,
        longitude: RetainedChartDisplacement,
    ) -> Self {
        let axis = frame.longitude();
        let canonical_start = longitude.endpoints()[0];
        let canonical_end = longitude.endpoints()[1];
        let longitude_delta_native = longitude.delta_native(axis);
        let start_latitude = Latitude::from_admitted(frame, start.y).get();
        let end_latitude = Latitude::from_admitted(frame, end.y).get();
        let latitude_delta = end_latitude - start_latitude;
        let meridian = (longitude_delta_native == 0.0 && latitude_delta != 0.0).then(|| {
            let (longitude, start_latitude) = frame
                .to_geodesic_xy(canonical_start.get(), start_latitude)
                .expect("chart-edge coordinates were admitted before measurement");
            let (_, end_latitude) = frame
                .to_geodesic_xy(canonical_end.get(), end_latitude)
                .expect("chart-edge coordinates were admitted before measurement");
            MeridianCoordinates {
                longitude,
                start_latitude,
                end_latitude,
            }
        });

        Self {
            frame,
            meridian,
            longitude,
            start_latitude,
            end_latitude,
            latitude_delta,
            longitude_delta_native,
            longitude_delta: retained_longitude_radians(
                longitude,
                axis,
                frame.longitude_orientation_sign(),
                longitude_delta_native,
            ),
            latitude_unit: frame.latitude_radians_per_unit() * frame.latitude_orientation_sign(),
            chart_pole: frame.pole().abs(),
        }
    }

    pub(super) fn geodetic_position(self, parameter: f64) -> Result<Point> {
        let axis = self.frame.longitude();
        let longitude = self.retained_longitude();
        let site: crate::geographic::RetainedLongitudeSite =
            axis.native_site_at(&longitude, parameter)?;
        let native = *site.native();
        let latitude = crate::geometry::interpolate_subdivision_ordinate(
            self.start_latitude,
            self.end_latitude,
            parameter,
        );
        let (longitude, latitude) = self
            .frame
            .to_geodesic_xy(native.get(), latitude)
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        Ok(Point::new_raw_xy(longitude, latitude))
    }

    const fn retained_longitude(self) -> RetainedChartDisplacement {
        self.longitude
    }

    #[expect(
        clippy::float_cmp,
        reason = "exact poles and explicit circle edges require a full-longitude bound"
    )]
    pub(crate) fn bounds(self) -> ChartBox {
        let [source_start, source_end] = *self.longitude.endpoints();
        let (start_longitude, start_latitude) = self
            .frame
            .to_geodesic_xy(source_start.get(), self.start_latitude)
            .expect("chart-edge endpoints were admitted");
        let (end_longitude, end_latitude) = self
            .frame
            .to_geodesic_xy(source_end.get(), self.end_latitude)
            .expect("chart-edge endpoints were admitted");
        let south = start_latitude.min(end_latitude).next_down().max(-90.0);
        let north = start_latitude.max(end_latitude).next_up().min(90.0);
        let axis = GeographicFrame::crs84().longitude();
        // `ChartEdge::new` stores the selected chart-step delta. A Circle step
        // spans one full period; shortest steps never exceed half a period.
        // Canonicalizing both endpoints would erase that distinction.
        let circle = self.longitude_delta_native.abs() > self.frame.longitude().half_period();
        let longitude_delta = self.longitude_delta.to_degrees();
        let interval = if start_latitude.abs() == 90.0 || end_latitude.abs() == 90.0 || circle {
            CircularInterval::Full
        } else {
            let (west, east) = if longitude_delta < 0.0 {
                (end_longitude, start_longitude)
            } else {
                (start_longitude, end_longitude)
            };
            CircularInterval::from_endpoints(
                axis.canonicalize(west)
                    .expect("chart-edge endpoint is an admitted longitude"),
                axis.canonicalize(east)
                    .expect("chart-edge endpoint is an admitted longitude"),
            )
        };
        let (west, east) = match interval.windows(axis) {
            Some(
                LongitudeWindows::One([west, east]) | LongitudeWindows::Two([west, _], [_, east]),
            ) => [west, east].into(),
            Some(LongitudeWindows::Full) | None => (-180.0, 180.0),
        };
        ChartBox {
            west,
            south,
            east,
            north,
        }
    }

    pub(super) fn witness(
        self,
        start: Point,
        end: Point,
        parameter: f64,
        total_length: Option<f64>,
        geodesic: &Geodesic,
    ) -> Result<Point> {
        self.witness_with_work(
            start,
            end,
            parameter,
            total_length,
            geodesic,
            &mut Unmetered,
        )
    }

    #[expect(
        clippy::float_cmp,
        reason = "exact minimizer endpoint parameters preserve input coordinates and ordinates"
    )]
    fn witness_with_work(
        self,
        start: Point,
        end: Point,
        parameter: f64,
        total_length: Option<f64>,
        geodesic: &Geodesic,
        work: &mut impl Work,
    ) -> Result<Point> {
        if parameter == 0.0 {
            return Ok(start);
        }
        if parameter == 1.0 {
            return Ok(end);
        }
        let axes = CoordinateAxes::new(
            HasZ(start.z().is_some() && end.z().is_some()),
            HasM(start.m().is_some() && end.m().is_some()),
        );
        let fraction = if axes.has_z() || axes.has_m() {
            let total = total_length.map_or_else(|| self.length_with_work(geodesic, work), Ok)?;
            Some(self.arclength_fraction_with_work(parameter, Some(total), geodesic, work)?)
        } else {
            None
        };
        self.witness_interior(start, end, parameter, axes, fraction)
    }

    fn witness_interior(
        self,
        start: Point,
        end: Point,
        parameter: f64,
        axes: CoordinateAxes,
        fraction: Option<f64>,
    ) -> Result<Point> {
        let position = self.chart_position(parameter)?;
        let (z, m) = fraction.map_or((ZOrdinate(None), MOrdinate(None)), |fraction| {
            crate::geometry::source_ordinate_values_at_fraction(start, end, axes, fraction)
        });
        let point = Point::new_axes(position.x, position.y, z, m)
            .expect("an admitted chart-edge witness is finite");
        Ok(self
            .frame
            .canonical_computed_point(point)
            .expect("an admitted chart-edge witness is finite"))
    }
}
