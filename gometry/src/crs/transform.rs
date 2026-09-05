#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! The public coordinate-transform API (`transform`/`transform_coordinates`/
//! `transform_bounds`/`transform_bounds_3d`).
//!
//! These take `&Shape`/`&str`/`&[f64]` and drive the FFI pipeline engine and
//! raw-pointer helpers (which stay private in the parent `crs` module) via
//! `use super::*`; re-exported at `crs` so `crs::transform` callers resolve.

use crate::geometry::types;
use std::ops::Range;
use std::sync::Arc;

use crate::crs::{
    CoordSeq, CrsError, GeometryErrorKind, Point, ProjDirection, ProjPipeline, RuntimeState, Shape,
    TRANSFORM_BOUNDS_MAX_DENSIFY, TransformExecution, TransformFailure, TransformOptions,
    TransformPlan, info_with_state, proj_error_message, runtime_state_snapshot,
    transform_in_core_xy_batch, transform_in_core_xy_batch_indexed, with_proj_operation_at,
};
use crate::error::Result;
use crate::geographic::{CircularInterval, GeographicError, GeographicFrame, Latitude};
use crate::geometry::{CoordWindow, LineSeq, MOrdinate, TopologyDomain, ZOrdinate};

pub(crate) type TransformedBounds = (f64, f64, f64, f64);
/// One transformed 3D bounds row: (minx, miny, minz, maxx, maxy, maxz).
pub(crate) type TransformedBounds3d = (f64, f64, f64, f64, f64, f64);

/// Backend-neutral edge samples for one bounds row. Geographic source windows
/// are split at their native seam in visualization axis order, so neither
/// execution backend has to interpret periodic topology.
pub(super) struct BoundsSamplePlan {
    pub(super) x: Vec<f64>,
    pub(super) y: Vec<f64>,
    pub(super) z: Option<Vec<f64>>,
    pub(super) epoch: Option<[f64; 1]>,
    source_windows: Vec<[f64; 2]>,
    source_latitude_bounds: [f64; 2],
    source_domain: TopologyDomain,
    target_domain: TopologyDomain,
    source_full_longitude: bool,
    target_poles: [bool; 2],
}

impl BoundsSamplePlan {
    pub(super) fn new_2d(
        bounds: (f64, f64, f64, f64),
        densify: u32,
        source_domain: TopologyDomain,
        target_domain: TopologyDomain,
        epoch: Option<f64>,
    ) -> Result<Self> {
        let (windows, source_full_longitude) = source_windows(bounds.0, bounds.2, source_domain)?;
        let source_latitude_bounds =
            admitted_source_latitude_bounds(source_domain, bounds.1, bounds.3)?;
        let edge_points = densify as usize + 2;
        let capacity = windows
            .len()
            .checked_mul(4)
            .and_then(|value| value.checked_mul(edge_points))
            .ok_or_else(|| GeometryErrorKind::message("bounds sample count overflows usize"))?;
        let mut x = Vec::new();
        let mut y = Vec::new();
        crate::try_reserve_vec(&mut x, capacity)?;
        crate::try_reserve_vec(&mut y, capacity)?;
        for [west, east] in windows.iter().copied() {
            let (south, north) = (bounds.1, bounds.3);
            push_edge_2d(&mut x, &mut y, (west, south), (east, south), edge_points);
            push_edge_2d(&mut x, &mut y, (east, south), (east, north), edge_points);
            push_edge_2d(&mut x, &mut y, (east, north), (west, north), edge_points);
            push_edge_2d(&mut x, &mut y, (west, north), (west, south), edge_points);
        }
        Ok(Self {
            x,
            y,
            z: None,
            epoch: epoch.map(|value| [value]),
            source_windows: windows,
            source_latitude_bounds,
            source_domain,
            target_domain,
            source_full_longitude,
            target_poles: [false; 2],
        })
    }

    pub(super) fn new_3d(
        bounds: TransformedBounds3d,
        densify: u32,
        source_domain: TopologyDomain,
        target_domain: TopologyDomain,
        epoch: Option<f64>,
    ) -> Result<Self> {
        let (windows, source_full_longitude) = source_windows(bounds.0, bounds.3, source_domain)?;
        let source_latitude_bounds =
            admitted_source_latitude_bounds(source_domain, bounds.1, bounds.4)?;
        let edge_points = densify as usize + 2;
        let capacity = windows
            .len()
            .checked_mul(12)
            .and_then(|value| value.checked_mul(edge_points))
            .ok_or_else(|| GeometryErrorKind::message("bounds sample count overflows usize"))?;
        let mut x = Vec::new();
        let mut y = Vec::new();
        let mut z = Vec::new();
        crate::try_reserve_vec(&mut x, capacity)?;
        crate::try_reserve_vec(&mut y, capacity)?;
        crate::try_reserve_vec(&mut z, capacity)?;
        for [west, east] in windows.iter().copied() {
            let (south, north) = (bounds.1, bounds.4);
            for (x0, x1, y0, y1, z0, z1) in [
                (west, east, south, south, bounds.2, bounds.2),
                (west, east, south, south, bounds.5, bounds.5),
                (west, east, north, north, bounds.2, bounds.2),
                (west, east, north, north, bounds.5, bounds.5),
                (west, west, south, north, bounds.2, bounds.2),
                (west, west, south, north, bounds.5, bounds.5),
                (east, east, south, north, bounds.2, bounds.2),
                (east, east, south, north, bounds.5, bounds.5),
                (west, west, south, south, bounds.2, bounds.5),
                (west, west, north, north, bounds.2, bounds.5),
                (east, east, south, south, bounds.2, bounds.5),
                (east, east, north, north, bounds.2, bounds.5),
            ] {
                push_edge_3d(
                    &mut x,
                    &mut y,
                    &mut z,
                    (x0, y0, z0),
                    (x1, y1, z1),
                    edge_points,
                );
            }
        }
        Ok(Self {
            x,
            y,
            z: Some(z),
            epoch: epoch.map(|value| [value]),
            source_windows: windows,
            source_latitude_bounds,
            source_domain,
            target_domain,
            source_full_longitude,
            target_poles: [false; 2],
        })
    }

    pub(super) fn include_target_poles(
        &mut self,
        mut inverse: impl FnMut(f64, f64) -> Result<Option<(f64, f64)>>,
    ) -> Result<()> {
        // An operation may have no inverse at one physical pole (for example,
        // a north-polar projection at the south pole). The executor reports
        // that domain miss as `None`; all actual execution failures remain
        // errors so they cannot silently erase a contained pole.
        let TopologyDomain::Geographic(frame) = self.target_domain else {
            return Ok(());
        };
        for (index, latitude) in [(1, frame.pole()), (0, -frame.pole())] {
            let Some((longitude, latitude)) = inverse(0.0, latitude)? else {
                continue;
            };
            if self.source_contains(longitude, latitude)? {
                self.target_poles[index] = true;
            }
        }
        Ok(())
    }

    fn source_contains(&self, longitude: f64, latitude: f64) -> Result<bool> {
        if !longitude.is_finite() || !latitude.is_finite() {
            return Err(GeometryErrorKind::projection(
                "inverse target-pole transform returned a non-finite coordinate",
            ));
        }
        let latitude = match self.source_domain {
            TopologyDomain::Cartesian => latitude,
            TopologyDomain::Geographic(frame) => Latitude::try_new(frame, latitude)
                .map_err(map_geographic_conversion_error)?
                .get(),
        };
        if !(self.source_latitude_bounds[0] < latitude && latitude < self.source_latitude_bounds[1])
        {
            return Ok(false);
        }
        let longitude = match self.source_domain {
            TopologyDomain::Cartesian => longitude,
            TopologyDomain::Geographic(frame) => frame
                .longitude()
                .canonicalize(longitude)
                .map_err(map_geographic_conversion_error)?
                .get(),
        };
        Ok(self
            .source_windows
            .iter()
            .any(|[west, east]| *west < longitude && longitude < *east))
    }

    pub(super) fn finish_2d(mut self) -> Result<(f64, f64, f64, f64)> {
        let horizontal = accumulate_horizontal(
            &mut self.x,
            &self.y,
            self.target_domain,
            self.source_full_longitude,
            self.target_poles,
        )?;
        Ok(horizontal.into())
    }

    pub(super) fn finish_3d(mut self) -> Result<TransformedBounds3d> {
        let horizontal = accumulate_horizontal(
            &mut self.x,
            &self.y,
            self.target_domain,
            self.source_full_longitude,
            self.target_poles,
        )?;
        let z = self.z.expect("3D bounds plan retains its z lane");
        let (minz, maxz) = ordered_extrema(&z);
        Ok((
            horizontal[0],
            horizontal[1],
            minz,
            horizontal[2],
            horizontal[3],
            maxz,
        ))
    }

    pub(super) fn enter_in_core(&mut self) -> Result<()> {
        if let TopologyDomain::Geographic(frame) = self.source_domain
            && frame != GeographicFrame::crs84()
        {
            frame
                .to_geodesic_lanes(&mut self.x, &mut self.y)
                .map_err(map_geographic_conversion_error)?;
        }
        Ok(())
    }

    pub(super) fn leave_in_core(&mut self) -> Result<()> {
        if let TopologyDomain::Geographic(frame) = self.target_domain {
            frame
                .lanes_from_geodesic(&mut self.x, &mut self.y)
                .map_err(map_geographic_conversion_error)?;
        }
        Ok(())
    }
}

pub(super) fn transform_in_core_xy_with_domains(
    operation: &crate::crs::InCoreTransform,
    source_domain: TopologyDomain,
    target_domain: TopologyDomain,
    x: &mut [f64],
    y: &mut [f64],
) -> Result<()> {
    if let TopologyDomain::Geographic(frame) = source_domain
        && frame != GeographicFrame::crs84()
    {
        frame
            .to_geodesic_lanes(x, y)
            .map_err(map_geographic_conversion_error)?;
    }
    transform_in_core_xy_batch(operation, x, y)?;
    if let TopologyDomain::Geographic(frame) = target_domain {
        frame
            .lanes_from_geodesic(x, y)
            .map_err(map_geographic_conversion_error)?;
    }
    Ok(())
}

pub(super) fn transform_in_core_xy_with_domains_indexed(
    operation: &crate::crs::InCoreTransform,
    source_domain: TopologyDomain,
    target_domain: TopologyDomain,
    x: &mut [f64],
    y: &mut [f64],
) -> std::result::Result<(), TransformFailure> {
    if let TopologyDomain::Geographic(frame) = source_domain
        && frame != GeographicFrame::crs84()
    {
        frame
            .to_geodesic_lanes_indexed(x, y)
            .map_err(|(coordinate, error)| {
                TransformFailure::at(map_geographic_conversion_error(error), coordinate)
            })?;
    }
    transform_in_core_xy_batch_indexed(operation, x, y)?;
    if let TopologyDomain::Geographic(frame) = target_domain {
        frame
            .lanes_indexed_from_geodesic(x, y)
            .map_err(|(coordinate, error)| {
                TransformFailure::at(map_geographic_conversion_error(error), coordinate)
            })?;
    }
    Ok(())
}

fn map_geographic_conversion_error(error: GeographicError) -> crate::error::Error {
    match error {
        GeographicError::LatitudeOutOfRange { value } => GeometryErrorKind::projection(format!(
            "latitude {value:e} is outside the geographic coordinate domain"
        )),
        GeographicError::NonFinite { .. } => GeometryErrorKind::NonFiniteCoordinate.into(),
        other => CrsError::invalid(other.to_string()),
    }
}

fn source_windows(west: f64, east: f64, domain: TopologyDomain) -> Result<(Vec<[f64; 2]>, bool)> {
    let TopologyDomain::Geographic(frame) = domain else {
        let mut windows = Vec::new();
        crate::try_reserve_vec(&mut windows, 1)?;
        windows.push([west, east]);
        return Ok((windows, false));
    };
    let axis = frame.longitude();
    let raw_span = east - west;
    let full = !raw_span.is_finite() || raw_span.abs() >= axis.period();
    let west = axis
        .canonicalize(west)
        .map_err(|error| CrsError::invalid(error.to_string()))?
        .get();
    let east = axis
        .canonicalize(east)
        .map_err(|error| CrsError::invalid(error.to_string()))?
        .get();
    let mut windows = Vec::new();
    crate::try_reserve_vec(&mut windows, 2)?;
    if full {
        windows.push([-axis.half_period(), axis.half_period()]);
        return Ok((windows, true));
    }
    if west <= east {
        windows.push([west, east]);
    } else {
        windows.push([west, axis.half_period()]);
        windows.push([-axis.half_period(), east]);
    }
    Ok((windows, false))
}

pub(super) fn canonical_identity_longitudes(
    west: f64,
    east: f64,
    frame: GeographicFrame,
) -> Result<(f64, f64)> {
    let axis = frame.longitude();
    let raw_span = east - west;
    if !raw_span.is_finite() || raw_span.abs() >= axis.period() {
        return Ok((-axis.half_period(), axis.half_period()));
    }
    let canonical = |value| {
        axis.canonicalize(value)
            .map(|value| value.get())
            .map_err(|error| CrsError::invalid(error.to_string()))
    };
    Ok((canonical(west)?, canonical(east)?))
}

pub(super) fn canonical_identity_latitudes(
    south: f64,
    north: f64,
    frame: GeographicFrame,
) -> Result<[f64; 2]> {
    Ok([
        Latitude::try_new(frame, south)
            .map_err(map_geographic_conversion_error)?
            .get(),
        Latitude::try_new(frame, north)
            .map_err(map_geographic_conversion_error)?
            .get(),
    ])
}

fn admitted_source_latitude_bounds(
    domain: TopologyDomain,
    south: f64,
    north: f64,
) -> Result<[f64; 2]> {
    match domain {
        TopologyDomain::Cartesian => Ok([south, north]),
        TopologyDomain::Geographic(frame) => Ok([
            Latitude::try_new(frame, south)
                .map_err(map_geographic_conversion_error)?
                .get(),
            Latitude::try_new(frame, north)
                .map_err(map_geographic_conversion_error)?
                .get(),
        ]),
    }
}

fn push_edge_2d(
    x: &mut Vec<f64>,
    y: &mut Vec<f64>,
    start: (f64, f64),
    end: (f64, f64),
    points: usize,
) {
    for index in 0..points {
        let fraction = index as f64 / (points - 1) as f64;
        x.push(interpolate(start.0, end.0, fraction));
        y.push(interpolate(start.1, end.1, fraction));
    }
}

fn push_edge_3d(
    x: &mut Vec<f64>,
    y: &mut Vec<f64>,
    z: &mut Vec<f64>,
    start: (f64, f64, f64),
    end: (f64, f64, f64),
    points: usize,
) {
    for index in 0..points {
        let fraction = index as f64 / (points - 1) as f64;
        x.push(interpolate(start.0, end.0, fraction));
        y.push(interpolate(start.1, end.1, fraction));
        z.push(interpolate(start.2, end.2, fraction));
    }
}

fn interpolate(start: f64, end: f64, fraction: f64) -> f64 {
    start * (1.0 - fraction) + end * fraction
}

fn accumulate_horizontal(
    x: &mut [f64],
    y: &[f64],
    target_domain: TopologyDomain,
    source_full_longitude: bool,
    target_poles: [bool; 2],
) -> Result<[f64; 4]> {
    let TopologyDomain::Geographic(frame) = target_domain else {
        let (minx, maxx) = ordered_extrema(x);
        let (miny, maxy) = ordered_extrema(y);
        return Ok([minx, miny, maxx, maxy]);
    };
    let axis = frame.longitude();
    let mut south = f64::INFINITY;
    let mut north = f64::NEG_INFINITY;
    for (longitude, &latitude) in x.iter_mut().zip(y) {
        let latitude = Latitude::try_new(frame, latitude)
            .map_err(|error| CrsError::invalid(error.to_string()))?;
        south = south.min(latitude.get());
        north = north.max(latitude.get());
        *longitude = axis
            .canonicalize(*longitude)
            .map_err(|error| CrsError::invalid(error.to_string()))?
            .get();
    }
    if target_poles[0] {
        south = -frame.pole();
    }
    if target_poles[1] {
        north = frame.pole();
    }
    let interval = if source_full_longitude || target_poles.into_iter().any(|pole| pole) {
        CircularInterval::Full
    } else {
        x.sort_unstable_by(f64::total_cmp);
        let mut best_gap = -1.0;
        let mut best_start = 0.0;
        for index in 0..x.len() {
            let (gap, start) = if index + 1 == x.len() {
                ((axis.period() - x[index]) + x[0], x[0])
            } else {
                (x[index + 1] - x[index], x[index + 1])
            };
            if gap > best_gap || (gap.total_cmp(&best_gap).is_eq() && start < best_start) {
                best_gap = gap;
                best_start = start;
            }
        }
        CircularInterval::try_arc(axis, best_start, axis.period() - best_gap)
            .map_err(|error| CrsError::invalid(error.to_string()))?
    };
    let (west, east) = match interval {
        CircularInterval::Full => (-axis.half_period(), axis.half_period()),
        CircularInterval::Arc { .. } => interval
            .west_east(axis)
            .expect("non-empty arc has public endpoints"),
        CircularInterval::Empty => unreachable!("bounds sample plan is non-empty"),
    };
    Ok([west, south, east, north])
}

fn ordered_extrema(values: &[f64]) -> (f64, f64) {
    let mut low = values[0];
    let mut high = values[0];
    for &value in &values[1..] {
        low = low.min(value);
        high = high.max(value);
    }
    (low, high)
}

#[cfg(test)]
mod bounds_plan_tests {
    use super::*;
    use crate::geographic::AngularFactor;

    fn frame(radians_per_unit: f64, prime_meridian: f64) -> GeographicFrame {
        GeographicFrame::try_new(
            AngularFactor::try_new("longitude", radians_per_unit).unwrap(),
            AngularFactor::try_new("latitude", radians_per_unit).unwrap(),
            prime_meridian,
        )
        .unwrap()
    }

    #[test]
    fn wrapped_source_is_split_into_two_bounded_backend_windows() {
        let degrees = frame(std::f64::consts::PI / 180.0, 0.0);
        let samples = BoundsSamplePlan::new_2d(
            (170.0, -10.0, -170.0, 10.0),
            3,
            TopologyDomain::Geographic(degrees),
            TopologyDomain::Cartesian,
            None,
        )
        .unwrap();
        assert_eq!(samples.x.len(), 2 * 4 * (3 + 2));
        assert!(
            samples
                .x
                .iter()
                .all(|value| (-180.0..=180.0).contains(value))
        );
        assert!(samples.x.contains(&-180.0));
        assert!(samples.x.contains(&180.0));
    }

    #[test]
    fn full_unwrapped_band_stays_full_in_target_native_units() {
        let grads = frame(std::f64::consts::PI / 200.0, 0.0);
        let samples = BoundsSamplePlan::new_2d(
            (0.0, -100.0, 400.0, 100.0),
            2,
            TopologyDomain::Geographic(grads),
            TopologyDomain::Geographic(grads),
            None,
        )
        .unwrap();
        assert_eq!(
            samples.finish_2d().unwrap(),
            (
                -grads.longitude().half_period(),
                -grads.pole(),
                grads.longitude().half_period(),
                grads.pole(),
            )
        );
    }

    #[test]
    fn geographic_target_uses_smallest_closed_periodic_cover() {
        let radians = frame(1.0, 2.0_f64.to_radians());
        let mut samples = BoundsSamplePlan::new_2d(
            (0.0, 0.0, 1.0, 1.0),
            2,
            TopologyDomain::Cartesian,
            TopologyDomain::Geographic(radians),
            None,
        )
        .unwrap();
        for (index, value) in samples.x.iter_mut().enumerate() {
            *value = if index % 2 == 0 {
                179.0_f64.to_radians()
            } else {
                (-179.0_f64).to_radians()
            };
        }
        samples.y.fill(45.0_f64.to_radians());
        let bounds = samples.finish_2d().unwrap();
        assert_eq!(bounds.0, 179.0_f64.to_radians());
        assert_eq!(bounds.2, (-179.0_f64).to_radians());
        assert_eq!(bounds.1, 45.0_f64.to_radians());
        assert_eq!(bounds.3, 45.0_f64.to_radians());
    }

    #[test]
    fn target_pole_flags_use_strict_split_source_containment() {
        let degrees = frame(std::f64::consts::PI / 180.0, 0.0);
        let mut samples = BoundsSamplePlan::new_2d(
            (170.0, -1.0, -170.0, 1.0),
            2,
            TopologyDomain::Geographic(degrees),
            TopologyDomain::Geographic(degrees),
            None,
        )
        .unwrap();
        samples
            .include_target_poles(|_, target_latitude| {
                Ok(Some((
                    if target_latitude > 0.0 { 175.0 } else { -175.0 },
                    0.0,
                )))
            })
            .unwrap();
        assert_eq!(samples.target_poles, [true, true]);

        let mut boundary = BoundsSamplePlan::new_2d(
            (0.0, -1.0, 10.0, 1.0),
            2,
            TopologyDomain::Geographic(degrees),
            TopologyDomain::Geographic(degrees),
            None,
        )
        .unwrap();
        boundary
            .include_target_poles(|_, _| Ok(Some((0.0, 0.0))))
            .unwrap();
        assert_eq!(boundary.target_poles, [false, false]);

        let mut admitted_boundary = BoundsSamplePlan::new_2d(
            (0.0, -1.0, 10.0, degrees.pole().next_up()),
            2,
            TopologyDomain::Geographic(degrees),
            TopologyDomain::Geographic(degrees),
            None,
        )
        .unwrap();
        admitted_boundary
            .include_target_poles(|_, _| Ok(Some((5.0, degrees.pole()))))
            .unwrap();
        assert_eq!(admitted_boundary.target_poles, [false, false]);
    }

    #[test]
    fn target_pole_probe_errors_are_not_treated_as_absent_poles() {
        let degrees = frame(std::f64::consts::PI / 180.0, 0.0);
        let mut samples = BoundsSamplePlan::new_2d(
            (-1.0, -1.0, 1.0, 1.0),
            2,
            TopologyDomain::Cartesian,
            TopologyDomain::Geographic(degrees),
            None,
        )
        .unwrap();
        let error = samples.include_target_poles(|_, _| {
            Err(GeometryErrorKind::projection("target pole probe failed"))
        });
        assert!(error.is_err());
    }

    #[test]
    fn target_pole_clamping_preserves_three_dimensional_vertical_extrema() {
        let degrees = frame(std::f64::consts::PI / 180.0, 0.0);
        let mut samples = BoundsSamplePlan::new_3d(
            (-1.0, -1.0, 3.0, 1.0, 1.0, 7.0),
            2,
            TopologyDomain::Cartesian,
            TopologyDomain::Geographic(degrees),
            None,
        )
        .unwrap();
        samples
            .include_target_poles(|_, _| Ok(Some((0.0, 0.0))))
            .unwrap();
        assert_eq!(
            samples.finish_3d().unwrap(),
            (-180.0, -90.0, 3.0, 180.0, 90.0, 7.0)
        );
    }

    #[test]
    fn xyz_and_epoch_plan_share_one_lane_cardinality() {
        let samples = BoundsSamplePlan::new_3d(
            (-1.0, -2.0, 3.0, 4.0, 5.0, 6.0),
            7,
            TopologyDomain::Cartesian,
            TopologyDomain::Cartesian,
            Some(2020.0),
        )
        .unwrap();
        assert_eq!(samples.x.len(), 12 * (7 + 2));
        assert_eq!(samples.x.len(), samples.y.len());
        assert_eq!(samples.x.len(), samples.z.as_ref().unwrap().len());
        assert_eq!(samples.epoch, Some([2020.0]));
    }
}

pub(crate) fn transform(shape: &Shape, source: &str, target: &str) -> Result<Shape> {
    let state = runtime_state_snapshot()?;
    transform_at(shape, source, target, &state)
}

pub(crate) fn transform_at(
    shape: &Shape,
    source: &str,
    target: &str,
    state: &RuntimeState,
) -> Result<Shape> {
    TransformPlan::new_with_options_at(
        source,
        target,
        TransformOptions::default(),
        std::sync::Arc::new(state.clone()),
    )?
    .transform_shape(shape)
}

pub(crate) fn transform_coordinates(
    source: &str,
    target: &str,
    x: &mut [f64],
    y: &mut [f64],
    zt: crate::ZtLanes<'_>,
    options: TransformOptions,
) -> Result<TransformExecution> {
    let state = runtime_state_snapshot()?;
    let plan = TransformPlan::new_with_options_at(source, target, options, state)?;
    plan.transform_coordinates(x, y, zt)?;
    Ok(plan.execution())
}

fn is_geographic_crs_with_state(value: &str, state: &RuntimeState) -> Result<bool> {
    Ok(info_with_state(value, state)?.geographic_frame().is_some())
}

/// Shared densify gate for the bounds transforms: capped, and at least 2 when
/// the output CRS is geographic (PROJ needs edge samples to track the
/// antimeridian).
fn validate_bounds_densify(target: &str, densify: u32, state: &RuntimeState) -> Result<()> {
    if densify > TRANSFORM_BOUNDS_MAX_DENSIFY {
        return Err(CrsError::invalid(format!(
            "bounds densify must be <= {TRANSFORM_BOUNDS_MAX_DENSIFY}"
        )));
    }
    if densify < 2 && is_geographic_crs_with_state(target, state)? {
        return Err(CrsError::invalid(
            "bounds densify must be at least 2 when the output CRS is geographic".to_owned(),
        ));
    }
    Ok(())
}

trait BoundsTransform: Copy {
    fn validate(source: &str, bounds: Self, state: &RuntimeState) -> Result<()>;
    fn transform_one(transformer: &TransformPlan, bounds: Self, densify: u32) -> Result<Self>;
    fn transform_many(
        transformer: &TransformPlan,
        rows: &[Self],
        densify: u32,
    ) -> Result<Vec<Self>>;
}

impl BoundsTransform for (f64, f64, f64, f64) {
    fn validate(source: &str, bounds: Self, state: &RuntimeState) -> Result<()> {
        validate_transform_bounds_with_state(source, bounds, state)
    }

    fn transform_one(transformer: &TransformPlan, bounds: Self, densify: u32) -> Result<Self> {
        transformer.transform_bounds(bounds, densify)
    }

    fn transform_many(
        transformer: &TransformPlan,
        rows: &[Self],
        densify: u32,
    ) -> Result<Vec<Self>> {
        transformer.transform_bounds_many(rows, densify)
    }
}

impl BoundsTransform for TransformedBounds3d {
    fn validate(source: &str, bounds: Self, state: &RuntimeState) -> Result<()> {
        validate_transform_bounds_3d_with_state(source, bounds, state)
    }

    fn transform_one(transformer: &TransformPlan, bounds: Self, densify: u32) -> Result<Self> {
        transformer.transform_bounds_3d(bounds, densify)
    }

    fn transform_many(
        transformer: &TransformPlan,
        rows: &[Self],
        densify: u32,
    ) -> Result<Vec<Self>> {
        transformer.transform_bounds_3d_many(rows, densify)
    }
}

fn transform_bounds_row<B: BoundsTransform>(
    source: &str,
    target: &str,
    bounds: B,
    densify: u32,
    options: TransformOptions,
    state: std::sync::Arc<RuntimeState>,
) -> Result<(B, TransformExecution)> {
    B::validate(source, bounds, &state)?;
    let validation_state = std::sync::Arc::clone(&state);
    let plan = TransformPlan::new_with_options_at(source, target, options, state)?;
    if plan.is_identity() {
        let transformed = B::transform_one(&plan, bounds, densify)?;
        return Ok((transformed, plan.execution()));
    }
    validate_bounds_densify(target, densify, &validation_state)?;
    let transformed = B::transform_one(&plan, bounds, densify)?;
    Ok((transformed, plan.execution()))
}

fn transform_bounds_rows<B: BoundsTransform>(
    source: &str,
    target: &str,
    rows: &[B],
    densify: u32,
    options: TransformOptions,
    state: std::sync::Arc<RuntimeState>,
) -> Result<(Vec<B>, TransformExecution)> {
    for &bounds in rows {
        B::validate(source, bounds, &state)?;
    }
    let validation_state = std::sync::Arc::clone(&state);
    let plan = TransformPlan::new_with_options_at(source, target, options, state)?;
    if plan.is_identity() {
        let transformed = B::transform_many(&plan, rows, densify)?;
        return Ok((transformed, plan.execution()));
    }
    validate_bounds_densify(target, densify, &validation_state)?;
    let transformed = B::transform_many(&plan, rows, densify)?;
    Ok((transformed, plan.execution()))
}

pub(crate) fn transform_bounds(
    source: &str,
    target: &str,
    bounds: (f64, f64, f64, f64),
    densify: u32,
    options: TransformOptions,
) -> Result<((f64, f64, f64, f64), TransformExecution)> {
    let state = runtime_state_snapshot()?;
    transform_bounds_row(source, target, bounds, densify, options, state)
}

pub(crate) fn transform_bounds_many(
    source: &str,
    target: &str,
    rows: &[(f64, f64, f64, f64)],
    densify: u32,
    options: TransformOptions,
) -> Result<(Vec<TransformedBounds>, TransformExecution)> {
    let state = runtime_state_snapshot()?;
    transform_bounds_rows(source, target, rows, densify, options, state)
}

pub(crate) fn transform_bounds_3d(
    source: &str,
    target: &str,
    bounds: (f64, f64, f64, f64, f64, f64),
    densify: u32,
    options: TransformOptions,
) -> Result<(TransformedBounds3d, TransformExecution)> {
    let state = runtime_state_snapshot()?;
    transform_bounds_row(source, target, bounds, densify, options, state)
}

pub(crate) fn transform_bounds_3d_many(
    source: &str,
    target: &str,
    rows: &[(f64, f64, f64, f64, f64, f64)],
    densify: u32,
    options: TransformOptions,
) -> Result<(Vec<TransformedBounds3d>, TransformExecution)> {
    let state = runtime_state_snapshot()?;
    transform_bounds_rows(source, target, rows, densify, options, state)
}

pub(crate) fn apply_operation(
    definition: &str,
    direction: ProjDirection,
    x: &mut [f64],
    y: &mut [f64],
    zt: crate::ZtLanes<'_>,
) -> Result<TransformExecution> {
    let definition = definition.trim();
    if definition.is_empty() {
        return Err(CrsError::invalid(
            "operation definition is required".to_owned(),
        ));
    }
    let state = runtime_state_snapshot()?;
    let execution = TransformExecution::default();
    with_proj_operation_at(definition, &state, &execution, |operation| {
        transform_coordinates_with_pipeline(operation, direction, x, y, zt, &execution)
    })?;
    Ok(execution)
}

pub(crate) fn parse_epsg(value: &str) -> Option<u32> {
    let (authority, code) = value.split_once(':')?;
    authority
        .eq_ignore_ascii_case("EPSG")
        .then(|| code.parse().ok())
        .flatten()
}

pub(crate) fn is_wgs84_lonlat(value: &str) -> bool {
    matches!(parse_epsg(value), Some(4326 | 4979))
}

pub(crate) fn validate_coordinate_slices(
    x: &[f64],
    y: &[f64],
    z: Option<&[f64]>,
    t: Option<&[f64]>,
) -> Result<()> {
    if let Some(other) = [Some(y.len()), z.map(<[f64]>::len), t.map(<[f64]>::len)]
        .into_iter()
        .flatten()
        .find(|&len| len != x.len())
    {
        return Err(GeometryErrorKind::CoordinateLength(x.len(), other).into());
    }
    if crate::geometry::column_all_finite(x)
        && crate::geometry::column_all_finite(y)
        && z.is_none_or(crate::geometry::column_all_finite)
        && t.is_none_or(crate::geometry::column_all_finite)
    {
        Ok(())
    } else {
        Err(GeometryErrorKind::NonFiniteCoordinate.into())
    }
}

pub(crate) fn validate_coordinate_lanes(
    x: &[f64],
    y: &[f64],
    zt: crate::ZtLaneRefs<'_>,
) -> Result<()> {
    match zt {
        crate::Zt::None => validate_coordinate_slices(x, y, None, None),
        crate::Zt::Z(z) => validate_coordinate_slices(x, y, Some(z), None),
        crate::Zt::T(t) => validate_coordinate_slices(x, y, None, Some(t)),
        crate::Zt::Zt { z, t } => validate_coordinate_slices(x, y, Some(z), Some(t)),
    }
}

pub(crate) fn coordinates_are_finite(
    x: &[f64],
    y: &[f64],
    z: Option<&[f64]>,
    t: Option<&[f64]>,
) -> bool {
    crate::geometry::column_all_finite(x)
        && crate::geometry::column_all_finite(y)
        && z.is_none_or(crate::geometry::column_all_finite)
        && t.is_none_or(crate::geometry::column_all_finite)
}

pub(crate) fn proj_transform_error(error: i32) -> Box<str> {
    if error == 0 {
        "projection returned a non-finite coordinate".into()
    } else {
        proj_error_message(error).into()
    }
}

pub(crate) const BOUNDS_2D_ERROR: &str =
    "bounds must be finite (minx, miny, maxx, maxy) with min <= max";
pub(crate) const BOUNDS_2D_GEO_ERROR: &str = "bounds must be finite (minx, miny, maxx, maxy) with min <= max, except antimeridian-crossing geographic bounds may use west > east";
pub(crate) const BOUNDS_3D_ERROR: &str =
    "bounds must be finite (minx, miny, minz, maxx, maxy, maxz) with min <= max";
pub(crate) const BOUNDS_3D_GEO_ERROR: &str = "bounds must be finite (minx, miny, minz, maxx, maxy, maxz) with min <= max, except antimeridian-crossing geographic bounds may use west > east";

pub(crate) fn all_finite(values: &[f64]) -> bool {
    values.iter().all(|value| value.is_finite())
}

pub(crate) fn validate_bounds(bounds: (f64, f64, f64, f64)) -> Result<()> {
    if all_finite(&<[f64; 4]>::from(bounds)) && bounds.0 <= bounds.2 && bounds.1 <= bounds.3 {
        return Ok(());
    }
    Err(CrsError::invalid(BOUNDS_2D_ERROR.to_owned()))
}

fn validate_transform_bounds_with_state(
    source: &str,
    bounds: (f64, f64, f64, f64),
    state: &RuntimeState,
) -> Result<()> {
    if let Some(frame) = info_with_state(source, state)?.geographic_frame() {
        if bounds.0.is_finite()
            && bounds.2.is_finite()
            && bounds.1 <= bounds.3
            && Latitude::try_new(frame, bounds.1).is_ok()
            && Latitude::try_new(frame, bounds.3).is_ok()
        {
            return Ok(());
        }
    } else if bounds.0 <= bounds.2 {
        return validate_bounds(bounds);
    }
    Err(CrsError::invalid(BOUNDS_2D_GEO_ERROR.to_owned()))
}

pub(crate) fn validate_bounds_3d(bounds: (f64, f64, f64, f64, f64, f64)) -> Result<()> {
    if all_finite(&<[f64; 6]>::from(bounds))
        && bounds.0 <= bounds.3
        && bounds.1 <= bounds.4
        && bounds.2 <= bounds.5
    {
        return Ok(());
    }
    Err(CrsError::invalid(BOUNDS_3D_ERROR.to_owned()))
}

fn validate_transform_bounds_3d_with_state(
    source: &str,
    bounds: (f64, f64, f64, f64, f64, f64),
    state: &RuntimeState,
) -> Result<()> {
    if let Some(frame) = info_with_state(source, state)?.geographic_frame() {
        if bounds.0.is_finite()
            && bounds.3.is_finite()
            && bounds.1 <= bounds.4
            && Latitude::try_new(frame, bounds.1).is_ok()
            && Latitude::try_new(frame, bounds.4).is_ok()
            && bounds.2.is_finite()
            && bounds.5.is_finite()
            && bounds.2 <= bounds.5
        {
            return Ok(());
        }
    } else if bounds.0 <= bounds.3 {
        return validate_bounds_3d(bounds);
    }
    Err(CrsError::invalid(BOUNDS_3D_GEO_ERROR.to_owned()))
}

pub(crate) fn transform_coordinates_with_pipeline(
    transformer: &ProjPipeline,
    direction: ProjDirection,
    x: &mut [f64],
    y: &mut [f64],
    zt: crate::ZtLanes<'_>,
    execution: &crate::crs::TransformExecution,
) -> Result<()> {
    match zt {
        crate::Zt::None => {
            validate_coordinate_lanes(x, y, crate::Zt::None)?;
            transformer.transform_lanes(x, y, None, None, direction, execution)
        },
        crate::Zt::Z(z) => {
            validate_coordinate_lanes(x, y, crate::Zt::Z(&*z))?;
            transformer.transform_lanes(x, y, Some(z), None, direction, execution)
        },
        crate::Zt::T(t) => {
            validate_coordinate_lanes(x, y, crate::Zt::T(&*t))?;
            transformer.transform_lanes(x, y, None, Some(t), direction, execution)
        },
        crate::Zt::Zt { z, t } => {
            validate_coordinate_lanes(x, y, crate::Zt::Zt { z: &*z, t: &*t })?;
            transformer.transform_lanes(x, y, Some(z), Some(t), direction, execution)
        },
    }
}

pub(crate) fn transform_proj_shapes_indexed(
    transformer: &ProjPipeline,
    shapes: &[&Shape],
    source_epoch: Option<f64>,
    execution: &crate::crs::TransformExecution,
) -> std::result::Result<Vec<Shape>, TransformFailure> {
    let mut output = TransformOutputPlan::try_new(shapes)?;
    if source_epoch.is_none() {
        transformer
            .require_epoch_lane()
            .map_err(TransformFailure::from)?;
    }
    output.execute_indexed(|x, y, z| {
        let mut epoch = source_epoch.map(|epoch| [epoch]);
        transformer.transform_lanes_indexed(
            x,
            y,
            z,
            epoch.as_mut().map(<[f64; 1]>::as_mut_slice),
            ProjDirection::Forward,
            execution,
        )
    })?;
    Ok(output.finish(shapes)?)
}

const TRANSFORM_CHUNK_BYTES: usize = 1024 * 1024;
const XY_CHUNK_VERTICES: usize = TRANSFORM_CHUNK_BYTES / (2 * size_of::<f64>());
const XYZ_CHUNK_VERTICES: usize = TRANSFORM_CHUNK_BYTES / (3 * size_of::<f64>());

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OutputBatch {
    Xy,
    Xyz,
}

#[derive(Clone, Copy)]
enum OutputCoordSource<'a> {
    Point(&'a Point),
    Sequence(&'a CoordSeq),
}

impl OutputCoordSource<'_> {
    fn batch(self) -> OutputBatch {
        match self {
            Self::Point(point) => point.z().map_or(OutputBatch::Xy, |_| OutputBatch::Xyz),
            Self::Sequence(seq) => seq.zs().map_or(OutputBatch::Xy, |_| OutputBatch::Xyz),
        }
    }

    fn len(self) -> usize {
        match self {
            Self::Point(..) => 1,
            Self::Sequence(seq) => seq.len(),
        }
    }

    fn carried_ms(self) -> Option<Arc<[f64]>> {
        match self {
            Self::Point(point) => point.m().map(|m| Arc::from([m])),
            Self::Sequence(seq) => seq.carried_ms(),
        }
    }
}

/// Visit every coordinate sequence in the same order used by reconstruction.
/// MultiPoint is one sequence even when its member map contains empty points.
fn visit_shape_sequences(
    shape: &Shape,
    visitor: &mut impl FnMut(OutputCoordSource<'_>) -> Result<()>,
) -> Result<()> {
    match shape {
        Shape::Point(point) => visitor(OutputCoordSource::Point(point))?,
        Shape::MultiPoint(points) => visitor(OutputCoordSource::Sequence(points))?,
        Shape::LineString(points) => visitor(OutputCoordSource::Sequence(points))?,
        Shape::MultiLineString(lines) => {
            for line in lines {
                visitor(OutputCoordSource::Sequence(line))?;
            }
        },
        Shape::Polygon(polygon) => {
            for ring in polygon.rings() {
                visitor(OutputCoordSource::Sequence(ring))?;
            }
        },
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons {
                for ring in polygon.rings() {
                    visitor(OutputCoordSource::Sequence(ring))?;
                }
            }
        },
        Shape::GeometryCollection(geometries) => {
            for geometry in geometries {
                visit_shape_sequences(geometry, visitor)?;
            }
        },
        Shape::Empty(..) => {},
    }
    Ok(())
}

#[derive(Default)]
struct OutputCounts {
    xy_vertices: usize,
    xyz_vertices: usize,
    sequences: usize,
}

impl OutputCounts {
    fn add(&mut self, source: OutputCoordSource<'_>) -> Result<()> {
        self.sequences = self.sequences.checked_add(1).ok_or_else(|| {
            GeometryErrorKind::message("transform sequence count overflows usize")
        })?;
        let vertices = source.len();
        let total = match source.batch() {
            OutputBatch::Xy => &mut self.xy_vertices,
            OutputBatch::Xyz => &mut self.xyz_vertices,
        };
        *total = total.checked_add(vertices).ok_or_else(|| {
            GeometryErrorKind::message("transform coordinate count overflows usize")
        })?;
        Ok(())
    }
}

/// One final contiguous coordinate batch. M is carried independently because
/// it is never transformed; full columns remain Arc-shared and windows are
/// copied by `carried_ms` before any fallible backend work is published.
struct OutputColumns {
    xs: Vec<f64>,
    ys: Vec<f64>,
    zs: Option<Vec<f64>>,
}

impl OutputColumns {
    fn try_with_capacity(capacity: usize, has_z: bool) -> Result<Self> {
        let mut xs = Vec::new();
        crate::try_reserve_vec(&mut xs, capacity)?;
        let mut ys = Vec::new();
        if let Err(error) = crate::try_reserve_vec(&mut ys, capacity) {
            drop(xs);
            return Err(error);
        }
        let zs = if has_z {
            let mut values = Vec::new();
            if let Err(error) = crate::try_reserve_vec(&mut values, capacity) {
                drop((xs, ys));
                return Err(error);
            }
            Some(values)
        } else {
            None
        };
        Ok(Self { xs, ys, zs })
    }

    fn append(&mut self, source: OutputCoordSource<'_>) -> Range<usize> {
        let start = self.xs.len();
        match source {
            OutputCoordSource::Point(point) => {
                debug_assert_eq!(self.zs.is_some(), point.z().is_some());
                self.xs.push(point.x);
                self.ys.push(point.y);
                if let Some(zs) = &mut self.zs {
                    zs.push(point.z().expect("XYZ output source carries Z"));
                }
            },
            OutputCoordSource::Sequence(seq) => {
                debug_assert_eq!(self.zs.is_some(), seq.zs().is_some());
                self.xs.extend_from_slice(seq.xs());
                self.ys.extend_from_slice(seq.ys());
                if let (Some(zs), Some(source_zs)) = (&mut self.zs, seq.zs()) {
                    zs.extend_from_slice(source_zs);
                }
            },
        }
        start..self.xs.len()
    }
}

struct OutputCoordSeq {
    batch: OutputBatch,
    range: Range<usize>,
    shape: usize,
    ms: Option<Arc<[f64]>>,
}

pub(crate) struct TransformOutputPlan {
    xy: OutputColumns,
    xyz: OutputColumns,
    sequences: Vec<OutputCoordSeq>,
}

impl TransformOutputPlan {
    pub(crate) fn try_new(shapes: &[&Shape]) -> Result<Self> {
        let mut counts = OutputCounts::default();
        for shape in shapes {
            visit_shape_sequences(shape, &mut |source| counts.add(source))?;
        }
        types::ensure_coordseq_vertex_capacity(counts.xy_vertices)?;
        types::ensure_coordseq_vertex_capacity(counts.xyz_vertices)?;
        let mut plan = Self {
            xy: OutputColumns::try_with_capacity(counts.xy_vertices, false)?,
            xyz: OutputColumns::try_with_capacity(counts.xyz_vertices, true)?,
            sequences: Vec::new(),
        };
        crate::try_reserve_vec(&mut plan.sequences, counts.sequences)?;
        for (shape_index, shape) in shapes.iter().enumerate() {
            visit_shape_sequences(shape, &mut |source| plan.push_source(source, shape_index))?;
        }
        Ok(plan)
    }

    fn push_source(&mut self, source: OutputCoordSource<'_>, shape: usize) -> Result<()> {
        let batch = source.batch();
        let range = match batch {
            OutputBatch::Xy => self.xy.append(source),
            OutputBatch::Xyz => self.xyz.append(source),
        };
        crate::try_push_vec(&mut self.sequences, OutputCoordSeq {
            batch,
            range,
            shape,
            ms: source.carried_ms(),
        })
    }

    pub(crate) fn execute_indexed(
        &mut self,
        mut transform: impl FnMut(
            &mut [f64],
            &mut [f64],
            Option<&mut [f64]>,
        ) -> std::result::Result<(), TransformFailure>,
    ) -> std::result::Result<(), TransformFailure> {
        self.execute_with_capacities_indexed(XY_CHUNK_VERTICES, XYZ_CHUNK_VERTICES, &mut transform)
    }

    fn execute_with_capacities_indexed(
        &mut self,
        xy_capacity: usize,
        xyz_capacity: usize,
        transform: &mut impl FnMut(
            &mut [f64],
            &mut [f64],
            Option<&mut [f64]>,
        ) -> std::result::Result<(), TransformFailure>,
    ) -> std::result::Result<(), TransformFailure> {
        // Reconstruction runs XY before XYZ. If XY identifies a coordinate
        // failure, one XYZ pass is still needed so the earliest input shape,
        // rather than lane order, owns the displayed error.
        match self.execute_axes_indexed(false, xy_capacity, transform) {
            Ok(()) => self.execute_axes_indexed(true, xyz_capacity, transform),
            Err(xy_failure) if xy_failure.coordinate.is_some() => {
                match self.execute_axes_indexed(true, xyz_capacity, transform) {
                    Err(xyz_failure)
                        if xyz_failure.coordinate.is_some()
                            && xyz_failure.coordinate < xy_failure.coordinate =>
                    {
                        Err(xyz_failure)
                    },
                    _ => Err(xy_failure),
                }
            },
            Err(failure) => Err(failure),
        }
    }

    fn execute_axes_indexed(
        &mut self,
        has_z: bool,
        capacity: usize,
        transform: &mut impl FnMut(
            &mut [f64],
            &mut [f64],
            Option<&mut [f64]>,
        ) -> std::result::Result<(), TransformFailure>,
    ) -> std::result::Result<(), TransformFailure> {
        let columns = if has_z { &mut self.xyz } else { &mut self.xy };
        execute_batch_indexed(columns, capacity, transform).map_err(|failure| {
            let shape = failure.coordinate.and_then(|coordinate| {
                self.sequences
                    .iter()
                    .find(|sequence| {
                        sequence.batch
                            == if has_z {
                                OutputBatch::Xyz
                            } else {
                                OutputBatch::Xy
                            }
                            && sequence.range.contains(&coordinate)
                    })
                    .map(|sequence| sequence.shape)
            });
            debug_assert!(failure.coordinate.is_none_or(|_| shape.is_some()));
            failure.with_coordinate(shape)
        })
    }

    pub(crate) fn finish(self, shapes: &[&Shape]) -> Result<Vec<Shape>> {
        let Self { xy, xyz, sequences } = self;
        let xy_xs: Arc<[f64]> = xy.xs.into();
        let xy_ys: Arc<[f64]> = xy.ys.into();
        let xyz_xs: Arc<[f64]> = xyz.xs.into();
        let xyz_ys: Arc<[f64]> = xyz.ys.into();
        let xyz_zs: Arc<[f64]> = xyz.zs.expect("XYZ output columns carry Z").into();
        let sequences = sequences
            .into_iter()
            .map(|owner| {
                let (xs, ys, zs) = match owner.batch {
                    OutputBatch::Xy => (&xy_xs, &xy_ys, None),
                    OutputBatch::Xyz => (&xyz_xs, &xyz_ys, Some(&xyz_zs)),
                };
                let window = CoordWindow::from_range(owner.range, xs.len());
                CoordSeq::from_columns_with_window(
                    Arc::clone(xs),
                    Arc::clone(ys),
                    zs.map(Arc::clone),
                    owner.ms,
                    window,
                )
            })
            .collect();
        let mut cursor = TransformCursor {
            sequences,
            offset: 0,
        };
        shapes
            .iter()
            .map(|shape| rebuild_shape_columns(shape, &mut cursor))
            .collect::<Result<Vec<_>>>()
    }
}

fn execute_batch_indexed(
    columns: &mut OutputColumns,
    capacity: usize,
    transform: &mut impl FnMut(
        &mut [f64],
        &mut [f64],
        Option<&mut [f64]>,
    ) -> std::result::Result<(), TransformFailure>,
) -> std::result::Result<(), TransformFailure> {
    let capacity = capacity.max(1);
    let OutputColumns { xs, ys, zs } = columns;
    let mut zs = zs.as_mut().map(Vec::as_mut_slice);
    let mut offset = 0;
    while offset < xs.len() {
        let end = xs.len().min(offset.saturating_add(capacity));
        transform(
            &mut xs[offset..end],
            &mut ys[offset..end],
            zs.as_deref_mut().map(|values| &mut values[offset..end]),
        )
        .map_err(|failure| failure.offset_coordinate(offset, end))?;
        offset = end;
    }
    Ok(())
}

/// Read cursor over the two transformed batches: each shape consumes exactly
/// the slices it contributed, in order, from the XY or XYZ batch matching its
/// axes.
struct TransformCursor {
    sequences: Vec<CoordSeq>,
    offset: usize,
}

impl TransformCursor {
    fn take(&mut self) -> CoordSeq {
        let seq = std::mem::replace(
            &mut self.sequences[self.offset],
            CoordSeq::empty(crate::geometry::CoordinateAxes::XY),
        );
        self.offset += 1;
        seq
    }
}

fn rebuild_point_columns(point: &Point, cursor: &mut TransformCursor) -> Point {
    let transformed = cursor.take().point_at(0);
    Point::new_raw_axes(
        transformed.x,
        transformed.y,
        ZOrdinate(transformed.z()),
        MOrdinate(point.m()),
    )
}

/// Rebuild one shape from the transformed batches — original axes are
/// preserved (2D stays 2D; 3D gets the transformed Z lane). Collection
/// reconstruction remains checked against the source depth.
fn rebuild_shape_columns(shape: &Shape, cursor: &mut TransformCursor) -> Result<Shape> {
    Ok(match shape {
        Shape::Point(point) => Shape::Point(rebuild_point_columns(point, cursor)),
        Shape::MultiPoint(points) => {
            Shape::MultiPoint(points.with_coords(rebuild_seq_columns(points, cursor)))
        },
        Shape::LineString(points) => {
            Shape::LineString(LineSeq::from_proven(rebuild_seq_columns(points, cursor)))
        },
        Shape::MultiLineString(lines) => {
            Shape::MultiLineString(lines.map_values(lines.axes(), |line| {
                LineSeq::from_proven(rebuild_seq_columns(line, cursor))
            }))
        },
        Shape::Polygon(polygon) => Shape::Polygon(rebuild_polygon_columns(polygon, cursor)),
        Shape::MultiPolygon(polygons) => {
            Shape::MultiPolygon(polygons.map_values(polygons.axes(), |polygon| {
                rebuild_polygon_columns(polygon, cursor)
            }))
        },
        Shape::GeometryCollection(geometries) => Shape::GeometryCollection(
            geometries.try_map_members(|geometry| rebuild_shape_columns(geometry, cursor))?,
        ),
        Shape::Empty(..) => shape.clone(),
    })
}

fn rebuild_polygon_columns(
    polygon: &crate::geometry::Polygon,
    cursor: &mut TransformCursor,
) -> crate::geometry::Polygon {
    // CRS rebuild only rewrites coordinates, preserving ring structure.
    polygon.map_ring_seqs(|ring| rebuild_seq_columns(ring, cursor))
}

fn rebuild_seq_columns(seq: &CoordSeq, cursor: &mut TransformCursor) -> CoordSeq {
    let transformed = cursor.take();
    debug_assert_eq!(transformed.len(), seq.len());
    transformed
}

#[cfg(test)]
mod lane_tests {
    use super::*;
    use crate::geometry::{CoordinateAxes, MultiPoint};

    #[test]
    fn chunks_cross_owners_and_run_xy_before_xyz() {
        let shapes = [
            Shape::Point(Point::new_raw_axes(
                1.0,
                2.0,
                ZOrdinate(None),
                MOrdinate(None),
            )),
            Shape::Point(Point::new_raw_axes(
                3.0,
                4.0,
                ZOrdinate(None),
                MOrdinate(None),
            )),
            Shape::Point(Point::new_raw_axes(
                5.0,
                6.0,
                ZOrdinate(None),
                MOrdinate(None),
            )),
            Shape::Point(Point::new_raw_axes(
                7.0,
                8.0,
                ZOrdinate(Some(9.0)),
                MOrdinate(None),
            )),
            Shape::Point(Point::new_raw_axes(
                10.0,
                11.0,
                ZOrdinate(Some(12.0)),
                MOrdinate(None),
            )),
        ];
        let refs: Vec<_> = shapes.iter().collect();
        let mut plan = TransformOutputPlan::try_new(&refs).unwrap();
        let mut calls = Vec::new();
        plan.execute_with_capacities_indexed(2, 1, &mut |x, y, z| {
            calls.push((z.is_some(), x.len()));
            for value in x.iter_mut().chain(y.iter_mut()) {
                *value += 100.0;
            }
            if let Some(z) = z {
                for value in z {
                    *value += 100.0;
                }
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(calls, [(false, 2), (false, 1), (true, 1), (true, 1)]);
        assert_eq!(plan.xy.xs, [101.0, 103.0, 105.0]);
        assert_eq!(plan.xy.ys, [102.0, 104.0, 106.0]);
        assert_eq!(plan.xyz.xs, [107.0, 110.0]);
        assert_eq!(plan.xyz.ys, [108.0, 111.0]);
        assert_eq!(plan.xyz.zs.as_deref(), Some([109.0, 112.0].as_slice()));
        assert!(plan.xy.xs.capacity() >= 3);
        assert!(plan.xy.ys.capacity() >= 3);
        assert!(plan.xyz.xs.capacity() >= 2);
        assert!(plan.xyz.ys.capacity() >= 2);
        assert!(plan.xyz.zs.as_ref().is_some_and(|zs| zs.capacity() >= 2));
        let output = plan.finish(&refs).unwrap();
        let coordinates = output
            .iter()
            .map(|shape| match shape {
                Shape::Point(point) => (point.x, point.y, point.z()),
                _ => unreachable!(),
            })
            .collect::<Vec<_>>();
        assert_eq!(coordinates, [
            (101.0, 102.0, None),
            (103.0, 104.0, None),
            (105.0, 106.0, None),
            (107.0, 108.0, Some(109.0)),
            (110.0, 111.0, Some(112.0)),
        ]);
    }

    #[test]
    fn indexed_execution_prefers_the_earliest_shape_across_axes() {
        let shapes = [
            Shape::Point(Point::new_raw_axes(
                0.0,
                0.0,
                ZOrdinate(Some(1.0)),
                MOrdinate(None),
            )),
            Shape::Point(Point::new_raw_xy(0.0, 0.0)),
        ];
        let refs: Vec<_> = shapes.iter().collect();
        let mut plan = TransformOutputPlan::try_new(&refs).unwrap();
        let failure = plan
            .execute_indexed(|_, _, z| {
                Err(TransformFailure::at(
                    GeometryErrorKind::projection(if z.is_some() {
                        "xyz failed"
                    } else {
                        "xy failed"
                    }),
                    0,
                ))
            })
            .unwrap_err();

        assert_eq!(failure.coordinate, Some(0));
        assert_eq!(failure.error.to_string(), "projection failed: xyz failed");
    }

    #[test]
    fn indexed_execution_rejects_a_backend_ordinal_outside_the_current_chunk() {
        let sequence =
            CoordSeq::from_columns([0.0, 1.0, 2.0].into(), [0.0, 1.0, 2.0].into(), None, None);
        let shape = Shape::MultiPoint(MultiPoint::from_coordseq(sequence).unwrap());
        let refs = [&shape];
        let mut plan = TransformOutputPlan::try_new(&refs).unwrap();
        let mut calls = 0;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            plan.execute_with_capacities_indexed(2, 2, &mut |x, _, _| {
                calls += 1;
                Err(TransformFailure::at(
                    GeometryErrorKind::projection("invalid chunk ordinal"),
                    x.len(),
                ))
            })
        }));

        result.unwrap_err();
        assert_eq!(calls, 1);
    }

    #[expect(clippy::too_many_lines, reason = "a table-driven corpus: the cases are data, and splitting the table hides which oracle they all answer to")]
    #[test]
    fn mixed_sequences_share_final_lanes_and_reconstruct_exactly() {
        let xy = CoordSeq::from_columns(
            [1.0, 2.0, 3.0].into(),
            [10.0, 20.0, 30.0].into(),
            None,
            None,
        );
        let xyz = CoordSeq::from_columns(
            [4.0, 5.0, 6.0].into(),
            [40.0, 50.0, 60.0].into(),
            Some([400.0, 500.0, 600.0].into()),
            None,
        );
        let shapes = [
            Shape::MultiPoint(MultiPoint::from_coordseq(xy).unwrap()),
            Shape::MultiPoint(
                MultiPoint::from_coordseq(CoordSeq::from_columns(
                    [7.0].into(),
                    [70.0].into(),
                    None,
                    None,
                ))
                .unwrap(),
            ),
            Shape::LineString(LineSeq::try_new(xyz).unwrap()),
            Shape::MultiPoint(
                MultiPoint::from_coordseq(CoordSeq::from_columns(
                    [8.0].into(),
                    [80.0].into(),
                    Some([800.0].into()),
                    None,
                ))
                .unwrap(),
            ),
        ];
        let refs: Vec<_> = shapes.iter().collect();
        let mut plan = TransformOutputPlan::try_new(&refs).unwrap();
        let mut calls = Vec::new();
        plan.execute_with_capacities_indexed(2, 2, &mut |x, y, z| {
            calls.push((z.is_some(), x.len()));
            for value in x {
                *value += 100.0;
            }
            for value in y {
                *value += 1000.0;
            }
            if let Some(z) = z {
                for value in z {
                    *value += 10_000.0;
                }
            }
            Ok(())
        })
        .unwrap();

        assert_eq!(calls, [(false, 2), (false, 2), (true, 2), (true, 2)]);
        assert_eq!(plan.sequences[0].batch, OutputBatch::Xy);
        assert_eq!(plan.sequences[0].range, 0..3);
        assert_eq!(plan.sequences[1].range, 3..4);
        assert_eq!(plan.sequences[2].batch, OutputBatch::Xyz);
        assert_eq!(plan.sequences[2].range, 0..3);
        assert_eq!(plan.sequences[3].range, 3..4);
        assert_eq!(plan.xy.xs, [101.0, 102.0, 103.0, 107.0]);
        assert_eq!(plan.xy.ys, [1010.0, 1020.0, 1030.0, 1070.0]);
        assert_eq!(plan.xyz.xs, [104.0, 105.0, 106.0, 108.0]);
        assert_eq!(plan.xyz.ys, [1040.0, 1050.0, 1060.0, 1080.0]);
        assert_eq!(
            plan.xyz.zs.as_deref(),
            Some([10_400.0, 10_500.0, 10_600.0, 10_800.0].as_slice())
        );
        assert_eq!(plan.xy.xs.len(), 4);
        assert_eq!(plan.xyz.xs.len(), 4);
        assert!(plan.xy.xs.capacity() >= plan.xy.xs.len());
        assert!(plan.xyz.xs.capacity() >= plan.xyz.xs.len());

        let output = plan.finish(&refs).unwrap();
        let Shape::MultiPoint(output_xy) = &output[0] else {
            unreachable!()
        };
        let Shape::MultiPoint(output_xy_point) = &output[1] else {
            unreachable!()
        };
        let Shape::LineString(output_xyz) = &output[2] else {
            unreachable!()
        };
        let Shape::MultiPoint(output_xyz_point) = &output[3] else {
            unreachable!()
        };
        assert_eq!(output_xy.xs(), [101.0, 102.0, 103.0]);
        assert_eq!(output_xy.ys(), [1010.0, 1020.0, 1030.0]);
        assert_eq!(output_xy_point.xs(), [107.0]);
        assert_eq!(output_xy_point.ys(), [1070.0]);
        assert_eq!(output_xyz.xs(), [104.0, 105.0, 106.0]);
        assert_eq!(output_xyz.ys(), [1040.0, 1050.0, 1060.0]);
        assert_eq!(
            output_xyz.zs(),
            Some([10_400.0, 10_500.0, 10_600.0].as_slice())
        );
        assert_eq!(output_xyz_point.xs(), [108.0]);
        assert_eq!(output_xyz_point.ys(), [1080.0]);
        assert_eq!(output_xyz_point.zs(), Some([10_800.0].as_slice()));

        let xy_columns = output_xy.column_arcs();
        let xy_point_columns = output_xy_point.column_arcs();
        assert!(Arc::ptr_eq(&xy_columns.xs, &xy_point_columns.xs));
        let xyz_columns = output_xyz.column_arcs();
        let xyz_point_columns = output_xyz_point.column_arcs();
        assert!(Arc::ptr_eq(&xyz_columns.xs, &xyz_point_columns.xs));
    }

    #[test]
    fn mixed_m_sequences_keep_local_windows_on_shared_coordinates() {
        let first_ms: Arc<[f64]> = [50.0, 60.0].into();
        let second_ms: Arc<[f64]> = [70.0].into();
        let shapes = [
            Shape::MultiPoint(
                MultiPoint::from_coordseq(CoordSeq::from_columns(
                    [1.0, 2.0].into(),
                    [10.0, 20.0].into(),
                    None,
                    Some(Arc::clone(&first_ms)),
                ))
                .unwrap(),
            ),
            Shape::MultiPoint(
                MultiPoint::from_coordseq(CoordSeq::from_columns(
                    [3.0].into(),
                    [30.0].into(),
                    None,
                    Some(Arc::clone(&second_ms)),
                ))
                .unwrap(),
            ),
        ];
        let refs: Vec<_> = shapes.iter().collect();
        let mut plan = TransformOutputPlan::try_new(&refs).unwrap();
        plan.execute_with_capacities_indexed(2, 2, &mut |x, y, _| {
            for value in x.iter_mut().chain(y.iter_mut()) {
                *value += 1.0;
            }
            Ok(())
        })
        .unwrap();
        let output = plan.finish(&refs).unwrap();
        let Shape::MultiPoint(first) = &output[0] else {
            unreachable!()
        };
        let Shape::MultiPoint(second) = &output[1] else {
            unreachable!()
        };
        assert_eq!(first.ms(), Some([50.0, 60.0].as_slice()));
        assert_eq!(second.ms(), Some([70.0].as_slice()));
        assert!(Arc::ptr_eq(
            &first.column_arcs().m_parts().unwrap().0,
            &first_ms
        ));
        assert!(Arc::ptr_eq(
            &second.column_arcs().m_parts().unwrap().0,
            &second_ms
        ));
        assert_eq!(first.column_arcs().window, 0..2);
        assert_eq!(first.column_arcs().m_parts().unwrap().1, 0..2);
        assert_eq!(second.column_arcs().window, 2..3);
        assert_eq!(second.column_arcs().m_parts().unwrap().1, 0..1);
    }

    #[test]
    fn full_window_m_remains_shared() {
        let ms: std::sync::Arc<[f64]> = [5.0, 6.0].into();
        let seq = CoordSeq::from_columns(
            [1.0, 2.0].into(),
            [3.0, 4.0].into(),
            None,
            Some(std::sync::Arc::clone(&ms)),
        );
        let shape = Shape::MultiPoint(crate::geometry::MultiPoint::from_coordseq(seq).unwrap());
        let refs = [&shape];
        let mut plan = TransformOutputPlan::try_new(&refs).unwrap();
        plan.execute_with_capacities_indexed(1, 1, &mut |_, _, _| Ok(()))
            .unwrap();
        assert!(Arc::ptr_eq(plan.sequences[0].ms.as_ref().unwrap(), &ms));
        let Shape::MultiPoint(output) = plan.finish(&refs).unwrap().pop().unwrap() else {
            unreachable!()
        };
        assert_eq!(output.axes(), CoordinateAxes::XYM);
    }

    #[test]
    fn windowed_m_copies_only_visible_values() {
        let ms: std::sync::Arc<[f64]> = [4.0, 5.0, 6.0, 7.0].into();
        let seq = CoordSeq::from_columns(
            [0.0, 1.0, 2.0, 3.0].into(),
            [10.0, 11.0, 12.0, 13.0].into(),
            None,
            Some(std::sync::Arc::clone(&ms)),
        )
        .view(crate::geometry::CoordWindow::from_range(1..3, 4));
        let shape = Shape::MultiPoint(crate::geometry::MultiPoint::from_coordseq(seq).unwrap());
        let refs = [&shape];
        let mut plan = TransformOutputPlan::try_new(&refs).unwrap();
        plan.execute_with_capacities_indexed(1, 1, &mut |_, _, _| Ok(()))
            .unwrap();
        assert_eq!(plan.sequences[0].ms.as_deref().unwrap(), [5.0, 6.0]);
        assert!(!Arc::ptr_eq(plan.sequences[0].ms.as_ref().unwrap(), &ms));
    }
}
