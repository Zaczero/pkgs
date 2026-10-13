use super::{
    CircularInterval, CrsError, ExactSum, GeodeticPole, GeographicFrame, GeometryErrorKind,
    Latitude, LongitudeWindows, Result, RuntimeState, TRANSFORM_BOUNDS_MAX_DENSIFY, TopologyDomain,
    TransformExecution, TransformOptions, TransformPlan, info_with_state,
    map_geographic_conversion_error, runtime_state_snapshot,
};

pub(crate) type TransformedBounds = (f64, f64, f64, f64);
/// One transformed 3D bounds row: (minx, miny, minz, maxx, maxy, maxz).
pub(crate) type TransformedBounds3d = (f64, f64, f64, f64, f64, f64);

#[derive(Clone, Copy)]
pub(in crate::crs) enum BoundsInterpretation {
    Periodic,
    Literal,
}

impl BoundsInterpretation {
    pub(in crate::crs) const fn from_force_over(force_over: bool) -> Self {
        if force_over {
            Self::Literal
        } else {
            Self::Periodic
        }
    }
}

/// Backend-neutral edge samples for one bounds row. Periodic geographic
/// windows are split at their native seam in visualization axis order; literal
/// bounds retain their numeric source segment.
pub(in crate::crs) struct BoundsSamplePlan {
    pub(in crate::crs) x: Vec<f64>,
    pub(in crate::crs) y: Vec<f64>,
    pub(in crate::crs) z: Option<Vec<f64>>,
    pub(in crate::crs) epoch: Option<[f64; 1]>,
    source_windows: Vec<[f64; 2]>,
    interpretation: BoundsInterpretation,
    source_latitude_bounds: [f64; 2],
    source_domain: TopologyDomain,
    target_domain: TopologyDomain,
    source_full_longitude: bool,
    /// Containment of geodetic north and south poles, in that order.
    target_poles: [bool; 2],
}

impl BoundsSamplePlan {
    pub(in crate::crs) fn new_2d(
        bounds: (f64, f64, f64, f64),
        densify: u32,
        interpretation: BoundsInterpretation,
        source_domain: TopologyDomain,
        target_domain: TopologyDomain,
        epoch: Option<f64>,
    ) -> Result<Self> {
        let (windows, source_full_longitude) =
            source_windows(bounds.0, bounds.2, source_domain, interpretation)?;
        let source_latitude_bounds =
            admitted_source_latitude_bounds(source_domain, bounds.1, bounds.3)?;
        let edge_points = (densify as usize).wrapping_add(2);
        let capacity = windows.len().wrapping_mul(4).wrapping_mul(edge_points);
        let mut x = Vec::with_capacity(capacity);
        let mut y = Vec::with_capacity(capacity);
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
            interpretation,
            source_latitude_bounds,
            source_domain,
            target_domain,
            source_full_longitude,
            target_poles: [false; 2],
        })
    }

    pub(in crate::crs) fn new_3d(
        bounds: TransformedBounds3d,
        densify: u32,
        interpretation: BoundsInterpretation,
        source_domain: TopologyDomain,
        target_domain: TopologyDomain,
        epoch: Option<f64>,
    ) -> Result<Self> {
        let (windows, source_full_longitude) =
            source_windows(bounds.0, bounds.3, source_domain, interpretation)?;
        let source_latitude_bounds =
            admitted_source_latitude_bounds(source_domain, bounds.1, bounds.4)?;
        let edge_points = (densify as usize).wrapping_add(2);
        let capacity = windows.len().wrapping_mul(12).wrapping_mul(edge_points);
        let mut x = Vec::with_capacity(capacity);
        let mut y = Vec::with_capacity(capacity);
        let mut z = Vec::with_capacity(capacity);
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
            interpretation,
            source_latitude_bounds,
            source_domain,
            target_domain,
            source_full_longitude,
            target_poles: [false; 2],
        })
    }

    pub(in crate::crs) fn include_target_poles(
        &mut self,
        mut inverse: impl FnMut(f64, f64) -> Result<Option<(f64, f64)>>,
    ) -> Result<()> {
        if matches!(self.interpretation, BoundsInterpretation::Literal) {
            return Ok(());
        }
        // An operation may have no inverse at one physical pole (for example,
        // a north-polar projection at the south pole). The executor reports
        // that domain miss as `None`; all actual execution failures remain
        // errors so they cannot silently erase a contained pole.
        let TopologyDomain::Geographic(frame) = self.target_domain else {
            return Ok(());
        };
        let mut target_poles = self.target_poles;
        for (pole_contained, pole) in target_poles
            .iter_mut()
            .zip([GeodeticPole::North, GeodeticPole::South])
        {
            let latitude = frame.native_latitude_pole(pole);
            let Some((longitude, latitude)) = inverse(0.0, latitude)? else {
                continue;
            };
            if self.source_contains(longitude, latitude)? {
                *pole_contained = true;
            }
        }
        self.target_poles = target_poles;
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

    pub(in crate::crs) fn finish_2d(mut self) -> Result<(f64, f64, f64, f64)> {
        let horizontal = self.finish_horizontal()?;
        Ok(horizontal.into())
    }

    pub(in crate::crs) fn finish_3d(mut self) -> Result<TransformedBounds3d> {
        let horizontal = self.finish_horizontal()?;
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

    fn finish_horizontal(&mut self) -> Result<[f64; 4]> {
        match self.interpretation {
            BoundsInterpretation::Literal => {
                accumulate_literal_horizontal(&self.x, &self.y, self.target_domain)
            },
            BoundsInterpretation::Periodic => accumulate_horizontal(
                &mut self.x,
                &self.y,
                self.target_domain,
                self.source_full_longitude,
                self.target_poles,
            ),
        }
    }

    pub(in crate::crs) fn enter_in_core(&mut self) -> Result<()> {
        if let TopologyDomain::Geographic(frame) = self.source_domain
            && frame != GeographicFrame::crs84()
        {
            frame
                .to_geodesic_lanes(&mut self.x, &mut self.y)
                .map_err(map_geographic_conversion_error)?;
        }
        Ok(())
    }

    pub(in crate::crs) fn leave_in_core(&mut self) -> Result<()> {
        if let TopologyDomain::Geographic(frame) = self.target_domain {
            frame
                .lanes_from_geodesic(&mut self.x, &mut self.y)
                .map_err(map_geographic_conversion_error)?;
        }
        Ok(())
    }
}

fn source_windows(
    west: f64,
    east: f64,
    domain: TopologyDomain,
    interpretation: BoundsInterpretation,
) -> Result<(Vec<[f64; 2]>, bool)> {
    let TopologyDomain::Geographic(frame) = domain else {
        return Ok((vec![[west, east]], false));
    };
    if matches!(interpretation, BoundsInterpretation::Literal) {
        return Ok((vec![[west.min(east), west.max(east)]], false));
    }
    let axis = frame.longitude();
    let interval = periodic_interval(west, east, axis)?;
    let full = matches!(interval, CircularInterval::Full);
    let windows = match interval.windows(axis) {
        Some(LongitudeWindows::One(window)) => vec![window],
        Some(LongitudeWindows::Two(first, second)) => vec![first, second],
        Some(LongitudeWindows::Full) => vec![[-axis.half_period(), axis.half_period()]],
        None => unreachable!("bounds interval is non-empty"),
    };
    Ok((windows, full))
}

pub(in crate::crs) fn canonical_identity_longitudes(
    west: f64,
    east: f64,
    frame: GeographicFrame,
) -> Result<(f64, f64)> {
    let axis = frame.longitude();
    match periodic_interval(west, east, axis)? {
        CircularInterval::Full => Ok((-axis.half_period(), axis.half_period())),
        interval @ CircularInterval::Arc { .. } => Ok(interval
            .west_east()
            .expect("periodic bounds interval has endpoints")),
        CircularInterval::Empty => unreachable!("periodic bounds interval is non-empty"),
    }
}

pub(in crate::crs) fn identity_bounds_2d(
    bounds: (f64, f64, f64, f64),
    target_domain: TopologyDomain,
    interpretation: BoundsInterpretation,
) -> Result<(f64, f64, f64, f64)> {
    let TopologyDomain::Geographic(frame) = target_domain else {
        return Ok(bounds);
    };
    let (west, east) = identity_longitudes(bounds.0, bounds.2, frame, interpretation)?;
    let [south, north] = canonical_identity_latitudes(bounds.1, bounds.3, frame)?;
    Ok((west, south, east, north))
}

pub(in crate::crs) fn identity_bounds_3d(
    bounds: TransformedBounds3d,
    target_domain: TopologyDomain,
    interpretation: BoundsInterpretation,
) -> Result<TransformedBounds3d> {
    let TopologyDomain::Geographic(frame) = target_domain else {
        return Ok(bounds);
    };
    let (west, east) = identity_longitudes(bounds.0, bounds.3, frame, interpretation)?;
    let [south, north] = canonical_identity_latitudes(bounds.1, bounds.4, frame)?;
    Ok((west, south, bounds.2, east, north, bounds.5))
}

fn identity_longitudes(
    west: f64,
    east: f64,
    frame: GeographicFrame,
    interpretation: BoundsInterpretation,
) -> Result<(f64, f64)> {
    match interpretation {
        BoundsInterpretation::Periodic => canonical_identity_longitudes(west, east, frame),
        BoundsInterpretation::Literal => Ok((west.min(east), west.max(east))),
    }
}

fn periodic_interval(
    west: f64,
    east: f64,
    axis: crate::geographic::PeriodicAxis,
) -> Result<CircularInterval> {
    if span_at_least_period(west, east, axis.period()) {
        return Ok(CircularInterval::Full);
    }
    let canonical = |value| {
        axis.canonicalize(value)
            .map_err(|error| CrsError::invalid(error.to_string()))
    };
    Ok(CircularInterval::from_endpoints(
        canonical(west)?,
        canonical(east)?,
    ))
}

fn span_at_least_period(west: f64, east: f64, period: f64) -> bool {
    let (larger, smaller) = if west <= east {
        (east, west)
    } else {
        (west, east)
    };
    // This boundary decision belongs to the represented endpoints, not to a
    // rounded or overflowing `east - west` intermediate.
    let mut difference = ExactSum::zero();
    difference.add_product(larger, 1.0);
    difference.subtract_product(smaller, 1.0);
    difference.subtract_product(period, 1.0);
    !difference.ordering().is_lt()
}

pub(in crate::crs) fn canonical_identity_latitudes(
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
    let edge_intervals = points.wrapping_sub(1);
    for index in 0..points {
        let fraction = index as f64 / edge_intervals as f64;
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
    let edge_intervals = points.wrapping_sub(1);
    for index in 0..points {
        let fraction = index as f64 / edge_intervals as f64;
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
    for (contained, pole) in target_poles
        .into_iter()
        .zip([GeodeticPole::North, GeodeticPole::South])
    {
        if contained {
            let latitude = frame.native_latitude_pole(pole);
            south = south.min(latitude);
            north = north.max(latitude);
        }
    }
    let interval = if source_full_longitude || target_poles.into_iter().any(|pole| pole) {
        CircularInterval::Full
    } else {
        x.sort_unstable_by(f64::total_cmp);
        CircularInterval::cover_sorted_canonical_points(axis, x)
    };
    let (west, east) = match interval {
        CircularInterval::Full => (-axis.half_period(), axis.half_period()),
        CircularInterval::Arc { .. } => interval
            .west_east()
            .expect("non-empty arc has public endpoints"),
        CircularInterval::Empty => unreachable!("bounds sample plan is non-empty"),
    };
    Ok([west, south, east, north])
}

fn accumulate_literal_horizontal(
    x: &[f64],
    y: &[f64],
    target_domain: TopologyDomain,
) -> Result<[f64; 4]> {
    let (minx, maxx) = ordered_extrema(x);
    let TopologyDomain::Geographic(frame) = target_domain else {
        let (miny, maxy) = ordered_extrema(y);
        return Ok([minx, miny, maxx, maxy]);
    };
    let mut south = f64::INFINITY;
    let mut north = f64::NEG_INFINITY;
    for &latitude in y {
        let latitude = Latitude::try_new(frame, latitude)
            .map_err(|error| CrsError::invalid(error.to_string()))?;
        south = south.min(latitude.get());
        north = north.max(latitude.get());
    }
    Ok([minx, south, maxx, north])
}

fn ordered_extrema(values: &[f64]) -> (f64, f64) {
    let mut values = values.iter().copied();
    let first = values.next().expect("bounds sample plan is non-empty");
    let mut low = first;
    let mut high = first;
    for value in values {
        low = low.min(value);
        high = high.max(value);
    }
    (low, high)
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
            BoundsInterpretation::Periodic,
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
    fn periodic_source_full_span_uses_exact_endpoints() {
        let degrees = frame(std::f64::consts::PI / 180.0, 0.0);
        let (near_period_windows, near_period_full) = source_windows(
            -0.1,
            359.9,
            TopologyDomain::Geographic(degrees),
            BoundsInterpretation::Periodic,
        )
        .unwrap();
        assert!(!near_period_full);
        assert_eq!(near_period_windows.len(), 2);
        assert_eq!(near_period_windows[0][0], -0.1);
        assert_eq!(
            near_period_windows[1][1],
            degrees.longitude().canonicalize(359.9).unwrap().get()
        );

        let (full_windows, is_full) = source_windows(
            -0.1,
            359.9_f64.next_up(),
            TopologyDomain::Geographic(degrees),
            BoundsInterpretation::Periodic,
        )
        .unwrap();
        assert!(is_full);
        assert_eq!(full_windows, vec![[-180.0, 180.0]]);
    }

    #[test]
    fn literal_descending_geographic_source_is_a_closed_numeric_segment() {
        let degrees = frame(std::f64::consts::PI / 180.0, 0.0);
        let samples = BoundsSamplePlan::new_2d(
            (170.0, -10.0, -170.0, 10.0),
            2,
            BoundsInterpretation::Literal,
            TopologyDomain::Geographic(degrees),
            TopologyDomain::Cartesian,
            None,
        )
        .unwrap();
        assert_eq!(samples.source_windows, [[-170.0, 170.0]]);
        assert!(!samples.source_full_longitude);
        assert_eq!(samples.x.len(), 4 * (2 + 2));
        assert!(!samples.x.contains(&-180.0));
        assert!(!samples.x.contains(&180.0));
    }

    #[test]
    fn literal_geographic_target_keeps_numeric_extrema_without_pole_probes() {
        let degrees = frame(std::f64::consts::PI / 180.0, 0.0);
        let mut samples = BoundsSamplePlan::new_2d(
            (0.0, -1.0, 1.0, 1.0),
            2,
            BoundsInterpretation::Literal,
            TopologyDomain::Cartesian,
            TopologyDomain::Geographic(degrees),
            None,
        )
        .unwrap();
        for (index, longitude) in samples.x.iter_mut().enumerate() {
            *longitude = if index % 2 == 0 { 0.0 } else { 360.0 };
        }
        samples.y.fill(0.0);
        samples
            .include_target_poles(|_, _| panic!("literal bounds do not probe poles"))
            .unwrap();
        assert_eq!(samples.finish_2d().unwrap(), (0.0, 0.0, 360.0, 0.0));

        let mut out_of_domain = BoundsSamplePlan::new_2d(
            (0.0, -1.0, 1.0, 1.0),
            2,
            BoundsInterpretation::Literal,
            TopologyDomain::Cartesian,
            TopologyDomain::Geographic(degrees),
            None,
        )
        .unwrap();
        out_of_domain.y.fill(91.0);
        out_of_domain.finish_2d().unwrap_err();
    }

    #[test]
    fn full_unwrapped_band_stays_full_in_target_native_units() {
        let grads = frame(std::f64::consts::PI / 200.0, 0.0);
        let samples = BoundsSamplePlan::new_2d(
            (0.0, -100.0, 400.0, 100.0),
            2,
            BoundsInterpretation::Periodic,
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
            BoundsInterpretation::Periodic,
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
            BoundsInterpretation::Periodic,
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
            BoundsInterpretation::Periodic,
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
            BoundsInterpretation::Periodic,
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
    fn target_geodetic_poles_are_probed_in_native_southward_axis_coordinates() {
        let unit = AngularFactor::try_new("degree", std::f64::consts::PI / 180.0).unwrap();
        let frame = GeographicFrame::try_new_with_orientation(
            unit,
            unit,
            0.0,
            crate::geographic::GeographicAxisOrientation::from_axis_directions(true, false),
        )
        .unwrap();
        let mut samples = BoundsSamplePlan::new_2d(
            (0.0, -1.0, 10.0, 1.0),
            2,
            BoundsInterpretation::Periodic,
            TopologyDomain::Cartesian,
            TopologyDomain::Geographic(frame),
            None,
        )
        .unwrap();
        let mut probed = Vec::new();
        samples
            .include_target_poles(|_, latitude| {
                probed.push(latitude);
                Ok(Some((5.0, 0.0)))
            })
            .unwrap();
        assert_eq!(probed, [-90.0, 90.0]);
        assert_eq!(samples.target_poles, [true, true]);
        assert_eq!(samples.finish_2d().unwrap(), (-180.0, -90.0, 180.0, 90.0));
    }

    #[test]
    fn target_pole_probe_errors_are_not_treated_as_absent_poles() {
        let degrees = frame(std::f64::consts::PI / 180.0, 0.0);
        let mut samples = BoundsSamplePlan::new_2d(
            (-1.0, -1.0, 1.0, 1.0),
            2,
            BoundsInterpretation::Periodic,
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
            BoundsInterpretation::Periodic,
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
            BoundsInterpretation::Periodic,
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
