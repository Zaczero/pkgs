//! Local conformal projection planning.
//!
//! The planner is deliberately extent-aware: every candidate is checked with
//! PROJ factors over the receiver's vertices and its spherical bounding frame.
//! A projection is returned only when its worst linear scale error is at most
//! [`LOCAL_SCALE_ERROR_LIMIT`].

use std::borrow::Cow;

use crate::crs::catalog::catalog_with_state;
use crate::crs::in_core::{DatumKey, ParamUnit, datum_key, parse_param_value};
use crate::crs::{
    AreaOfInterest, CrsCatalogOptions, CrsCoordinateOperationInfo, CrsError, CrsInfo,
    CrsObjectKind, DEGREE_TO_RADIAN, MethodInfo, OperationInfo, OperationParameterInfo,
    PrimeMeridianInfo, RuntimeState, Shape, TransformOptions, UTM_K0, factors_at, info_with_state,
    is_wgs84_lonlat, normalize, operation_info_with_state, parse_epsg, preserve_resource_error,
    to_projjson_with_options_at, transform_at, utm_crs,
};
use crate::error::{Error, Result};
use crate::geographic::retained_exact::sites::compare_three_f64_products;
use crate::geographic::{AngularFactor, GeodeticPole};
use crate::geometry::ShapeCountQuery;

/// Maximum admitted meridional or parallel scale error (0.1%).
pub(super) const LOCAL_SCALE_ERROR_LIMIT: f64 = 0.001;
const LOCAL_CRS_DISPLAY_NAME: &str = "Local conformal CRS";

#[derive(Debug)]
pub(super) struct LocalCrsDefinition {
    pub(super) definition: String,
    pub(super) display_name: Option<&'static str>,
    pub(super) projected_pole: Option<(f64, f64)>,
}

impl LocalCrsDefinition {
    const fn authority(definition: String) -> Self {
        Self {
            definition,
            display_name: None,
            projected_pole: None,
        }
    }

    const fn custom(definition: String) -> Self {
        Self {
            definition,
            display_name: Some(LOCAL_CRS_DISPLAY_NAME),
            projected_pole: None,
        }
    }

    pub(crate) fn annotate_error(&self, mut error: Error) -> Error {
        error.annotate_crs_display(&self.definition, self.display_name);
        error
    }

    pub(super) const fn projected_pole(&self) -> Option<(f64, f64)> {
        self.projected_pole
    }

    fn with_projected_pole_at(
        mut self,
        source: &str,
        pole: GeodeticPole,
        state: &RuntimeState,
    ) -> Result<Self> {
        let frame = info_with_state(source, state)?
            .geographic_frame()
            .ok_or_else(|| {
                CrsError::invalid("polar local frame requires geographic source coordinates")
            })?;
        let pole = crate::geometry::Point::new_raw_xy(0.0, frame.native_latitude_pole(pole));
        let Shape::Point(pole) =
            transform_at(&Shape::Point(pole), source, &self.definition, state)?
        else {
            unreachable!("a projected pole remains a point")
        };
        self.projected_pole = Some((pole.x, pole.y));
        Ok(self)
    }
}

/// Estimate one conformal metric CRS for all coordinates in `shape`.
///
/// `source_crs` is mandatory by design: interpreting unframed coordinates as
/// longitude/latitude would turn a convenience method into a relabeling trap.
pub(crate) fn estimate_local_crs(shape: &Shape, source_crs: &str) -> Result<String> {
    let state = crate::crs::runtime_state_snapshot()?;
    Ok(estimate_local_crs_at(&[shape], source_crs, &state)?.definition)
}

/// One conformal metric CRS for all coordinates of `shapes` together.
pub(super) fn estimate_local_crs_at(
    shapes: &[&Shape],
    source_crs: &str,
    state: &RuntimeState,
) -> Result<LocalCrsDefinition> {
    let extent =
        LocalExtent::from_shapes(shapes, source_crs, state)?.ok_or(CrsError::EmptyGeometry)?;

    let geodetic_pole = if extent.latitude >= 0.0 {
        GeodeticPole::North
    } else {
        GeodeticPole::South
    };
    if extent.latitude > 84.0 || extent.latitude < -80.0 {
        if has_wgs84_datum_fast_path(source_crs) {
            let ups = match geodetic_pole {
                GeodeticPole::North => "EPSG:5041",
                GeodeticPole::South => "EPSG:5042",
            };
            if candidate_fits(ups, &extent.samples, state)? {
                return validated_local_crs(
                    LocalCrsDefinition::authority(ups.to_owned()),
                    source_crs,
                    state,
                )?
                .with_projected_pole_at(source_crs, geodetic_pole, state);
            }
        }
        let local = best_custom_candidate(source_crs, &extent, state)?;
        return validated_local_crs(local, source_crs, state)?.with_projected_pole_at(
            source_crs,
            geodetic_pole,
            state,
        );
    }

    let utm = best_utm_from_anchor(
        LocalProjectionAnchor {
            longitude: extent.longitude,
            latitude: extent.latitude,
        },
        source_crs,
        state,
    )?;
    if let Some(utm) = utm
        && candidate_fits(&utm, &extent.samples, state)?
    {
        return validated_local_crs(LocalCrsDefinition::authority(utm), source_crs, state);
    }

    let local =
        best_custom_candidate(source_crs, &extent, state).map_err(best_custom_candidate_error)?;
    validated_local_crs(local, source_crs, state)
}

fn best_custom_candidate_error(error: crate::error::Error) -> crate::error::Error {
    preserve_resource_error(error, |_| {
        CrsError::invalid(format!(
            "geometry cannot be represented by one local conformal CRS within the {:.1}% linear scale-error limit; split the geometry or choose a projected CRS explicitly",
            LOCAL_SCALE_ERROR_LIMIT * 100.0
        ))
    })
}

pub(super) fn local_crs_fits_at(
    shape: &Shape,
    source_crs: &str,
    candidate: &str,
    state: &RuntimeState,
) -> Result<bool> {
    let Some(extent) = LocalExtent::from_shapes(&[shape], source_crs, state)? else {
        return Ok(true);
    };
    candidate_fits(candidate, &extent.samples, state)
}

/// Check the resolved operation that a local-frame certificate will cover.
///
/// Projection curvature does not bound a datum transformation. Local frames
/// must retain the source ellipsoid and prime meridian, and the selected
/// operation may contain only its projection and coordinate-convention steps.
fn ensure_datum_preserving_local_operation(
    source_crs: &str,
    local_crs: &str,
    state: &RuntimeState,
) -> Result<()> {
    let source_crs_info = info_with_state(source_crs, state)?;
    let source = geodetic_info(source_crs, state)?;
    let local = info_with_state(local_crs, state)?;
    let fail = || {
        CrsError::invalid(format!(
            "local projection from {source_crs} to {local_crs} changes the source datum or contains an unowned coordinate operation"
        ))
    };

    if !same_ellipsoid(&source, &local)
        || !same_prime_meridian(&source, &local)
        || !same_datum(&source, &local)
    {
        return Err(fail());
    }

    // Projected inputs already carry a projection in their source operation.
    // The local constructive path does not use these frames; their base datum
    // and the destination ellipsoid are still checked above.
    if !source_crs_info.is_geographic() {
        return Ok(());
    }

    let Some(expected_method) = local
        .coordinate_operation
        .as_ref()
        .and_then(|operation| operation.method.as_ref())
        .and_then(|method| method.code.as_deref())
        .filter(|code| matches!(*code, "9807" | "9810"))
    else {
        return Err(fail());
    };
    let operation =
        operation_info_with_state(source_crs, local_crs, &TransformOptions::default(), state)?;
    if !operation_is_datum_preserving(&operation, expected_method) {
        return Err(fail());
    }
    Ok(())
}

fn validated_local_crs(
    local: LocalCrsDefinition,
    source_crs: &str,
    state: &RuntimeState,
) -> Result<LocalCrsDefinition> {
    ensure_datum_preserving_local_operation(source_crs, &local.definition, state)?;
    Ok(local)
}

fn geodetic_info(crs: &str, state: &RuntimeState) -> Result<std::sync::Arc<CrsInfo>> {
    let info = info_with_state(crs, state)?;
    if info.kind.starts_with("geographic") {
        return Ok(info);
    }
    let Some(geodetic_crs) = info.geodetic_crs.as_ref() else {
        return Err(CrsError::invalid(format!(
            "{crs} does not expose a geodetic CRS for a local projection"
        )));
    };
    info_with_state(
        geodetic_crs
            .definition
            .as_deref()
            .unwrap_or(&geodetic_crs.crs),
        state,
    )
}

const fn same_ellipsoid(left: &CrsInfo, right: &CrsInfo) -> bool {
    let (Some(left), Some(right)) = (&left.ellipsoid, &right.ellipsoid) else {
        return false;
    };
    left.semi_major_metre.to_bits() == right.semi_major_metre.to_bits()
        && left.semi_minor_metre.to_bits() == right.semi_minor_metre.to_bits()
}

fn same_prime_meridian(left: &CrsInfo, right: &CrsInfo) -> bool {
    same_prime_meridian_values(left.prime_meridian.as_ref(), right.prime_meridian.as_ref())
}

fn same_prime_meridian_values(
    left: Option<&PrimeMeridianInfo>,
    right: Option<&PrimeMeridianInfo>,
) -> bool {
    let Some((left_value, left_factor, left_period)) = selected_prime_meridian_factors(left) else {
        return false;
    };
    let Some((right_value, right_factor, right_period)) = selected_prime_meridian_factors(right)
    else {
        return false;
    };
    let (left_product, right_product) = match (left_period, right_period) {
        (Some(left_period), Some(right_period)) => {
            ([left_value, std::f64::consts::TAU, right_period], [
                right_value,
                std::f64::consts::TAU,
                left_period,
            ])
        },
        (Some(left_period), None) => ([left_value, std::f64::consts::TAU, 1.0], [
            right_value,
            right_factor,
            left_period,
        ]),
        (None, Some(right_period)) => ([left_value, left_factor, right_period], [
            right_value,
            std::f64::consts::TAU,
            1.0,
        ]),
        (None, None) => ([left_value, left_factor, 1.0], [
            right_value,
            right_factor,
            1.0,
        ]),
    };
    // Compare original ordinate/unit expressions before PM phase rounding.
    compare_three_f64_products(left_product, right_product) == std::cmp::Ordering::Equal
}

fn selected_prime_meridian_factors(
    meridian: Option<&PrimeMeridianInfo>,
) -> Option<(f64, f64, Option<f64>)> {
    let Some(meridian) = meridian else {
        return Some((0.0, 1.0, None));
    };
    let unit =
        AngularFactor::try_new("prime meridian unit", meridian.unit_conversion_factor).ok()?;
    meridian.longitude.is_finite().then_some((
        meridian.longitude,
        meridian.unit_conversion_factor,
        unit.recognized_period(),
    ))
}

fn same_datum(left: &CrsInfo, right: &CrsInfo) -> bool {
    match (datum_key(left), datum_key(right)) {
        (Some(left), Some(right)) => left == right,
        _ => left
            .datum
            .as_ref()
            .zip(right.datum.as_ref())
            .is_some_and(|(left, right)| left.kind == right.kind && left.name == right.name),
    }
}

fn operation_is_datum_preserving(operation: &OperationInfo, expected_method: &str) -> bool {
    if operation.has_ballpark_transformation
        || operation.requires_coordinate_epoch
        || !operation.grids.is_empty()
    {
        return false;
    }

    let mut projection_steps = 0_usize;
    if let Some(method) = operation.method.as_ref()
        && !allow_local_operation_method(
            method,
            &operation.parameters,
            expected_method,
            &mut projection_steps,
        )
    {
        return false;
    }
    operation.steps.iter().all(|step| {
        coordinate_operation_is_datum_preserving(step, expected_method, &mut projection_steps)
    }) && projection_steps == 1
}

fn coordinate_operation_is_datum_preserving(
    operation: &CrsCoordinateOperationInfo,
    expected_method: &str,
    projection_steps: &mut usize,
) -> bool {
    if operation.has_ballpark_transformation
        || operation.requires_coordinate_epoch
        || !operation.grids.is_empty()
        || (operation.method.is_none() && operation.steps.is_empty())
    {
        return false;
    }
    if let Some(method) = operation.method.as_ref()
        && !allow_local_operation_method(
            method,
            &operation.parameters,
            expected_method,
            projection_steps,
        )
    {
        return false;
    }
    operation.steps.iter().all(|step| {
        coordinate_operation_is_datum_preserving(step, expected_method, projection_steps)
    })
}

fn allow_local_operation_method(
    method: &MethodInfo,
    parameters: &[OperationParameterInfo],
    expected_method: &str,
    projection_steps: &mut usize,
) -> bool {
    if method.authority.as_deref() == Some("EPSG")
        && method.code.as_deref() == Some(expected_method)
    {
        *projection_steps = projection_steps.wrapping_add(1);
        return true;
    }
    matches!(method.code.as_deref(), Some("9601" | "9843" | "9844"))
        || method.name.as_deref().is_some_and(|name| {
            name.starts_with("Axis Order Reversal")
                || name.eq_ignore_ascii_case("Unit conversion")
                || name.eq_ignore_ascii_case("Longitude rotation")
        })
        || (method.authority.as_deref() == Some("EPSG")
            && method.code.as_deref() == Some("9660")
            && null_geographic_3d_offsets(parameters))
}

fn null_geographic_3d_offsets(parameters: &[OperationParameterInfo]) -> bool {
    !parameters.is_empty() && parameters.iter().all(|parameter| parameter.value == 0.0)
}

#[derive(Debug)]
struct LocalExtent {
    longitude: f64,
    latitude: f64,
    samples: Vec<(f64, f64)>,
}

impl LocalExtent {
    fn from_shapes(
        shapes: &[&Shape],
        source_crs: &str,
        state: &RuntimeState,
    ) -> Result<Option<Self>> {
        let mut sources = Vec::with_capacity(shapes.len());
        for &shape in shapes {
            if matches!(shape, Shape::GeometryCollection(_)) && shape.has_repeated_shared_owner() {
                sources.push(Cow::Owned(shape.distinct_point_cloud()?));
            } else {
                sources.push(Cow::Borrowed(shape));
            }
        }
        let mut counts = ShapeCountQuery::default();
        let coordinate_count =
            counts.coordinates_for_shapes(sources.iter().map(std::convert::AsRef::as_ref))?;
        drop(counts);
        let capacity = if coordinate_count == 0 {
            0
        } else {
            coordinate_count
                .checked_add(9)
                .ok_or(crate::error::ResourceError::SizeOverflow)?
        };
        let mut points = Vec::with_capacity(capacity);
        for shape in &sources {
            let shape = shape.as_ref();
            let lonlat: Cow<'_, Shape> = if is_wgs84_lonlat(source_crs) {
                Cow::Borrowed(shape)
            } else {
                Cow::Owned(transform_at(shape, source_crs, "EPSG:4326", state)?)
            };
            lonlat.for_each_point(|point| {
                points.push((normalize_longitude(point.x), point.y));
            });
        }

        if points.is_empty() {
            return Ok(None);
        }
        if points
            .iter()
            .any(|(lon, lat)| !lon.is_finite() || !lat.is_finite() || !(-90.0..=90.0).contains(lat))
        {
            return Err(CrsError::invalid(
                "local CRS estimation requires finite longitude/latitude coordinates within the geographic domain",
            ));
        }

        let mut longitudes: Vec<f64> = points.iter().map(|(lon, _)| *lon).collect();
        let (west, east, longitude) = circular_longitude_extent(&mut longitudes);
        let (min_lat, max_lat) = points.iter().fold(
            (f64::INFINITY, f64::NEG_INFINITY),
            |(min_lat, max_lat), (_, lat)| (min_lat.min(*lat), max_lat.max(*lat)),
        );
        let latitude = f64::midpoint(min_lat, max_lat);

        // Vertices catch the real data. Corners and edge midpoints catch the
        // interior extrema of sparse long segments and make the error promise
        // independent of vertex density.
        let longitude_edges = [west, longitude, east];
        let latitude_edges = [min_lat, latitude, max_lat];
        for lon in longitude_edges {
            for lat in latitude_edges {
                points.push((normalize_longitude(lon), lat));
            }
        }
        points.sort_unstable_by(|left, right| {
            left.0.total_cmp(&right.0).then(left.1.total_cmp(&right.1))
        });
        points.dedup_by(|left, right| {
            left.0.to_bits() == right.0.to_bits() && left.1.to_bits() == right.1.to_bits()
        });
        Ok(Some(Self {
            longitude,
            latitude,
            samples: points,
        }))
    }
}

fn circular_longitude_extent(longitudes: &mut Vec<f64>) -> (f64, f64, f64) {
    longitudes.sort_unstable_by(f64::total_cmp);
    longitudes.dedup_by(|left, right| left.total_cmp(right).is_eq());
    if let [longitude] = longitudes.as_slice() {
        return (*longitude, *longitude, *longitude);
    }
    let first = *longitudes
        .first()
        .expect("a multi-coordinate extent has a first longitude");
    let last = *longitudes
        .last()
        .expect("a multi-coordinate extent has a last longitude");
    let mut largest_gap = f64::NEG_INFINITY;
    let mut west = first;
    let mut east = last;
    for &[left, right] in longitudes.array_windows::<2>() {
        let gap = right - left;
        if gap > largest_gap {
            largest_gap = gap;
            west = right;
            east = left;
        }
    }
    let seam_next = first + 360.0;
    let seam_gap = seam_next - last;
    if seam_gap > largest_gap {
        west = first;
        east = last;
    }
    if east < west {
        east += 360.0;
    }
    let midpoint = f64::midpoint(west, east);
    (west, east, normalize_longitude(midpoint))
}

fn candidate_fits(candidate: &str, samples: &[(f64, f64)], state: &RuntimeState) -> Result<bool> {
    for &(longitude, latitude) in samples {
        let factor = factors_at(candidate, longitude, latitude, false, state)?;
        let error = (factor.meridional_scale - 1.0)
            .abs()
            .max((factor.parallel_scale - 1.0).abs());
        if !error.is_finite() || error > LOCAL_SCALE_ERROR_LIMIT {
            return Ok(false);
        }
    }
    Ok(true)
}

fn best_custom_candidate(
    source_crs: &str,
    extent: &LocalExtent,
    state: &RuntimeState,
) -> Result<LocalCrsDefinition> {
    let info = info_with_state(source_crs, state)?;
    let base = if info.kind.starts_with("geographic") {
        source_crs
    } else {
        info.geodetic_crs.as_ref().map_or(source_crs, |geodetic| {
            geodetic
                .definition
                .as_deref()
                .unwrap_or(geodetic.crs.as_str())
        })
    };
    let mut base_crs: serde_json::Value = serde_json::from_str(&to_projjson_with_options_at(
        base,
        &crate::crs::CrsProjJsonOptions::default(),
        state,
    )?)
    .map_err(|e| {
        CrsError::invalid(format!(
            "could not preserve the source datum in a local projected CRS: {e}"
        ))
    })?;
    base_crs
        .as_object_mut()
        .ok_or_else(|| CrsError::invalid("source geodetic CRS did not export as an object"))?
        .remove("$schema");

    let candidate = serde_json::json!({
        "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
        "type": "ProjectedCRS",
        "name": LOCAL_CRS_DISPLAY_NAME,
        "base_crs": base_crs,
        "conversion": {
            "name": "Local Transverse Mercator",
            "method": {"name": "Transverse Mercator", "id": {"authority": "EPSG", "code": 9807}},
            "parameters": [
                {"name": "Latitude of natural origin", "value": extent.latitude, "unit": "degree", "id": {"authority": "EPSG", "code": 8801}},
                {"name": "Longitude of natural origin", "value": extent.longitude, "unit": "degree", "id": {"authority": "EPSG", "code": 8802}},
                {"name": "Scale factor at natural origin", "value": 1.0, "unit": "unity", "id": {"authority": "EPSG", "code": 8805}},
                {"name": "False easting", "value": 0.0, "unit": "metre", "id": {"authority": "EPSG", "code": 8806}},
                {"name": "False northing", "value": 0.0, "unit": "metre", "id": {"authority": "EPSG", "code": 8807}}
            ]
        },
        "coordinate_system": {
            "subtype": "Cartesian",
            "axis": [
                {"name": "Easting", "abbreviation": "E", "direction": "east", "unit": "metre"},
                {"name": "Northing", "abbreviation": "N", "direction": "north", "unit": "metre"}
            ]
        }
    })
    .to_string();
    let candidates = [candidate];
    for candidate in candidates {
        if candidate_fits(&candidate, &extent.samples, state)? {
            return Ok(LocalCrsDefinition::custom(
                normalize(&candidate)?.into_owned(),
            ));
        }
    }
    Err(CrsError::invalid(
        "no safe local conformal projection found",
    ))
}

#[derive(Debug, Clone, Copy)]
struct LocalProjectionAnchor {
    longitude: f64,
    latitude: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct UtmZone(u8);

impl UtmZone {
    pub(crate) fn try_new(zone: u32) -> Result<Self> {
        if (1..=60).contains(&zone) {
            Ok(Self(u8::try_from(zone).expect("UTM zone <= 60 fits u8")))
        } else {
            Err(CrsError::invalid(format!(
                "UTM zone must be between 1 and 60, got {zone}"
            )))
        }
    }

    pub(crate) const fn get(self) -> u8 {
        self.0
    }
}

fn best_utm_from_anchor(
    local: LocalProjectionAnchor,
    crs: &str,
    state: &RuntimeState,
) -> Result<Option<String>> {
    let zone = utm_zone(local.longitude, local.latitude);
    let geodetic_pole = if local.latitude >= 0.0 {
        GeodeticPole::North
    } else {
        GeodeticPole::South
    };
    if has_wgs84_datum_fast_path(crs) {
        return Ok(Some(wgs84_utm_crs(zone, geodetic_pole)));
    }
    let info = info_with_state(crs, state)?;
    let Some(source_datum) = datum_key(&info) else {
        return Ok(None);
    };
    best_utm_from_catalog(
        &source_datum,
        local.longitude,
        local.latitude,
        zone,
        geodetic_pole,
        state,
    )
}

fn has_wgs84_datum_fast_path(crs: &str) -> bool {
    is_wgs84_lonlat(crs) || parse_epsg(crs) == Some(3857) || utm_crs(crs).is_some()
}

fn best_utm_from_catalog(
    source_datum: &DatumKey,
    lon: f64,
    lat: f64,
    zone: UtmZone,
    pole: GeodeticPole,
    state: &RuntimeState,
) -> Result<Option<String>> {
    let items = catalog_with_state(
        Some("EPSG"),
        &CrsCatalogOptions {
            kind: Some(CrsObjectKind::ProjectedCrs),
            area: Some(AreaOfInterest {
                west: lon,
                south: lat,
                east: lon,
                north: lat,
            }),
            contains_area: false,
            allow_deprecated: false,
            celestial_body: Some("Earth".to_owned()),
        },
        state,
    )?;
    let mut best: Option<String> = None;
    for item in items {
        if item.projection_method_name.as_deref() != Some("Transverse Mercator") {
            continue;
        }
        let info = info_with_state(&item.crs, state)?;
        if datum_key(&info).as_ref() != Some(source_datum) {
            continue;
        }
        if !is_east_north_metre_axes(&info) {
            continue;
        }
        if !is_utm_operation(&info, zone, pole) {
            continue;
        }
        if best
            .as_ref()
            .is_none_or(|current| utm_survivor_rank(&item.crs) < utm_survivor_rank(current))
        {
            best = Some(item.crs);
        }
    }
    Ok(best)
}

fn utm_survivor_rank(crs: &str) -> (u32, &str) {
    (parse_epsg(crs).unwrap_or(u32::MAX), crs)
}

#[expect(
    clippy::float_cmp,
    reason = "metre axes are the conversion-factor identity 1, not an approximate linear unit"
)]
pub(super) fn is_east_north_metre_axes(info: &CrsInfo) -> bool {
    let [east, north] = info.axes.as_slice() else {
        return false;
    };
    east.direction.as_deref() == Some("east")
        && north.direction.as_deref() == Some("north")
        && east.unit_conversion_factor == 1.0
        && north.unit_conversion_factor == 1.0
}

#[expect(
    clippy::float_cmp,
    reason = "projected metre axes require the exact unit-conversion identity 1"
)]
pub(super) fn is_projected_metre_cartesian_axes(info: &CrsInfo) -> bool {
    if !info.is_projected() || info.coordinate_system != Some("cartesian") {
        return false;
    }
    let [first, second] = info.axes.as_slice() else {
        return false;
    };
    first.unit_conversion_factor == 1.0 && second.unit_conversion_factor == 1.0
}

fn is_utm_operation(info: &CrsInfo, zone: UtmZone, pole: GeodeticPole) -> bool {
    let Some(operation) = info.coordinate_operation.as_ref() else {
        return false;
    };
    let Some(method) = operation.method.as_ref() else {
        return false;
    };
    if method.authority.as_deref() != Some("EPSG") || method.code.as_deref() != Some("9807") {
        return false;
    }
    let expected_lon0 = f64::from(zone.get()) * 6.0 - 183.0;
    let expected_northing = match pole {
        GeodeticPole::North => 0.0,
        GeodeticPole::South => 10_000_000.0,
    };
    angular_param_matches_degrees(operation, "8801", 0.0)
        && angular_param_matches_degrees(operation, "8802", expected_lon0)
        && linear_or_unity_param_matches(operation, "8805", ParamUnit::Unity, UTM_K0)
        && linear_or_unity_param_matches(operation, "8806", ParamUnit::Metre, 500_000.0)
        && linear_or_unity_param_matches(operation, "8807", ParamUnit::Metre, expected_northing)
}

#[expect(
    clippy::float_cmp,
    clippy::float_cmp_const,
    reason = "UTM origin identity is the stored degree spelling and the exact radian conversion factor, not a nearby angle"
)]
fn angular_param_matches_degrees(
    operation: &CrsCoordinateOperationInfo,
    code: &str,
    expected_degrees: f64,
) -> bool {
    let Some(param) = unique_epsg_param(operation, code) else {
        return false;
    };
    param.value.is_finite()
        && param.unit_conversion_factor == DEGREE_TO_RADIAN
        && param.value == expected_degrees
}

fn linear_or_unity_param_matches(
    operation: &CrsCoordinateOperationInfo,
    code: &str,
    unit: ParamUnit,
    expected: f64,
) -> bool {
    let Some(param) = unique_epsg_param(operation, code) else {
        return false;
    };
    parse_param_value(param, unit) == Some(expected)
}

fn unique_epsg_param<'a>(
    operation: &'a CrsCoordinateOperationInfo,
    code: &str,
) -> Option<&'a OperationParameterInfo> {
    let mut found = None;
    for param in &operation.parameters {
        if param.authority.as_deref() != Some("EPSG") || param.code.as_deref() != Some(code) {
            continue;
        }
        if found.is_some() {
            return None;
        }
        if !param.unit_conversion_factor.is_finite() || param.unit_conversion_factor <= 0.0 {
            return None;
        }
        found = Some(param);
    }
    found
}

pub(super) fn local_projection_parameter(
    operation: &CrsCoordinateOperationInfo,
    code: &str,
    unit: ParamUnit,
) -> Option<f64> {
    parse_param_value(unique_epsg_param(operation, code)?, unit)
}

fn wgs84_utm_crs(zone: UtmZone, pole: GeodeticPole) -> String {
    let base: u32 = match pole {
        GeodeticPole::North => 32600,
        GeodeticPole::South => 32700,
    };
    format!("EPSG:{}", base.wrapping_add(u32::from(zone.get())))
}

/// Clockwise bearing in `[0, 360)` from an azimuth in `(-180, 180]`, the range
/// `atan2` and geographiclib produce. There the branch is bit-identical to
/// `rem_euclid(360)` without the `fmod`.
pub(crate) fn bearing_from_azimuth(azimuth: f64) -> f64 {
    if azimuth < 0.0 {
        azimuth + 360.0
    } else {
        azimuth
    }
}

pub(crate) fn normalize_longitude(longitude: f64) -> f64 {
    let shifted = longitude + 180.0;
    let normalized = if (0.0..360.0).contains(&shifted) {
        shifted - 180.0
    } else {
        shifted.rem_euclid(360.0) - 180.0
    };
    if normalized.to_bits() == (-180.0_f64).to_bits() {
        180.0
    } else {
        normalized
    }
}

fn utm_zone(lon: f64, lat: f64) -> UtmZone {
    if (56.0..64.0).contains(&lat) && (3.0..12.0).contains(&lon) {
        return UtmZone::try_new(32).expect("literal UTM zone is valid");
    }
    if (72.0..84.0).contains(&lat) {
        if (0.0..9.0).contains(&lon) {
            return UtmZone::try_new(31).expect("literal UTM zone is valid");
        }
        if (9.0..21.0).contains(&lon) {
            return UtmZone::try_new(33).expect("literal UTM zone is valid");
        }
        if (21.0..33.0).contains(&lon) {
            return UtmZone::try_new(35).expect("literal UTM zone is valid");
        }
        if (33.0..42.0).contains(&lon) {
            return UtmZone::try_new(37).expect("literal UTM zone is valid");
        }
    }
    let zone = (((lon + 180.0) / 6.0).floor().clamp(0.0, 59.0) as u32).wrapping_add(1);
    UtmZone::try_new(zone).expect("clamped UTM zone is valid")
}

#[cfg(test)]
mod metadata_tests {
    use super::*;
    use crate::error::{ErrorKind, ResourceError};

    fn epsg_param(code: &str, value: f64, factor: f64) -> OperationParameterInfo {
        OperationParameterInfo {
            name: None,
            authority: Some("EPSG".to_owned()),
            code: Some(code.to_owned()),
            value,
            value_string: None,
            unit_conversion_factor: factor,
            unit_name: None,
            unit_authority: None,
            unit_code: None,
            unit_category: None,
        }
    }

    fn operation(parameters: Vec<OperationParameterInfo>) -> CrsCoordinateOperationInfo {
        CrsCoordinateOperationInfo {
            name: None,
            definition: None,
            description: None,
            accuracy: None,
            has_inverse: false,
            has_ballpark_transformation: false,
            requires_coordinate_epoch: false,
            instantiable: true,
            method: None,
            parameters,
            grids: Vec::new(),
            steps: Vec::new(),
            area_of_use: None,
        }
    }

    fn operation_method(name: &str, code: &str) -> MethodInfo {
        MethodInfo {
            name: Some(name.to_owned()),
            authority: Some("EPSG".to_owned()),
            code: Some(code.to_owned()),
        }
    }

    fn operation_info(steps: Vec<CrsCoordinateOperationInfo>) -> OperationInfo {
        OperationInfo {
            name: None,
            definition: None,
            description: None,
            accuracy: None,
            has_inverse: true,
            has_ballpark_transformation: false,
            requires_coordinate_epoch: false,
            instantiable: true,
            method: None,
            parameters: Vec::new(),
            grids: Vec::new(),
            steps,
            area_of_use: None,
            source: "EPSG:4326".to_owned(),
            target: "EPSG:32634".to_owned(),
            source_epoch: None,
            target_epoch: None,
        }
    }

    fn prime_meridian(longitude: f64, unit_conversion_factor: f64) -> PrimeMeridianInfo {
        PrimeMeridianInfo {
            name: Some("selected prime meridian".to_owned()),
            longitude,
            unit_name: Some("test unit".to_owned()),
            unit_conversion_factor,
        }
    }

    #[expect(
        clippy::float_cmp,
        reason = "the fixture hunts bit-adjacent degree spellings whose radian products collide; a tolerance would hide the identity the matcher must reject"
    )]
    #[test]
    fn stored_degree_equality_rejects_adjacent_floats_that_share_a_radian_product() {
        let factor = crate::crs::DEGREE_TO_RADIAN;
        let op = |code: &str, value: f64| operation(vec![epsg_param(code, value, factor)]);
        assert!(angular_param_matches_degrees(
            &op("8802", 15.0),
            "8802",
            15.0
        ));
        assert!(angular_param_matches_degrees(&op("8801", 0.0), "8801", 0.0));
        for expected in [-117.0_f64, -15.0, 15.0, 123.0] {
            let up = expected.next_up();
            let down = expected.next_down();
            let adjacent = if up * factor == expected * factor {
                up
            } else if down * factor == expected * factor {
                down
            } else {
                panic!("{expected} has no adjacent float with the same radian product");
            };
            assert_ne!(adjacent, expected);
            assert!(!angular_param_matches_degrees(
                &op("8802", adjacent),
                "8802",
                expected
            ));
        }
        let min_sub = f64::from_bits(1);
        assert_eq!(min_sub * factor, 0.0);
        assert!(!angular_param_matches_degrees(
            &op("8801", min_sub),
            "8801",
            0.0
        ));
    }

    #[test]
    fn invalid_matched_param_is_not_replaced_by_a_later_duplicate() {
        let op = operation(vec![
            epsg_param("8802", 9.0, f64::NAN),
            epsg_param("8802", 9.0, crate::crs::DEGREE_TO_RADIAN),
        ]);
        assert!(unique_epsg_param(&op, "8802").is_none());
        assert!(!angular_param_matches_degrees(&op, "8802", 9.0));
    }

    #[test]
    fn prime_meridian_comparison_uses_selected_angular_meaning() {
        let standard_grad = std::f64::consts::TAU / 400.0;
        let source = prime_meridian(2.0, standard_grad);
        let target = prime_meridian(2.0_f64.next_down(), standard_grad.next_up());
        assert_eq!(
            source.longitude * source.unit_conversion_factor,
            target.longitude * target.unit_conversion_factor
        );
        assert!(!same_prime_meridian_values(Some(&source), Some(&target)));

        let same_selected_meridian = prime_meridian(2.0, standard_grad.next_up());
        assert!(same_prime_meridian_values(
            Some(&source),
            Some(&same_selected_meridian)
        ));
        assert!(same_prime_meridian_values(
            None,
            Some(&prime_meridian(0.0, standard_grad))
        ));

        let unit = AngularFactor::try_new("prime meridian unit", standard_grad).unwrap();
        let rounded_source = unit.to_radians(0.1);
        let rounded_target = unit.to_radians(0.1_f64.next_down());
        assert_eq!(rounded_source.to_bits(), 0x3F59_BC65_B68B_71C3);
        assert_eq!(rounded_target.to_bits(), 0x3F59_BC65_B68B_71C3);
        assert!(!same_prime_meridian_values(
            Some(&prime_meridian(0.1, standard_grad)),
            Some(&prime_meridian(0.1_f64.next_down(), standard_grad))
        ));

        assert!(same_prime_meridian_values(
            Some(&prime_meridian(2.0, standard_grad)),
            Some(&prime_meridian(1.5, std::f64::consts::TAU / 300.0))
        ));
        assert!(same_prime_meridian_values(
            Some(&prime_meridian(2.0, 0.01)),
            Some(&prime_meridian(1.0, 0.02))
        ));
        assert!(same_prime_meridian_values(
            Some(&prime_meridian(-100.0, standard_grad)),
            Some(&prime_meridian(-90.0, crate::crs::DEGREE_TO_RADIAN))
        ));
        let arbitrary_unit = prime_meridian(-16.0 * std::f64::consts::TAU, 1.0 / 256.0);
        let selected_grad = prime_meridian(-25.0, standard_grad);
        assert!(same_prime_meridian_values(
            Some(&selected_grad),
            Some(&arbitrary_unit)
        ));
        assert!(same_prime_meridian_values(
            Some(&arbitrary_unit),
            Some(&selected_grad)
        ));
        let negative_source = prime_meridian(-2.0, 0.01);
        let negative_equivalent = prime_meridian(-1.0, 0.02);
        assert!(same_prime_meridian_values(
            Some(&negative_source),
            Some(&negative_equivalent)
        ));
        assert!(!same_prime_meridian_values(
            Some(&prime_meridian(2.0, 0.01)),
            Some(&negative_source)
        ));
        assert!(same_prime_meridian_values(
            Some(&prime_meridian(100.0, standard_grad)),
            Some(&prime_meridian(std::f64::consts::FRAC_PI_2, 1.0))
        ));
    }

    #[test]
    fn datum_preservation_proves_the_typed_operation_tree_without_export_text() {
        let mut axis_change = operation(Vec::new());
        axis_change.method = Some(operation_method("Axis Order Reversal (2D)", "9843"));
        let mut projection = operation(Vec::new());
        projection.method = Some(operation_method("Transverse Mercator", "9807"));
        let permitted = operation_info(vec![axis_change.clone(), projection.clone()]);
        assert!(permitted.definition.is_none());
        assert!(operation_is_datum_preserving(&permitted, "9807"));

        let untyped_leaf = operation(Vec::new());
        let unowned = operation_info(vec![axis_change, projection, untyped_leaf]);
        assert!(!operation_is_datum_preserving(&unowned, "9807"));
    }

    #[test]
    fn custom_candidate_mapping_preserves_resource_errors_before_formatting() {
        let error = Error::from(ResourceError::SizeOverflow);
        let mapped = best_custom_candidate_error(error);
        assert!(matches!(
            mapped.kind(),
            ErrorKind::Resource(ResourceError::SizeOverflow)
        ));
    }
}

#[cfg(test)]
mod datum_preserving_tests {
    use super::super::certificate::LocalProjectionCertificate;
    use super::{
        LocalCrsDefinition, ensure_datum_preserving_local_operation, estimate_local_crs_at,
    };
    use crate::crs::runtime_state_snapshot;
    use crate::geographic::GeographicFrame;
    use crate::geometry::{CoordSeq, LineSeq, Point, Shape, XY};

    struct Case {
        name: &'static str,
        crs: &'static str,
        point: (f64, f64),
        expected_authority: Option<&'static str>,
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn selected_local_frames_preserve_the_source_datum() {
        let state = runtime_state_snapshot().unwrap();
        let cases = [
            Case {
                name: "WGS 84 UTM",
                crs: "EPSG:4326",
                point: (-74.0, 40.0),
                expected_authority: Some("EPSG:32618"),
            },
            Case {
                name: "WGS 84 3D horizontal projection",
                crs: "EPSG:4979",
                point: (-74.0, 40.0),
                expected_authority: Some("EPSG:32618"),
            },
            Case {
                name: "NAD83 datum-matched UTM",
                crs: "EPSG:4269",
                point: (-74.0, 41.0),
                expected_authority: Some("EPSG:26918"),
            },
            Case {
                name: "NAD83 polar custom TM",
                crs: "EPSG:4269",
                point: (0.0, -81.0),
                expected_authority: None,
            },
            Case {
                name: "NTF Paris grads custom TM",
                crs: "EPSG:4807",
                point: (0.0, 55.5),
                expected_authority: None,
            },
            Case {
                name: "ETRS89 datum-matched UTM",
                crs: "EPSG:4258",
                point: (10.0, 51.0),
                expected_authority: Some("EPSG:25832"),
            },
            Case {
                name: "GDA2020 datum-matched UTM",
                crs: "EPSG:7844",
                point: (151.2, -33.9),
                expected_authority: Some("EPSG:7856"),
            },
            Case {
                name: "authalic sphere custom TM",
                crs: "EPSG:4047",
                point: (0.0, 0.0),
                expected_authority: None,
            },
            Case {
                name: "WGS 84 polar UPS",
                crs: "EPSG:4326",
                point: (0.0, -81.0),
                expected_authority: Some("EPSG:5042"),
            },
        ];

        for case in cases {
            let shape = Shape::Point(Point::new_raw_xy(case.point.0, case.point.1));
            let local = estimate_local_crs_at(&[&shape], case.crs, &state)
                .unwrap_or_else(|error| panic!("{}: {error}", case.name));
            ensure_datum_preserving_local_operation(case.crs, &local.definition, &state)
                .unwrap_or_else(|error| {
                    panic!("{} selected {}: {error}", case.name, local.definition)
                });
            match case.expected_authority {
                Some(expected) => assert_eq!(local.definition, expected, "{}", case.name),
                None => assert_eq!(
                    local.display_name,
                    Some(super::LOCAL_CRS_DISPLAY_NAME),
                    "{}",
                    case.name,
                ),
            }
        }
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn pipeline_owner_rejects_the_previous_cross_datum_fallbacks() {
        let state = runtime_state_snapshot().unwrap();
        for (source, previous_target) in [("EPSG:4807", "EPSG:32631"), ("EPSG:4269", "EPSG:5042")] {
            assert!(
                ensure_datum_preserving_local_operation(source, previous_target, &state).is_err(),
                "{source} → {previous_target} must not hide a datum operation",
            );
        }
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn analytic_piece_counts_bound_the_600_km_parallel_at_both_budgets() {
        let west = -0.463_147_336_714_631_5;
        let east = 6.463_147_336_714_631_5;
        let points = CoordSeq::try_from_xy(&[
            XY::new(west, 39.0),
            XY::new(east, 39.0),
            XY::new(east, 39.2),
            XY::new(west, 39.2),
            XY::new(west, 39.0),
        ])
        .unwrap();
        let shape = Shape::LineString(LineSeq::try_new(points).unwrap());
        let state = runtime_state_snapshot().unwrap();
        let local = estimate_local_crs_at(&[&shape], "EPSG:4326", &state).unwrap();
        assert_eq!(local.definition, "EPSG:32631");
        let geodesic = geographiclib_rs::Geodesic::new(6_378_137.0, 1.0 / 298.257_223_563);
        let certificate = LocalProjectionCertificate::from_selected_frame(
            &local,
            &geodesic,
            GeographicFrame::crs84(),
            &state,
        )
        .unwrap();

        assert_eq!(
            certificate
                .piece_count(
                    Point::new_raw_xy(west, 39.0),
                    Point::new_raw_xy(east, 39.0),
                    0.1,
                )
                .unwrap()
                .get(),
            253
        );
        assert_eq!(
            certificate
                .piece_count(
                    Point::new_raw_xy(west, 39.0),
                    Point::new_raw_xy(east, 39.0),
                    1.0,
                )
                .unwrap()
                .get(),
            80
        );
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn arithmetic_screen_scans_straight_edges_without_allocating() {
        let points =
            CoordSeq::try_from_xy(&[XY::new(0.0, 0.0), XY::new(0.1, 0.0), XY::new(0.2, 0.0)])
                .unwrap();
        let shape = Shape::LineString(LineSeq::try_new(points.clone()).unwrap());
        let state = runtime_state_snapshot().unwrap();
        let local = estimate_local_crs_at(&[&shape], "EPSG:4326", &state).unwrap();
        let geodesic = geographiclib_rs::Geodesic::new(6_378_137.0, 1.0 / 298.257_223_563);
        let certificate = LocalProjectionCertificate::from_selected_frame(
            &local,
            &geodesic,
            GeographicFrame::crs84(),
            &state,
        )
        .unwrap();

        let (needs_refinement, allocations) = crate::test_support::allocation_count(|| {
            certificate.has_edges_to_refine(&points, 0.1).unwrap()
        });
        assert!(!needs_refinement);
        assert_eq!(allocations, 0);
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn spherical_transverse_mercator_has_finite_analytic_piece_counts() {
        let points = CoordSeq::try_from_xy(&[XY::new(-0.2, -0.1), XY::new(0.2, 0.2)]).unwrap();
        let shape = Shape::LineString(LineSeq::try_new(points).unwrap());
        let state = runtime_state_snapshot().unwrap();
        let local = estimate_local_crs_at(&[&shape], "EPSG:4047", &state).unwrap();
        let geodesic = geographiclib_rs::Geodesic::new(6_371_007.0, 0.0);
        let certificate = LocalProjectionCertificate::from_selected_frame(
            &local,
            &geodesic,
            GeographicFrame::crs84(),
            &state,
        )
        .unwrap();

        assert!(
            certificate
                .piece_count(
                    Point::new_raw_xy(-0.2, -0.1),
                    Point::new_raw_xy(0.2, 0.2),
                    0.1,
                )
                .unwrap()
                .get()
                > 1
        );
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn ups_pole_parallel_is_recognized_in_native_latitude_units() {
        let state = runtime_state_snapshot().unwrap();
        let local = LocalCrsDefinition::authority("EPSG:5042".to_owned());
        let geodesic = geographiclib_rs::Geodesic::new(6_378_137.0, 1.0 / 298.257_223_563);
        let certificate = LocalProjectionCertificate::from_selected_frame(
            &local,
            &geodesic,
            GeographicFrame::crs84(),
            &state,
        )
        .unwrap();

        assert_eq!(
            certificate
                .piece_count(
                    Point::new_raw_xy(0.0, -90.0),
                    Point::new_raw_xy(10.0, -90.0),
                    0.1,
                )
                .unwrap()
                .get(),
            1
        );
    }
}
