//! Local conformal projection planning.
//!
//! The planner is deliberately extent-aware: every candidate is checked with
//! PROJ factors over the receiver's vertices and its spherical bounding frame.
//! A projection is returned only when its worst linear scale error is at most
//! [`LOCAL_SCALE_ERROR_LIMIT`].

use std::borrow::Cow;

use super::catalog::catalog_with_state;
use crate::crs::in_core::{DatumKey, ParamUnit, Pole, datum_key, parse_param_value};
use crate::crs::{
    AreaOfInterest, CrsCatalogOptions, CrsCoordinateOperationInfo, CrsError, CrsInfo,
    CrsObjectKind, OperationParameterInfo, RuntimeState, Shape, factors_at, info_with_state,
    is_wgs84_lonlat, normalize, parse_epsg, to_projjson_with_options_at, transform_at, utm_crs,
};
use crate::error::{Error, ErrorKind, Result};

/// Maximum admitted meridional or parallel scale error (0.1%).
pub(crate) const LOCAL_SCALE_ERROR_LIMIT: f64 = 0.001;
const LOCAL_CRS_DISPLAY_NAME: &str = "Local conformal CRS";

#[derive(Debug)]
pub(crate) struct LocalCrsDefinition {
    pub(crate) definition: String,
    pub(crate) display_name: Option<&'static str>,
}

impl LocalCrsDefinition {
    fn authority(definition: String) -> Self {
        Self {
            definition,
            display_name: None,
        }
    }

    fn custom(definition: String) -> Self {
        Self {
            definition,
            display_name: Some(LOCAL_CRS_DISPLAY_NAME),
        }
    }

    pub(crate) fn annotate_error(&self, mut error: Error) -> Error {
        error.annotate_crs_display(&self.definition, self.display_name);
        error
    }
}

/// Estimate one conformal metric CRS for all coordinates in `shape`.
///
/// `source_crs` is mandatory by design: interpreting unframed coordinates as
/// longitude/latitude would turn a convenience method into a relabeling trap.
pub(crate) fn estimate_local_crs(shape: &Shape, source_crs: &str) -> Result<String> {
    let state = crate::crs::runtime_state_snapshot()?;
    Ok(estimate_local_crs_at(shape, source_crs, &state)?.definition)
}

pub(crate) fn estimate_local_crs_at(
    shape: &Shape,
    source_crs: &str,
    state: &RuntimeState,
) -> Result<LocalCrsDefinition> {
    let lonlat: Cow<'_, Shape> = if is_wgs84_lonlat(source_crs) {
        Cow::Borrowed(shape)
    } else {
        Cow::Owned(transform_at(shape, source_crs, "EPSG:4326", state)?)
    };
    let extent = LocalExtent::from_shape(&lonlat)?.ok_or(CrsError::EmptyGeometry)?;

    let pole = if extent.latitude >= 0.0 {
        Pole::North
    } else {
        Pole::South
    };
    if extent.latitude > 84.0 || extent.latitude < -80.0 {
        let ups = match pole {
            Pole::North => "EPSG:5041",
            Pole::South => "EPSG:5042",
        };
        if candidate_fits(ups, &extent.samples, state)? {
            return Ok(LocalCrsDefinition::authority(ups.to_owned()));
        }
        return best_custom_candidate(source_crs, &extent, state);
    }

    let utm = best_utm_from_anchor(
        LocalProjectionAnchor {
            longitude: extent.longitude,
            latitude: extent.latitude,
        },
        source_crs,
        state,
    )?;
    if candidate_fits(&utm, &extent.samples, state)? {
        return Ok(LocalCrsDefinition::authority(utm));
    }

    best_custom_candidate(source_crs, &extent, state).map_err(best_custom_candidate_error)
}

fn best_custom_candidate_error(error: crate::error::Error) -> crate::error::Error {
    if matches!(error.kind(), ErrorKind::Resource(_)) {
        return error;
    }
    CrsError::invalid(format!(
        "geometry cannot be represented by one local conformal CRS within the {:.1}% linear scale-error limit; split the geometry or choose a projected CRS explicitly",
        LOCAL_SCALE_ERROR_LIMIT * 100.0
    ))
}

pub(crate) fn local_crs_fits_at(
    shape: &Shape,
    source_crs: &str,
    candidate: &str,
    state: &RuntimeState,
) -> Result<bool> {
    let lonlat: Cow<'_, Shape> = if is_wgs84_lonlat(source_crs) {
        Cow::Borrowed(shape)
    } else {
        Cow::Owned(transform_at(shape, source_crs, "EPSG:4326", state)?)
    };
    let Some(extent) = LocalExtent::from_shape(&lonlat)? else {
        return Ok(true);
    };
    candidate_fits(candidate, &extent.samples, state)
}

#[derive(Debug)]
struct LocalExtent {
    longitude: f64,
    latitude: f64,
    samples: Vec<(f64, f64)>,
}

impl LocalExtent {
    fn from_shape(shape: &Shape) -> Result<Option<Self>> {
        let mut points = Vec::with_capacity(shape.coord_count());
        shape.for_each_point(|point| {
            points.push((normalize_longitude(point.x), point.y));
        });
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
    let mut largest_gap = f64::NEG_INFINITY;
    let mut gap_start = 0;
    for index in 0..longitudes.len() {
        let next = if index + 1 == longitudes.len() {
            longitudes[0] + 360.0
        } else {
            longitudes[index + 1]
        };
        let gap = next - longitudes[index];
        if gap > largest_gap {
            largest_gap = gap;
            gap_start = index;
        }
    }
    let west = longitudes[(gap_start + 1) % longitudes.len()];
    let mut east = longitudes[gap_start];
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
    let base = info
        .geodetic_crs
        .as_ref()
        .map_or(source_crs, |geodetic| geodetic.crs.as_str());
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
            return Ok(LocalCrsDefinition::custom(normalize(&candidate)?));
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
) -> Result<String> {
    let zone = utm_zone(local.longitude, local.latitude);
    let pole = if local.latitude >= 0.0 {
        Pole::North
    } else {
        Pole::South
    };
    if !has_wgs84_datum_fast_path(crs) {
        let info = info_with_state(crs, state)?;
        if let Some(source_datum) = datum_key(&info)
            && let Some(candidate) = best_utm_from_catalog(
                &source_datum,
                local.longitude,
                local.latitude,
                zone,
                pole,
                state,
            )?
        {
            return Ok(candidate);
        }
    }
    Ok(wgs84_utm_crs(zone, pole))
}

fn has_wgs84_datum_fast_path(crs: &str) -> bool {
    is_wgs84_lonlat(crs) || parse_epsg(crs) == Some(3857) || utm_crs(crs).is_some()
}

fn best_utm_from_catalog(
    source_datum: &DatumKey,
    lon: f64,
    lat: f64,
    zone: UtmZone,
    pole: Pole,
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

fn is_east_north_metre_axes(info: &CrsInfo) -> bool {
    let [east, north] = info.axes.as_slice() else {
        return false;
    };
    east.direction.as_deref() == Some("east")
        && north.direction.as_deref() == Some("north")
        && east.unit_conversion_factor == 1.0
        && north.unit_conversion_factor == 1.0
}

fn is_utm_operation(info: &CrsInfo, zone: UtmZone, pole: Pole) -> bool {
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
        Pole::North => 0.0,
        Pole::South => 10_000_000.0,
    };
    angular_param_matches_degrees(operation, "8801", 0.0)
        && angular_param_matches_degrees(operation, "8802", expected_lon0)
        && linear_or_unity_param_matches(operation, "8805", ParamUnit::Unity, super::UTM_K0)
        && linear_or_unity_param_matches(operation, "8806", ParamUnit::Metre, 500_000.0)
        && linear_or_unity_param_matches(operation, "8807", ParamUnit::Metre, expected_northing)
}

fn angular_param_matches_degrees(
    operation: &CrsCoordinateOperationInfo,
    code: &str,
    expected_degrees: f64,
) -> bool {
    let Some(param) = unique_epsg_param(operation, code) else {
        return false;
    };
    param.value.is_finite()
        && param.unit_conversion_factor == super::DEGREE_TO_RADIAN
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

fn wgs84_utm_crs(zone: UtmZone, pole: Pole) -> String {
    let base = match pole {
        Pole::North => 32600,
        Pole::South => 32700,
    };
    format!("EPSG:{}", base + u32::from(zone.get()))
}

pub(super) fn normalize_longitude(longitude: f64) -> f64 {
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
    let zone = ((lon + 180.0) / 6.0).floor().clamp(0.0, 59.0) as u32 + 1;
    UtmZone::try_new(zone).expect("clamped UTM zone is valid")
}

#[cfg(test)]
mod metadata_tests {
    use super::*;
    use crate::error::{Error, ErrorKind, ResourceError};
    use crate::test_support::fail_allocations_after;

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

    #[test]
    fn stored_degree_equality_rejects_adjacent_floats_that_share_a_radian_product() {
        let factor = super::super::DEGREE_TO_RADIAN;
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
            epsg_param("8802", 9.0, super::super::DEGREE_TO_RADIAN),
        ]);
        assert!(unique_epsg_param(&op, "8802").is_none());
        assert!(!angular_param_matches_degrees(&op, "8802", 9.0));
    }

    #[test]
    fn custom_candidate_mapping_preserves_resource_errors_before_formatting() {
        let error = Error::from(ResourceError::Allocation);
        let mapped = fail_allocations_after(0, || best_custom_candidate_error(error));
        assert!(matches!(
            mapped.kind(),
            ErrorKind::Resource(ResourceError::Allocation)
        ));
    }
}
