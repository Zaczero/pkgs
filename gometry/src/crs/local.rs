#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Local conformal projection planning.
//!
//! The planner is deliberately extent-aware: every candidate is checked with
//! PROJ factors over the receiver's vertices and its spherical bounding frame.
//! A projection is returned only when its worst linear scale error is at most
//! [`LOCAL_SCALE_ERROR_LIMIT`].

use std::borrow::Cow;

use super::*;
use crate::crs::in_core::Pole;
use crate::error::Result;

/// Maximum admitted meridional or parallel scale error (0.1%).
pub(crate) const LOCAL_SCALE_ERROR_LIMIT: f64 = 0.001;

/// Estimate one conformal metric CRS for all coordinates in `shape`.
///
/// `source_crs` is mandatory by design: interpreting unframed coordinates as
/// longitude/latitude would turn a convenience method into a relabeling trap.
pub(crate) fn estimate_local_crs(shape: &Shape, source_crs: &str) -> Result<String> {
    let lonlat: Cow<'_, Shape> = if is_wgs84_lonlat(source_crs) {
        Cow::Borrowed(shape)
    } else {
        Cow::Owned(transform(shape, source_crs, "EPSG:4326")?)
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
        if candidate_fits(ups, &extent.samples)? {
            return Ok(ups.to_owned());
        }
        return best_custom_candidate(source_crs, &extent);
    }

    let utm = best_utm_from_anchor(
        LocalProjectionAnchor {
            longitude: extent.longitude,
            latitude: extent.latitude,
        },
        source_crs,
    )?;
    if candidate_fits(&utm, &extent.samples)? {
        return Ok(utm);
    }

    best_custom_candidate(source_crs, &extent).map_err(|_| {
        CrsError::invalid(format!(
            "geometry cannot be represented by one local conformal CRS within the {:.1}% linear scale-error limit; split the geometry or choose a projected CRS explicitly",
            LOCAL_SCALE_ERROR_LIMIT * 100.0
        ))
    })
}

/// Whether `candidate` satisfies the local scale-error contract over `shape`.
pub(crate) fn local_crs_fits(shape: &Shape, source_crs: &str, candidate: &str) -> Result<bool> {
    let lonlat: Cow<'_, Shape> = if is_wgs84_lonlat(source_crs) {
        Cow::Borrowed(shape)
    } else {
        Cow::Owned(transform(shape, source_crs, "EPSG:4326")?)
    };
    let Some(extent) = LocalExtent::from_shape(&lonlat)? else {
        return Ok(true);
    };
    candidate_fits(candidate, &extent.samples)
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

fn candidate_fits(candidate: &str, samples: &[(f64, f64)]) -> Result<bool> {
    for &(longitude, latitude) in samples {
        let factor = factors(candidate, longitude, latitude, false)?;
        let error = (factor.meridional_scale - 1.0)
            .abs()
            .max((factor.parallel_scale - 1.0).abs());
        if !error.is_finite() || error > LOCAL_SCALE_ERROR_LIMIT {
            return Ok(false);
        }
    }
    Ok(true)
}

fn best_custom_candidate(source_crs: &str, extent: &LocalExtent) -> Result<String> {
    let info = info(source_crs)?;
    let base = info
        .geodetic_crs
        .as_ref()
        .map_or(source_crs, |geodetic| geodetic.crs.as_str());
    let mut base_crs: serde_json::Value =
        serde_json::from_str(&to_projjson(base)?).map_err(|e| {
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
        "name": "Local conformal CRS",
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
        if candidate_fits(&candidate, &extent.samples)? {
            return normalize(&candidate);
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

fn best_utm_from_anchor(local: LocalProjectionAnchor, crs: &str) -> Result<String> {
    let zone = utm_zone(local.longitude, local.latitude);
    let pole = if local.latitude >= 0.0 {
        Pole::North
    } else {
        Pole::South
    };
    if !has_wgs84_datum_fast_path(crs)
        && let Some(source_datum) = source_utm_datum_name(crs)?
        && !is_wgs84_utm_datum_name(&source_datum)
        && let Some(candidate) =
            best_utm_from_catalog(&source_datum, local.longitude, local.latitude, zone, pole)?
    {
        return Ok(candidate);
    }
    Ok(wgs84_utm_crs(zone, pole))
}

fn has_wgs84_datum_fast_path(crs: &str) -> bool {
    is_wgs84_lonlat(crs) || parse_epsg(crs) == Some(3857) || utm_crs(crs).is_some()
}

fn source_utm_datum_name(crs: &str) -> Result<Option<String>> {
    let info = info(crs)?;
    Ok(info
        .geodetic_crs
        .as_ref()
        .and_then(|geodetic| geodetic.name.clone())
        .or_else(|| info.datum.as_ref().and_then(|datum| datum.name.clone())))
}

fn is_wgs84_utm_datum_name(name: &str) -> bool {
    matches!(name, "WGS 84" | "World Geodetic System 1984 ensemble")
}

fn best_utm_from_catalog(
    datum_name: &str,
    lon: f64,
    lat: f64,
    zone: UtmZone,
    pole: Pole,
) -> Result<Option<String>> {
    let items = utm_zones(&UtmCatalogOptions {
        datum_name: Some(datum_name.to_owned()),
        area: Some(AreaOfInterest {
            west: lon,
            south: lat,
            east: lon,
            north: lat,
        }),
        contains_area: false,
        allow_deprecated: false,
    })?;
    Ok(items
        .into_iter()
        .find(|item| {
            item.name.as_deref().is_some_and(|name| {
                let Some((_, suffix)) = name.rsplit_once(" / UTM zone ") else {
                    return false;
                };
                let zone_text = zone.get().to_string();
                let Some(hemisphere) = suffix.strip_prefix(&zone_text) else {
                    return false;
                };
                matches!(
                    (pole, hemisphere.chars().next()),
                    (Pole::North, Some('N')) | (Pole::South, Some('S'))
                )
            })
        })
        .map(|item| item.crs))
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
