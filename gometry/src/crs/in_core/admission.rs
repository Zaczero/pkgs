//! Conservative in-core transform admission — literal EPSG fast paths plus
//! descriptor-gated closed-form Transverse Mercator and Web Mercator.

use super::super::{
    CRS_IN_CORE_DESCRIPTOR_CACHE, CRS_IN_CORE_DESCRIPTOR_CACHE_CAPACITY,
    CrsCoordinateOperationInfo, CrsInfo, DEGREE_TO_RADIAN, EllipsoidInfo, PrimeMeridianInfo,
    TransformOptions, ensure_thread_caches_current, info, lru_resolve, normalize, parse_epsg,
};
use super::kernel::Ellipsoid;
use super::params::Projected;
use super::{InCoreStep, InCoreTransform, ProjSetup, UtmZone, lcc, stere, tmerc, webmerc};
use crate::error::Result;

pub(in crate::crs) struct CachedInCoreDescriptor {
    pub crs: String,
    pub descriptor: Option<InCoreDescriptor>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DatumKey {
    authority: String,
    code: String,
}

#[derive(Clone, Copy, Debug)]
enum InCoreCrsKind {
    Geographic,
    WebMercator(Projected<webmerc::WebMerc>),
    Tmerc(Projected<tmerc::Tmerc>),
    Lcc(Projected<lcc::Lcc>),
    PolarStereo(Projected<stere::PolarStereo>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::crs) enum StepDirection {
    Forward,
    Inverse,
}

#[derive(Clone, Debug)]
pub(in crate::crs) struct InCoreDescriptor {
    kind: InCoreCrsKind,
    datum: DatumKey,
}

pub(crate) fn try_in_core_transform(
    source: &str,
    target: &str,
    options: &TransformOptions,
) -> Option<InCoreTransform> {
    if !options.allows_in_core() {
        return None;
    }
    if let Some(transform) = try_literal_in_core(source, target) {
        return Some(transform);
    }
    let source_desc = resolve_in_core_descriptor(source).ok()??;
    let target_desc = resolve_in_core_descriptor(target).ok()??;
    if source_desc.datum != target_desc.datum {
        return None;
    }
    build_in_core_transform(&source_desc, &target_desc)
}

fn try_literal_in_core(source: &str, target: &str) -> Option<InCoreTransform> {
    if is_literal_wgs84_lonlat(source) && is_literal_web_mercator(target) {
        return Some(InCoreTransform::lon_lat_to_web_mercator());
    }
    if is_literal_web_mercator(source) && is_literal_wgs84_lonlat(target) {
        return Some(InCoreTransform::web_mercator_to_lon_lat());
    }
    if let Some(zone) = literal_utm_zone(target)
        && is_literal_wgs84_lonlat(source)
    {
        return Some(InCoreTransform::lon_lat_to_utm(zone));
    }
    if let Some(zone) = literal_utm_zone(source)
        && is_literal_wgs84_lonlat(target)
    {
        return Some(InCoreTransform::utm_to_lon_lat(zone));
    }
    if let Some(zone) = literal_utm_zone(target)
        && is_literal_web_mercator(source)
    {
        return Some(InCoreTransform::web_mercator_to_utm(zone));
    }
    if let Some(zone) = literal_utm_zone(source)
        && is_literal_web_mercator(target)
    {
        return Some(InCoreTransform::utm_to_web_mercator(zone));
    }
    None
}

fn build_in_core_transform(
    source: &InCoreDescriptor,
    target: &InCoreDescriptor,
) -> Option<InCoreTransform> {
    match (&source.kind, &target.kind) {
        (InCoreCrsKind::Geographic, InCoreCrsKind::Geographic) => None,
        (InCoreCrsKind::Geographic, target_kind) => Some(InCoreTransform::One(descriptor_to_step(
            target_kind,
            StepDirection::Forward,
        )?)),
        (source_kind, InCoreCrsKind::Geographic) => Some(InCoreTransform::One(descriptor_to_step(
            source_kind,
            StepDirection::Inverse,
        )?)),
        (source_kind, target_kind) => Some(InCoreTransform::Two {
            inverse: descriptor_to_setup(source_kind)?,
            forward: descriptor_to_setup(target_kind)?,
        }),
    }
}

fn descriptor_to_step(kind: &InCoreCrsKind, direction: StepDirection) -> Option<InCoreStep> {
    let setup = descriptor_to_setup(kind)?;
    Some(match direction {
        StepDirection::Forward => InCoreStep::Forward(setup),
        StepDirection::Inverse => InCoreStep::Inverse(setup),
    })
}

const fn descriptor_to_setup(kind: &InCoreCrsKind) -> Option<ProjSetup> {
    let setup = match kind {
        InCoreCrsKind::Geographic => return None,
        InCoreCrsKind::WebMercator(projected) => ProjSetup::WebMercator(*projected),
        InCoreCrsKind::Tmerc(projected) => ProjSetup::Tmerc(*projected),
        InCoreCrsKind::Lcc(projected) => ProjSetup::Lcc(*projected),
        InCoreCrsKind::PolarStereo(projected) => ProjSetup::PolarStereo(*projected),
    };
    Some(setup)
}

fn resolve_in_core_descriptor(crs: &str) -> Result<Option<InCoreDescriptor>> {
    let normalized = normalize(crs)?;
    ensure_thread_caches_current();
    CRS_IN_CORE_DESCRIPTOR_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let index = lru_resolve(
            &mut cache,
            CRS_IN_CORE_DESCRIPTOR_CACHE_CAPACITY,
            |item| item.crs == normalized,
            || {
                let descriptor = admit_in_core_descriptor(&normalized)?;
                Ok(CachedInCoreDescriptor {
                    crs: normalized.clone(),
                    descriptor,
                })
            },
        )?;
        Ok(cache[index].descriptor.clone())
    })
}

fn admit_in_core_descriptor(crs: &str) -> Result<Option<InCoreDescriptor>> {
    let info = info(crs)?;
    if !admit_base_crs(&info) {
        return Ok(None);
    }
    let Some(datum) = datum_key(&info) else {
        return Ok(None);
    };
    let celestial_body = info.celestial_body.as_deref().unwrap_or("Earth");
    if celestial_body != "Earth" {
        return Ok(None);
    }
    let kind = if info.is_geographic() {
        if !axes_are_geographic_degrees(&info) {
            return Ok(None);
        }
        InCoreCrsKind::Geographic
    } else if info.is_projected() {
        // Descriptor admission is keyed on PROJ `info()` operation parameters.
        // ProjJSON (and other non-authority definitions) can canonicalize to a
        // database EPSG record with different conversion params than the
        // definition PROJ actually transforms — refuse so the pipeline path
        // uses the caller's CRS string verbatim.
        if parse_epsg(crs).is_none() {
            return Ok(None);
        }
        if !axes_are_projected_metres(&info) {
            return Ok(None);
        }
        let Some(operation) = info.coordinate_operation.as_ref() else {
            return Ok(None);
        };
        if !admit_projected_operation(operation) {
            return Ok(None);
        }
        let Some(ellipsoid) = ellipsoid_from_info(info.ellipsoid.as_ref()) else {
            return Ok(None);
        };
        if let Some(projected) = webmerc::admit(operation, ellipsoid) {
            InCoreCrsKind::WebMercator(projected)
        } else if let Some(projected) = tmerc::admit(operation, ellipsoid) {
            InCoreCrsKind::Tmerc(projected)
        } else if let Some(projected) = lcc::admit(operation, ellipsoid) {
            InCoreCrsKind::Lcc(projected)
        } else if let Some(projected) = stere::admit(operation, ellipsoid) {
            InCoreCrsKind::PolarStereo(projected)
        } else {
            return Ok(None);
        }
    } else {
        return Ok(None);
    };
    Ok(Some(InCoreDescriptor { kind, datum }))
}

fn admit_base_crs(info: &CrsInfo) -> bool {
    if info.is_compound()
        || info.is_vertical()
        || info.is_geocentric()
        || info.is_engineering()
        || info.is_bound()
    {
        return false;
    }
    if !(info.is_geographic() || info.is_projected()) {
        return false;
    }
    if info.has_point_motion_operation {
        return false;
    }
    if let Some(datum) = info.datum.as_ref()
        && (datum.frame_reference_epoch.is_some() || datum.kind.contains("dynamic"))
    {
        return false;
    }
    prime_meridian_is_greenwich(info.prime_meridian.as_ref())
}

fn prime_meridian_is_greenwich(prime_meridian: Option<&PrimeMeridianInfo>) -> bool {
    let Some(prime_meridian) = prime_meridian else {
        return false;
    };
    let converted = prime_meridian.longitude * prime_meridian.unit_conversion_factor;
    converted.is_finite() && converted.abs() <= 1e-12
}

fn axes_are_geographic_degrees(info: &CrsInfo) -> bool {
    if info.axes.len() < 2 {
        return false;
    }
    info.axes.iter().take(2).all(|axis| {
        axis.unit_conversion_factor.is_finite()
            && (axis.unit_conversion_factor - DEGREE_TO_RADIAN).abs() <= 1e-12
            && is_horizontal_direction(axis.direction.as_deref())
    })
}

fn axes_are_projected_metres(info: &CrsInfo) -> bool {
    if info.axes.len() < 2 {
        return false;
    }
    info.axes.iter().take(2).all(|axis| {
        is_horizontal_direction(axis.direction.as_deref())
            && axis.unit_conversion_factor.is_finite()
            && (axis.unit_conversion_factor - 1.0).abs() <= 1e-9
    })
}

fn is_horizontal_direction(direction: Option<&str>) -> bool {
    matches!(
        direction,
        Some("east" | "west" | "north" | "south" | "geocentric X" | "geocentric Y")
    )
}

const fn admit_projected_operation(operation: &CrsCoordinateOperationInfo) -> bool {
    !operation.has_ballpark_transformation
        && !operation.requires_coordinate_epoch
        && operation.grids.is_empty()
        && operation.steps.is_empty()
}

fn ellipsoid_from_info(ellipsoid: Option<&EllipsoidInfo>) -> Option<Ellipsoid> {
    let ellipsoid = ellipsoid?;
    if !ellipsoid.semi_major_metre.is_finite() || !ellipsoid.inverse_flattening.is_finite() {
        return None;
    }
    if ellipsoid.semi_major_metre <= 0.0 || ellipsoid.inverse_flattening <= 0.0 {
        return None;
    }
    Some(Ellipsoid::from_a_f(
        ellipsoid.semi_major_metre,
        1.0 / ellipsoid.inverse_flattening,
    ))
}

fn datum_key(info: &CrsInfo) -> Option<DatumKey> {
    if let Some(datum) = info.horizontal_datum.as_ref() {
        return datum_key_from_authority(datum.authority.as_deref(), datum.code.as_deref());
    }
    info.datum.as_ref().and_then(|datum| {
        datum_key_from_authority(datum.authority.as_deref(), datum.code.as_deref())
    })
}

fn datum_key_from_authority(authority: Option<&str>, code: Option<&str>) -> Option<DatumKey> {
    let authority = authority?;
    let code = code?;
    if authority.is_empty() || code.is_empty() {
        return None;
    }
    Some(DatumKey {
        authority: authority.to_owned(),
        code: code.to_owned(),
    })
}

pub(crate) fn is_literal_wgs84_lonlat(value: &str) -> bool {
    matches!(
        value,
        "EPSG:4326" | "EPSG:4979" | "OGC:CRS84" | "CRS84" | "urn:ogc:def:crs:OGC:1.3:CRS84"
    )
}

pub(crate) fn is_literal_web_mercator(value: &str) -> bool {
    matches!(
        super::super::parse_epsg(value),
        Some(3857 | 900_913 | 3785 | 102_100)
    )
}

fn literal_utm_zone(value: &str) -> Option<UtmZone> {
    UtmZone::from_crs(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crs::TransformOptions;

    #[test]
    fn admits_etrs89_utm_pair() {
        let options = TransformOptions::default();
        assert!(admit_in_core_descriptor("EPSG:4258").unwrap().is_some());
        assert!(admit_in_core_descriptor("EPSG:25832").unwrap().is_some());
        assert!(try_in_core_transform("EPSG:4258", "EPSG:25832", &options).is_some());
    }

    #[test]
    fn admits_epsg_102100_web_mercator_alias() {
        assert!(admit_in_core_descriptor("EPSG:102100").unwrap().is_some());
    }

    #[test]
    fn admits_nad83_spcs_tm_pair() {
        let options = TransformOptions::default();
        assert!(admit_in_core_descriptor("EPSG:4269").unwrap().is_some());
        assert!(admit_in_core_descriptor("EPSG:32145").unwrap().is_some());
        assert!(try_in_core_transform("EPSG:4269", "EPSG:32145", &options).is_some());
    }
}
