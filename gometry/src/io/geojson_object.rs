//! Shared GeoJSON object rules and numeric conversion helpers.

use serde_json::Value;

use crate::io::{IoError, Result};

/// Presence of RFC 7946 defining members used for §7.1 cross-type checks.
/// Packed bit flags (one bool each would trip `struct_excessive_bools`).
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct DefiningMembers(u8);

impl DefiningMembers {
    pub(crate) const COORDINATES: u8 = 1 << 0;
    pub(crate) const GEOMETRIES: u8 = 1 << 1;
    pub(crate) const GEOMETRY: u8 = 1 << 2;
    pub(crate) const PROPERTIES: u8 = 1 << 3;
    pub(crate) const FEATURES: u8 = 1 << 4;

    pub(crate) const fn set(&mut self, flag: u8) {
        self.0 |= flag;
    }

    pub(crate) const fn has(self, flag: u8) -> bool {
        self.0 & flag != 0
    }
}

/// RFC 7946 §7.1: defining members of other GeoJSON types MUST NOT appear.
pub(crate) fn reject_rfc7946_cross_type_members(
    geometry_type: &str,
    members: DefiningMembers,
) -> Result<()> {
    let is_feature = geometry_type == "Feature";
    let is_feature_collection = geometry_type == "FeatureCollection";
    let is_geometry = !is_feature && !is_feature_collection;

    if (is_feature || is_feature_collection)
        && (members.has(DefiningMembers::COORDINATES) || members.has(DefiningMembers::GEOMETRIES))
    {
        return Err(IoError::geojson(format!(
            "GeoJSON {geometry_type} must not contain a \"coordinates\" or \"geometries\" member \
             (RFC 7946 §7.1)"
        )));
    }
    if (is_feature_collection || is_geometry)
        && (members.has(DefiningMembers::GEOMETRY) || members.has(DefiningMembers::PROPERTIES))
    {
        return Err(IoError::geojson(format!(
            "GeoJSON {geometry_type} must not contain a \"geometry\" or \"properties\" member \
             (RFC 7946 §7.1)"
        )));
    }
    if (is_feature || is_geometry) && members.has(DefiningMembers::FEATURES) {
        return Err(IoError::geojson(format!(
            "GeoJSON {geometry_type} must not contain a \"features\" member (RFC 7946 §7.1)"
        )));
    }
    if geometry_type == "GeometryCollection" && members.has(DefiningMembers::COORDINATES) {
        return Err(IoError::geojson(
            "GeoJSON GeometryCollection must not contain a \"coordinates\" member \
             (RFC 7946 §7.1)",
        ));
    }
    if is_geometry
        && geometry_type != "GeometryCollection"
        && members.has(DefiningMembers::GEOMETRIES)
    {
        return Err(IoError::geojson(format!(
            "GeoJSON {geometry_type} must not contain a \"geometries\" member (RFC 7946 §7.1)"
        )));
    }
    Ok(())
}

pub(crate) fn validate_geojson_bbox(value: &Value) -> Result<()> {
    let Some(bounds) = value.as_array() else {
        return Err(IoError::geojson(
            "GeoJSON bbox must contain 4 or 6 finite numbers",
        ));
    };
    if !matches!(bounds.len(), 4 | 6) {
        return Err(IoError::geojson(
            "GeoJSON bbox must contain 4 or 6 finite numbers",
        ));
    }
    for bound in bounds {
        let Value::Number(number) = bound else {
            return Err(IoError::geojson(
                "GeoJSON bbox must contain 4 or 6 finite numbers",
            ));
        };
        json_number_to_f64(number)
            .map_err(|_| IoError::geojson("GeoJSON bbox must contain 4 or 6 finite numbers"))?;
    }
    Ok(())
}

pub(crate) fn validate_geojson_bbox_member(object: &serde_json::Map<String, Value>) -> Result<()> {
    if let Some(bbox) = object.get("bbox") {
        validate_geojson_bbox(bbox)?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
pub(crate) enum GeoJsonMemberKind {
    Missing,
    Null,
    Object,
    String,
    Number,
    NonFinite,
    Other,
}

pub(crate) fn validate_geojson_feature_properties(kind: GeoJsonMemberKind) -> Result<()> {
    if matches!(
        kind,
        GeoJsonMemberKind::Missing | GeoJsonMemberKind::Null | GeoJsonMemberKind::Object
    ) {
        Ok(())
    } else {
        Err(IoError::geojson(
            "feature properties must be a mapping or None",
        ))
    }
}

pub(crate) fn validate_geojson_feature_id(kind: GeoJsonMemberKind) -> Result<()> {
    match kind {
        GeoJsonMemberKind::Missing
        | GeoJsonMemberKind::Null
        | GeoJsonMemberKind::String
        | GeoJsonMemberKind::Number => Ok(()),
        GeoJsonMemberKind::NonFinite => Err(IoError::geojson("JSON number must be finite")),
        GeoJsonMemberKind::Object | GeoJsonMemberKind::Other => Err(IoError::geojson(
            "feature id must be a string or finite number",
        )),
    }
}

pub(crate) fn validate_geojson_feature_members(
    object: &serde_json::Map<String, Value>,
) -> Result<()> {
    let properties = match object.get("properties") {
        None => GeoJsonMemberKind::Missing,
        Some(Value::Null) => GeoJsonMemberKind::Null,
        Some(Value::Object(_)) => GeoJsonMemberKind::Object,
        Some(_) => GeoJsonMemberKind::Other,
    };
    validate_geojson_feature_properties(properties)?;

    let id = match object.get("id") {
        None => GeoJsonMemberKind::Missing,
        Some(Value::Null) => GeoJsonMemberKind::Null,
        Some(Value::String(_)) => GeoJsonMemberKind::String,
        Some(Value::Number(_)) => GeoJsonMemberKind::Number,
        Some(_) => GeoJsonMemberKind::Other,
    };
    validate_geojson_feature_id(id)?;
    validate_geojson_bbox_member(object)
}

/// Admit a signed integer coordinate only when it is exactly representable as
/// binary64. Non-exact integers are rejected rather than silently rounded.
pub(crate) fn i64_to_exact_f64(value: i64) -> Result<f64> {
    let float = value as f64;
    if float as i128 == i128::from(value) {
        Ok(float)
    } else {
        Err(IoError::geojson(format!(
            "GeoJSON coordinate {value} is not exactly representable as f64"
        )))
    }
}

/// Same exactness rule for unsigned JSON integer tokens.
pub(crate) fn u64_to_exact_f64(value: u64) -> Result<f64> {
    let float = value as f64;
    if float as u128 == u128::from(value) {
        Ok(float)
    } else {
        Err(IoError::geojson(format!(
            "GeoJSON coordinate {value} is not exactly representable as f64"
        )))
    }
}

pub(crate) fn json_number_to_f64(number: &serde_json::Number) -> Result<f64> {
    if let Some(value) = number.as_i64() {
        return i64_to_exact_f64(value);
    }
    if let Some(value) = number.as_u64() {
        return u64_to_exact_f64(value);
    }
    number
        .as_f64()
        .filter(|value| value.is_finite())
        .ok_or_else(|| IoError::geojson("coordinate must be numeric"))
}

#[cfg(test)]
mod exact_int_cast_tests {
    use super::*;

    #[test]
    fn rejects_i64_max_cast_saturation_edge() {
        i64_to_exact_f64(i64::MAX).unwrap_err();
    }

    #[test]
    fn rejects_u64_max_cast_saturation_edge() {
        u64_to_exact_f64(u64::MAX).unwrap_err();
    }

    #[test]
    fn admits_i64_min_power_of_two() {
        let f = i64_to_exact_f64(i64::MIN).expect("i64::MIN is exact");
        assert_eq!(f.to_bits(), (i64::MIN as f64).to_bits());
    }

    #[test]
    fn admits_exact_beyond_2_pow_53() {
        let a = i64_to_exact_f64((1_i64 << 53) + 2).unwrap();
        assert_eq!(a.to_bits(), (((1_i64 << 53) + 2) as f64).to_bits());
        let b = u64_to_exact_f64(1_u64 << 60).unwrap();
        assert_eq!(b.to_bits(), ((1_u64 << 60) as f64).to_bits());
    }

    #[test]
    fn rejects_inexact_beyond_2_pow_53() {
        i64_to_exact_f64((1_i64 << 53) + 1).unwrap_err();
        u64_to_exact_f64((1_u64 << 53) + 1).unwrap_err();
    }
}
