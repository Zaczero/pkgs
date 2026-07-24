//! The serde value model for whole GeoJSON documents (objects,
//! features, ragged coordinate payloads). Child module of [`super`].

use super::*;

pub(super) struct GeoJsonObject {
    pub(super) geometry_type: String,
    pub(super) coordinates: Option<JsonCoordinates>,
    pub(super) geometries: Option<Vec<GeoJsonGeometry>>,
    pub(super) geometry: FeatureGeometry,
    pub(super) features: Option<Vec<GeoJsonFeature>>,
    /// True when a ``properties`` member was present (value ignored for
    /// geometry decode). Used for RFC 7946 §7.1 cross-type exclusions.
    pub(super) has_properties: bool,
}

pub(super) struct GeoJsonGeometry(pub(super) Shape);

pub(super) struct GeoJsonFeature {
    pub(super) geometry: Option<Shape>,
}

pub(super) enum FeatureGeometry {
    Missing,
    Null,
    Geometry(GeoJsonGeometry),
}

pub(super) enum JsonCoordinates {
    Number(f64),
    Array(Vec<Self>),
}

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
///
/// - ``coordinates`` / ``geometries`` define Geometry → forbidden on Feature
///   and FeatureCollection
/// - ``geometry`` / ``properties`` define Feature → forbidden on
///   FeatureCollection and Geometry
/// - ``features`` defines FeatureCollection → forbidden on Feature and Geometry
///
/// Arbitrary other foreign members remain valid.
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
    // GeometryCollection is a Geometry but uses geometries, not coordinates.
    if geometry_type == "GeometryCollection" && members.has(DefiningMembers::COORDINATES) {
        return Err(IoError::geojson(
            "GeoJSON GeometryCollection must not contain a \"coordinates\" member \
             (RFC 7946 §7.1)",
        ));
    }
    // Coordinate-based geometries use coordinates, not geometries.
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

pub(super) fn defining_members_from_object(object: &GeoJsonObject) -> DefiningMembers {
    let mut members = DefiningMembers::default();
    if object.coordinates.is_some() {
        members.set(DefiningMembers::COORDINATES);
    }
    if object.geometries.is_some() {
        members.set(DefiningMembers::GEOMETRIES);
    }
    if !matches!(object.geometry, FeatureGeometry::Missing) {
        members.set(DefiningMembers::GEOMETRY);
    }
    if object.has_properties {
        members.set(DefiningMembers::PROPERTIES);
    }
    if object.features.is_some() {
        members.set(DefiningMembers::FEATURES);
    }
    members
}

impl<'de> Deserialize<'de> for GeoJsonGeometry {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let object = GeoJsonObject::deserialize(deserializer)?;
        geojson_object_to_shape(object)
            .map(Self)
            .map_err(de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for GeoJsonFeature {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let object = GeoJsonObject::deserialize(deserializer)?;
        if object.geometry_type != "Feature" {
            return Err(de::Error::custom(
                "GeoJSON FeatureCollection features must be Features",
            ));
        }
        // Nested Features in a FeatureCollection must still obey §7.1.
        reject_rfc7946_cross_type_members(
            &object.geometry_type,
            defining_members_from_object(&object),
        )
        .map_err(de::Error::custom)?;
        let geometry = feature_geometry(object.geometry).map_err(de::Error::custom)?;
        Ok(Self { geometry })
    }
}

impl<'de> Deserialize<'de> for GeoJsonObject {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(GeoJsonObjectVisitor)
    }
}

pub(super) struct GeoJsonObjectVisitor;

impl<'de> Visitor<'de> for GeoJsonObjectVisitor {
    type Value = GeoJsonObject;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a GeoJSON object")
    }

    fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        // Final-member (last-value-wins) policy for every recognized key,
        // including a shadowed non-string ``type`` that is later replaced by a
        // string (R18: str/bytes/dict agreement with serde_json map semantics).
        let mut geometry_type: Option<Value> = None;
        let mut coordinates = None;
        let mut geometries = None;
        let mut geometry = FeatureGeometry::Missing;
        let mut features = None;
        let mut has_properties = false;
        while let Some(key) = map.next_key::<Cow<'de, str>>()? {
            match key.as_ref() {
                "type" => geometry_type = Some(map.next_value()?),
                "coordinates" => coordinates = Some(map.next_value()?),
                "geometries" => geometries = Some(map.next_value()?),
                "geometry" => {
                    geometry = map
                        .next_value::<Option<GeoJsonGeometry>>()?
                        .map_or(FeatureGeometry::Null, FeatureGeometry::Geometry);
                },
                "features" => features = Some(map.next_value()?),
                "properties" => {
                    has_properties = true;
                    let _ = map.next_value::<IgnoredAny>()?;
                },
                // Legacy ``crs`` and other foreign members are ignored here.
                _ => {
                    let _ = map.next_value::<IgnoredAny>()?;
                },
            }
        }
        let geometry_type = match geometry_type {
            Some(Value::String(s)) => s,
            Some(_) => {
                return Err(de::Error::custom(
                    "invalid type: expected a string for GeoJSON type",
                ));
            },
            None => {
                return Err(de::Error::custom("GeoJSON geometry requires a type"));
            },
        };
        Ok(GeoJsonObject {
            geometry_type,
            coordinates,
            geometries,
            geometry,
            features,
            has_properties,
        })
    }
}

impl<'de> Deserialize<'de> for JsonCoordinates {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(JsonCoordinatesVisitor)
    }
}

pub(super) struct JsonCoordinatesVisitor;

/// Largest magnitude representable exactly as an ``f64`` (2^53).
const F64_MAX_EXACT_INT: i64 = 1_i64 << 53;

pub(super) fn reject_inexact_json_integer(value: i64) -> Result<()> {
    if !(-F64_MAX_EXACT_INT..=F64_MAX_EXACT_INT).contains(&value) {
        return Err(IoError::geojson(format!(
            "GeoJSON coordinate {value} exceeds f64 exact integer range"
        )));
    }
    Ok(())
}

pub(super) fn json_number_to_f64(number: &serde_json::Number) -> Result<f64> {
    if let Some(value) = number.as_i64() {
        reject_inexact_json_integer(value)?;
        return Ok(value as f64);
    }
    if let Some(value) = number.as_u64() {
        reject_inexact_json_integer(i64::try_from(value).unwrap_or(i64::MAX))?;
        return Ok(value as f64);
    }
    number
        .as_f64()
        .filter(|value| value.is_finite())
        .ok_or_else(|| IoError::geojson("coordinate must be numeric"))
}

impl<'de> Visitor<'de> for JsonCoordinatesVisitor {
    type Value = JsonCoordinates;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a GeoJSON coordinate array")
    }

    fn visit_f64<E>(self, value: f64) -> std::result::Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(JsonCoordinates::Number(value))
    }

    fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E>
    where
        E: de::Error,
    {
        reject_inexact_json_integer(value).map_err(E::custom)?;
        Ok(JsonCoordinates::Number(value as f64))
    }

    fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E>
    where
        E: de::Error,
    {
        reject_inexact_json_integer(i64::try_from(value).unwrap_or(i64::MAX)).map_err(E::custom)?;
        Ok(JsonCoordinates::Number(value as f64))
    }

    fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(value) = seq.next_element()? {
            values.push(value);
        }
        Ok(JsonCoordinates::Array(values))
    }
}
