//! Shared `GeoArrow` logical encoding: the 7 geometry kinds and the
//! extension-name <-> kind mapping used by both the `PyArrow` path
//! (`arrow.rs`) and the C Data Interface path (`arrow_c.rs`).
//!
//! Ordinate/storage classification for geoarrow point structs is also here so
//! every frontend (PyArrow, Arrow-C array, stream) shares one rule set before
//! any coordinate buffer is read.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GeometryEncoding {
    Point,
    MultiPoint,
    LineString,
    MultiLineString,
    Polygon,
    MultiPolygon,
    Wkb,
}

impl GeometryEncoding {
    pub(crate) const fn extension_name(self) -> &'static str {
        match self {
            Self::Point => "geoarrow.point",
            Self::MultiPoint => "geoarrow.multipoint",
            Self::LineString => "geoarrow.linestring",
            Self::MultiLineString => "geoarrow.multilinestring",
            Self::Polygon => "geoarrow.polygon",
            Self::MultiPolygon => "geoarrow.multipolygon",
            Self::Wkb => "geoarrow.wkb",
        }
    }
    pub(crate) fn from_extension_name(name: &str) -> Option<Self> {
        Some(match name {
            "geoarrow.point" => Self::Point,
            "geoarrow.multipoint" => Self::MultiPoint,
            "geoarrow.linestring" => Self::LineString,
            "geoarrow.multilinestring" => Self::MultiLineString,
            "geoarrow.polygon" => Self::Polygon,
            "geoarrow.multipolygon" => Self::MultiPolygon,
            "geoarrow.wkb" => Self::Wkb,
            _ => return None,
        })
    }
    /// Shared error catalogue for "expected a geoarrow.* extension array".
    pub(crate) const EXPECTED_EXTENSION: &'static str = "expected a 'geoarrow.point', 'geoarrow.multipoint', 'geoarrow.linestring', \
         'geoarrow.multilinestring', 'geoarrow.polygon', 'geoarrow.multipolygon', or \
         'geoarrow.wkb' extension array";
}

/// Result of classifying a geoarrow point-struct field list.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct GeoarrowOrdinateLayout {
    pub(crate) has_z: bool,
    pub(crate) has_m: bool,
}

/// Mandatory ordinate/storage classifier for geoarrow coordinate structs.
///
/// Every frontend must run this **before** reading any coordinate buffer.
/// Rules (GeoArrow interleaved-struct storage):
/// - every leaf is exact float64
/// - exactly one `x` and one `y`
/// - at most one `z` and one `m`
/// - no unsupported / extra / duplicate field names
///
/// `fields` is `(name, is_float64)` in schema order.
pub(crate) fn classify_geoarrow_ordinates(
    fields: impl IntoIterator<Item = (impl AsRef<str>, bool)>,
) -> Result<GeoarrowOrdinateLayout, &'static str> {
    let mut n_x = 0_u8;
    let mut n_y = 0_u8;
    let mut n_z = 0_u8;
    let mut n_m = 0_u8;
    let mut any = false;
    for (name, is_float64) in fields {
        any = true;
        if !is_float64 {
            return Err("geoarrow point ordinate children must be float64");
        }
        match name.as_ref() {
            "x" => n_x = n_x.saturating_add(1),
            "y" => n_y = n_y.saturating_add(1),
            "z" => n_z = n_z.saturating_add(1),
            "m" => n_m = n_m.saturating_add(1),
            _ => {
                return Err(
                    "geoarrow point struct allows only x, y, optional z, and optional m fields",
                );
            },
        }
    }
    if !any || n_x != 1 || n_y != 1 {
        return Err("geoarrow point struct requires exactly one x and one y field");
    }
    if n_z > 1 || n_m > 1 {
        return Err("geoarrow point struct allows at most one z and one m field");
    }
    Ok(GeoarrowOrdinateLayout {
        has_z: n_z == 1,
        has_m: n_m == 1,
    })
}
