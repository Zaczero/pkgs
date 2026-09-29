use crate::io::{
    CoordSeq, CoordinateAxes, Coordinates, EWKB_M_FLAG, EWKB_SRID_FLAG, EWKB_Z_FLAG,
    GeometryErrorKind, GeometryKind, IoError, LineSeq, MAX_NESTING, ParseFormat, Point, Polygon,
    Result, Ring, Shape, WKB_M_OFFSET, WKB_Z_OFFSET, WKB_ZM_OFFSET, WkbGeometry,
    extended_srid_code, parse_content,
};

const WKB_HEADER_BASE: usize = 1 + 4; // byte-order byte + 4-byte geometry type
const WKB_COUNT: usize = 4; // 4-byte element/ring count

/// The table must expand both ID directions from the same rows, so a geometry
/// kind cannot acquire a writer ID without also becoming a reader kind.
///
/// This macro emits definitions only; it performs no caller-side control flow.
macro_rules! wkb_geometry_types {
    ($( $kind:ident => $id:ident = $value:literal, $container_name:literal; )+) => {
        $(const $id: u32 = $value;)+

        const fn wkb_type(kind: GeometryKind, axes: CoordinateAxes, extended: bool) -> u32 {
            let mut value = match kind {
                $(GeometryKind::$kind => $id,)+
            };
            if extended {
                if axes.has_z() {
                    value |= EWKB_Z_FLAG;
                }
                if axes.has_m() {
                    value |= EWKB_M_FLAG;
                }
            } else {
                value = value.wrapping_add(iso_dimension_offset(axes.has_z(), axes.has_m()));
            }
            value
        }

        const fn wkb_geometry_kind(base: u32) -> Option<GeometryKind> {
            match base {
                $($id => Some(GeometryKind::$kind),)+
                _ => None,
            }
        }

        const fn wkb_container_name(base: u32) -> &'static str {
            match base {
                $($id => $container_name,)+
                _ => "geometry",
            }
        }
    };
}

const fn wkb_dimension(axes: CoordinateAxes) -> usize {
    axes.dimension()
}

const fn wkb_coords_len(count: usize, axes: CoordinateAxes) -> usize {
    count
        .wrapping_mul(wkb_dimension(axes))
        .wrapping_mul(size_of::<f64>())
}

const fn iso_dimension_offset(z: bool, m: bool) -> u32 {
    match (z, m) {
        (false, false) => 0,
        (false, true) => WKB_M_OFFSET,
        (true, false) => WKB_Z_OFFSET,
        (true, true) => WKB_ZM_OFFSET,
    }
}

wkb_geometry_types! {
    Point => WKB_POINT = 1, "geometry";
    MultiPoint => WKB_MULTIPOINT = 4, "MULTIPOINT";
    LineString => WKB_LINESTRING = 2, "geometry";
    MultiLineString => WKB_MULTILINESTRING = 5, "MULTILINESTRING";
    Polygon => WKB_POLYGON = 3, "geometry";
    MultiPolygon => WKB_MULTIPOLYGON = 6, "MULTIPOLYGON";
    GeometryCollection => WKB_GEOMETRYCOLLECTION = 7, "GEOMETRYCOLLECTION";
}

mod read;
mod write;

pub(crate) use read::parse_wkb;
pub(crate) use write::{to_wkb, wkb_len, write_wkb_to_srid};
