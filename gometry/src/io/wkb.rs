use crate::io::{
    CoordSeq, CoordinateAxes, Coordinates, EWKB_M_FLAG, EWKB_SRID_FLAG, EWKB_Z_FLAG,
    GeometryErrorKind, GeometryKind, IoError, LineSeq, MAX_NESTING, ParseFormat, Point, Polygon,
    Result, Ring, Shape, WKB_GEOMETRYCOLLECTION, WKB_M_OFFSET, WKB_MULTILINESTRING, WKB_MULTIPOINT,
    WKB_MULTIPOLYGON, WKB_Z_OFFSET, WKB_ZM_OFFSET, WkbGeometry, extended_srid_code, parse_content,
};

const WKB_POINT: u32 = 1;
const WKB_LINESTRING: u32 = 2;
const WKB_POLYGON: u32 = 3;
const WKB_HEADER_BASE: usize = 1 + 4; // byte-order byte + 4-byte geometry type
const WKB_COUNT: usize = 4; // 4-byte element/ring count

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

fn wkb_type(kind: GeometryKind, axes: CoordinateAxes, extended: bool) -> u32 {
    let mut value = match kind {
        GeometryKind::Point => WKB_POINT,
        GeometryKind::MultiPoint => WKB_MULTIPOINT,
        GeometryKind::LineString => WKB_LINESTRING,
        GeometryKind::MultiLineString => WKB_MULTILINESTRING,
        GeometryKind::Polygon => WKB_POLYGON,
        GeometryKind::MultiPolygon => WKB_MULTIPOLYGON,
        GeometryKind::GeometryCollection => WKB_GEOMETRYCOLLECTION,
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

mod read;
mod write;

pub(crate) use read::parse_wkb;
pub(crate) use write::{to_wkb, wkb_len, write_wkb_to_srid};
