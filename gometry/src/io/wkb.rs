use crate::geometry::{HasM, HasZ};
use crate::io::{
    CoordSeq, CoordinateAxes, Coordinates, EWKB_M_FLAG, EWKB_SRID_FLAG, EWKB_Z_FLAG, EmptyKind,
    GeometryErrorKind, IoError, IoGeometryKind, LineSeq, MAX_PARSE_DEPTH, ParseFormat, Point,
    Polygon, Result, Ring, Shape, WKB_GEOMETRYCOLLECTION, WKB_M_OFFSET, WKB_MULTILINESTRING,
    WKB_MULTIPOINT, WKB_MULTIPOLYGON, WKB_Z_OFFSET, WKB_ZM_OFFSET, WkbAxes, WkbGeometry,
    extended_srid_code, parse_content, require_serializable_axes,
};

const WKB_POINT: u32 = 1;
const WKB_LINESTRING: u32 = 2;
const WKB_POLYGON: u32 = 3;
const WKB_HEADER_BASE: usize = 1 + 4; // byte-order byte + 4-byte geometry type
const WKB_COUNT: usize = 4; // 4-byte element/ring count

const fn wkb_dimension(axes: WkbAxes) -> usize {
    2 + axes.z as usize + axes.m as usize
}

const fn wkb_coords_len(count: usize, axes: WkbAxes) -> usize {
    count * wkb_dimension(axes) * size_of::<f64>()
}

impl WkbAxes {
    fn from_point(point: Point) -> Self {
        Self {
            z: point.z().is_some(),
            m: point.m().is_some(),
        }
    }

    fn from_polygon(polygon: &Polygon) -> Self {
        let mut axes = Self { z: false, m: false };
        for ring in polygon.rings() {
            let ring_axes = ring.axes();
            axes.z |= ring_axes.has_z();
            axes.m |= ring_axes.has_m();
        }
        axes
    }

    const fn from_coordinate_axes(axes: CoordinateAxes) -> Self {
        Self {
            z: axes.has_z(),
            m: axes.has_m(),
        }
    }

    const fn coordinate_axes(self) -> CoordinateAxes {
        CoordinateAxes::new(HasZ(self.z), HasM(self.m))
    }
}

/// ISO 1000/2000/3000 dimension offsets indexed by `(z as usize) << 1 | (m as
/// usize)`.
const ISO_DIM_OFFSET: [u32; 4] = [0, WKB_M_OFFSET, WKB_Z_OFFSET, WKB_ZM_OFFSET];

fn wkb_type(kind: IoGeometryKind, axes: WkbAxes, extended: bool) -> u32 {
    let mut value = match kind {
        IoGeometryKind::Point => WKB_POINT,
        IoGeometryKind::MultiPoint => WKB_MULTIPOINT,
        IoGeometryKind::LineString => WKB_LINESTRING,
        IoGeometryKind::MultiLineString => WKB_MULTILINESTRING,
        IoGeometryKind::Polygon => WKB_POLYGON,
        IoGeometryKind::MultiPolygon => WKB_MULTIPOLYGON,
        IoGeometryKind::GeometryCollection => WKB_GEOMETRYCOLLECTION,
    };
    if extended {
        if axes.z {
            value |= EWKB_Z_FLAG;
        }
        if axes.m {
            value |= EWKB_M_FLAG;
        }
    } else {
        value += ISO_DIM_OFFSET[(usize::from(axes.z) << 1) | usize::from(axes.m)];
    }
    value
}

mod read;
mod write;

pub(crate) use read::{WkbCoordArena, parse_wkb, parse_wkb_batch};
pub(crate) use write::{to_wkb, to_wkb_len, wkb_len, write_wkb_into, write_wkb_to};
