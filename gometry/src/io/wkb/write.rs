use crate::geometry::{MultiLineString, MultiMember, MultiPoint, MultiPolygon};
use crate::io::wkb::{
    Coordinates, EWKB_SRID_FLAG, GeometryKind, IoError, IoGeometryKind, LineSeq, Point, Polygon,
    Result, Shape, WKB_COUNT, WKB_HEADER_BASE, WkbAxes, extended_srid_code,
    require_serializable_axes, wkb_coords_len, wkb_dimension, wkb_type,
};

pub(crate) fn to_wkb(shape: &Shape, crs: Option<&str>, include_srid: bool) -> Result<Vec<u8>> {
    // One path, no coordinate-count gate (the old `< 32` cliff). Classify once,
    // compute the checked byte length, reserve, and write.
    // Reject heterogeneous-axis multiparts before inventing missing Z/M as 0.
    require_serializable_axes(shape)?;
    // Resolve the SRID once and thread it through length + header so the
    // wire-alias table (CRS84→4326) cannot diverge between the two.
    let srid = extended_srid_code(crs, include_srid, "EWKB")?;
    let geometry = WkbShape::classify(shape);
    let capacity = geometry.encoded_len(srid.is_some())?;
    let mut out = Vec::new();
    crate::boundary::try_reserve_vec(&mut out, capacity)?;
    geometry.write(&mut out, srid)?;
    Ok(out)
}

/// Exact serialized length (including EWKB SRID when applicable).
pub(crate) fn to_wkb_len(shape: &Shape, crs: Option<&str>, include_srid: bool) -> Result<usize> {
    require_serializable_axes(shape)?;
    let srid = extended_srid_code(crs, include_srid, "EWKB")?;
    WkbShape::classify(shape).encoded_len(srid.is_some())
}

/// Write WKB/EWKB into a pre-sized buffer (exact length from [`to_wkb_len`]).
/// Used by `PyBytes::new_with` so large scalar encodes skip a Vec→bytes copy.
pub(crate) fn write_wkb_into(
    out: &mut [u8],
    shape: &Shape,
    crs: Option<&str>,
    include_srid: bool,
) -> Result<()> {
    require_serializable_axes(shape)?;
    let srid = extended_srid_code(crs, include_srid, "EWKB")?;
    let geometry = WkbShape::classify(shape);
    debug_assert_eq!(
        out.len(),
        geometry.encoded_len(srid.is_some())?,
        "WKB destination length drifted from classification"
    );
    let mut cursor = WkbBuf { buf: out, pos: 0 };
    geometry.write(&mut cursor, srid)?;
    debug_assert_eq!(
        cursor.pos,
        out.len(),
        "WKB writer under/over-filled the destination"
    );
    Ok(())
}

pub(crate) fn write_wkb_to(
    out: &mut Vec<u8>,
    shape: &Shape,
    crs: Option<&str>,
    include_srid: bool,
) -> Result<()> {
    require_serializable_axes(shape)?;
    let srid = extended_srid_code(crs, include_srid, "EWKB")?;
    crate::boundary::try_reserve_vec(out, wkb_len(shape, srid.is_some())?)?;
    write_wkb_resolved(out, shape, srid)
}

/// Write one row using an SRID resolved by the owning batch.
///
/// Resolving an array's uniform CRS in the row loop repeats the same parse for
/// every row. The batch path resolves once and still validates each shape's
/// axes here, preserving row-level axis errors and notes.
pub(crate) fn write_wkb_to_srid(out: &mut Vec<u8>, shape: &Shape, srid: Option<u32>) -> Result<()> {
    require_serializable_axes(shape)?;
    crate::boundary::try_reserve_vec(out, wkb_len(shape, srid.is_some())?)?;
    write_wkb_resolved(out, shape, srid)
}

fn write_wkb_resolved(out: &mut Vec<u8>, shape: &Shape, srid: Option<u32>) -> Result<()> {
    WkbShape::classify(shape).write(out, srid)
}

/// Cursor over a pre-sized mutable byte buffer for exact-size WKB fills.
struct WkbBuf<'a> {
    buf: &'a mut [u8],
    pos: usize,
}

impl WkbBuf<'_> {
    fn write_at(&mut self, bytes: &[u8]) {
        let end = self.pos + bytes.len();
        self.buf[self.pos..end].copy_from_slice(bytes);
        self.pos = end;
    }
}

trait WkbOut {
    fn push_byte(&mut self, value: u8);
    fn extend_bytes(&mut self, bytes: &[u8]);

    /// Append `xs.len()` interleaved little-endian vertices in `x, y, (z), (m)`
    /// order. Every supplied ordinate column MUST have exactly `xs.len()`
    /// elements; a mismatch is an error, never silent truncation.
    fn write_columns(
        &mut self,
        xs: &[f64],
        ys: &[f64],
        zs: Option<&[f64]>,
        ms: Option<&[f64]>,
    ) -> Result<()>;
}

impl WkbOut for WkbBuf<'_> {
    fn push_byte(&mut self, value: u8) {
        self.buf[self.pos] = value;
        self.pos += 1;
    }

    fn extend_bytes(&mut self, bytes: &[u8]) {
        self.write_at(bytes);
    }

    fn write_columns(
        &mut self,
        xs: &[f64],
        ys: &[f64],
        zs: Option<&[f64]>,
        ms: Option<&[f64]>,
    ) -> Result<()> {
        let count = xs.len();
        if ys.len() != count
            || zs.is_some_and(|z| z.len() != count)
            || ms.is_some_and(|m| m.len() != count)
        {
            return Err(IoError::wkb(
                "WKB coordinate columns have mismatched lengths",
            ));
        }
        let dimension = 2 + usize::from(zs.is_some()) + usize::from(ms.is_some());
        let byte_len = count * dimension * size_of::<f64>();
        let end = self.pos + byte_len;
        let bytes = &mut self.buf[self.pos..end];
        let (lanes, rest) = bytes.as_chunks_mut::<8>();
        debug_assert!(rest.is_empty());
        match (zs, ms) {
            (None, None) => {
                let stream = std::iter::zip(xs, ys).flat_map(|(&x, &y)| [x, y]);
                for (lane, value) in std::iter::zip(lanes, stream) {
                    *lane = value.to_le_bytes();
                }
            },
            (Some(zs), None) => {
                let stream =
                    std::iter::zip(std::iter::zip(xs, ys), zs).flat_map(|((&x, &y), &z)| [x, y, z]);
                for (lane, value) in std::iter::zip(lanes, stream) {
                    *lane = value.to_le_bytes();
                }
            },
            (None, Some(ms)) => {
                let stream =
                    std::iter::zip(std::iter::zip(xs, ys), ms).flat_map(|((&x, &y), &m)| [x, y, m]);
                for (lane, value) in std::iter::zip(lanes, stream) {
                    *lane = value.to_le_bytes();
                }
            },
            (Some(zs), Some(ms)) => {
                let stream = std::iter::zip(std::iter::zip(std::iter::zip(xs, ys), zs), ms)
                    .flat_map(|(((&x, &y), &z), &m)| [x, y, z, m]);
                for (lane, value) in std::iter::zip(lanes, stream) {
                    *lane = value.to_le_bytes();
                }
            },
        }
        self.pos = end;
        Ok(())
    }
}

impl WkbOut for Vec<u8> {
    fn push_byte(&mut self, value: u8) {
        self.push(value);
    }

    fn extend_bytes(&mut self, bytes: &[u8]) {
        self.extend_from_slice(bytes);
    }

    fn write_columns(
        &mut self,
        xs: &[f64],
        ys: &[f64],
        zs: Option<&[f64]>,
        ms: Option<&[f64]>,
    ) -> Result<()> {
        let count = xs.len();
        // The unsafe fill commits `count * dimension` vertices out of spare
        // capacity, one `[u8; 8]` lane per ordinate. A column shorter than
        // `count` would leave committed bytes uninitialized once `set_len`
        // runs, so the equal-length invariant (upheld by every `CoordSeq`
        // caller) is validated HERE rather than assumed — the unsafe block's
        // soundness is then locally provable from this check alone.
        if ys.len() != count
            || zs.is_some_and(|z| z.len() != count)
            || ms.is_some_and(|m| m.len() != count)
        {
            return Err(IoError::wkb(
                "WKB coordinate columns have mismatched lengths",
            ));
        }
        let dimension = 2 + usize::from(zs.is_some()) + usize::from(ms.is_some());
        let byte_len = count
            .checked_mul(dimension)
            .and_then(|bytes| bytes.checked_mul(size_of::<f64>()))
            .ok_or_else(|| IoError::wkb("WKB coordinate byte length overflows usize"))?;
        // `write_wkb_to` reserves the exact classified body before entering
        // this sink; keep this writer allocation-free so a successful reserve
        // cannot be followed by an infallible growth path.
        debug_assert!(self.capacity() - self.len() >= byte_len);
        let expected_end = self
            .len()
            .checked_add(byte_len)
            .ok_or_else(|| IoError::wkb("WKB output length overflows usize"))?;
        match (zs, ms) {
            (None, None) => {
                for (&x, &y) in std::iter::zip(xs, ys) {
                    self.extend_from_slice(&x.to_le_bytes());
                    self.extend_from_slice(&y.to_le_bytes());
                }
            },
            (Some(zs), None) => {
                for ((&x, &y), &z) in std::iter::zip(std::iter::zip(xs, ys), zs) {
                    self.extend_from_slice(&x.to_le_bytes());
                    self.extend_from_slice(&y.to_le_bytes());
                    self.extend_from_slice(&z.to_le_bytes());
                }
            },
            (None, Some(ms)) => {
                for ((&x, &y), &m) in std::iter::zip(std::iter::zip(xs, ys), ms) {
                    self.extend_from_slice(&x.to_le_bytes());
                    self.extend_from_slice(&y.to_le_bytes());
                    self.extend_from_slice(&m.to_le_bytes());
                }
            },
            (Some(zs), Some(ms)) => {
                for (((&x, &y), &z), &m) in
                    std::iter::zip(std::iter::zip(std::iter::zip(xs, ys), zs), ms)
                {
                    self.extend_from_slice(&x.to_le_bytes());
                    self.extend_from_slice(&y.to_le_bytes());
                    self.extend_from_slice(&z.to_le_bytes());
                    self.extend_from_slice(&m.to_le_bytes());
                }
            },
        }
        debug_assert_eq!(self.len(), expected_end);
        Ok(())
    }
}

/// One geometry's complete WKB classification. Size calculation and writing
/// consume this same kind/axes/body value, so nested heterogeneous dimensions
/// cannot drift between the allocation pass and the emitted bytes.
#[derive(Clone, Copy)]
struct WkbShape<'a> {
    kind: IoGeometryKind,
    axes: WkbAxes,
    body: WkbBody<'a>,
}

#[derive(Clone, Copy)]
enum WkbBody<'a> {
    Point(Point),
    MultiPoint(&'a MultiPoint),
    LineString(&'a LineSeq),
    MultiLineString(&'a MultiLineString),
    Polygon(&'a Polygon),
    MultiPolygon(&'a MultiPolygon),
    GeometryCollection(&'a [Shape]),
    EmptyPoint,
    EmptyMultiPoint,
    EmptyLineString,
    EmptyMultiLineString,
    EmptyMultiPolygon,
    EmptyPolygon,
}

impl<'a> WkbShape<'a> {
    fn classify(shape: &'a Shape) -> Self {
        match shape {
            Shape::Point(point) => Self::point(*point),
            Shape::MultiPoint(points) => Self {
                kind: IoGeometryKind::MultiPoint,
                axes: WkbAxes::from_coordinate_axes(points.axes()),
                body: WkbBody::MultiPoint(points),
            },
            Shape::LineString(line) => Self::line(line),
            Shape::MultiLineString(lines) => Self {
                kind: IoGeometryKind::MultiLineString,
                axes: WkbAxes::from_coordinate_axes(shape.axes()),
                body: WkbBody::MultiLineString(lines),
            },
            Shape::Polygon(polygon) => Self::polygon(polygon),
            Shape::MultiPolygon(polygons) => Self {
                kind: IoGeometryKind::MultiPolygon,
                axes: WkbAxes::from_coordinate_axes(shape.axes()),
                body: WkbBody::MultiPolygon(polygons),
            },
            Shape::GeometryCollection(geometries) => Self {
                kind: IoGeometryKind::GeometryCollection,
                axes: WkbAxes::from_coordinate_axes(shape.axes()),
                body: WkbBody::GeometryCollection(geometries),
            },
            // A typed empty writes its declared axes into the header type code
            // (1001-style for `POINT Z EMPTY`) with an axes-matched body: the
            // NaN sentinel per present ordinate for a point, a zero count for
            // the counted kinds.
            Shape::Empty(kind, axes) => {
                let axes = WkbAxes::from_coordinate_axes(*axes);
                match kind {
                    GeometryKind::Point => Self {
                        kind: IoGeometryKind::Point,
                        axes,
                        body: WkbBody::EmptyPoint,
                    },
                    GeometryKind::MultiPoint => Self {
                        kind: IoGeometryKind::MultiPoint,
                        axes,
                        body: WkbBody::EmptyMultiPoint,
                    },
                    GeometryKind::LineString => Self {
                        kind: IoGeometryKind::LineString,
                        axes,
                        body: WkbBody::EmptyLineString,
                    },
                    GeometryKind::Polygon => Self {
                        kind: IoGeometryKind::Polygon,
                        axes,
                        body: WkbBody::EmptyPolygon,
                    },
                    GeometryKind::MultiLineString => Self {
                        kind: IoGeometryKind::MultiLineString,
                        axes,
                        body: WkbBody::EmptyMultiLineString,
                    },
                    GeometryKind::MultiPolygon => Self {
                        kind: IoGeometryKind::MultiPolygon,
                        axes,
                        body: WkbBody::EmptyMultiPolygon,
                    },
                    GeometryKind::GeometryCollection => Self {
                        kind: IoGeometryKind::GeometryCollection,
                        axes,
                        body: WkbBody::GeometryCollection(&[]),
                    },
                }
            },
        }
    }

    fn point(point: Point) -> Self {
        Self {
            kind: IoGeometryKind::Point,
            axes: WkbAxes::from_point(point),
            body: WkbBody::Point(point),
        }
    }

    fn line(line: &'a LineSeq) -> Self {
        Self {
            kind: IoGeometryKind::LineString,
            axes: WkbAxes::from_coordinate_axes(line.axes()),
            body: WkbBody::LineString(line),
        }
    }

    fn polygon(polygon: &'a Polygon) -> Self {
        Self {
            kind: IoGeometryKind::Polygon,
            axes: WkbAxes::from_polygon(polygon),
            body: WkbBody::Polygon(polygon),
        }
    }

    fn encoded_len(self, srid: bool) -> Result<usize> {
        let header = WKB_HEADER_BASE
            .checked_add(if srid { 4 } else { 0 })
            .ok_or_else(|| IoError::wkb("WKB encoded length overflows usize"))?;
        header
            .checked_add(self.body.encoded_len(self.axes)?)
            .ok_or_else(|| IoError::wkb("WKB encoded length overflows usize"))
    }

    fn write(self, out: &mut impl WkbOut, srid: Option<u32>) -> Result<()> {
        write_wkb_header(out, self.kind, self.axes, srid);
        self.body.write(out, self.axes)
    }
}

impl WkbBody<'_> {
    fn encoded_len(self, axes: WkbAxes) -> Result<usize> {
        match self {
            Self::Point(_) | Self::EmptyPoint => wkb_coords_len(1, axes),
            Self::MultiPoint(points) => {
                let child = WKB_HEADER_BASE
                    .checked_add(wkb_coords_len(1, axes)?)
                    .ok_or_else(|| IoError::wkb("WKB encoded length overflows usize"))?;
                WKB_COUNT
                    .checked_add(
                        points
                            .len()
                            .checked_mul(child)
                            .ok_or_else(|| IoError::wkb("WKB encoded length overflows usize"))?,
                    )
                    .ok_or_else(|| IoError::wkb("WKB encoded length overflows usize"))
            },
            Self::LineString(line) => WKB_COUNT
                .checked_add(wkb_coords_len(line.len(), axes)?)
                .ok_or_else(|| IoError::wkb("WKB encoded length overflows usize")),
            // Homogeneous multiparts promote every member to the multipart's
            // union axes, so each member header + body is sized against `axes`
            // (the parent's), NOT the member's own axes — a mixed XY/XYZ
            // multipart must not size a child at XY while the outer type is Z.
            Self::MultiLineString(lines) => {
                let total = lines.members().try_fold(0_usize, |total, member| {
                    let child = match member {
                        MultiMember::Empty => WKB_HEADER_BASE.checked_add(WKB_COUNT),
                        MultiMember::Value(index) => {
                            WKB_HEADER_BASE.checked_add(WKB_COUNT).and_then(|prefix| {
                                wkb_coords_len(lines.as_slice()[index].len(), axes)
                                    .ok()
                                    .and_then(|body| prefix.checked_add(body))
                            })
                        },
                    }
                    .ok_or_else(|| IoError::wkb("WKB encoded length overflows usize"))?;
                    total
                        .checked_add(child)
                        .ok_or_else(|| IoError::wkb("WKB encoded length overflows usize"))
                })?;
                WKB_COUNT
                    .checked_add(total)
                    .ok_or_else(|| IoError::wkb("WKB encoded length overflows usize"))
            },
            Self::Polygon(polygon) => wkb_polygon_body_len(polygon, axes),
            Self::MultiPolygon(polygons) => {
                let total = polygons.members().try_fold(0_usize, |total, member| {
                    let child = match member {
                        MultiMember::Empty => WKB_HEADER_BASE.checked_add(WKB_COUNT),
                        MultiMember::Value(index) => WKB_HEADER_BASE
                            .checked_add(wkb_polygon_body_len(&polygons.as_slice()[index], axes)?),
                    }
                    .ok_or_else(|| IoError::wkb("WKB encoded length overflows usize"))?;
                    total
                        .checked_add(child)
                        .ok_or_else(|| IoError::wkb("WKB encoded length overflows usize"))
                })?;
                WKB_COUNT
                    .checked_add(total)
                    .ok_or_else(|| IoError::wkb("WKB encoded length overflows usize"))
            },
            Self::GeometryCollection(geometries) => {
                let total = geometries.iter().try_fold(0_usize, |total, geometry| {
                    total
                        .checked_add(WkbShape::classify(geometry).encoded_len(false)?)
                        .ok_or_else(|| IoError::wkb("WKB encoded length overflows usize"))
                })?;
                WKB_COUNT
                    .checked_add(total)
                    .ok_or_else(|| IoError::wkb("WKB encoded length overflows usize"))
            },
            Self::EmptyMultiPoint
            | Self::EmptyLineString
            | Self::EmptyMultiLineString
            | Self::EmptyMultiPolygon
            | Self::EmptyPolygon => Ok(WKB_COUNT),
        }
    }

    fn write(self, out: &mut impl WkbOut, axes: WkbAxes) -> Result<()> {
        match self {
            Self::Point(point) => {
                write_point(out, point, axes);
                Ok(())
            },
            // Every homogeneous-multipart member is written with the parent's
            // axes, and explicit empty members retain their count and position.
            Self::MultiPoint(points) => {
                write_u32(out, checked_len(points.len())?);
                for member in points.members() {
                    write_wkb_header(out, IoGeometryKind::Point, axes, None);
                    match member {
                        MultiMember::Empty => {
                            for _ in 0..wkb_dimension(axes) {
                                out.extend_bytes(&f64::NAN.to_le_bytes());
                            }
                        },
                        MultiMember::Value(index) => {
                            write_point(out, points.point_at(index), axes);
                        },
                    }
                }
                Ok(())
            },
            Self::LineString(line) => write_wkb_sequence(out, line, axes),
            Self::MultiLineString(lines) => {
                write_u32(out, checked_len(lines.len())?);
                for member in lines.members() {
                    write_wkb_header(out, IoGeometryKind::LineString, axes, None);
                    match member {
                        MultiMember::Empty => write_u32(out, 0),
                        MultiMember::Value(index) => {
                            write_wkb_sequence(out, &lines.as_slice()[index], axes)?;
                        },
                    }
                }
                Ok(())
            },
            Self::Polygon(polygon) => write_wkb_polygon(out, polygon, axes),
            Self::MultiPolygon(polygons) => {
                write_u32(out, checked_len(polygons.len())?);
                for member in polygons.members() {
                    write_wkb_header(out, IoGeometryKind::Polygon, axes, None);
                    match member {
                        MultiMember::Empty => write_u32(out, 0),
                        MultiMember::Value(index) => {
                            write_wkb_polygon(out, &polygons.as_slice()[index], axes)?;
                        },
                    }
                }
                Ok(())
            },
            Self::GeometryCollection(geometries) => {
                write_u32(out, checked_len(geometries.len())?);
                for geometry in geometries {
                    WkbShape::classify(geometry).write(out, None)?;
                }
                Ok(())
            },
            Self::EmptyPoint => {
                // The OGC empty-point sentinel: NaN for EVERY ordinate the
                // header's axes declare (2 for XY, 3 for Z/M, 4 for ZM) —
                // symmetric with the reader's `wkb_point_is_empty`.
                for _ in 0..wkb_dimension(axes) {
                    out.extend_bytes(&f64::NAN.to_le_bytes());
                }
                Ok(())
            },
            Self::EmptyMultiPoint
            | Self::EmptyLineString
            | Self::EmptyMultiLineString
            | Self::EmptyMultiPolygon => {
                write_u32(out, 0);
                Ok(())
            },
            Self::EmptyPolygon => {
                write_u32(out, 0);
                Ok(())
            },
        }
    }
}

/// Exact serialized WKB/EWKB byte length from the same classification used by
/// the writer. `srid` means the top-level header carries an EWKB SRID.
pub(crate) fn wkb_len(shape: &Shape, srid: bool) -> Result<usize> {
    WkbShape::classify(shape).encoded_len(srid)
}

fn wkb_polygon_body_len(polygon: &Polygon, axes: WkbAxes) -> Result<usize> {
    let total = std::iter::once(&polygon.shell)
        .chain(polygon.holes.iter())
        .try_fold(0_usize, |total, ring| {
            let ring_len = WKB_COUNT
                .checked_add(wkb_coords_len(ring.coord_count(), axes)?)
                .ok_or_else(|| IoError::wkb("WKB encoded length overflows usize"))?;
            total
                .checked_add(ring_len)
                .ok_or_else(|| IoError::wkb("WKB encoded length overflows usize"))
        })?;
    WKB_COUNT
        .checked_add(total)
        .ok_or_else(|| IoError::wkb("WKB encoded length overflows usize"))
}

fn write_wkb_header(out: &mut impl WkbOut, kind: IoGeometryKind, axes: WkbAxes, srid: Option<u32>) {
    out.push_byte(1);
    // `srid` is the already-resolved integer from `extended_srid_code` —
    // never re-parse the CRS string here (wire aliases live only in that
    // single resolver).
    let mut geometry_type = wkb_type(kind, axes, srid.is_some());
    if srid.is_some() {
        geometry_type |= EWKB_SRID_FLAG;
    }
    write_u32(out, geometry_type);
    if let Some(srid) = srid {
        write_u32(out, srid);
    }
}

fn write_wkb_polygon(out: &mut impl WkbOut, polygon: &Polygon, axes: WkbAxes) -> Result<()> {
    write_u32(out, checked_len(polygon.holes.len() + 1)?);
    write_wkb_sequence(out, &polygon.shell, axes)?;
    for hole in polygon.holes.iter() {
        write_wkb_sequence(out, hole, axes)?;
    }
    Ok(())
}

/// One body writer for every counted point run (linestring bodies and polygon
/// rings): the u32 count, then the column-direct interleave when the sequence's
/// own ordinate columns exactly match the requested `axes`, else the per-point
/// fallback that fills any promoted-but-absent ordinate with 0.0.
fn write_wkb_sequence<C: Coordinates + ?Sized>(
    out: &mut impl WkbOut,
    points: &C,
    axes: WkbAxes,
) -> Result<()> {
    write_u32(out, checked_len(points.coord_count())?);
    // The column-direct fast path applies only when the sequence carries
    // exactly the requested ordinates (the scalar and uniform-multipart case).
    // `axes` is the union over the enclosing multipart, so it is always a
    // superset of a member's columns; a member NARROWER than `axes` takes the
    // per-point path and fills the missing ordinate with 0.0 (the `force_3d`
    // convention) rather than a fabricated NaN.
    if let Some((xs, ys)) = points.xy_columns() {
        let zs = points.z_column();
        let ms = points.m_column();
        if axes.z == zs.is_some() && axes.m == ms.is_some() {
            return out.write_columns(xs, ys, zs, ms);
        }
    }
    for point in points.iter_coords() {
        write_point(out, point, axes);
    }
    Ok(())
}

/// Write one vertex under `axes`. A promoted member may lack an ordinate the
/// union `axes` requires; the absent Z/M is filled with 0.0 (the `force_3d`
/// fill convention). For a scalar or uniform sequence `axes` matches the data,
/// so no fill occurs.
fn write_point(out: &mut impl WkbOut, point: Point, axes: WkbAxes) {
    out.extend_bytes(&point.x.to_le_bytes());
    out.extend_bytes(&point.y.to_le_bytes());
    if axes.z {
        out.extend_bytes(&point.z().unwrap_or(0.0).to_le_bytes());
    }
    if axes.m {
        out.extend_bytes(&point.m().unwrap_or(0.0).to_le_bytes());
    }
}

fn write_u32(out: &mut impl WkbOut, value: u32) {
    out.extend_bytes(&value.to_le_bytes());
}

fn checked_len(value: usize) -> Result<u32> {
    value
        .try_into()
        .map_err(|_| IoError::wkb("geometry is too large for WKB"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::{MOrdinate, ZOrdinate};
    use crate::io::parse_wkb;
    use crate::{CoordSeq, Ring};

    fn xy(x: f64, y: f64) -> Point {
        Point::new_raw_xy(x, y)
    }

    fn xyz(x: f64, y: f64, z: f64) -> Point {
        Point::new_raw_axes(x, y, ZOrdinate(Some(z)), MOrdinate(None))
    }

    fn xym(x: f64, y: f64, m: f64) -> Point {
        Point::new_raw_axes(x, y, ZOrdinate(None), MOrdinate(Some(m)))
    }

    fn line(points: &[Point]) -> LineSeq {
        LineSeq::try_new(CoordSeq::from_points(points)).expect("test line is valid")
    }

    fn polygon(points: &[Point]) -> Polygon {
        Polygon::new(Ring::from_closed(CoordSeq::from_points(points)), vec![])
    }

    #[test]
    fn mixed_axes_multiline_is_unrepresentable() {
        let result = MultiLineString::try_new(crate::geometry::CoordinateAxes::XY, vec![
            MultiMember::Value(line(&[xy(0.0, 0.0), xy(1.0, 1.0)])),
            MultiMember::Value(line(&[xyz(2.0, 2.0, 3.0), xyz(3.0, 3.0, 4.0)])),
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn mixed_axes_multipolygon_is_unrepresentable() {
        let result = MultiPolygon::try_new(crate::geometry::CoordinateAxes::XY, vec![
            MultiMember::Value(polygon(&[
                xy(0.0, 0.0),
                xy(1.0, 0.0),
                xy(1.0, 1.0),
                xy(0.0, 0.0),
            ])),
            MultiMember::Value(polygon(&[
                xyz(2.0, 2.0, 1.0),
                xyz(3.0, 2.0, 2.0),
                xyz(3.0, 3.0, 3.0),
                xyz(2.0, 2.0, 1.0),
            ])),
        ]);
        assert!(result.is_err());
    }

    /// Collections keep genuinely heterogeneous members (no axis promotion),
    /// so size classification and writing must agree per nested member.
    fn assert_exact_mixed_nested_round_trip(shape: &Shape) {
        let expected_len = wkb_len(shape, false).expect("test shape has a finite WKB length");
        let bytes = to_wkb(shape, None, false).expect("mixed nested WKB writes");
        assert_eq!(bytes.len(), expected_len);
        assert_eq!(&parse_wkb(&bytes).expect("written WKB parses").shape, shape);
    }

    #[test]
    fn mixed_axes_collection_classifies_every_nested_header_once() {
        assert_exact_mixed_nested_round_trip(&Shape::GeometryCollection(
            vec![
                Shape::Point(xy(0.0, 0.0)),
                Shape::Point(xym(1.0, 1.0, 7.0)),
                Shape::LineString(line(&[xyz(2.0, 2.0, 3.0), xyz(3.0, 3.0, 4.0)])),
                Shape::empty_polygon(),
            ]
            .into(),
        ));
    }

    #[test]
    fn column_writer_rejects_mismatched_lengths_before_initializing_output() {
        let mut out = vec![7_u8, 8];
        let error = out
            .write_columns(&[1.0, 2.0], &[3.0], None, None)
            .expect_err("short y column must be rejected");
        assert_eq!(
            error.to_string(),
            "invalid WKB: WKB coordinate columns have mismatched lengths"
        );
        assert_eq!(out, [7, 8], "failed write leaves existing bytes untouched");
    }
}
