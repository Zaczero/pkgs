use std::num::{NonZeroU32, NonZeroUsize};

use crate::geometry::{
    CoordinateAxes, HasM, HasZ, MOrdinate, MultiLineString, MultiMember, MultiPoint, MultiPolygon,
    ZOrdinate, column_all_finite,
};
use crate::io::wkb::{
    CoordSeq, EWKB_M_FLAG, EWKB_SRID_FLAG, EWKB_Z_FLAG, GeometryErrorKind, GeometryKind, IoError,
    LineSeq, MAX_NESTING, ParseFormat, Point, Polygon, Result, Ring, Shape, WKB_COUNT,
    WKB_HEADER_BASE, WkbGeometry, iso_dimension_offset, parse_content, wkb_dimension,
};

pub(crate) fn parse_wkb(value: &[u8]) -> Result<WkbGeometry> {
    parse_wkb_inner(value)
        .map_err(|error| parse_content(ParseFormat::Wkb, error))
        .map_err(|error| error.with_parse_position(value.len()))
}

fn parse_wkb_inner(value: &[u8]) -> Result<WkbGeometry> {
    let mut reader = WkbReader { value, offset: 0 };
    let (shape, srid) = reader
        .read_shape()
        .map_err(|error| error.with_parse_position(reader.offset))?;
    if reader.offset != value.len() {
        return Err(IoError::wkb_at(
            reader.offset,
            format!(
                "trailing bytes after WKB geometry ({} bytes remain)",
                value.len().saturating_sub(reader.offset)
            ),
        ));
    }
    Ok(WkbGeometry { shape, srid })
}
fn parse_wkb_type(value: u32) -> Result<(u32, CoordinateAxes)> {
    let ewkb_z = value & EWKB_Z_FLAG != 0;
    let ewkb_m = value & EWKB_M_FLAG != 0;
    let mut base = value & !(EWKB_Z_FLAG | EWKB_M_FLAG | EWKB_SRID_FLAG);
    let mut iso_z = false;
    let mut iso_m = false;
    for (candidate_z, candidate_m) in [(true, true), (false, true), (true, false)] {
        let offset = iso_dimension_offset(candidate_z, candidate_m);
        if base >= offset {
            base = base.wrapping_sub(offset);
            iso_z = candidate_z;
            iso_m = candidate_m;
            break;
        }
    }
    if (ewkb_z || ewkb_m) && (iso_z || iso_m) {
        return Err(IoError::wkb(
            "WKB geometry type mixes ISO and EWKB dimensional encodings",
        ));
    }
    Ok((
        base,
        CoordinateAxes::new(HasZ(ewkb_z || iso_z), HasM(ewkb_m || iso_m)),
    ))
}

/// OGC empty-point sentinel: every ordinate present in the WKB type is `NaN`.
fn wkb_point_is_empty(
    x: f64,
    y: f64,
    z: Option<f64>,
    m: Option<f64>,
    axes: CoordinateAxes,
) -> bool {
    if !x.is_nan() || !y.is_nan() {
        return false;
    }
    if axes.has_z() && !z.is_some_and(f64::is_nan) {
        return false;
    }
    if axes.has_m() && !m.is_some_and(f64::is_nan) {
        return false;
    }
    true
}

#[derive(Clone, Copy)]
enum WkbByteOrder {
    BigEndian,
    LittleEndian,
}

struct WkbReader<'a> {
    value: &'a [u8],
    offset: usize,
}

impl WkbReader<'_> {
    fn error<T>(&self, detail: impl Into<Box<str>>) -> Result<T> {
        Err(IoError::wkb_at(self.offset, detail))
    }

    const fn remaining_bytes(&self) -> usize {
        self.value.len().saturating_sub(self.offset)
    }

    /// Homogeneous multi members must carry the same axes as the outer type.
    fn require_member_axes(outer: CoordinateAxes, member: CoordinateAxes) -> Result<()> {
        if outer == member {
            Ok(())
        } else {
            Err(IoError::wkb(
                "WKB multi geometry member axes must match the outer type",
            ))
        }
    }

    fn validate_element_count(&self, count: usize) -> Result<()> {
        // Nested members: each costs at least a WKB header (byte order + type).
        self.validate_count_bytes(
            count,
            NonZeroUsize::new(WKB_HEADER_BASE).expect("WKB header is nonempty"),
            "element",
        )
    }

    fn validate_ring_count(&self, ring_count: usize) -> Result<()> {
        // Encoding-true bound: each ring starts with a 4-byte vertex count.
        // Empty / short rings fail in `read_ring` via `Ring::MIN_VERTICES_CLOSED`
        // before materializing the ring set (first ring fails immediately).
        self.validate_count_bytes(
            ring_count,
            NonZeroUsize::new(WKB_COUNT).expect("WKB count is nonempty"),
            "ring",
        )
    }

    /// Encoding-true bound: `count * minimum_bytes` must fit in remaining input.
    fn validate_count_bytes(
        &self,
        count: usize,
        minimum_bytes: NonZeroUsize,
        kind: &str,
    ) -> Result<()> {
        if count > self.remaining_bytes() / minimum_bytes {
            return Err(IoError::wkb(format!(
                "WKB {kind} count exceeds remaining input"
            )));
        }
        Ok(())
    }
}

/// Resolve runtime axes to the const-generic decoder instantiations.
fn decode_coordseq_axes<const BIG: bool>(bytes: &[u8], axes: CoordinateAxes) -> Result<CoordSeq> {
    crate::geometry::dispatch_axes!((axes.has_z(), axes.has_m()), HAS_Z, HAS_M, {
        decode_coordseq::<BIG, HAS_Z, HAS_M>(bytes)
    })
}

/// Decode one coordinate run into separated columns, monomorphized over
/// byte order AND axes so the hot loop carries no per-vertex branch or
/// `Option` at all — eight straight-line loop bodies, dispatched once.
///
/// Vertex **count is derived from the byte span length** (no external
/// `size_hint` / trusted-count parameter). Each final column reserves that
/// proven count once. Every WKB caller shares this decoder.
fn decode_coordseq<const BIG: bool, const Z: bool, const M: bool>(
    bytes: &[u8],
) -> Result<CoordSeq> {
    let decode = |array: [u8; 8]| {
        if BIG {
            f64::from_be_bytes(array)
        } else {
            f64::from_le_bytes(array)
        }
    };
    let stride = NonZeroUsize::new(
        2_usize
            .wrapping_add(usize::from(Z))
            .wrapping_add(usize::from(M))
            .wrapping_mul(size_of::<f64>()),
    )
    .expect("WKB vertices contain XY");
    debug_assert!(bytes.len().is_multiple_of(stride.get()));
    // Count proven by byte length ÷ stride (no separate trusted count).
    let count = bytes.len() / stride;
    let mut xs = Vec::with_capacity(count);
    let mut ys = Vec::with_capacity(count);
    let mut zs = Z.then(|| Vec::with_capacity(count));
    let mut ms = M.then(|| Vec::with_capacity(count));
    for vertex in bytes.chunks_exact(stride.get()) {
        let (ordinates, _) = vertex.as_chunks::<{ size_of::<f64>() }>();
        let (&[x_bytes, y_bytes], remaining) = ordinates
            .split_first_chunk::<2>()
            .expect("every WKB vertex contains XY");
        xs.push(decode(x_bytes));
        ys.push(decode(y_bytes));
        let mut remaining = remaining.iter();
        if let Some(column) = &mut zs {
            column.push(decode(*remaining.next().expect("WKB stride includes Z")));
        }
        if let Some(column) = &mut ms {
            column.push(decode(*remaining.next().expect("WKB stride includes M")));
        }
    }
    if !column_all_finite(&xs)
        || !column_all_finite(&ys)
        || zs
            .as_deref()
            .is_some_and(|column| !column_all_finite(column))
        || ms
            .as_deref()
            .is_some_and(|column| !column_all_finite(column))
    {
        return Err(GeometryErrorKind::NonFiniteCoordinate.into());
    }
    CoordSeq::try_from_vecs(xs, ys, zs, ms)
}

/// A member's position inside a nested WKB container. Threaded only so a
/// nested-SRID conflict can name the offending path; the string is built
/// lazily, on error, from this parent-linked stack (no per-element alloc).
#[derive(Clone, Copy)]
struct WkbMemberPath<'a> {
    parent: Option<&'a Self>,
    container: u32,
    index: usize,
}

impl WkbMemberPath<'_> {
    fn render(&self) -> String {
        let mut segments = Vec::new();
        let mut node = Some(self);
        while let Some(current) = node {
            segments.push(format!(
                "{} member {}",
                super::wkb_container_name(current.container),
                current.index
            ));
            node = current.parent;
        }
        segments.reverse();
        segments.join(" > ")
    }
}

/// Reconcile an element's own EWKB SRID with the SRID inherited from an
/// enclosing element. PostGIS may stamp the SRID flag on nested members, so a
/// nested SRID equal to (or absent against) the parent's is accepted; only a
/// genuine parent/child disagreement is rejected, naming both codes and the
/// member path. Wire SRID 0 is normalized to `None` before this comparison.
fn resolve_nested_srid(
    inherited: Option<NonZeroU32>,
    local: Option<NonZeroU32>,
    path: Option<&WkbMemberPath<'_>>,
) -> Result<Option<NonZeroU32>> {
    match (inherited, local) {
        (Some(parent), Some(child)) if parent != child => {
            let location = path.map_or_else(|| "nested geometry".to_owned(), WkbMemberPath::render);
            Err(IoError::wkb(format!(
                "nested WKB SRID {} conflicts with enclosing SRID {} at {location}",
                child.get(),
                parent.get(),
            )))
        },
        (_, Some(child)) => Ok(Some(child)),
        (inherited, None) => Ok(inherited),
    }
}

impl WkbReader<'_> {
    /// Decode one top-level geometry and its normalized embedded SRID.
    ///
    /// SRID resolution is deferred to the caller ([`crate::io::crs_from_srid`])
    /// until a fallback check or final frame resolution needs the definition.
    /// Nested SRID conflicts are still checked here.
    fn read_shape(&mut self) -> Result<(Shape, Option<NonZeroU32>)> {
        self.read_geometry(None, None, 0)
    }

    /// Read one geometry, possibly a nested member. `inherited` is the SRID
    /// declared by an enclosing element and `path` its position; a nested SRID
    /// is reconciled against them by [`resolve_nested_srid`]. Returns the
    /// resolved SRID so it flows down to this geometry's own members.
    fn read_geometry(
        &mut self,
        inherited: Option<NonZeroU32>,
        path: Option<&WkbMemberPath<'_>>,
        depth: usize,
    ) -> Result<(Shape, Option<NonZeroU32>)> {
        if depth > MAX_NESTING {
            return Err(IoError::wkb(format!(
                "WKB geometry nesting exceeds the limit of {MAX_NESTING}"
            )));
        }
        let byte_order = match self.read_u8()? {
            0 => WkbByteOrder::BigEndian,
            1 => WkbByteOrder::LittleEndian,
            _ => return self.error("invalid WKB byte order"),
        };
        let raw_geometry_type = self.read_u32(byte_order)?;
        let (geometry_type, axes) = parse_wkb_type(raw_geometry_type)?;
        let local_srid = if raw_geometry_type & EWKB_SRID_FLAG != 0 {
            Some(self.read_u32(byte_order)?)
        } else {
            None
        };
        let local_srid = crate::io::normalize_wire_srid(local_srid);
        let mut srid = resolve_nested_srid(inherited, local_srid, path)?;
        let Some(kind) = super::wkb_geometry_kind(geometry_type) else {
            return Err(IoError::wkb(unsupported_wkb_geometry_type_message(
                raw_geometry_type,
                geometry_type,
            )));
        };
        let shape = match kind {
            GeometryKind::Point => self.read_point_shape(byte_order, axes)?,
            GeometryKind::MultiPoint => {
                let (members, effective) = self.read_nested_typed(
                    byte_order,
                    geometry_type,
                    srid,
                    path,
                    |shape| match shape {
                        Shape::Point(point) => {
                            Self::require_member_axes(axes, CoordinateAxes::from_point(point))?;
                            Ok(MultiMember::Value(point))
                        },
                        Shape::Empty(GeometryKind::Point, empty_axes) => {
                            Self::require_member_axes(axes, empty_axes)?;
                            Ok(MultiMember::Empty)
                        },
                        Shape::MultiPoint(_)
                        | Shape::LineString(_)
                        | Shape::MultiLineString(_)
                        | Shape::Polygon(_)
                        | Shape::MultiPolygon(_)
                        | Shape::GeometryCollection(_)
                        | Shape::Empty(..) => Err(IoError::wkb("MULTIPOINT member must be Point")),
                    },
                    depth,
                )?;
                srid = effective;
                if members.is_empty() {
                    Shape::typed_empty(GeometryKind::MultiPoint, axes)
                } else {
                    Shape::MultiPoint(MultiPoint::try_new(axes, members)?)
                }
            },
            GeometryKind::LineString => {
                let count = self.read_u32(byte_order)? as usize;
                if count == 0 {
                    return Ok((Shape::typed_empty(GeometryKind::LineString, axes), srid));
                }
                let points = self.read_coordseq(byte_order, axes, count)?;
                Shape::LineString(LineSeq::try_new(points)?)
            },
            GeometryKind::MultiLineString => {
                let (members, effective) = self.read_nested_typed(
                    byte_order,
                    geometry_type,
                    srid,
                    path,
                    |shape| match shape {
                        Shape::LineString(points) => {
                            Self::require_member_axes(axes, points.axes())?;
                            Ok(MultiMember::Value(points))
                        },
                        Shape::Empty(GeometryKind::LineString, empty_axes) => {
                            Self::require_member_axes(axes, empty_axes)?;
                            Ok(MultiMember::Empty)
                        },
                        Shape::Point(_)
                        | Shape::MultiPoint(_)
                        | Shape::MultiLineString(_)
                        | Shape::Polygon(_)
                        | Shape::MultiPolygon(_)
                        | Shape::GeometryCollection(_)
                        | Shape::Empty(..) => {
                            Err(IoError::wkb("MULTILINESTRING member must be LineString"))
                        },
                    },
                    depth,
                )?;
                srid = effective;
                if members.is_empty() {
                    Shape::typed_empty(GeometryKind::MultiLineString, axes)
                } else {
                    Shape::MultiLineString(MultiLineString::try_new(axes, members)?)
                }
            },
            GeometryKind::Polygon => self.read_polygon_shape(byte_order, axes)?,
            GeometryKind::MultiPolygon => {
                let (members, effective) = self.read_nested_typed(
                    byte_order,
                    geometry_type,
                    srid,
                    path,
                    |shape| match shape {
                        Shape::Polygon(polygon) => {
                            Self::require_member_axes(axes, polygon.axes())?;
                            Ok(MultiMember::Value(polygon))
                        },
                        // Matching typed empty Polygon members remain explicit;
                        // validate their axes against the outer type.
                        Shape::Empty(GeometryKind::Polygon, empty_axes) => {
                            Self::require_member_axes(axes, empty_axes)?;
                            Ok(MultiMember::Empty)
                        },
                        Shape::Point(_)
                        | Shape::MultiPoint(_)
                        | Shape::LineString(_)
                        | Shape::MultiLineString(_)
                        | Shape::MultiPolygon(_)
                        | Shape::GeometryCollection(_)
                        | Shape::Empty(..) => {
                            Err(IoError::wkb("MULTIPOLYGON member must be Polygon"))
                        },
                    },
                    depth,
                )?;
                srid = effective;
                if members.is_empty() {
                    Shape::typed_empty(GeometryKind::MultiPolygon, axes)
                } else {
                    Shape::MultiPolygon(MultiPolygon::try_new(axes, members)?)
                }
            },
            GeometryKind::GeometryCollection => {
                let (members, effective) =
                    self.read_nested_shapes(byte_order, geometry_type, srid, path, depth)?;
                srid = effective;
                if members.is_empty() {
                    Shape::typed_empty(GeometryKind::GeometryCollection, axes)
                } else {
                    Shape::geometry_collection(members)?
                }
            },
        };
        Ok((shape, srid))
    }

    fn read_u8(&mut self) -> Result<u8> {
        let value = *self
            .value
            .get(self.offset)
            .ok_or_else(|| IoError::wkb_at(self.offset, "unexpected end of WKB"))?;
        self.offset = self.offset.wrapping_add(1);
        Ok(value)
    }

    fn read_u32(&mut self, byte_order: WkbByteOrder) -> Result<u32> {
        let bytes = self.read_array::<4>()?;
        Ok(match byte_order {
            WkbByteOrder::BigEndian => u32::from_be_bytes(bytes),
            WkbByteOrder::LittleEndian => u32::from_le_bytes(bytes),
        })
    }

    fn read_f64(&mut self, byte_order: WkbByteOrder) -> Result<f64> {
        let bytes = self.read_array::<8>()?;
        Ok(match byte_order {
            WkbByteOrder::BigEndian => f64::from_be_bytes(bytes),
            WkbByteOrder::LittleEndian => f64::from_le_bytes(bytes),
        })
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        // `first_chunk` borrows `[u8; N]` directly (no `offset + N` that could
        // overflow, no intermediate copy); the success proves `offset + N <= len`
        // so the advance below cannot overflow.
        let out = *self
            .value
            .get(self.offset..)
            .and_then(<[u8]>::first_chunk::<N>)
            .ok_or_else(|| IoError::wkb_at(self.offset, "unexpected end of WKB"))?;
        self.offset = self.offset.wrapping_add(N);
        Ok(out)
    }

    /// Read a WKB point, decoding the OGC empty-point convention (every present
    /// ordinate `NaN`) to the typed empty carrying the header's axes. Raw
    /// ordinates are read first so the all-`NaN` sentinel is detected before
    /// `Point::new_axes` rejects non-finite coordinates.
    fn read_point_shape(
        &mut self,
        byte_order: WkbByteOrder,
        axes: CoordinateAxes,
    ) -> Result<Shape> {
        let x = self.read_f64(byte_order)?;
        let y = self.read_f64(byte_order)?;
        let z = axes
            .has_z()
            .then(|| self.read_f64(byte_order))
            .transpose()?;
        let m = axes
            .has_m()
            .then(|| self.read_f64(byte_order))
            .transpose()?;
        if wkb_point_is_empty(x, y, z, m, axes) {
            return Ok(Shape::typed_empty(GeometryKind::Point, axes));
        }
        Ok(Shape::Point(Point::new_axes(
            x,
            y,
            ZOrdinate(z),
            MOrdinate(m),
        )?))
    }

    /// Read a `count`-vertex coordinate run straight into separated `CoordSeq`
    /// columns. WKB stores interleaved `x, y, (z), (m)` per vertex, so a copy
    /// to deinterleave is unavoidable, but this skips any per-vertex `Point`
    /// build and `Vec<Point>` re-gather. (A `bytemuck` whole-run cast is not
    /// used here: WKB byte buffers are rarely `f64`-aligned, so the guarded
    /// cast would fall back to this scalar decode anyway.)
    fn read_coordseq(
        &mut self,
        byte_order: WkbByteOrder,
        axes: CoordinateAxes,
        count: usize,
    ) -> Result<CoordSeq> {
        let stride = NonZeroUsize::new(wkb_dimension(axes).wrapping_mul(size_of::<f64>()))
            .expect("WKB vertices contain XY");
        if count > self.remaining_bytes() / stride {
            return Err(IoError::wkb("WKB coordinate count exceeds remaining input"));
        }
        let byte_len = count.wrapping_mul(stride.get());
        let bytes = self
            .value
            .get(self.offset..)
            .and_then(|remaining| remaining.get(..byte_len))
            .expect("WKB coordinate span was admitted against remaining input");
        self.offset = self.offset.wrapping_add(byte_len);
        // Dispatch ONCE on (byte order, axes) — the loop body is fully
        // monomorphized, so the hot loop re-tests neither.
        match byte_order {
            WkbByteOrder::BigEndian => decode_coordseq_axes::<true>(bytes, axes),
            WkbByteOrder::LittleEndian => decode_coordseq_axes::<false>(bytes, axes),
        }
    }

    fn read_ring(&mut self, byte_order: WkbByteOrder, axes: CoordinateAxes) -> Result<CoordSeq> {
        let count = self.read_u32(byte_order)? as usize;
        // Floor only: ≥ MIN_VERTICES_OPEN corners. Shared `admit_closed_ring`
        // then silent-closes XY-open rings or rejects Z/M-open / too-short
        // closed rings — same policy as WKT (A1 uniformity).
        if count < Ring::MIN_VERTICES_OPEN {
            return Err(IoError::wkb(format!(
                "WKB polygon ring requires at least {} coordinates, got {count}",
                Ring::MIN_VERTICES_OPEN
            )));
        }
        self.read_coordseq(byte_order, axes, count)
    }

    /// Read a WKB polygon, decoding a zero-ring body to the typed empty
    /// polygon carrying the header's axes (the OGC empty-polygon convention)
    /// rather than erroring.
    fn read_polygon_shape(
        &mut self,
        byte_order: WkbByteOrder,
        axes: CoordinateAxes,
    ) -> Result<Shape> {
        let ring_count = self.read_u32(byte_order)? as usize;
        self.validate_ring_count(ring_count)?;
        if ring_count == 0 {
            return Ok(Shape::typed_empty(GeometryKind::Polygon, axes));
        }
        // Shared untrusted ring admission (same policy as WKT): silent-close
        // XY-open rings; reject Z/M-open; then pack. Winding/simplicity stay
        // for `Geometry.validate()`. Shell first, holes into final Vec.
        let shell = crate::io::admit_closed_ring(self.read_ring(byte_order, axes)?)?;
        let mut holes = Vec::with_capacity(ring_count.wrapping_sub(1));
        for _ in 1..ring_count {
            holes.push(crate::io::admit_closed_ring(
                self.read_ring(byte_order, axes)?,
            )?);
        }
        Ok(Shape::Polygon(Polygon::new(shell, holes)))
    }

    /// Read a container's members, threading the effective SRID across
    /// siblings: the first member that declares an EWKB SRID under an
    /// SRID-less container ESTABLISHES it (PostGIS payloads may stamp members
    /// only), and every later member reconciles against it, so sibling
    /// disagreement rejects with the member path. The established SRID is
    /// returned so the container adopts it.
    fn read_nested_shapes(
        &mut self,
        byte_order: WkbByteOrder,
        container: u32,
        inherited: Option<NonZeroU32>,
        parent: Option<&WkbMemberPath<'_>>,
        depth: usize,
    ) -> Result<(Vec<Shape>, Option<NonZeroU32>)> {
        let count = self.read_u32(byte_order)? as usize;
        self.validate_element_count(count)?;
        let mut shapes = Vec::with_capacity(count);
        let mut effective = inherited;
        for index in 0..count {
            let node = WkbMemberPath {
                parent,
                container,
                index,
            };
            let (shape, member_srid) =
                self.read_geometry(effective, Some(&node), depth.wrapping_add(1))?;
            effective = effective.or(member_srid);
            shapes.push(shape);
        }
        Ok((shapes, effective))
    }

    /// Decode a homogeneous nested container one member at a time, moving
    /// each concrete payload straight into the final typed vector. `extract`
    /// returns a typed member, including `MultiMember::Empty` for a matching
    /// typed empty, or `Err` for wrong kind / axes. A
    /// malformed or wrong-kind member is still fully decoded before the same
    /// container error is raised, preserving the generic reader's error
    /// precedence.
    fn read_nested_typed<T>(
        &mut self,
        byte_order: WkbByteOrder,
        container: u32,
        inherited: Option<NonZeroU32>,
        parent: Option<&WkbMemberPath<'_>>,
        mut extract: impl FnMut(Shape) -> Result<MultiMember<T>>,
        depth: usize,
    ) -> Result<(Vec<MultiMember<T>>, Option<NonZeroU32>)> {
        let count = self.read_u32(byte_order)? as usize;
        self.validate_element_count(count)?;
        let mut items = Vec::with_capacity(count);
        let mut effective = inherited;
        let mut member_error = None;
        for index in 0..count {
            let node = WkbMemberPath {
                parent,
                container,
                index,
            };
            let (shape, member_srid) =
                self.read_geometry(effective, Some(&node), depth.wrapping_add(1))?;
            effective = effective.or(member_srid);
            match extract(shape) {
                Ok(item) => items.push(item),
                Err(error) => {
                    // The old generic staging lane decoded every sibling
                    // before validating container member kinds. Retain that
                    // boundary error precedence without retaining each Shape.
                    member_error.get_or_insert(error);
                },
            }
        }
        if let Some(error) = member_error {
            return Err(error);
        }
        Ok((items, effective))
    }
}

fn unsupported_wkb_geometry_type_message(raw: u32, base: u32) -> String {
    let name = match base {
        8 => Some("CircularString"),
        9 => Some("CompoundCurve"),
        10 => Some("CurvePolygon"),
        11 => Some("MultiCurve"),
        12 => Some("MultiSurface"),
        13 => Some("Curve"),
        14 => Some("Surface"),
        15 => Some("PolyhedralSurface"),
        16 => Some("TIN"),
        17 => Some("Triangle"),
        _ => None,
    };
    match name {
        Some(name) if raw == base => format!("unsupported WKB geometry type {name} ({base})"),
        Some(name) => format!("unsupported WKB geometry type {name} ({base}; raw {raw})"),
        None if raw == base => format!("unsupported WKB geometry type {base}"),
        None => format!("unsupported WKB geometry type {base} (raw {raw})"),
    }
}
