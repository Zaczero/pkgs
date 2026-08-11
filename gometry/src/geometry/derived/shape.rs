use crate::error::Result;
use crate::geometry::derived::{
    areal_centroid, canonicalize_concave_hull_points, collection_surface_point, lineal_centroid,
    lineal_surface_point, minimum_area_rectangle, monotone_chain_hull, multipoint_surface_point,
    native_concave_hull, point_centroid, polygonal_surface_point, polylabel_point,
    shape_from_open_hull, smallest_enclosing_circle,
};
use crate::geometry::{
    BufferCapStyle, BufferJoinStyle, DEFAULT_MITER_LIMIT, EmptyKind, GeometryErrorKind, LineSeq,
    Point, Polygon, Ring, Shape, bounds_to_shape, carry_ordinates, empty_geometry,
};
use crate::{NonNegative, Positive};

impl Shape {
    pub fn minimum_rotated_rectangle(&self) -> Result<Self> {
        // Empty input has no rectangle: `POLYGON EMPTY` (the operation's areal
        // output type), not the untyped collection the hull yields.
        if self.is_empty() {
            return Ok(Self::empty_polygon());
        }
        let points = self.points_vec();
        let open = monotone_chain_hull(&points);
        if open.len() < 3 {
            return carry_ordinates(
                shape_from_open_hull(&open, Self::empty_polygon),
                &[self],
                "minimum_rotated_rectangle",
                false,
            );
        }
        Ok(Self::Polygon(Polygon {
            shell: Ring::from_trusted_closed(minimum_area_rectangle(&open)?),
            holes: Vec::new().into(),
        }))
    }

    pub fn centroid(&self) -> Result<Self> {
        // The centroid is always a point; an empty geometry has the empty point
        // as its centroid (Shapely semantics). Own kernel — the JTS/GEOS
        // dimensional cascade: the HIGHEST nonzero dimension present wins,
        // areal (polygons) over lineal (lines + polygon boundaries) over points.
        // Each dimension is a focused pass evaluated LAZILY in cascade order, so
        // a normal nonzero-area polygon runs only the cheap triangle fold and
        // never pays the lineal pass's per-edge `sqrt` (which `finish` would
        // discard). The per-dimension sums are independent, so this is
        // bit-identical to one interleaved pass (WKT-parity with shapely is
        // oracle-tested), with no per-call geo-rs conversion.
        if self.is_empty() {
            return Ok(Self::empty_point());
        }
        areal_centroid(self)
            .or_else(|| lineal_centroid(self))
            .or_else(|| point_centroid(self))
            .map_or_else(
                || Ok(Self::empty_point()),
                |(x, y)| Ok(Self::Point(Point::new(x, y)?)),
            )
    }

    /// A representative point guaranteed to lie *on* the geometry — for a
    /// polygon an interior point (GEOS/Shapely `point_on_surface` semantics),
    /// not a boundary vertex. Empty geometries yield an empty geometry.
    pub fn point_on_surface(&self) -> Result<Self> {
        if self.is_empty() {
            return Ok(Self::empty_point());
        }
        // Polygonal input takes the own scanline kernel: geo's
        // `interior_point` pays a full geometry conversion plus per-call
        // allocation machinery (measured 1.5 us per small box, 8x
        // shapely); one half-open chord scan at the bbox bisector is a
        // few hundred ns and needs no conversion.
        if matches!(self, Self::Polygon(_) | Self::MultiPolygon(_))
            && let Some(point) = polygonal_surface_point(self)
        {
            return Ok(Self::Point(point));
        }
        // Lineal and puntal representatives are own columnar kernels too — a
        // line's arc-length midpoint and a multipoint's nearest-centroid vertex
        // both lie ON the geometry (the only contract). Collections and
        // degenerate polygons take the dimensional cascade below.
        let native = match self {
            Self::Point(point) => Some(Point::new_unchecked_xy(point.x, point.y)),
            Self::MultiPoint(points) => multipoint_surface_point(points),
            Self::LineString(line) => lineal_surface_point(std::iter::once(line.as_coords())),
            Self::MultiLineString(lines) => {
                lineal_surface_point(lines.iter().map(LineSeq::as_coords))
            },
            // Mixed/nested collections and degenerate polygons (whose interior
            // the scanline could not find) fall to the JTS dimensional
            // cascade: highest dimension first, areal degeneracy down to its
            // boundary linework and vertices.
            _ => collection_surface_point(std::slice::from_ref(self)),
        };
        native.map_or_else(|| Ok(Self::empty_point()), |point| Ok(Self::Point(point)))
    }

    pub fn envelope(&self) -> Self {
        // `envelope` produces bounding boxes, so its natural output type is
        // `Polygon` — an empty input yields `POLYGON EMPTY` (the output-type
        // convention), identical on the scalar and array surfaces. A
        // *non-empty* degenerate box still collapses to its lowest-dimensional
        // form (point/line) via `bounds_to_shape`.
        self.bounds()
            .map_or_else(Self::empty_polygon, bounds_to_shape)
    }

    pub fn convex_hull(&self) -> Result<Self> {
        if self.is_empty() {
            return Ok(empty_geometry());
        }
        // Native monotone chain over the coordinates (robust orientation
        // turns, collinear vertices dropped — the GEOS/strict convention);
        // no geo conversion round-trip. Degenerate hulls reduce to
        // Point/LineString like every degenerate polygon.
        let hull = monotone_chain_hull(&self.points_vec());
        let shape = shape_from_open_hull(&hull, empty_geometry);
        // Hull vertices are a subset of the input vertices, so Z/M always
        // resolve by exact match.
        carry_ordinates(shape, &[self], "convex_hull", false)
    }

    pub fn concave_hull(&self, concavity: f64, length_threshold: f64) -> Result<Self> {
        let concavity = NonNegative::try_new("concavity", concavity)?.get();
        let length_threshold = NonNegative::try_new("length_threshold", length_threshold)?.get();
        let mut points = self.unique_xy_points();
        if points.is_empty() {
            return Ok(Self::empty_polygon());
        }
        canonicalize_concave_hull_points(&mut points);
        let hull_indices = native_concave_hull(&points, concavity, length_threshold);
        let hull: Vec<Point> = hull_indices
            .into_iter()
            .map(|index| points[index])
            .collect();
        let hull = shape_from_open_hull(&hull, Self::empty_polygon);
        // Hull vertices are a subset of the input vertices (see convex_hull).
        carry_ordinates(hull, &[self], "concave_hull", false)
    }

    /// The west/south/east/north extreme vertices `[west, south, east,
    /// north]` (numeric X/Y; ties keep the first vertex in storage order;
    /// Z/M ride along). `None` for empty geometry.
    pub fn extremes(&self) -> Option<[Point; 4]> {
        let mut found: Option<[Point; 4]> = None;
        self.for_each_point(|point| {
            let extremes = found.get_or_insert([point; 4]);
            if point.x < extremes[0].x {
                extremes[0] = point;
            }
            if point.y < extremes[1].y {
                extremes[1] = point;
            }
            if point.x > extremes[2].x {
                extremes[2] = point;
            }
            if point.y > extremes[3].y {
                extremes[3] = point;
            }
        });
        found
    }

    pub fn polylabel(&self, tolerance: Option<f64>) -> Result<Self> {
        let tolerance = tolerance
            .map(|value| Positive::try_new("tolerance", value).map(Positive::get))
            .transpose()?;
        match self {
            Self::Polygon(_) | Self::MultiPolygon(_) if !self.is_empty() => {
                Ok(Self::Point(polylabel_point(self, tolerance)?.0))
            },
            // The representative point of any empty polygonal input is the
            // empty point, matching Shapely's
            // `point_on_surface(POLYGON EMPTY) == POINT EMPTY`.
            Self::MultiPolygon(_)
            | Self::Empty(EmptyKind::Polygon | EmptyKind::MultiPolygon, _) => {
                Ok(Self::empty_point())
            },
            _ => Err(GeometryErrorKind::PolygonRequired.into()),
        }
    }

    /// The smallest circle enclosing the geometry, as a `Polygon` — the
    /// standard shape. A single distinct vertex returns itself (a radius-zero
    /// circle); empty
    /// input returns `POLYGON EMPTY`. Computed with Welzl's algorithm over the
    /// geometry's vertices, then realized as a round 64-gon about the exact
    /// center and radius.
    pub fn minimum_bounding_circle(&self) -> Result<Self> {
        let points = self.unique_xy_points();
        if points.is_empty() {
            return Ok(Self::empty_polygon());
        }
        let (center, support) = smallest_enclosing_circle(&points);
        // Overflow-safe radius (`hypot` scales internally) — a support vertex at
        // ~1e155 would overflow a squared term but its distance stays finite.
        let radius = (center.x - support.x).hypot(center.y - support.y);
        if radius == 0.0 {
            // A single distinct vertex: the degenerate circle is that point.
            // (Consistent for any all-coincident input — unlike GEOS, which
            // returns the point for a `Point` yet an empty for a coincident
            // `MultiPoint`.)
            return Ok(Self::Point(center));
        }
        // The buffer emitter places 64 vertices on its requested radius, so
        // using the exact radius would create an inscribed polygon that can
        // exclude support points between vertices. Scale to the 64-gon's
        // circumradius so every edge is tangent to the exact enclosing circle.
        let polygon_radius = radius / (std::f64::consts::PI / 64.0).cos();
        Self::Point(center).buffer_with_style(
            polygon_radius,
            BufferCapStyle::Round,
            BufferJoinStyle::Round,
            std::num::NonZeroU32::new(16).expect("16 is non-zero"),
            Positive::try_new("miter_limit", DEFAULT_MITER_LIMIT)?,
        )
    }

    /// Radius of the smallest circle enclosing the geometry. Empty input
    /// returns ``NaN``; a single distinct vertex returns ``0``.
    pub fn minimum_bounding_radius(&self) -> f64 {
        let points = self.unique_xy_points();
        if points.is_empty() {
            return f64::NAN;
        }
        let (center, support) = smallest_enclosing_circle(&points);
        (center.x - support.x).hypot(center.y - support.y)
    }

    /// The largest circle inscribed in a polygonal geometry: the filled disk
    /// centered at the pole of inaccessibility, with radius reaching the
    /// nearest boundary point. Mirrors [`minimum_bounding_circle`](Self::minimum_bounding_circle)
    /// (both are filled `Polygon`s); the radius alone is
    /// [`maximum_inscribed_radius`](Self::maximum_inscribed_radius).
    pub fn maximum_inscribed_circle(&self, tolerance: Option<f64>) -> Result<Self> {
        let tolerance = tolerance
            .map(|value| Positive::try_new("tolerance", value).map(Positive::get))
            .transpose()?;
        match self {
            // The search yields both the center and its boundary contact point
            // from one shared boundary index — no second `boundary()` clone or
            // brute nearest-point scan.
            Self::Polygon(_) | Self::MultiPolygon(_) if !self.is_empty() => {
                let (center, witness) = polylabel_point(self, tolerance)?;
                let radius = (center.x - witness.x).hypot(center.y - witness.y);
                if radius == 0.0 {
                    // A degenerate (zero-area) polygon has no inscribed disk;
                    // the pole is its own witness — return that point.
                    return Ok(Self::Point(center));
                }
                Self::Point(center).buffer_with_style(
                    radius,
                    BufferCapStyle::Round,
                    BufferJoinStyle::Round,
                    std::num::NonZeroU32::new(16).expect("16 is non-zero"),
                    Positive::try_new("miter_limit", DEFAULT_MITER_LIMIT)?,
                )
            },
            Self::MultiPolygon(_)
            | Self::Empty(EmptyKind::Polygon | EmptyKind::MultiPolygon, _) => {
                Ok(Self::empty_polygon())
            },
            _ => Err(GeometryErrorKind::PolygonRequired.into()),
        }
    }

    /// Radius of the largest inscribed circle — the distance from the pole of
    /// inaccessibility to the nearest boundary point. Empty input returns
    /// ``NaN``. Twin of [`minimum_bounding_radius`](Self::minimum_bounding_radius).
    pub fn maximum_inscribed_radius(&self, tolerance: Option<f64>) -> Result<f64> {
        let tolerance = tolerance
            .map(|value| Positive::try_new("tolerance", value).map(Positive::get))
            .transpose()?;
        match self {
            Self::Polygon(_) | Self::MultiPolygon(_) if !self.is_empty() => {
                let (center, witness) = polylabel_point(self, tolerance)?;
                Ok((center.x - witness.x).hypot(center.y - witness.y))
            },
            Self::MultiPolygon(_)
            | Self::Empty(EmptyKind::Polygon | EmptyKind::MultiPolygon, _) => Ok(f64::NAN),
            _ => Err(GeometryErrorKind::PolygonRequired.into()),
        }
    }
}
