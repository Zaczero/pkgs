use std::num::NonZeroU32;

use crate::error::Result;
use crate::geometry::derived::{
    areal_centroid, build_inscribed_circle, canonicalize_concave_hull_points,
    collection_surface_point, emit_minimum_bounding_circle, emit_zero_radius_point,
    lineal_centroid, lineal_surface_point, minimum_area_rectangle, monotone_chain_hull,
    multipoint_surface_point, native_concave_hull, point_centroid, polygonal_surface_point,
    polylabel_center_radius, polylabel_point, scale_aware_default_tolerance, shape_from_open_hull,
    shape_from_selected_hull, smallest_enclosing_circle,
};
use crate::geometry::{
    DirectedOptimizationResult, GeometryErrorKind, LineSeq, Point, Polygon, Ring, Shape,
    bounds_to_shape, carry_ordinates, directed_inradius_scaled,
};
use crate::{NonNegative, Positive};

const CIRCLE_QUADRANT_SEGMENTS: NonZeroU32 = NonZeroU32::new(16).expect("16 is non-zero");

impl Shape {
    pub fn minimum_rotated_rectangle(&self) -> Result<Self> {
        // Empty input has no rectangle: `POLYGON EMPTY` (the operation's areal
        // output type), not the untyped collection the hull yields.
        if self.is_empty() {
            return Ok(Self::empty_polygon());
        }
        let points = self.points_vec()?;
        let open: Vec<Point> = monotone_chain_hull(&points)
            .into_iter()
            .map(|index| points[index])
            .collect();
        if open.len() < 3 {
            return carry_ordinates(
                shape_from_open_hull(&open, Self::empty_polygon)?,
                &[self],
                "minimum_rotated_rectangle",
                false,
            );
        }
        Ok(Self::Polygon(Polygon::new(
            Ring::try_from_closed(minimum_area_rectangle(&open)?)?,
            Vec::new(),
        )))
    }

    pub fn centroid(&self) -> Result<Self> {
        // The centroid is always a point; an empty geometry has the empty point
        // as its centroid. The highest nonzero dimension present wins: areal
        // (polygons) over lineal (lines and polygon boundaries) over points.
        // Each dimension is a focused pass evaluated lazily in cascade order,
        // so a normal nonzero-area polygon runs only the triangle fold and
        // never pays the lineal pass's per-edge `sqrt` that its result would
        // discard. The per-dimension sums are independent and require no
        // per-call geometry conversion.
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
    /// polygon an interior point, not a boundary vertex. Empty geometries
    /// yield an empty geometry.
    pub fn point_on_surface(&self) -> Result<Self> {
        if self.is_empty() {
            return Ok(Self::empty_point());
        }
        // Polygonal input takes the own scanline kernel: one half-open chord
        // scan at the bbox bisector is enough to find a representative without
        // converting the geometry or allocating per edge.
        let polygonal = match self {
            Self::Polygon(polygon) => polygonal_surface_point(std::iter::once(polygon))?,
            Self::MultiPolygon(polygons) => polygonal_surface_point(polygons.as_slice().iter())?,
            _ => None,
        };
        if let Some(point) = polygonal {
            return Ok(Self::Point(point));
        }
        // Lineal and puntal representatives are own columnar kernels too — a
        // line's arc-length midpoint and a multipoint's nearest-centroid vertex
        // both lie ON the geometry (the only contract). Collections and
        // degenerate polygons take the dimensional cascade below.
        let native = match self {
            Self::Point(point) => Some(Point::new_raw_xy(point.x, point.y)),
            Self::MultiPoint(points) => multipoint_surface_point(points.xs(), points.ys()),
            Self::LineString(line) => lineal_surface_point(std::iter::once(line.as_coords())),
            Self::MultiLineString(lines) => {
                lineal_surface_point(lines.iter().map(LineSeq::as_coords))
            },
            // Mixed/nested collections and degenerate polygons (whose interior
            // the scanline could not find) fall to the same dimensional
            // cascade: highest dimension first, areal degeneracy down to its
            // boundary linework and vertices.
            _ => collection_surface_point(std::slice::from_ref(self))?,
        };
        native.map_or_else(|| Ok(Self::empty_point()), |point| Ok(Self::Point(point)))
    }

    pub fn envelope(&self) -> Result<Self> {
        // `envelope` produces bounding boxes, so its natural output type is
        // `Polygon` — an empty input yields `POLYGON EMPTY` (the output-type
        // convention), identical on the scalar and array surfaces. A
        // *non-empty* degenerate box still collapses to its lowest-dimensional
        // form (point/line) via `bounds_to_shape`.
        self.bounds()
            .map_or_else(|| Ok(Self::empty_polygon()), bounds_to_shape)
    }

    pub fn convex_hull(&self) -> Result<Self> {
        if self.is_empty() {
            return Ok(Self::empty_polygon());
        }
        // Native monotone chain over first-seen XY vertices (robust
        // orientation turns, collinear vertices dropped). Degenerate hulls
        // reduce to Point/LineString like every degenerate polygon. Surviving
        // vertices are copies of those source points, including Z/M and
        // signed-zero spelling.
        let points = self.first_seen_xy_vertices();
        let hull: Vec<Point> = monotone_chain_hull(&points)
            .into_iter()
            .map(|index| points[index])
            .collect();
        shape_from_selected_hull(&hull, Self::empty_polygon)
    }

    pub fn concave_hull(&self, concavity: f64, length_threshold: f64) -> Result<Self> {
        let concavity = NonNegative::try_new("concavity", concavity)?.get();
        let length_threshold = NonNegative::try_new("length_threshold", length_threshold)?.get();
        let mut points = self.first_seen_xy_vertices();
        if points.is_empty() {
            return Ok(Self::empty_polygon());
        }
        canonicalize_concave_hull_points(&mut points);
        let hull_indices = native_concave_hull(&points, concavity, length_threshold)?;
        let hull: Vec<Point> = hull_indices
            .into_iter()
            .map(|index| points[index])
            .collect();
        shape_from_selected_hull(&hull, Self::empty_polygon)
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
        self.polygonal().ok_or(GeometryErrorKind::PolygonRequired)?;
        // The representative point of any empty polygonal input is the empty
        // point.
        if self.is_empty() {
            return Ok(Self::empty_point());
        }
        Ok(Self::Point(polylabel_point(self, tolerance)?))
    }

    /// The smallest circle enclosing the geometry, as a `Polygon` — the
    /// standard shape. A single distinct vertex returns itself (a radius-zero
    /// circle); empty
    /// input returns `POLYGON EMPTY`. The scalar minimum radius is the
    /// correctly rounded mathematical source minimum. The geometry is a
    /// certified circumscribed 64-gon whose edges enclose that minimum disk and
    /// whose vertices lie outside it.
    pub fn minimum_bounding_circle(&self) -> Result<Self> {
        if self.is_empty() {
            return Ok(Self::empty_polygon());
        }
        let mut source_finite = true;
        self.for_each_point(|point| {
            source_finite &= point.x.is_finite() && point.y.is_finite();
        });
        if !source_finite {
            return Err(GeometryErrorKind::unrepresentable_output(
                "minimum_bounding_circle",
            ));
        }
        let points = self.try_unique_xy_points()?;
        if points.is_empty() {
            return Ok(Self::empty_polygon());
        }
        let circle = smallest_enclosing_circle(&points)?;
        let disk = circle.certified_disk(&points)?;
        if disk.is_exact_zero() {
            // A single distinct vertex: the degenerate circle is that source
            // point, including its stored signed-zero spelling.
            return Ok(Self::Point(disk.center()?));
        }
        emit_minimum_bounding_circle(
            &disk,
            CIRCLE_QUADRANT_SEGMENTS,
            &mut crate::geometry::ExpansionBudget::new("minimum_bounding_circle", "realization"),
        )
    }

    /// Radius of the smallest circle enclosing the geometry. Empty input
    /// returns ``NaN``; a single distinct vertex returns ``0``.
    #[cfg(test)]
    pub fn minimum_bounding_radius(&self) -> Result<f64> {
        if self.is_empty() {
            return Ok(f64::NAN);
        }
        let points = self.try_unique_xy_points()?;
        if points.is_empty() {
            return Ok(f64::NAN);
        }
        let circle = smallest_enclosing_circle(&points)?;
        circle.certified_disk(&points)?.radius()
    }

    /// The largest circle inscribed in a planar polygonal geometry, emitted as
    /// a contained fixed-resolution filled disk. The center is one
    /// representable point whose exact boundary clearance is within
    /// `tolerance` of the global inradius; the radius alone is
    /// [`maximum_inscribed_radius`](Self::maximum_inscribed_radius).
    ///
    /// Geographic callers use the documented local-projection approximation
    /// through the CRS-aware wrapper.
    pub fn maximum_inscribed_circle(&self, tolerance: Option<f64>) -> Result<Self> {
        self.maximum_inscribed_circle_in_frame(tolerance, true)
    }

    /// Run the planar maximum-circle kernel in a local projected frame. The
    /// geographic caller opts out of source-frame containment because that
    /// frame is an explicitly approximate local-projection operation.
    pub(crate) fn maximum_inscribed_circle_in_frame(
        &self,
        tolerance: Option<f64>,
        certify_source: bool,
    ) -> Result<Self> {
        let tolerance = tolerance
            .map(|value| Positive::try_new("tolerance", value).map(Positive::get))
            .transpose()?;
        self.polygonal().ok_or(GeometryErrorKind::PolygonRequired)?;
        if self.is_empty() {
            return Ok(Self::empty_polygon());
        }
        let (center, radius, result) = if certify_source {
            let result = self.certified_inradius(tolerance, "maximum_inscribed_circle", 1.0)?;
            let center = result.center();
            let radius = result.radius_in_scale(1.0)?;
            (center, radius, Some(result))
        } else {
            let (center, radius) = polylabel_center_radius(self, tolerance)?;
            (center, radius, None)
        };
        if radius == 0.0 || (radius > 0.0 && radius < f64::MIN_POSITIVE) {
            return emit_zero_radius_point(self, center);
        }
        build_inscribed_circle(
            center,
            radius,
            CIRCLE_QUADRANT_SEGMENTS,
            result
                .as_ref()
                .map(|result| (result.source_support(), result.boundary_support())),
        )
    }

    pub(crate) fn maximum_inscribed_radius_in_frame(
        &self,
        tolerance: Option<f64>,
        certify_source: bool,
    ) -> Result<f64> {
        self.maximum_inscribed_radius_in_scale(tolerance, certify_source, 1.0)
    }

    pub(crate) fn maximum_inscribed_radius_in_scale(
        &self,
        tolerance: Option<f64>,
        certify_source: bool,
        scale: f64,
    ) -> Result<f64> {
        let tolerance = tolerance
            .map(|value| Positive::try_new("tolerance", value).map(Positive::get))
            .transpose()?;
        self.polygonal().ok_or(GeometryErrorKind::PolygonRequired)?;
        if self.is_empty() {
            return Ok(f64::NAN);
        }
        if certify_source {
            Ok(self
                .certified_inradius(tolerance, "maximum_inscribed_radius", scale)?
                .radius_in_scale(scale)?)
        } else {
            Ok(polylabel_center_radius(self, tolerance)?.1 * scale)
        }
    }

    fn certified_inradius(
        &self,
        tolerance: Option<f64>,
        operation: &'static str,
        scale: f64,
    ) -> Result<DirectedOptimizationResult> {
        let mut finite = true;
        self.for_each_point(|point| {
            finite &= point.x.is_finite() && point.y.is_finite();
        });
        if !finite {
            return Err(GeometryErrorKind::unrepresentable_output(operation));
        }
        let default_tolerance = scale_aware_default_tolerance(self)?;
        let tolerance = tolerance.unwrap_or(default_tolerance * scale);
        directed_inradius_scaled(self, tolerance, scale)
    }
}
