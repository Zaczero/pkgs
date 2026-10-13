use super::{
    AffineLinear, MOrdinate, Point, Result, Shape, ZOrdinate, affine_x_row_identity,
    affine_y_row_identity, stable_affine_xy,
};

impl Shape {
    /// Fallible columnar XY transform generic over the error — the in-core
    /// CRS projections' engine (domain errors propagate per pair; Z/M carry
    /// through untouched, matching the per-point `with_xy` semantics).
    #[expect(
        clippy::impl_trait_in_params,
        reason = "the one-pass coordinate callback is deliberately existential"
    )]
    pub fn map_xy_with<E>(
        &self,
        op: impl Fn(f64, f64) -> Result<(f64, f64), E> + Copy,
    ) -> Result<Self, E>
    where
        E: From<crate::error::Error>,
    {
        Ok(match self {
            Self::Point(point) => {
                let (x, y) = op(point.x, point.y)?;
                Self::Point(point.with_xy_raw(x, y))
            },
            Self::MultiPoint(points) => Self::MultiPoint(points.try_map_xy_with(op)?),
            Self::LineString(points) => Self::LineString(points.try_map_xy_with(op)?),
            Self::MultiLineString(lines) => Self::MultiLineString(
                lines.try_map_values(lines.axes(), |line| line.try_map_xy_with(op))?,
            ),
            Self::Polygon(polygon) => Self::Polygon(polygon.map_rings_xy_with(op)?),
            Self::MultiPolygon(polygons) => Self::MultiPolygon(
                polygons
                    .try_map_values(polygons.axes(), |polygon| polygon.map_rings_xy_with(op))?,
            ),
            Self::GeometryCollection(geometries) => Self::GeometryCollection(
                geometries.try_map_members(|geometry| geometry.map_xy_with(op))?,
            ),
            Self::Empty(..) => self.clone(),
        })
    }

    /// Columnar XY transform over every coordinate sequence of the shape
    /// (Z/M untouched), validating finiteness once per column. This is the
    /// matrix-form affine path; origin-relative transforms use their exact
    /// pivot evaluator below.
    #[expect(
        clippy::many_single_char_names,
        reason = "a,b,d,e are the canonical affine matrix coefficients"
    )]
    fn affine(&self, matrix: &[f64; 6]) -> Result<Self> {
        let [a, b, d, e, xoff, yoff] = *matrix;
        self.try_map_coordseqs(
            |seq| seq.try_affine(matrix),
            |point| {
                let (x, y) =
                    if affine_x_row_identity(a, b, xoff) && affine_y_row_identity(d, e, yoff) {
                        (point.x, point.y)
                    } else {
                        stable_affine_xy(a, b, d, e, xoff, yoff, point.x, point.y)
                    };
                point.with_xy(x, y)
            },
        )
    }
}

impl Shape {
    /// Apply a 2D affine transform `(a, b, d, e, xoff, yoff)` to every
    /// coordinate: `x' = a*x + b*y + xoff`, `y' = d*x + e*y + yoff`. The z
    /// and m axes pass through unchanged via [`Shape::map_points`]. Plain
    /// multiply-adds: `mul_add` is a libm `fma` call per ordinate at the
    /// x86-64-v2 baseline, and affine placement is not a robustness seam.
    pub fn affine_transform(&self, matrix: &[f64; 6]) -> Result<Self> {
        // Identity placement is common in parameterized pipelines —
        // translate(0, 0), rotate(0), scale(1) all reduce to it here, the
        // one seam they share — so share the input instead of rebuilding.
        // Exact float equality is the point: only the bit-identical (or
        // -0.0) identity may skip.
        if affine_x_row_identity(matrix[0], matrix[1], matrix[4])
            && affine_y_row_identity(matrix[2], matrix[3], matrix[5])
        {
            return Ok(self.clone());
        }
        self.affine(matrix)
    }

    pub fn rotate(&self, angle: f64, origin: (f64, f64)) -> Result<Self> {
        let (sin, cos) = angle.sin_cos();
        self.affine_about_origin(cos, -sin, sin, cos, origin)
    }

    pub fn scale(&self, xfact: f64, yfact: f64, origin: (f64, f64)) -> Result<Self> {
        self.affine_about_origin(xfact, 0.0, 0.0, yfact, origin)
    }

    pub fn skew(&self, xs: f64, ys: f64, origin: (f64, f64)) -> Result<Self> {
        self.affine_about_origin(1.0, xs.tan(), ys.tan(), 1.0, origin)
    }

    /// Linear map about `origin` evaluated as `origin + M*(p - origin)` so
    /// extreme-but-finite centers do not overflow the expanded
    /// `xoff = ox - a*ox` form (which can produce `inf - inf` NaNs).
    fn affine_about_origin(
        &self,
        m00: f64,
        m01: f64,
        m10: f64,
        m11: f64,
        origin: (f64, f64),
    ) -> Result<Self> {
        let linear = AffineLinear::new([m00, m01, m10, m11])?;
        if linear.is_identity() {
            return Ok(self.clone());
        }
        let transform = linear.at(origin)?;
        self.try_map_coordseqs(
            |seq| seq.try_affine_about_origin(&transform),
            |point| {
                let (x, y) = transform.map_xy(point.x, point.y)?;
                Point::new_axes(x, y, ZOrdinate(point.z()), MOrdinate(point.m()))
            },
        )
    }

    pub fn translate(&self, xoff: f64, yoff: f64) -> Result<Self> {
        self.affine_transform(&[1.0, 0.0, 0.0, 1.0, xoff, yoff])
    }

    /// Origin used by the ergonomic helpers when none is supplied: the
    /// centroid, or `(0, 0)` for empty geometries.
    pub fn centroid_xy(&self) -> Result<(f64, f64)> {
        Ok(match self.centroid()? {
            Self::Point(point) => (point.x, point.y),
            Self::MultiPoint(_)
            | Self::LineString(_)
            | Self::MultiLineString(_)
            | Self::Polygon(_)
            | Self::MultiPolygon(_)
            | Self::GeometryCollection(_)
            | Self::Empty(..) => (0.0, 0.0),
        })
    }

    /// Bounding-box center, or `(0, 0)` for empty geometries.
    pub fn bounds_center_xy(&self) -> (f64, f64) {
        self.bounds().map_or((0.0, 0.0), |bounds| {
            (
                f64::midpoint(bounds.minx(), bounds.maxx()),
                f64::midpoint(bounds.miny(), bounds.maxy()),
            )
        })
    }
}
