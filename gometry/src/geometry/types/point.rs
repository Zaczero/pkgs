use super::*;
use crate::error::Result;
use crate::geometry::{decimal_scale, quantize_to_scale};

impl Point {
    pub fn new(x: f64, y: f64) -> Result<Self> {
        Self::new_axes(x, y, ZOrdinate(None), MOrdinate(None))
    }

    pub(crate) fn new_axes(x: f64, y: f64, z: ZOrdinate, m: MOrdinate) -> Result<Self> {
        let ZOrdinate(z) = z;
        let MOrdinate(m) = m;
        if !x.is_finite()
            || !y.is_finite()
            || z.is_some_and(|value| !value.is_finite())
            || m.is_some_and(|value| !value.is_finite())
        {
            return Err(GeometryErrorKind::NonFiniteCoordinate.into());
        }
        Ok(Self {
            x,
            y,
            z: z.unwrap_or(0.0),
            m: m.unwrap_or(0.0),
            axes: CoordinateAxes::new(HasZ(z.is_some()), HasM(m.is_some())),
        })
    }

    /// Trusted constructor for already-validated coordinate flows (PROJ
    /// output after the pipeline's own finite check) — no revalidation, so a
    /// projection failure surfaces as the projection error, never as
    /// `NonFiniteCoordinate`.
    pub(crate) fn new_unchecked_axes(x: f64, y: f64, z: ZOrdinate, m: MOrdinate) -> Self {
        let ZOrdinate(z) = z;
        let MOrdinate(m) = m;
        Self {
            x,
            y,
            z: z.unwrap_or(0.0),
            m: m.unwrap_or(0.0),
            axes: CoordinateAxes::new(HasZ(z.is_some()), HasM(m.is_some())),
        }
    }

    /// The planar engine view of this point (drops Z/M/axes).
    pub const fn xy(&self) -> XY {
        XY {
            x: self.x,
            y: self.y,
        }
    }

    pub const fn new_unchecked_xy(x: f64, y: f64) -> Self {
        Self {
            x,
            y,
            z: 0.0,
            m: 0.0,
            axes: CoordinateAxes::XY,
        }
    }

    /// This point reduced to bare XY — the canonical form for FRESH derived
    /// output linework (witness pairs, circle radii, dedup'd point clouds).
    /// 2D math neither carries nor fabricates Z/M into invented geometry,
    /// and witness pairs mix vertex copies with computed points, so their
    /// axes MUST be normalized before entering one `CoordSeq`.
    pub const fn to_xy(self) -> Self {
        Self::new_unchecked_xy(self.x, self.y)
    }

    pub(crate) fn z(self) -> Option<f64> {
        self.axes.has_z().then_some(self.z)
    }

    pub(crate) fn m(self) -> Option<f64> {
        self.axes.has_m().then_some(self.m)
    }

    /// [`with_xy`](Self::with_xy) without the finiteness validation — for
    /// callers whose coordinates are already validated (the fallible column
    /// maps, whose `op` owns the domain checking).
    pub(crate) const fn with_xy_unchecked(self, x: f64, y: f64) -> Self {
        Self { x, y, ..self }
    }

    /// Replace the planar coordinates, carrying the Z/M ordinates and their
    /// presence unchanged. The new coordinates must be finite.
    pub fn with_xy(self, x: f64, y: f64) -> Result<Self> {
        if !x.is_finite() || !y.is_finite() {
            return Err(GeometryErrorKind::NonFiniteCoordinate.into());
        }
        Ok(Self { x, y, ..self })
    }

    /// Translate the planar coordinates by `(dx, dy)`, carrying Z/M unchanged.
    /// The resulting coordinates must be finite.
    pub fn translate_xy(self, dx: f64, dy: f64) -> Result<Self> {
        self.with_xy(self.x + dx, self.y + dy)
    }

    pub fn quantize(self, precision: i32) -> Self {
        // Table lookup once per point (precision is boundary-validated).
        let scale = decimal_scale(precision);
        Self {
            x: quantize_to_scale(self.x, scale),
            y: quantize_to_scale(self.y, scale),
            z: self
                .z()
                .map_or(0.0, |value| quantize_to_scale(value, scale)),
            m: self
                .m()
                .map_or(0.0, |value| quantize_to_scale(value, scale)),
            axes: self.axes,
        }
    }

    pub const fn force_2d(self) -> Self {
        Self {
            x: self.x,
            y: self.y,
            z: 0.0,
            m: 0.0,
            axes: CoordinateAxes::XY,
        }
    }
}
