//! Exact homogeneous point construction and finite materialization.

use std::cell::OnceCell;
use std::cmp::Ordering;
use std::rc::Rc;

use super::incidence::{intersection_homogeneous, line_intersection, orient_points};
use super::{
    Coords, ExactDyadic, ExactLine, ExactPoint, ExactRational, FrameHit, Homogeneous, Interval,
    Recipe, VERIFY_INTERVALS, compare_ratios, orient2d_dyadic_points, ratio_interval, round_ratio,
    try_difference,
};
use crate::error::{Error, ErrorKind};
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::tessellation::circumcenter::CertifiedCenter;
use crate::geometry::{GeometryErrorKind, Result, XY};

impl Recipe {
    /// The same geometric construction, up to the order of its sites and of a
    /// side's two corners: a bisector is symmetric in its sites, a
    /// circumcenter in its three, so the exact points are equal.
    fn same_construction(&self, other: &Self) -> bool {
        let same_pair = |a: [XY; 2], b: [XY; 2]| {
            (a[0] == b[0] && a[1] == b[1]) || (a[0] == b[1] && a[1] == b[0])
        };
        match (self, other) {
            (Self::Circumcenter(a), Self::Circumcenter(b)) => {
                let key = |sites: &[XY; 3]| {
                    let mut keys = sites.map(|xy| (xy.x.to_bits(), xy.y.to_bits()));
                    keys.sort_unstable();
                    keys
                };
                key(a) == key(b)
            },
            (
                Self::BisectorHit { sites: a, side: sa },
                Self::BisectorHit { sites: b, side: sb },
            ) => same_pair(*a, *b) && same_pair(*sa, *sb),
            _ => false,
        }
    }
}

impl ExactPoint {
    pub(in crate::geometry) fn from_dyadics(x: ExactDyadic, y: ExactDyadic) -> Self {
        Self::homogeneous(x, y, ExactDyadic::one())
    }

    pub(in crate::geometry) fn from_homogeneous_dyadics([x, y, w]: [ExactDyadic; 3]) -> Self {
        Self::homogeneous(x, y, w)
    }

    /// The ONLY constructor. Every homogeneous triple gets its certified
    /// interval here, so a point can never exist without its filter bound.
    pub(super) fn homogeneous(x: ExactDyadic, y: ExactDyadic, w: ExactDyadic) -> Self {
        let bounds = [ratio_interval(&x, &w), ratio_interval(&y, &w)];
        if VERIFY_INTERVALS {
            verify_interval(&x, &w, bounds[0]);
            verify_interval(&y, &w, bounds[1]);
        }
        Self {
            coords: Coords::Exact(Rc::new(Homogeneous { x, y, w })),
            bounds,
        }
    }

    /// A circumcenter whose nearest-even rounding is certified: the exact
    /// value lies within half an ulp of `center.rounded`, so within its two
    /// binary64 neighbours, which are the bounds every comparison reads first.
    pub(super) fn certified_circumcenter(sites: [XY; 3], center: &CertifiedCenter) -> Self {
        Self {
            coords: Coords::Certified {
                recipe: Recipe::Circumcenter(sites),
                rounded: center.rounded,
                exact: OnceCell::new(),
            },
            bounds: [
                Interval {
                    lo: center.bounds[0].0,
                    hi: center.bounds[0].1,
                },
                Interval {
                    lo: center.bounds[1].0,
                    hi: center.bounds[1].1,
                },
            ],
        }
    }

    pub(super) fn coords(&self) -> Result<&Homogeneous> {
        let coords = match &self.coords {
            Coords::Exact(coords) => coords,
            Coords::Double { xy, exact } => exact.get_or_init(|| {
                Rc::new(Homogeneous {
                    x: ExactDyadic::from_f64(xy.x),
                    y: ExactDyadic::from_f64(xy.y),
                    w: ExactDyadic::one(),
                })
            }),
            Coords::Certified { recipe, exact, .. } => exact.get_or_try_init(|| {
                let coords = match *recipe {
                    Recipe::Circumcenter(sites) => intersection_homogeneous(
                        &ExactLine::perpendicular_bisector(sites[0], sites[1])?,
                        &ExactLine::perpendicular_bisector(sites[0], sites[2])?,
                    )?,
                    Recipe::BisectorHit { sites, side } => intersection_homogeneous(
                        &ExactLine::perpendicular_bisector(sites[0], sites[1])?,
                        &ExactLine::through_points(
                            &Self::from_xy(side[0]),
                            &Self::from_xy(side[1]),
                        )?,
                    )?,
                };
                Ok::<_, Error>(Rc::new(coords))
            })?,
        };
        Ok(coords)
    }

    pub(in crate::geometry::tessellation) fn projected_coordinate(
        &self,
        x_coefficient: &ExactDyadic,
        y_coefficient: &ExactDyadic,
    ) -> Result<ExactRational> {
        let coords = self.coords()?;
        let x = coords.x.try_product(x_coefficient)?;
        let y = coords.y.try_product(y_coefficient)?;
        ExactRational::new(x.try_add(y)?, coords.w.clone())
    }

    /// The stored double behind a point made from one, `None` for any other
    /// construction.
    pub(in crate::geometry::tessellation) fn as_double(&self) -> Option<XY> {
        match &self.coords {
            Coords::Double { xy, .. } => Some(*xy),
            Coords::Exact(_) | Coords::Certified { .. } => None,
        }
    }

    /// Where the perpendicular bisector of `sites` meets the frame side
    /// through `side`, certified.
    #[expect(
        clippy::float_cmp,
        reason = "an axis-aligned side's two corners share a coordinate exactly, by construction"
    )]
    pub(in crate::geometry::tessellation) fn certified_bisector_hit(
        sites: [XY; 2],
        side: [XY; 2],
    ) -> FrameHit {
        use crate::geometry::tessellation::circumcenter::{
            BisectorHit, SideAxis, certified_bisector_hit,
        };
        let (axis, value) = if side[0].x == side[1].x {
            (SideAxis::FixedX, side[0].x)
        } else if side[0].y == side[1].y {
            (SideAxis::FixedY, side[0].y)
        } else {
            return FrameHit::Declined;
        };
        let free = match certified_bisector_hit(sites[0], sites[1], axis, value) {
            BisectorHit::Declined => return FrameHit::Declined,
            BisectorHit::Parallel => return FrameHit::Parallel,
            BisectorHit::Free(free) => free,
        };
        let rounded = match axis {
            SideAxis::FixedX => XY::new(value, free),
            SideAxis::FixedY => XY::new(free, value),
        };
        let bounds = match axis {
            SideAxis::FixedX => [Interval::exact(value), Interval {
                lo: free.next_down(),
                hi: free.next_up(),
            }],
            SideAxis::FixedY => [
                Interval {
                    lo: free.next_down(),
                    hi: free.next_up(),
                },
                Interval::exact(value),
            ],
        };
        FrameHit::Point(Self {
            coords: Coords::Certified {
                recipe: Recipe::BisectorHit { sites, side },
                rounded,
                exact: OnceCell::new(),
            },
            bounds,
        })
    }

    pub(in crate::geometry::tessellation) fn x(&self) -> Result<&ExactDyadic> {
        Ok(&self.coords()?.x)
    }

    pub(in crate::geometry::tessellation) fn y(&self) -> Result<&ExactDyadic> {
        Ok(&self.coords()?.y)
    }

    pub(in crate::geometry::tessellation) fn w(&self) -> Result<&ExactDyadic> {
        Ok(&self.coords()?.w)
    }
}

pub(super) fn verify_interval(
    numerator: &ExactDyadic,
    denominator: &ExactDyadic,
    bounds: Interval,
) {
    if denominator.is_zero() {
        return;
    }
    let one = ExactDyadic::one();
    if bounds.lo.is_finite() {
        assert!(
            compare_ratios(
                &ExactDyadic::from_f64(bounds.lo),
                &one,
                numerator,
                denominator
            )
            .is_ok_and(|order| order != Ordering::Greater),
            "interval lower bound {} exceeds the exact value",
            bounds.lo
        );
    }
    if bounds.hi.is_finite() {
        assert!(
            compare_ratios(
                &ExactDyadic::from_f64(bounds.hi),
                &one,
                numerator,
                denominator
            )
            .is_ok_and(|order| order != Ordering::Less),
            "interval upper bound {} is below the exact value",
            bounds.hi
        );
    }
}

impl ExactPoint {
    pub(in crate::geometry) fn from_xy(point: XY) -> Self {
        Self {
            coords: Coords::Double {
                xy: point,
                exact: OnceCell::new(),
            },
            bounds: [Interval::exact(point.x), Interval::exact(point.y)],
        }
    }

    pub(in crate::geometry::tessellation) fn compare_lex(&self, other: &Self) -> Result<Ordering> {
        Ok(match self.compare_x(other)? {
            Ordering::Equal => self.compare_y(other)?,
            order @ (Ordering::Less | Ordering::Greater) => order,
        })
    }

    pub(in crate::geometry) fn same_position(&self, other: &Self) -> Result<bool> {
        if let (Coords::Certified { recipe: left, .. }, Coords::Certified { recipe: right, .. }) =
            (&self.coords, &other.coords)
            && left.same_construction(right)
        {
            // One construction reached from two cells: the same exact point,
            // without materialising either side.
            return Ok(true);
        }
        Ok(!self.separated_from(other)
            && self.compare_x(other)?.is_eq()
            && self.compare_y(other)?.is_eq())
    }

    /// Whether the point is finite (nonzero weight). A certified circumcenter
    /// is finite by construction, so this never materializes its coordinates.
    pub(in crate::geometry::tessellation) fn is_finite(&self) -> bool {
        match &self.coords {
            Coords::Certified { .. } | Coords::Double { .. } => true,
            Coords::Exact(coords) => !coords.w.is_zero(),
        }
    }

    pub(in crate::geometry::tessellation) fn midpoint(&self, other: &Self) -> Result<Self> {
        let (left, right) = (self.coords()?, other.coords()?);
        let denominator = left
            .w
            .try_product(&right.w)?
            .try_product(&ExactDyadic::from_f64(2.0))?;
        Ok(Self::homogeneous(
            left.x
                .try_product(&right.w)?
                .try_add(right.x.try_product(&left.w)?)?,
            left.y
                .try_product(&right.w)?
                .try_add(right.y.try_product(&left.w)?)?,
            denominator,
        ))
    }

    pub(in crate::geometry) fn compare_x(&self, other: &Self) -> Result<Ordering> {
        if let Some(order) = self.bounds[0].compare(other.bounds[0]) {
            return Ok(order);
        }
        compare_ratios(self.x()?, self.w()?, other.x()?, other.w()?)
    }

    pub(in crate::geometry) fn compare_y(&self, other: &Self) -> Result<Ordering> {
        if let Some(order) = self.bounds[1].compare(other.bounds[1]) {
            return Ok(order);
        }
        compare_ratios(self.y()?, self.w()?, other.y()?, other.w()?)
    }

    /// The certified outward bounds on `x` and `y`.
    pub(in crate::geometry::tessellation) fn bounds(&self) -> [Interval; 2] {
        self.bounds
    }

    /// Certified positive: both coordinates lie strictly inside
    /// `[minx, maxx, miny, maxy]`, decided on the bounds alone.
    pub(in crate::geometry::tessellation) fn strictly_inside(&self, bounds: [f64; 4]) -> bool {
        self.bounds[0].lo > bounds[0]
            && self.bounds[0].hi < bounds[1]
            && self.bounds[1].lo > bounds[2]
            && self.bounds[1].hi < bounds[3]
    }

    /// Certified negative: two points whose x- or y-intervals are strictly
    /// separated cannot be the same position.
    pub(in crate::geometry::tessellation) fn separated_from(&self, other: &Self) -> bool {
        self.bounds[0].disjoint_from(other.bounds[0])
            || self.bounds[1].disjoint_from(other.bounds[1])
    }

    /// Materialize one shared exact vertex exactly once. Construction code must
    /// intern the returned coordinate by vertex identity and reuse it for every
    /// incident half-edge. Under gometry's topological-snap contract a clip event may
    /// differ from the source clip at the ULP level when exact incidence has no
    /// binary64 representation; validity, event order, empty differences, and
    /// interior disjointness remain mandatory, while bit-exact `equals` does
    /// not. Recomputing a shared event per cell can reintroduce cracks between
    /// adjacent cells.
    pub(in crate::geometry::tessellation) fn round_nearest_even(&self) -> Result<XY> {
        match &self.coords {
            Coords::Certified { rounded, .. } => return Ok(*rounded),
            Coords::Double { xy, .. } => return Ok(*xy),
            Coords::Exact(_) => {},
        }
        if !self.is_finite() {
            return Err(GeometryErrorKind::voronoi("exact vertex lies at infinity"));
        }
        Ok(XY::new(
            round_ratio(self.x()?, self.w()?)?,
            round_ratio(self.y()?, self.w()?)?,
        ))
    }

    /// The nearest-even rounding, or `None` when the point lies at infinity
    /// or rounds outside the finite binary64 range.
    pub(in crate::geometry::tessellation) fn finite_rounding(&self) -> Result<Option<XY>> {
        if !self.is_finite() {
            return Ok(None);
        }
        match self.round_nearest_even() {
            Ok(point) => Ok(Some(point)),
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::Geometry(GeometryErrorKind::ExactCoordinateOutOfRange)
                ) =>
            {
                Ok(None)
            },
            Err(error) => Err(error),
        }
    }
}

impl ExactPoint {
    pub(super) fn from_rationals(x: &ExactRational, y: &ExactRational) -> Result<Self> {
        let numerator_x = x.numerator.try_product(&y.denominator)?;
        let numerator_y = y.numerator.try_product(&x.denominator)?;
        let denominator = x.denominator.try_product(&y.denominator)?;
        Ok(Self::homogeneous(numerator_x, numerator_y, denominator))
    }
}

const SQUARED_DISTANCE_ERRBOUND: f64 = 128.0 * f64::EPSILON;

fn normal_or_zero(value: f64) -> bool {
    value == 0.0 || value.is_normal()
}

fn squared_distance(origin: XY, point: XY) -> Result<ExactDyadic> {
    let dx = try_difference(point.x, origin.x)?;
    let dy = try_difference(point.y, origin.y)?;
    dx.try_product(&dx)?.try_add(dy.try_product(&dy)?)
}

fn distance_within_filtered(origin: XY, point: XY, radius: f64) -> Option<bool> {
    let dx = point.x - origin.x;
    let dy = point.y - origin.y;
    let x_squared = dx * dx;
    let y_squared = dy * dy;
    let distance_squared = x_squared + y_squared;
    let radius_squared = radius * radius;
    let difference = distance_squared - radius_squared;
    let magnitude = distance_squared + radius_squared;
    if ![
        dx,
        dy,
        x_squared,
        y_squared,
        distance_squared,
        radius_squared,
        difference,
        magnitude,
    ]
    .into_iter()
    .all(normal_or_zero)
    {
        return None;
    }
    let bound = (magnitude * SQUARED_DISTANCE_ERRBOUND).next_up();
    if difference > bound {
        Some(false)
    } else if difference < -bound {
        Some(true)
    } else {
        None
    }
}

pub(in crate::geometry::tessellation) fn distance_within(
    origin: XY,
    point: XY,
    radius: f64,
) -> Result<bool> {
    if let Some(within) = distance_within_filtered(origin, point, radius) {
        if VERIFY_INTERVALS && within != distance_within_exact(origin, point, radius)? {
            return Err(GeometryErrorKind::voronoi(
                "filtered distance inclusion disagrees",
            ));
        }
        return Ok(within);
    }
    distance_within_exact(origin, point, radius)
}

fn distance_within_exact(origin: XY, point: XY, radius: f64) -> Result<bool> {
    let radius = ExactDyadic::from_f64(radius);
    Ok(squared_distance(origin, point)?.cmp(&radius.try_product(&radius)?) == Ordering::Less)
}

pub(in crate::geometry) fn circumcenter_points(
    a: &ExactPoint,
    b: &ExactPoint,
    c: &ExactPoint,
) -> Result<ExactPoint> {
    if orient_points(a, b, c)? == ExactSign::Zero {
        return Err(GeometryErrorKind::voronoi(
            "cannot construct a circumcenter for collinear sites",
        ));
    }
    line_intersection(
        &ExactLine::perpendicular_bisector_points(a, b)?,
        &ExactLine::perpendicular_bisector_points(a, c)?,
    )
}

pub(in crate::geometry::tessellation) fn distance_within_points(
    origin: &ExactPoint,
    point: &ExactPoint,
    radius: f64,
) -> Result<bool> {
    let x_axis = ExactDyadic::one();
    let zero = ExactDyadic::zero();
    let x = |point: &ExactPoint| point.projected_coordinate(&x_axis, &zero);
    let y = |point: &ExactPoint| point.projected_coordinate(&zero, &x_axis);
    let dx = x(point)?.try_subtract(&x(origin)?)?;
    let dy = y(point)?.try_subtract(&y(origin)?)?;
    let distance_squared = dx.try_product(&dx)?.try_add(&dy.try_product(&dy)?)?;
    let radius = ExactRational::from_f64(radius);
    let radius_squared = radius.try_product(&radius)?;
    Ok(distance_squared.try_cmp(&radius_squared)?.is_lt())
}

/// Round the exact circumcenter of retained dyadic chart sites to nearest
/// binary64. `None` means the center does not have a finite representation.
pub(in crate::geometry) fn circumcenter_dyadic_points(
    a: &[ExactDyadic; 2],
    b: &[ExactDyadic; 2],
    c: &[ExactDyadic; 2],
) -> Result<Option<XY>> {
    if orient2d_dyadic_points((*a).clone(), (*b).clone(), (*c).clone())? == ExactSign::Zero {
        return Err(GeometryErrorKind::triangulation(
            "cannot construct a circumcenter for collinear sites",
        ));
    }
    let [ax, ay] = a;
    let [bx, by] = b;
    let [cx, cy] = c;
    let center = line_intersection(
        &ExactLine::perpendicular_bisector_dyadics([ax, ay], [bx, by])?,
        &ExactLine::perpendicular_bisector_dyadics([ax, ay], [cx, cy])?,
    )?;
    center.finite_rounding()
}
