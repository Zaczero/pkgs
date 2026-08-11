use crate::error::Result;
use crate::geometry::types::{CoordIter, CoordSeq, CoordSeqBuilder, GeometryErrorKind, Point, XY};
use crate::geometry::{same_active_position, same_point};

/// A polygon ring: a closed (first == last) vertex sequence with ≥3 corners,
/// stored as a [`CoordSeq`].
///
/// [`Polygon`] holds `Ring`s so a too-short or unclosed ring is
/// unrepresentable. Construct via [`Ring::closed`] / [`Ring::closed_coordseq`]
/// at **every** input boundary (constructors, WKT, WKB, GeoArrow, pickle) and
/// [`Ring::from_trusted_closed`] for coordinate-preserving transforms of an
/// existing ring. Read access mirrors a slice via the delegating accessors.
///
/// **Admission policy (one owner, all ingresses):**
/// - fewer than [`Ring::MIN_VERTICES_OPEN`] corners → reject
/// - first and last match on **every active ordinate** and length ≥
///   [`Ring::MIN_VERTICES_CLOSED`] → accept as closed
/// - XY-closed but Z/M-open (active-ordinate mismatch) → reject (never invent a
///   closing Z/M)
/// - otherwise XY-open with ≥3 corners → **silently close** by appending the
///   first vertex (all ordinates)
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Ring(CoordSeq);

impl Ring {
    /// Minimum distinct vertices for an open ring (before closing).
    pub const MIN_VERTICES_OPEN: usize = 3;
    /// Minimum stored vertices for a closed ring (includes repeated endpoint).
    pub const MIN_VERTICES_CLOSED: usize = 4;

    /// Validate and close `points` under the shared ring-admission policy.
    /// The boundary constructor for parsed / constructed input.
    pub fn closed(points: Vec<Point>) -> Result<Self> {
        Self::closed_coordseq(CoordSeq::from(points))
    }

    /// Validate and close a columnar coordinate sequence without staging it
    /// through `Vec<Point>`, preserving explicit empty/non-empty Z/M lanes.
    ///
    /// This is the **single** untrusted ring admission owner — constructors,
    /// WKT/WKB, GeoArrow, and pickle all route here (via [`crate::io::admit_closed_ring`]
    /// or directly).
    pub(crate) fn closed_coordseq(coords: CoordSeq) -> Result<Self> {
        if coords.len() < Self::MIN_VERTICES_OPEN {
            return Err(GeometryErrorKind::RingTooShort(coords.len()).into());
        }
        let first = coords.point_at(0);
        let last = coords.point_at(coords.len() - 1);
        if same_active_position(first, last) {
            if coords.len() < Self::MIN_VERTICES_CLOSED {
                return Err(GeometryErrorKind::RingTooShort(coords.len()).into());
            }
            return Ok(Self(coords));
        }
        // XY closed / Z-or-M open: reject rather than invent a closing ordinate.
        if same_point(first, last) {
            return Err(GeometryErrorKind::message(
                "polygon ring must be closed on all active ordinates",
            ));
        }
        // XY-open: silent-close (WKT/WKB/shapely/constructor convention).
        let mut closed = CoordSeqBuilder::like_coords(&coords, 0);
        closed
            .try_reserve_exact(coords.len() + 1)
            .map_err(|_| GeometryErrorKind::message("could not allocate closed polygon ring"))?;
        for index in 0..coords.len() {
            closed.push_at(&coords, index);
        }
        closed.push(first);
        Ok(Self(closed.finish()?))
    }

    /// Wrap a coordinate sequence already known to form a valid closed ring —
    /// e.g. a geo-rs conversion, a coordinate-preserving transform of an
    /// existing `Ring`, or a WKB/Arrow column run. Accepts either a
    /// `Vec<Point>` or a `CoordSeq`. Skips validation (mirrors
    /// [`Point::new_unchecked_xy`]).
    pub(crate) fn from_trusted_closed(coords: impl Into<CoordSeq>) -> Self {
        Self(coords.into())
    }

    /// The ring's coordinate columns.
    pub const fn coords(&self) -> &CoordSeq {
        &self.0
    }

    pub fn into_vec(self) -> Vec<Point> {
        self.0.to_vec()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn point_at(&self, index: usize) -> Point {
        self.0.point_at(index)
    }

    pub fn get(&self, index: usize) -> Option<Point> {
        self.0.get(index)
    }

    pub fn first(&self) -> Option<Point> {
        self.0.first()
    }

    pub fn last(&self) -> Option<Point> {
        self.0.last()
    }

    pub fn iter(&self) -> CoordIter<'_> {
        self.0.iter()
    }
}

impl<'a> IntoIterator for &'a Ring {
    type Item = Point;
    type IntoIter = CoordIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

/// Uniform read access over the coordinate-sequence representations.
///
/// The geometry kernels run the same way on stored [`CoordSeq`]/[`Ring`]
/// columns and on scratch `Vec<Point>`/`[Point]` slices without per-call-site
/// conversions. Accessors reconstruct `Point`s by value; the hot reducers that
/// want contiguous ordinates instead take a concrete `&CoordSeq` and read its
/// [`xs`](CoordSeq::xs)/[`ys`](CoordSeq::ys) columns directly.
///
/// Method names are deliberately distinct from the slice/`CoordSeq` inherent
/// methods (`coord_count`/`nth_coord`/…) so calls never resolve ambiguously.
pub(crate) trait Coordinates {
    /// Number of coordinates.
    fn coord_count(&self) -> usize;

    /// The coordinate at `index`, reconstructed as a `Point` (panics out of
    /// bounds, like slice indexing).
    fn nth_coord(&self, index: usize) -> Point;

    fn coord_is_empty(&self) -> bool {
        self.coord_count() == 0
    }

    fn first_coord(&self) -> Option<Point> {
        (!self.coord_is_empty()).then(|| self.nth_coord(0))
    }

    fn last_coord(&self) -> Option<Point> {
        self.coord_count()
            .checked_sub(1)
            .map(|index| self.nth_coord(index))
    }

    /// Iterate the coordinates as `Point`s by value.
    fn iter_coords(&self) -> impl DoubleEndedIterator<Item = Point> + '_ {
        (0..self.coord_count()).map(|index| self.nth_coord(index))
    }

    /// Adjacent vertex pairs `[v[i - 1], v[i]]` — the slice-`array_windows`
    /// replacement.
    fn segment_pairs(&self) -> impl Iterator<Item = [Point; 2]> + '_ {
        let mut columns = self
            .xy_columns()
            .filter(|_| self.z_column().is_none() && self.m_column().is_none())
            .map(|(xs, ys)| (xs.array_windows::<2>(), ys.array_windows::<2>()));
        let mut index = 1;
        let len = self.coord_count();
        std::iter::from_fn(move || {
            if let Some((xs, ys)) = &mut columns {
                let x = xs.next()?;
                let y = ys.next()?;
                return Some([
                    Point::new_unchecked_xy(x[0], y[0]),
                    Point::new_unchecked_xy(x[1], y[1]),
                ]);
            }
            (index < len).then(|| {
                let pair = [self.nth_coord(index - 1), self.nth_coord(index)];
                index += 1;
                pair
            })
        })
    }

    /// The contiguous `x`/`y` ordinate columns, when the representation stores
    /// them (`SoA` [`CoordSeq`]/[`Ring`]). `None` for `Point` slices, whose hot
    /// reducers gather into scratch buffers instead. Lets the shoelace/length
    /// kernels read columns directly with no per-vertex gather.
    fn xy_columns(&self) -> Option<(&[f64], &[f64])> {
        None
    }

    /// The contiguous `z` column when stored and present (`SoA` carrying Z).
    /// `None` for non-column representations or when the sequence is
    /// `XY`/`XYM`. Lets columnar I/O (Arrow export) bulk-copy ordinate
    /// runs.
    fn z_column(&self) -> Option<&[f64]> {
        None
    }

    /// The contiguous `m` column when stored and present (see `z_column`).
    fn m_column(&self) -> Option<&[f64]> {
        None
    }
}

impl Coordinates for [XY] {
    fn coord_count(&self) -> usize {
        self.len()
    }

    fn nth_coord(&self, index: usize) -> Point {
        self[index].point()
    }
}

impl Coordinates for Vec<XY> {
    fn coord_count(&self) -> usize {
        self.len()
    }

    fn nth_coord(&self, index: usize) -> Point {
        self[index].point()
    }
}

impl Coordinates for CoordSeq {
    fn coord_count(&self) -> usize {
        self.len()
    }

    fn nth_coord(&self, index: usize) -> Point {
        self.point_at(index)
    }

    fn xy_columns(&self) -> Option<(&[f64], &[f64])> {
        Some((self.xs(), self.ys()))
    }

    fn z_column(&self) -> Option<&[f64]> {
        self.zs()
    }

    fn m_column(&self) -> Option<&[f64]> {
        self.ms()
    }
}

impl Coordinates for Ring {
    fn coord_count(&self) -> usize {
        self.0.len()
    }

    fn nth_coord(&self, index: usize) -> Point {
        self.0.point_at(index)
    }

    fn xy_columns(&self) -> Option<(&[f64], &[f64])> {
        Some((self.0.xs(), self.0.ys()))
    }

    fn z_column(&self) -> Option<&[f64]> {
        self.0.zs()
    }

    fn m_column(&self) -> Option<&[f64]> {
        self.0.ms()
    }
}

impl Coordinates for [Point] {
    fn coord_count(&self) -> usize {
        <[Point]>::len(self)
    }

    fn nth_coord(&self, index: usize) -> Point {
        self[index]
    }
}

impl Coordinates for Vec<Point> {
    fn coord_count(&self) -> usize {
        self.len()
    }

    fn nth_coord(&self, index: usize) -> Point {
        self[index]
    }
}

/// Forward through references so call sites can pass `&CoordSeq`, `&[Point]`,
/// `&Vec<Point>`, or a doubly-borrowed sequence (e.g. from `.map`/closures)
/// without `&`-juggling at every generic boundary.
impl<C: Coordinates + ?Sized> Coordinates for &C {
    fn coord_count(&self) -> usize {
        (**self).coord_count()
    }

    fn nth_coord(&self, index: usize) -> Point {
        (**self).nth_coord(index)
    }

    fn xy_columns(&self) -> Option<(&[f64], &[f64])> {
        (**self).xy_columns()
    }

    fn z_column(&self) -> Option<&[f64]> {
        (**self).z_column()
    }

    fn m_column(&self) -> Option<&[f64]> {
        (**self).m_column()
    }
}
