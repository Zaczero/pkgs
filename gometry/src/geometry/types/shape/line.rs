use super::{
    CoordIter, CoordSeq, CoordWindow, Coordinates, Deref, GeometryErrorKind, Point, Result,
    coordseq,
};

/// Coordinate sequence carried by lineal shapes. A `LineSeq` always has at
/// least two vertices, so an empty or one-vertex `LineString` is
/// unrepresentable. Empty line members use [`MultiMember::Empty`] and
/// top-level empties use [`Shape::Empty`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LineSeq(CoordSeq);

impl LineSeq {
    /// Admit a coordinate sequence of unknown length — parsed input, a noded
    /// or merged chain, anything whose vertex count is not already proven.
    pub(crate) fn try_new(coords: CoordSeq) -> Result<Self> {
        if coords.len() < 2 {
            return Err(GeometryErrorKind::LineStringTooShort.into());
        }
        Ok(Self(coords))
    }

    /// Wrap a sequence whose length this crate already knows to be at least
    /// two: a length-preserving edit, a pass that keeps both endpoints, a ring,
    /// a stored row, a caller-side `len()` match. **Parsed or otherwise
    /// untrusted input uses [`Self::try_new`]**, whose typed error says what is
    /// wrong with the input instead of aborting on our own bug. The check
    /// stays in release builds — one comparison against a column allocation
    /// that just happened.
    pub(crate) fn from_proven(coords: CoordSeq) -> Self {
        assert!(
            coords.len() >= 2,
            "LineSeq::from_proven requires at least two vertices",
        );
        Self(coords)
    }

    /// The two-vertex line through `start` and `end`. Endpoints are coerced to
    /// the ordinate columns both carry, so a 2D endpoint never gains fabricated
    /// Z/M; this is the one owner of that rule for `shortest_line`,
    /// `minimum_clearance_line`, and the circle-radius lines.
    pub(crate) fn segment(start: Point, end: Point) -> Result<Self> {
        Ok(Self(coordseq::coerced_pair(start, end)?))
    }

    /// A stored line row addressed by `window` into packed columns. Line
    /// storage admits its rows at ingress, so a stored row is never shorter
    /// than two vertices.
    pub(crate) fn row_view(coords: &CoordSeq, window: CoordWindow) -> Self {
        Self::from_proven(coords.view(window))
    }

    pub(crate) const fn as_coords(&self) -> &CoordSeq {
        &self.0
    }

    pub(crate) fn logical_heap_bytes_saturating(&self) -> usize {
        self.as_coords().logical_heap_bytes_saturating()
    }
}

impl Deref for LineSeq {
    type Target = CoordSeq;

    fn deref(&self) -> &Self::Target {
        self.as_coords()
    }
}

impl AsRef<CoordSeq> for LineSeq {
    fn as_ref(&self) -> &CoordSeq {
        self.as_coords()
    }
}

impl<'a> IntoIterator for &'a LineSeq {
    type Item = Point;
    type IntoIter = CoordIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl Coordinates for LineSeq {
    fn coord_count(&self) -> usize {
        self.len()
    }

    fn nth_coord(&self, index: usize) -> Point {
        self.point_at(index)
    }

    fn iter_coords(&self) -> impl DoubleEndedIterator<Item = Point> + '_ {
        self.iter()
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

impl crate::HeapSize for LineSeq {
    fn heap_bytes(&self) -> usize {
        self.as_coords().heap_bytes()
    }
}
