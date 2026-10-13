use super::{CoordSeq, CoordinateAxes, HasM, HasZ, Point};

macro_rules! dispatch_coord_iter {
    ($self:expr, $method:ident $(, $arg:expr)*) => {
        match $self {
            CoordIter::Xy(iter) => iter.$method($($arg),*),
            CoordIter::Xyz(iter) => iter.$method($($arg),*),
            CoordIter::Xym(iter) => iter.$method($($arg),*),
            CoordIter::Xyzm(iter) => iter.$method($($arg),*),
        }
    };
}

impl CoordSeq {
    /// Iterate the coordinates as `Point`s by value.
    pub fn iter(&self) -> CoordIter<'_> {
        let xs = self.xs();
        let ys = self.ys();
        match (self.zs(), self.ms()) {
            (None, None) => CoordIter::Xy(CoordIterColumns::new(xs, ys, &[], &[])),
            (Some(zs), None) => CoordIter::Xyz(CoordIterColumns::new(xs, ys, zs, &[])),
            (None, Some(ms)) => CoordIter::Xym(CoordIterColumns::new(xs, ys, &[], ms)),
            (Some(zs), Some(ms)) => CoordIter::Xyzm(CoordIterColumns::new(xs, ys, zs, ms)),
        }
    }

    /// Alias for [`iter`](Self::iter); mirrors the shape-level vertex walk.
    pub fn points(&self) -> CoordIter<'_> {
        self.iter()
    }
}

pub struct CoordIterColumns<'a, const HAS_Z: bool, const HAS_M: bool> {
    xs: std::slice::Iter<'a, f64>,
    ys: std::slice::Iter<'a, f64>,
    zs: std::slice::Iter<'a, f64>,
    ms: std::slice::Iter<'a, f64>,
}

impl<'a, const HAS_Z: bool, const HAS_M: bool> CoordIterColumns<'a, HAS_Z, HAS_M> {
    fn new(xs: &'a [f64], ys: &'a [f64], zs: &'a [f64], ms: &'a [f64]) -> Self {
        debug_assert!(!HAS_Z || zs.len() == xs.len());
        debug_assert!(!HAS_M || ms.len() == xs.len());
        Self {
            xs: xs.iter(),
            ys: ys.iter(),
            zs: zs.iter(),
            ms: ms.iter(),
        }
    }
}

impl<const HAS_Z: bool, const HAS_M: bool> Iterator for CoordIterColumns<'_, HAS_Z, HAS_M> {
    type Item = Point;

    fn next(&mut self) -> Option<Point> {
        let x = *self.xs.next()?;
        Some(Point {
            x,
            y: *self
                .ys
                .next()
                .expect("Y coordinates share the admitted X window"),
            z: if HAS_Z {
                *self
                    .zs
                    .next()
                    .expect("Z coordinates share the admitted X window")
            } else {
                0.0
            },
            m: if HAS_M {
                *self
                    .ms
                    .next()
                    .expect("M coordinates share the admitted X window")
            } else {
                0.0
            },
            axes: CoordinateAxes::new(HasZ(HAS_Z), HasM(HAS_M)),
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.xs.len();
        (remaining, Some(remaining))
    }
}

impl<const HAS_Z: bool, const HAS_M: bool> DoubleEndedIterator
    for CoordIterColumns<'_, HAS_Z, HAS_M>
{
    fn next_back(&mut self) -> Option<Point> {
        let x = *self.xs.next_back()?;
        Some(Point {
            x,
            y: *self
                .ys
                .next_back()
                .expect("Y coordinates share the admitted X window"),
            z: if HAS_Z {
                *self
                    .zs
                    .next_back()
                    .expect("Z coordinates share the admitted X window")
            } else {
                0.0
            },
            m: if HAS_M {
                *self
                    .ms
                    .next_back()
                    .expect("M coordinates share the admitted X window")
            } else {
                0.0
            },
            axes: CoordinateAxes::new(HasZ(HAS_Z), HasM(HAS_M)),
        })
    }
}

impl<const HAS_Z: bool, const HAS_M: bool> ExactSizeIterator
    for CoordIterColumns<'_, HAS_Z, HAS_M>
{
}

/// By-value `Point` iterator specialized once for the sequence's axes.
pub enum CoordIter<'a> {
    Xy(CoordIterColumns<'a, false, false>),
    Xyz(CoordIterColumns<'a, true, false>),
    Xym(CoordIterColumns<'a, false, true>),
    Xyzm(CoordIterColumns<'a, true, true>),
}

impl Iterator for CoordIter<'_> {
    type Item = Point;

    fn next(&mut self) -> Option<Point> {
        dispatch_coord_iter!(self, next)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        dispatch_coord_iter!(self, size_hint)
    }
}

impl DoubleEndedIterator for CoordIter<'_> {
    fn next_back(&mut self) -> Option<Point> {
        dispatch_coord_iter!(self, next_back)
    }
}

impl ExactSizeIterator for CoordIter<'_> {}

impl<'a> IntoIterator for &'a CoordSeq {
    type Item = Point;
    type IntoIter = CoordIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
