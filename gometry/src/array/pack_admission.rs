//! Shared structural pack-admission for polygon coordinate windows.

use std::fmt;
use std::ops::Range;

use crate::geometry::{CoordSeq, CoordinateAxes, Polygon, Ring};

pub(crate) trait RingCoordinateColumns {
    fn width(&self) -> usize;
    fn x(&self, index: usize) -> f64;
    fn y(&self, index: usize) -> f64;
    fn z(&self, index: usize) -> Option<f64>;
    fn m(&self, index: usize) -> Option<f64>;
}

impl RingCoordinateColumns for crate::geometry::Ring {
    fn width(&self) -> usize {
        self.coords().width()
    }

    fn x(&self, index: usize) -> f64 {
        self.coords().x(index)
    }

    fn y(&self, index: usize) -> f64 {
        self.coords().y(index)
    }

    fn z(&self, index: usize) -> Option<f64> {
        self.coords().z(index)
    }

    fn m(&self, index: usize) -> Option<f64> {
        self.coords().m(index)
    }
}

impl RingCoordinateColumns for CoordSeq {
    fn width(&self) -> usize {
        self.len()
    }

    fn x(&self, index: usize) -> f64 {
        self.xs()[index]
    }

    fn y(&self, index: usize) -> f64 {
        self.ys()[index]
    }

    fn z(&self, index: usize) -> Option<f64> {
        self.zs().map(|values| values[index])
    }

    fn m(&self, index: usize) -> Option<f64> {
        self.ms().map(|values| values[index])
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RingWindowError {
    Inverted,
    OutOfRange,
    TooShort(usize),
    Open,
}

impl fmt::Display for RingWindowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inverted => f.write_str("ring range is inverted"),
            Self::OutOfRange => f.write_str("ring range is outside the coordinate columns"),
            Self::TooShort(len) => {
                write!(f, "ring has {len} coordinates; at least four are required")
            },
            Self::Open => f.write_str("ring is not closed on every active ordinate"),
        }
    }
}

impl std::error::Error for RingWindowError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CheckedRingWindow(Range<usize>);

impl CheckedRingWindow {
    pub(crate) fn from_ring(ring: &Ring) -> Self {
        Self(0..ring.len())
    }

    pub(crate) fn checked(
        columns: &impl RingCoordinateColumns,
        window: Range<usize>,
    ) -> Result<Self, RingWindowError> {
        Self::checked_in(columns, window, 0..columns.width())
    }

    pub(crate) fn checked_in(
        columns: &impl RingCoordinateColumns,
        window: Range<usize>,
        bounds: Range<usize>,
    ) -> Result<Self, RingWindowError> {
        let len = window
            .end
            .checked_sub(window.start)
            .ok_or(RingWindowError::Inverted)?;
        if bounds.end < bounds.start
            || bounds.end > columns.width()
            || window.start < bounds.start
            || window.end > bounds.end
        {
            return Err(RingWindowError::OutOfRange);
        }
        if len < Ring::MIN_VERTICES_CLOSED {
            return Err(RingWindowError::TooShort(len));
        }
        let first = window.start;
        let last = window.end - 1;
        #[expect(
            clippy::float_cmp,
            reason = "ring closure requires exact ordinate identity"
        )]
        if columns.x(first) != columns.x(last)
            || columns.y(first) != columns.y(last)
            || columns.z(first) != columns.z(last)
            || columns.m(first) != columns.m(last)
        {
            return Err(RingWindowError::Open);
        }
        Ok(Self(window))
    }

    pub(crate) const fn range(&self) -> &Range<usize> {
        &self.0
    }
}

pub(crate) fn polygon_pack_axes(polygon: &Polygon) -> Option<CoordinateAxes> {
    let mut axes: Option<CoordinateAxes> = None;
    let mut n_rings = 0_usize;
    for seq in polygon.rings() {
        CheckedRingWindow::checked(seq, 0..seq.len()).ok()?;
        let row_axes = seq.axes();
        match axes {
            None => axes = Some(row_axes),
            Some(existing) if existing != row_axes => return None,
            Some(_) => {},
        }
        n_rings += 1;
    }
    if n_rings == 0 {
        return None;
    }
    axes
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seq(rows: &[[f64; 4]], axes: CoordinateAxes) -> CoordSeq {
        CoordSeq::try_from_vecs(
            rows.iter().map(|row| row[0]).collect::<Vec<_>>(),
            rows.iter().map(|row| row[1]).collect::<Vec<_>>(),
            axes.has_z()
                .then(|| rows.iter().map(|row| row[2]).collect::<Vec<_>>()),
            axes.has_m()
                .then(|| rows.iter().map(|row| row[3]).collect::<Vec<_>>()),
        )
        .unwrap()
    }

    #[test]
    fn checked_ring_window_owns_structure_and_active_closure() {
        let xy = CoordinateAxes::XY;
        let xyzm = CoordinateAxes::XYZM;
        let closed = [
            [0.0, 0.0, 1.0, 2.0],
            [1.0, 0.0, 3.0, 4.0],
            [0.0, 1.0, 5.0, 6.0],
            [-0.0, -0.0, 1.0, 2.0],
        ];
        let xy_seq = seq(&closed, xy);
        let inverted_start = 2;
        let inverted_end = 1;
        assert_eq!(
            CheckedRingWindow::checked(&xy_seq, inverted_start..inverted_end),
            Err(RingWindowError::Inverted)
        );
        assert_eq!(
            CheckedRingWindow::checked(&xy_seq, 0..0),
            Err(RingWindowError::TooShort(0))
        );
        assert_eq!(
            CheckedRingWindow::checked(&xy_seq, 0..5),
            Err(RingWindowError::OutOfRange)
        );
        assert_eq!(
            CheckedRingWindow::checked_in(&xy_seq, 0..4, 1..4),
            Err(RingWindowError::OutOfRange)
        );
        assert_eq!(
            CheckedRingWindow::checked(&xy_seq, 0..3),
            Err(RingWindowError::TooShort(3))
        );
        CheckedRingWindow::checked(&xy_seq, 0..4).unwrap();

        let mut open = closed;
        open[3][0] = 9.0;
        assert_eq!(
            CheckedRingWindow::checked(&seq(&open, xy), 0..4),
            Err(RingWindowError::Open)
        );
        let mut y_open = closed;
        y_open[3][1] = 9.0;
        assert_eq!(
            CheckedRingWindow::checked(&seq(&y_open, xy), 0..4),
            Err(RingWindowError::Open)
        );
        let mut z_open = closed;
        z_open[3][2] = 9.0;
        assert_eq!(
            CheckedRingWindow::checked(&seq(&z_open, xyzm), 0..4),
            Err(RingWindowError::Open)
        );
        let mut m_open = closed;
        m_open[3][3] = 9.0;
        assert_eq!(
            CheckedRingWindow::checked(&seq(&m_open, xyzm), 0..4),
            Err(RingWindowError::Open)
        );
        CheckedRingWindow::checked(&seq(&closed, xyzm), 0..4).unwrap();
    }
}
