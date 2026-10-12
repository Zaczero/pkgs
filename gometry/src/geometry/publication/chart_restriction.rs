use crate::geographic::ChartPole;

/// Which copy of the longitude cut a synthetic chart vertex occupies.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum ChartCutSide {
    East,
    West,
}

/// A generated edge that restricts publication to a chart boundary.
///
/// Authentic source intervals remain in producer source-use receipts; these
/// roles describe only generated presentation geometry.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum ChartRestriction {
    CutChord,
    PolarCap { pole: ChartPole },
}

/// Synthetic vertices introduced to close a chart-cut or polar boundary.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum SyntheticChartVertex {
    Cut {
        /// Producer-local identity; the side retains the quotient spelling.
        node: u32,
        side: ChartCutSide,
    },
    Pole {
        /// Producer-local pole node identity.
        node: u32,
        pole: ChartPole,
        /// Pole vertices share a quotient node but retain their native spelling.
        native_longitude_bits: u64,
    },
}

/// One directed generated chart edge; `start` to `end` is its published use.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DirectedChartRestrictionUse {
    pub(crate) restriction: ChartRestriction,
    pub(crate) start: SyntheticChartVertex,
    pub(crate) end: SyntheticChartVertex,
}
