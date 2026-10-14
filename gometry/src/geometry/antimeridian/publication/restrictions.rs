use crate::geographic::ChartPole;
use crate::geometry::Point;
use crate::geometry::publication::ChartRestriction;

const fn chart_pole(point: Point) -> Option<ChartPole> {
    if point.y.to_bits() == 90.0_f64.to_bits() {
        Some(ChartPole::North)
    } else if point.y.to_bits() == (-90.0_f64).to_bits() {
        Some(ChartPole::South)
    } else {
        None
    }
}

#[expect(
    clippy::float_cmp,
    reason = "this operation classifies exact antimeridian or pole coordinates before chart mapping"
)]
pub(in crate::geometry::antimeridian) fn restriction_for_edge(
    start: Point,
    end: Point,
) -> Option<ChartRestriction> {
    let start_cut = start.x.abs() == 180.0;
    let end_cut = end.x.abs() == 180.0;
    let start_pole = chart_pole(start);
    let end_pole = chart_pole(end);
    match (start_pole, end_pole, start_cut, end_cut) {
        (Some(left), Some(right), ..) if left == right && start.x != end.x => {
            Some(ChartRestriction::PolarCap { pole: left })
        },
        (Some(pole), None, _, true) | (None, Some(pole), true, _) => {
            Some(ChartRestriction::PolarCap { pole })
        },
        (Some(_), Some(_), true, true) if start.x == end.x => Some(ChartRestriction::CutChord),
        (_, _, true, true) => Some(ChartRestriction::CutChord),
        _ => None,
    }
}
