use crate::error::Result;
use crate::geometry::Work;
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::topology::{
    ExactChartPoint, point_on_exact_segment_with_work, vector_cross_with_work,
    vector_difference_with_work,
};

pub(in crate::geometry) fn exact_segments_intersect(
    left_start: &ExactChartPoint,
    left_end: &ExactChartPoint,
    right_start: &ExactChartPoint,
    right_end: &ExactChartPoint,
) -> Result<bool> {
    let mut work = crate::geometry::Unmetered;
    exact_segments_intersect_with_work(left_start, left_end, right_start, right_end, &mut work)
}

pub(in crate::geometry) fn exact_segments_intersect_with_work(
    left_start: &ExactChartPoint,
    left_end: &ExactChartPoint,
    right_start: &ExactChartPoint,
    right_end: &ExactChartPoint,
    work: &mut impl Work,
) -> Result<bool> {
    let left_start_orientation =
        exact_orientation_with_work(left_start, left_end, right_start, work)?;
    let left_end_orientation = exact_orientation_with_work(left_start, left_end, right_end, work)?;
    let right_start_orientation =
        exact_orientation_with_work(right_start, right_end, left_start, work)?;
    let right_end_orientation =
        exact_orientation_with_work(right_start, right_end, left_end, work)?;
    if signs_are_opposite(left_start_orientation, left_end_orientation)
        && signs_are_opposite(right_start_orientation, right_end_orientation)
    {
        return Ok(true);
    }
    Ok((left_start_orientation == ExactSign::Zero
        && point_on_exact_segment_with_work(right_start, left_start, left_end, work)?)
        || (left_end_orientation == ExactSign::Zero
            && point_on_exact_segment_with_work(right_end, left_start, left_end, work)?)
        || (right_start_orientation == ExactSign::Zero
            && point_on_exact_segment_with_work(left_start, right_start, right_end, work)?)
        || (right_end_orientation == ExactSign::Zero
            && point_on_exact_segment_with_work(left_end, right_start, right_end, work)?))
}

pub(in crate::geometry) fn exact_orientation_with_work(
    start: &ExactChartPoint,
    end: &ExactChartPoint,
    point: &ExactChartPoint,
    work: &mut impl Work,
) -> Result<ExactSign> {
    let direction = vector_difference_with_work(start, end, work)?;
    let offset = vector_difference_with_work(start, point, work)?;
    Ok(vector_cross_with_work(&direction, &offset, work)?.sign())
}

const fn signs_are_opposite(left: ExactSign, right: ExactSign) -> bool {
    matches!(
        (left, right),
        (ExactSign::Positive, ExactSign::Negative) | (ExactSign::Negative, ExactSign::Positive)
    )
}
