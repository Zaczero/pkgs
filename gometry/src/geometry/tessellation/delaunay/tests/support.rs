use super::super::*;

pub(super) fn sites(points: &[(f64, f64)]) -> Vec<Site> {
    points
        .iter()
        .enumerate()
        .map(|(id, &(x, y))| Site::raw(id, Point::new_raw_xy(x, y), id))
        .collect()
}

pub(super) fn geographic_ring(points: &[(f64, f64)]) -> crate::geometry::Ring {
    crate::geometry::Ring::closed(
        points
            .iter()
            .map(|&(x, y)| Point::new_raw_xy(x, y))
            .collect(),
    )
    .unwrap()
}
