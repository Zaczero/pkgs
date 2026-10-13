use pyo3::prelude::PyResult;

use crate::geographic::GeographicFrame;
use crate::geometry::{CoordSeq, LineSeq, Shape};
use crate::py::cells::Typed;

pub(super) fn polygonize_outline(outline: Vec<CoordSeq>) -> PyResult<Option<Typed>> {
    if outline.is_empty() {
        return Ok(None);
    }
    let outline = Shape::multi_line_string(
        crate::geometry::CoordinateAxes::XY,
        outline.into_iter().map(LineSeq::from_proven).collect(),
    );
    let polygons: Vec<_> = Shape::build_area_all_with_context(
        &[&outline],
        true,
        &crate::geometry::SourceArcContext::cartesian(),
    )?
    .into_iter()
    .filter_map(|shape| match shape {
        Shape::Polygon(polygon) => Some(polygon),
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::MultiPolygon(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..) => None,
    })
    .collect();
    if polygons.is_empty() {
        return Ok(None);
    }
    let shape = Shape::narrow_polygons(crate::geometry::CoordinateAxes::XY, polygons);
    Ok(Some(publish_grid_outline(&shape, "grid dissolve")?))
}

pub(super) fn publish_grid_outline(shape: &Shape, operation: &'static str) -> PyResult<Typed> {
    let frame = GeographicFrame::crs84();
    let mut published = crate::geometry::publish_raw_computed_shape(shape, frame, operation)?;
    if matches!(published, Shape::MultiPolygon(_))
        && crate::geometry::crosses_antimeridian_in_frame(&published, frame)
        && let Some(chart) = crate::geometry::unwrap_contact_chart(&published, frame)?
    {
        // Publication splits seam pieces into a common chart. Cancel their
        // shared cut edge here, before the later receipt sink seals the result.
        let selected = Shape::union_all(&[chart], crate::geometry::Strictness::Lenient)?;
        published = crate::geometry::publish_raw_computed_shape(&selected, frame, operation)?;
    }
    let finished =
        crate::geometry::finish_grid_outline(published, crate::Frame::Crs(crate::wgs84_crs()))?;
    let (shape, frame) = finished.into_scalar()?;
    Ok(Typed(crate::PyGeometry::with_frame(shape, frame)))
}

pub(crate) fn coordseq_crosses_lon_seam(segment: &CoordSeq) -> bool {
    let (mut lo, mut hi) = (f64::INFINITY, f64::NEG_INFINITY);
    for point in segment.points() {
        lo = lo.min(point.x);
        hi = hi.max(point.x);
    }
    hi - lo > 180.0
}

#[cfg(test)]
mod tests {
    use super::{polygonize_outline, publish_grid_outline};
    use crate::geographic::GeographicFrame;
    use crate::geometry::{CoordSeq, Point, Shape};

    #[test]
    fn computed_grid_outline_removes_a_shared_cut_meridian() {
        let source = crate::io::parse_wkt(
            "MULTIPOLYGON (((-180 -2, -177 -2, -177 2, -180 2, -180 -2)), \
             ((177 -2, 180 -2, 180 2, 177 2, 177 -2)))",
        )
        .unwrap();
        let output = publish_grid_outline(&source, "grid dissolve").unwrap();
        let output = output.0.shape.shape();
        assert!(matches!(output, Shape::Polygon(_)));
        let issue = crate::geometry::validate_shape_in_frame(
            output,
            crate::geometry::TopologyDomain::Geographic(GeographicFrame::crs84()),
        )
        .unwrap();
        assert!(issue.is_none(), "{issue:?}");
        let Shape::Polygon(polygon) = output else {
            unreachable!()
        };
        assert!(polygon.shell.iter().all(|point| point.x < 180.0));
        assert!(polygon.holes.is_empty());
    }

    #[test]
    fn omitting_a_selected_outline_use_removes_the_face() {
        let square = [
            Point::new_raw_xy(-2.0, -2.0),
            Point::new_raw_xy(2.0, -2.0),
            Point::new_raw_xy(2.0, 2.0),
            Point::new_raw_xy(-2.0, 2.0),
        ];
        let mut outline = (0..square.len())
            .map(|index| {
                let next = (index + 1) % square.len();
                CoordSeq::try_from_points(&[
                    *square.get(index).expect("square vertex exists"),
                    *square.get(next).expect("next square vertex exists"),
                ])
                .unwrap()
            })
            .collect::<Vec<_>>();

        assert!(polygonize_outline(outline.clone()).unwrap().is_some());
        outline.remove(1);
        assert!(polygonize_outline(outline).unwrap().is_none());
    }
}
