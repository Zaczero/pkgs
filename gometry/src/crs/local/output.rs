use super::frame::LocalCrsDefinition;
use crate::error::Result;
use crate::geographic::{
    GeographicFrame, publish_certified_minor_chain, publish_preserved_minor_chain,
};
use crate::geometry::{
    Coordinates as _, ExpansionBudget, GeometryErrorKind, Point, Shape, map_computed_shape_chains,
    point_on_segment,
};

pub(super) fn publish(
    projected: &Shape,
    restored: &Shape,
    local: &LocalCrsDefinition,
    frame: GeographicFrame,
    operation: &'static str,
    preserve_input_vertices: bool,
    budget: &mut ExpansionBudget,
) -> Result<Shape> {
    if let Shape::Point(point) = restored {
        if preserve_input_vertices {
            return Ok(Shape::Point(*point));
        }
        return Ok(Shape::Point(
            frame
                .canonical_computed_point(*point)
                .map_err(|error| crate::crs::CrsError::invalid(error.to_string()))?,
        ));
    }
    let pole = local.projected_pole().map(|(x, y)| Point::new_raw_xy(x, y));
    map_computed_shape_chains(projected, restored, |projected, restored, closed| {
        check_local_edges(projected, pole, operation)?;
        if preserve_input_vertices {
            publish_preserved_minor_chain(frame, restored, closed, budget)
        } else {
            publish_certified_minor_chain(frame, restored, closed, budget)
        }
    })
}

fn check_local_edges(
    projected: &crate::geometry::CoordSeq,
    pole: Option<Point>,
    operation: &'static str,
) -> Result<()> {
    let Some(pole) = pole else {
        return Ok(());
    };
    if projected
        .segment_pairs()
        .any(|[start, end]| point_on_segment(pole.xy(), start.xy(), end.xy()))
    {
        return Err(GeometryErrorKind::unrepresentable_output(operation));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::check_local_edges;
    use crate::geometry::{CoordSeq, GeometryErrorKind, Point};

    #[test]
    fn polar_certificate_uses_exact_closed_segment_containment() {
        let pole = Point::new_raw_xy(2_000_000.0, 2_000_000.0);
        let through_pole = CoordSeq::try_from_points(&[
            Point::new_raw_xy(1_999_999.0, 2_000_000.0),
            Point::new_raw_xy(2_000_001.0, 2_000_000.0),
        ])
        .unwrap();
        let error = check_local_edges(&through_pole, Some(pole), "polar test").unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::UnrepresentableOutput {
                operation: "polar test",
                ..
            })
        ));

        let near_miss = CoordSeq::try_from_points(&[
            Point::new_raw_xy(1_999_999.0, 2_000_000.0),
            Point::new_raw_xy(2_000_001.0, 2_000_000.0_f64.next_up()),
        ])
        .unwrap();
        check_local_edges(&near_miss, Some(pole), "polar test")
            .expect("an adjacent-coordinate near miss does not contain the pole");
    }
}
