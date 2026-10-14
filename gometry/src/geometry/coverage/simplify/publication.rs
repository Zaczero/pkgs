use super::{
    GeometryErrorKind, Polygon, Result, Shape, SimplifyMethod, SourceArcContext, TopologyDomain,
    line_segments, same_point, undirected_segment_edge_key,
};

pub(super) fn simplify_unshared_geographic_rows<S: std::borrow::Borrow<Shape>>(
    rows: &[S],
    tolerance: f64,
    method: SimplifyMethod,
    simplify_boundary: bool,
    frame: crate::geographic::GeographicFrame,
    source_context: &SourceArcContext<'_, '_, crate::crs::GeodesicMetric>,
) -> Result<Vec<Shape>> {
    // With no shared Q span, rows have no boundary chain to replay. Simplify
    // them in their geographic frame, then keep the group only if its shared
    // Q validation still passes.
    let original: Vec<_> = rows.iter().map(|row| row.borrow().clone()).collect();
    if !simplify_boundary {
        return Ok(original);
    }

    let mut computed_edge_budget =
        crate::geometry::ExpansionBudget::new("coverage_simplify", "computed edges");
    let mut simplified = Vec::with_capacity(rows.len());
    for row in rows {
        let source = row.borrow();
        let output = match method {
            SimplifyMethod::Vw => {
                source.simplify_vw_geographic(tolerance, true, frame.longitude())?
            },
            SimplifyMethod::Dp => {
                source.simplify_dp_geographic(tolerance, true, frame.longitude())?
            },
        };
        let Some(edge_roles) = geographic_edge_roles(source, &output)? else {
            return Ok(original);
        };
        simplified.push(publish_coverage_edge_roles(
            output,
            edge_roles,
            frame,
            source_context,
            &mut computed_edge_budget,
        )?);
    }

    if crate::geometry::coverage_is_valid_with_domain(
        &simplified,
        0.0,
        TopologyDomain::Geographic(frame),
    )? {
        Ok(simplified)
    } else {
        Ok(original)
    }
}

fn geographic_edge_roles(
    source: &Shape,
    output: &Shape,
) -> Result<Option<crate::geometry::PolygonPartEdgeRoles>> {
    match (source, output) {
        (Shape::Polygon(source), Shape::Polygon(output)) => {
            Ok(polygon_edge_roles(source, output)?.map(|roles| vec![roles]))
        },
        (Shape::MultiPolygon(source), Shape::MultiPolygon(output))
            if source.len() == output.len() =>
        {
            let mut roles = Vec::with_capacity(source.len());
            for (source_member, output_member) in source
                .members_with_values()
                .zip(output.members_with_values())
            {
                match (source_member, output_member) {
                    (
                        crate::geometry::MultiMember::Value(source),
                        crate::geometry::MultiMember::Value(output),
                    ) => {
                        let Some(edge_roles) = polygon_edge_roles(source, output)? else {
                            return Ok(None);
                        };
                        roles.push(edge_roles);
                    },
                    (crate::geometry::MultiMember::Empty, crate::geometry::MultiMember::Empty) => {
                        roles.push(Vec::new());
                    },
                    _ => return Ok(None),
                }
            }
            Ok(Some(roles))
        },
        (Shape::Empty(..), Shape::Empty(..)) if source == output => Ok(Some(Vec::new())),
        _ => Ok(None),
    }
}

fn polygon_edge_roles(source: &Polygon, output: &Polygon) -> Result<Option<Vec<Vec<bool>>>> {
    let source_rings: Vec<_> = source.rings().collect();
    let output_rings: Vec<_> = output.rings().collect();
    if source_rings.len() != output_rings.len() {
        return Ok(None);
    }
    Ok(Some(
        source_rings
            .iter()
            .zip(output_rings)
            .map(|(source_ring, output_ring)| {
                let mut source_edges =
                    crate::collections::try_hash_set_with_capacity(source_ring.len())?;
                for edge in line_segments(source_ring) {
                    if !same_point(edge.start, edge.end) {
                        source_edges.insert(undirected_segment_edge_key(edge));
                    }
                }
                Ok(line_segments(output_ring)
                    .map(|edge| !source_edges.contains(&undirected_segment_edge_key(edge)))
                    .collect())
            })
            .collect::<Result<Vec<_>>>()?,
    ))
}

pub(super) fn publish_coverage_edge_roles(
    shape: Shape,
    edge_roles: Vec<Vec<Vec<bool>>>,
    frame: crate::geographic::GeographicFrame,
    source_context: &SourceArcContext<'_, '_, crate::crs::GeodesicMetric>,
    budget: &mut crate::geometry::ExpansionBudget,
) -> Result<Shape> {
    match shape {
        Shape::Polygon(polygon) => {
            let [roles] = edge_roles.as_slice() else {
                return Err(GeometryErrorKind::Invalid(
                    "coverage polygon roles do not match the rebuilt polygon".into(),
                )
                .into());
            };
            Ok(Shape::Polygon(
                crate::geometry::publish_polygon_with_edge_roles_and_context(
                    &polygon,
                    roles,
                    frame,
                    source_context,
                    budget,
                )?,
            ))
        },
        Shape::MultiPolygon(polygons) => {
            if polygons.len() != edge_roles.len() {
                return Err(GeometryErrorKind::Invalid(
                    "coverage multipolygon roles do not match its polygon parts".into(),
                )
                .into());
            }
            let mut edge_roles = edge_roles.into_iter();
            let output = polygons.try_map_values(polygons.axes(), |polygon| {
                let roles = edge_roles.next().ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "coverage multipolygon is missing a polygon role row".into(),
                    )
                })?;
                crate::geometry::publish_polygon_with_edge_roles_and_context(
                    polygon,
                    &roles,
                    frame,
                    source_context,
                    budget,
                )
            })?;
            if edge_roles.next().is_some() {
                return Err(GeometryErrorKind::Invalid(
                    "coverage multipolygon has unused polygon role rows".into(),
                )
                .into());
            }
            Ok(Shape::MultiPolygon(output))
        },
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..) => {
            if edge_roles.is_empty() {
                Ok(shape)
            } else {
                Err(GeometryErrorKind::Invalid(
                    "coverage edge roles do not match the rebuilt geometry".into(),
                )
                .into())
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::polygon_edge_roles;
    use crate::geometry::{CoordSeq, Point, Polygon, Ring, ZOrdinate};

    fn point(x: f64, y: f64, z: f64) -> Point {
        Point::new_axes(x, y, ZOrdinate(Some(z)), crate::geometry::MOrdinate(None))
            .expect("test coordinate is valid")
    }

    fn polygon(points: &[Point]) -> Polygon {
        Polygon::new(
            Ring::try_from_closed(
                CoordSeq::try_from_points(points).expect("test coordinates form a sequence"),
            )
            .expect("test coordinates form a closed ring"),
            Vec::new(),
        )
    }

    #[test]
    fn a_constructed_chord_is_not_recovered_from_a_repeated_source_edge_key() {
        let a = point(0.0, 0.0, 10.0);
        let b = point(2.0, 0.0, 20.0);
        let source = polygon(&[
            a,
            point(1.0, 0.1, 15.0),
            b,
            point(0.0, 0.0, 10.0),
            point(2.0, 0.0, 200.0),
            point(2.0, 2.0, 30.0),
            a,
        ]);
        let simplified = polygon(&[a, b, point(2.0, 2.0, 30.0), a]);

        let roles = polygon_edge_roles(&source, &simplified)
            .expect("polygon role analysis succeeds")
            .expect("the rings have matching layouts");
        let shortcut_role = roles
            .first()
            .and_then(|ring| ring.first())
            .copied()
            .expect("the simplified shell has an A-B edge");

        assert!(
            shortcut_role,
            "the A-B shortcut is constructed even though another source occurrence has the same XY key"
        );
    }
}
