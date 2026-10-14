use super::{
    CoverageEdgeIntent, CoverageEdgeRoles, GeometryErrorKind, Polygon, Result, Shape,
    SimplifyMethod, SourceArcContext, TopologyDomain,
};
use crate::geometry::constructive::PolygonalSimplificationPlan;

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
        let (output, plan) =
            source.simplify_geographic_with_plan(tolerance, method, frame.longitude())?;
        let edge_roles = geographic_edge_roles_from_plan(&output, &plan)?;
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

fn edge_intents_from_kept_vertices(kept_vertices: &[usize]) -> Vec<CoverageEdgeIntent> {
    kept_vertices
        .windows(2)
        .map(|pair| match pair {
            [left, right] if left.checked_add(1) == Some(*right) => CoverageEdgeIntent::Source,
            _ => CoverageEdgeIntent::Constructed,
        })
        .collect()
}

fn polygon_edge_intents_from_plan(
    polygon: &Polygon,
    ring_plans: &[Vec<usize>],
) -> Result<Vec<Vec<CoverageEdgeIntent>>> {
    if polygon.rings().count() != ring_plans.len() {
        return Err(GeometryErrorKind::Invalid(
            "coverage simplification plan does not match the polygon rings".into(),
        )
        .into());
    }
    polygon
        .rings()
        .zip(ring_plans)
        .map(|(ring, kept_vertices)| {
            if ring.len() != kept_vertices.len() {
                return Err(GeometryErrorKind::Invalid(
                    "coverage simplification plan does not match a ring's retained vertices".into(),
                )
                .into());
            }
            Ok(edge_intents_from_kept_vertices(kept_vertices))
        })
        .collect()
}

fn geographic_edge_roles_from_plan(
    shape: &Shape,
    plan: &PolygonalSimplificationPlan,
) -> Result<CoverageEdgeRoles> {
    match shape {
        Shape::Polygon(polygon) => {
            let [ring_plans] = plan.polygons.as_slice() else {
                return Err(GeometryErrorKind::Invalid(
                    "coverage polygon simplification plan has the wrong polygon count".into(),
                )
                .into());
            };
            Ok(vec![polygon_edge_intents_from_plan(polygon, ring_plans)?])
        },
        Shape::MultiPolygon(polygons) => {
            if polygons.len() != plan.polygons.len() {
                return Err(GeometryErrorKind::Invalid(
                    "coverage multipolygon simplification plan has the wrong polygon count".into(),
                )
                .into());
            }
            polygons
                .as_slice()
                .iter()
                .zip(&plan.polygons)
                .map(|(polygon, ring_plans)| polygon_edge_intents_from_plan(polygon, ring_plans))
                .collect()
        },
        Shape::Empty(..) if plan.polygons.is_empty() => Ok(Vec::new()),
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..) => Err(GeometryErrorKind::Invalid(
            "coverage simplification plan does not match a polygonal output".into(),
        )
        .into()),
    }
}

pub(super) fn publish_coverage_edge_roles(
    shape: Shape,
    edge_roles: CoverageEdgeRoles,
    frame: crate::geographic::GeographicFrame,
    source_context: &SourceArcContext<'_, '_, crate::crs::GeodesicMetric>,
    budget: &mut crate::geometry::ExpansionBudget,
) -> Result<Shape> {
    let computed_edge_flags = |roles: Vec<Vec<CoverageEdgeIntent>>| {
        roles
            .into_iter()
            .map(|ring| {
                ring.into_iter()
                    .map(|edge| matches!(edge, CoverageEdgeIntent::Constructed))
                    .collect()
            })
            .collect::<Vec<Vec<bool>>>()
    };
    match shape {
        Shape::Polygon(polygon) => {
            let mut edge_roles = edge_roles.into_iter();
            let roles = edge_roles.next().ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "coverage polygon roles do not match the rebuilt polygon".into(),
                )
            })?;
            if edge_roles.next().is_some() {
                return Err(GeometryErrorKind::Invalid(
                    "coverage polygon has unused simplification edge roles".into(),
                )
                .into());
            }
            let computed_edges = computed_edge_flags(roles);
            Ok(Shape::Polygon(
                crate::geometry::publish_polygon_with_edge_roles_and_context(
                    &polygon,
                    &computed_edges,
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
                let computed_edges = computed_edge_flags(roles);
                crate::geometry::publish_polygon_with_edge_roles_and_context(
                    polygon,
                    &computed_edges,
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
    use super::{CoverageEdgeIntent, geographic_edge_roles_from_plan};
    use crate::geographic::GeographicFrame;
    use crate::geometry::{CoordSeq, Point, Polygon, Ring, Shape, SimplifyMethod, ZOrdinate};

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
    fn a_constructed_chord_keeps_its_plan_role_when_xy_matches_another_source_edge() {
        let a = point(0.0, 0.0, 10.0);
        let source = Shape::Polygon(polygon(&[
            a,
            point(1.0, 0.1, 15.0),
            point(2.0, 0.0, 20.0),
            point(0.0, 0.0, 10.0),
            point(2.0, 0.0, 200.0),
            point(2.0, 2.0, 30.0),
            a,
        ]));
        let (simplified, plan) = source
            .simplify_geographic_with_plan(
                1.0,
                SimplifyMethod::Vw,
                GeographicFrame::crs84().longitude(),
            )
            .expect("the geographic polygon simplification plan is valid");
        let roles = geographic_edge_roles_from_plan(&simplified, &plan)
            .expect("the plan follows the simplified ring layout");
        let shortcut_role = roles
            .first()
            .and_then(|polygon| polygon.first())
            .and_then(|ring| ring.first())
            .copied()
            .expect("the simplified shell starts with its constructed chord");

        assert_eq!(
            shortcut_role,
            CoverageEdgeIntent::Constructed,
            "the A-B shortcut has a different source occurrence with the same undirected XY and a different Z"
        );
    }
}
