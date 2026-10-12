use super::earcut::{EarcutCornerId, EarcutOccurrenceId, EarcutSiteId};
use super::prepared_mesh::{
    MeshEdgeRole, MeshProducer, MeshSiteLocation, MeshUseId, PreparedMeshBoundaryUse,
    PreparedMeshFace, PreparedMeshPublication, PreparedMeshSite,
};
use crate::error::Result;
use crate::geometry::{
    Coordinates as _, ExpansionBudget, GeometryErrorKind, HashMap, Polygon, Polygonal, Shape,
};

pub(in crate::geometry::tessellation) fn prepare_earcut_publication(
    shape: &Shape,
    budget: &mut ExpansionBudget,
) -> Result<PreparedMeshPublication> {
    let mut publication =
        PreparedMeshPublication::new(MeshProducer::Earcut, Vec::new(), Vec::new(), Vec::new());
    let mut source_offset = 0_usize;
    match shape
        .polygonal()
        .ok_or(GeometryErrorKind::PolygonRequired)?
    {
        Polygonal::Polygon(polygon) => {
            append_earcut_polygon(&mut publication, polygon, &mut source_offset, budget)?
        },
        Polygonal::MultiPolygon(polygons) => {
            for polygon in polygons.iter() {
                append_earcut_polygon(&mut publication, polygon, &mut source_offset, budget)?;
            }
        },
        Polygonal::Empty => {},
    }
    Ok(publication)
}

fn append_earcut_polygon(
    publication: &mut PreparedMeshPublication,
    polygon: &Polygon,
    source_offset: &mut usize,
    budget: &mut ExpansionBudget,
) -> Result<()> {
    let mut source_ranges = Vec::new();
    let mut next_source = *source_offset;
    for ring in polygon.rings() {
        let open_count = ring
            .coord_count()
            .checked_sub(1)
            .expect("admitted polygon rings contain their closing coordinate");
        let source_end = next_source
            .checked_add(open_count)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        source_ranges.push(next_source..source_end);
        next_source = source_end;
    }

    let first_face = publication.faces.len();
    let mut occurrences = HashMap::<EarcutOccurrenceId, (EarcutSiteId, usize)>::default();
    let mut emitted_uses =
        HashMap::<(usize, usize), ((EarcutCornerId, EarcutCornerId), MeshUseId)>::default();

    super::earcut::earcut_polygon_with_ids(polygon, &mut |triangle| {
        let local_face = publication.faces.len().wrapping_sub(first_face);
        if triangle.face != local_face {
            return Err(GeometryErrorKind::Invalid(
                "earcut face identities are not in emission order".into(),
            )
            .into());
        }
        budget.check_additional(4)?;
        let face_id = publication.faces.len();
        let [first_point, second_point, third_point] = triangle.points;
        let mut output_sites = [0_usize; 3];
        for (corner_index, (corner, point)) in triangle
            .corners
            .iter()
            .copied()
            .zip(triangle.points.iter().copied())
            .enumerate()
        {
            let source_range = source_ranges.get(corner.site.ring).ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "earcut corner is outside its source ring inventory".into(),
                )
            })?;
            let source_ordinal = source_range
                .start
                .checked_add(corner.site.vertex)
                .filter(|&ordinal| ordinal < source_range.end)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "earcut corner is outside its source ring inventory".into(),
                    )
                })?;
            let producer_ordinal = match occurrences.get(&corner.occurrence) {
                Some(&(site, producer_ordinal)) => {
                    if site != corner.site
                        || publication
                            .sites
                            .get(producer_ordinal)
                            .is_none_or(|prepared| {
                                prepared.source_ordinal != source_ordinal
                                    || prepared.record != point
                            })
                    {
                        return Err(GeometryErrorKind::Invalid(
                            "earcut occurrence changed its source site".into(),
                        )
                        .into());
                    }
                    producer_ordinal
                },
                None => {
                    let producer_ordinal = publication.sites.len();
                    publication.sites.push(PreparedMeshSite::source(
                        producer_ordinal,
                        source_ordinal,
                        point,
                        MeshSiteLocation::Cartesian,
                    ));
                    occurrences.insert(corner.occurrence, (corner.site, producer_ordinal));
                    producer_ordinal
                },
            };
            *output_sites.get_mut(corner_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("earcut face has an invalid corner".into())
            })? = producer_ordinal;
        }

        let output_start = publication.points.len();
        publication.points.reserve(4);
        publication.points.extend_from_slice(&[
            first_point,
            second_point,
            third_point,
            first_point,
        ]);
        let output_end = output_start
            .checked_add(4)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;

        let [first_site, _, _] = output_sites;
        let placeholder = PreparedMeshBoundaryUse::new(
            0,
            MeshUseId::new(face_id, 0),
            first_site,
            first_site,
            None,
            false,
            false,
            MeshEdgeRole::EarcutBoundary,
        );
        let mut boundary_uses = [placeholder; 3];
        for (side, &start_corner) in triangle.corners.iter().enumerate() {
            let end_corner = triangle
                .corners
                .get(side.wrapping_add(1) % triangle.corners.len())
                .copied()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("earcut face has an invalid side".into())
                })?;
            let use_id = MeshUseId::new(
                face_id,
                u8::try_from(side).expect("an earcut face has three sides"),
            );
            let start_site = *output_sites.get(side).ok_or_else(|| {
                GeometryErrorKind::Invalid("earcut face has an invalid side".into())
            })?;
            let end_side = side.wrapping_add(1) % output_sites.len();
            let end_site = *output_sites.get(end_side).ok_or_else(|| {
                GeometryErrorKind::Invalid("earcut face has an invalid side".into())
            })?;
            let start_source = publication
                .sites
                .get(start_site)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("earcut side start is outside its site table".into())
                })?
                .source_ordinal;
            let end_source = publication
                .sites
                .get(end_site)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("earcut side end is outside its site table".into())
                })?
                .source_ordinal;
            let side_key = (start_source, end_source);
            let reverse_key = (end_source, start_source);
            let mut role = MeshEdgeRole::EarcutBoundary;
            let reverse = if let Some(((reverse_start, reverse_end), reverse)) =
                emitted_uses.remove(&reverse_key)
            {
                let diagonal = reverse_start == end_corner && reverse_end == start_corner;
                let bridge = reverse_start.site == end_corner.site
                    && reverse_end.site == start_corner.site
                    && reverse_start.occurrence != end_corner.occurrence
                    && reverse_end.occurrence != start_corner.occurrence;
                if !diagonal && !bridge {
                    return Err(GeometryErrorKind::Invalid(
                        "earcut reciprocal sides do not share selected occurrences or bridge sites"
                            .into(),
                    )
                    .into());
                }
                let paired_role = if bridge {
                    MeshEdgeRole::EarcutBridge
                } else {
                    MeshEdgeRole::EarcutDiagonal
                };
                let previous_face = publication.faces.get_mut(reverse.face).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "earcut reciprocal use references an unknown face".into(),
                    )
                })?;
                let previous = previous_face
                    .boundary_uses
                    .get_mut(usize::from(reverse.side))
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "earcut reciprocal use references an unknown side".into(),
                        )
                    })?;
                if previous.reverse.is_some() {
                    return Err(GeometryErrorKind::Invalid(
                        "earcut side has more than one reciprocal use".into(),
                    )
                    .into());
                }
                previous.reverse = Some(use_id);
                previous.role = paired_role;
                role = paired_role;
                Some(reverse)
            } else {
                if emitted_uses
                    .insert(side_key, ((start_corner, end_corner), use_id))
                    .is_some()
                {
                    return Err(GeometryErrorKind::Invalid(
                        "earcut emits a directed side more than once".into(),
                    )
                    .into());
                }
                None
            };
            *boundary_uses.get_mut(side).ok_or_else(|| {
                GeometryErrorKind::Invalid("earcut face has an invalid side".into())
            })? = PreparedMeshBoundaryUse::new(
                u8::try_from(side).expect("an earcut face has three sides"),
                use_id,
                start_site,
                end_site,
                reverse,
                false,
                false,
                role,
            );
        }
        publication.faces.push(PreparedMeshFace::new(
            face_id,
            output_sites,
            output_sites,
            [0, 1, 2],
            false,
            output_start..output_end,
            &boundary_uses,
        ));
        budget.add(4)?;
        Ok(())
    })?;
    *source_offset = next_source;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::prepare_earcut_publication;
    use crate::geometry::tessellation::{MeshEdgeRole, MeshProducer};
    use crate::geometry::{ExpansionBudget, MOrdinate, Point, Polygon, Ring, Shape, ZOrdinate};

    fn xy(x: f64, y: f64) -> Point {
        Point::new(x, y).expect("finite test coordinate")
    }

    fn xyz(x: f64, y: f64, z: f64) -> Point {
        Point::new_axes(x, y, ZOrdinate(Some(z)), MOrdinate(None)).expect("finite test coordinate")
    }

    fn polygon(shell: Vec<Point>, holes: Vec<Vec<Point>>) -> Polygon {
        Polygon::new(
            Ring::closed(shell).expect("valid test shell"),
            holes
                .into_iter()
                .map(|hole| Ring::closed(hole).expect("valid test hole"))
                .collect(),
        )
    }

    #[test]
    fn earcut_mesh_publication_keeps_face_sides_and_bridge_twins() {
        let source = Shape::Polygon(polygon(
            vec![
                xyz(0.0, 0.0, 1.0),
                xyz(4.0, 0.0, 2.0),
                xyz(8.0, 0.0, 3.0),
                xyz(8.0, 8.0, 4.0),
                xyz(0.0, 8.0, 5.0),
                xyz(0.0, 0.0, 1.0),
            ],
            vec![vec![
                xy(2.0, 2.0),
                xy(2.0, 4.0),
                xy(4.0, 4.0),
                xy(4.0, 2.0),
                xy(2.0, 2.0),
            ]],
        ));
        let mut budget = ExpansionBudget::new("triangulate", "method");
        let publication = prepare_earcut_publication(&source, &mut budget)
            .expect("public Earcut selection has a prepared mesh product");

        assert_eq!(publication.producer, MeshProducer::Earcut);
        assert_eq!(publication.faces.len(), 8);
        assert_eq!(publication.points.len(), publication.faces.len() * 4);
        let output_shapes = Shape::earcut_triangle_shapes_from_publication(&publication)
            .expect("published ears retain their per-face common axes");
        assert_eq!(output_shapes.len(), publication.faces.len());
        let mut bridge_uses = 0;
        let mut diagonal_uses = 0;
        for (face_index, (face, output_shape)) in
            publication.faces.iter().zip(&output_shapes).enumerate()
        {
            assert_eq!(face.selected_face, face_index);
            assert_eq!(face.output_vertices, face_index * 4..face_index * 4 + 4);
            assert_eq!(face.output_to_selected, [0, 1, 2]);
            assert!(!face.reversed);
            assert_eq!(
                output_shape.axes().has_z(),
                face.output_sites.iter().all(|&site| {
                    publication
                        .sites
                        .get(site)
                        .expect("selected face site belongs to the occurrence table")
                        .record
                        .axes
                        .has_z()
                })
            );
            let output = publication
                .points
                .get(face.output_vertices.clone())
                .expect("each selected face owns its emitted ring");
            assert_eq!(output.len(), 4);
            assert_eq!(output.first(), output.last());
            for (side, boundary) in face.boundary_uses.iter().enumerate() {
                assert_eq!(usize::from(boundary.output_side), side);
                assert_eq!(boundary.selected_use.face, face.selected_face);
                assert_eq!(
                    boundary.start_site,
                    *face.output_sites.get(side).expect("three face corners")
                );
                assert_eq!(
                    boundary.end_site,
                    *face
                        .output_sites
                        .get(side.wrapping_add(1) % 3)
                        .expect("three face corners")
                );
                match boundary.role {
                    MeshEdgeRole::EarcutBoundary => assert!(boundary.reverse.is_none()),
                    MeshEdgeRole::EarcutDiagonal | MeshEdgeRole::EarcutBridge => {
                        let reverse = boundary
                            .reverse
                            .expect("every selected internal side has its reciprocal use");
                        let reciprocal = publication
                            .faces
                            .get(reverse.face)
                            .and_then(|face| face.boundary_uses.get(usize::from(reverse.side)))
                            .expect("reciprocal use belongs to a selected face");
                        assert_eq!(reciprocal.reverse, Some(boundary.selected_use));
                        assert_eq!(reciprocal.role, boundary.role);
                        let start = publication
                            .sites
                            .get(boundary.start_site)
                            .expect("side start belongs to the occurrence table");
                        let end = publication
                            .sites
                            .get(boundary.end_site)
                            .expect("side end belongs to the occurrence table");
                        let reverse_start = publication
                            .sites
                            .get(reciprocal.start_site)
                            .expect("twin start belongs to the occurrence table");
                        let reverse_end = publication
                            .sites
                            .get(reciprocal.end_site)
                            .expect("twin end belongs to the occurrence table");
                        assert_eq!(reverse_start.source_ordinal, end.source_ordinal);
                        assert_eq!(reverse_end.source_ordinal, start.source_ordinal);
                        match boundary.role {
                            MeshEdgeRole::EarcutDiagonal => diagonal_uses += 1,
                            MeshEdgeRole::EarcutBridge => bridge_uses += 1,
                            MeshEdgeRole::Delaunay | MeshEdgeRole::EarcutBoundary => {
                                unreachable!("the twin roles were matched above")
                            },
                        }
                    },
                    MeshEdgeRole::Delaunay => {
                        unreachable!("Earcut does not issue Delaunay side roles")
                    },
                }
            }
        }
        assert_eq!(bridge_uses, 2, "one bridge has two directed occurrences");
        assert_eq!(
            diagonal_uses, 14,
            "seven triangulation diagonals have twins"
        );
    }
}
