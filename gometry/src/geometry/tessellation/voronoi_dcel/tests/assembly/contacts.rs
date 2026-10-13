fn with_part_cycles<T>(
    subdivision: &ExactSubdivision,
    part: &ExactCellPart,
    visit: impl FnOnce(&[&[exact::ExactPoint]], &[NodedExactCycle<'_>]) -> Result<T>,
) -> Result<T> {
    let rings: Vec<_> = std::iter::once(&part.shell)
        .chain(part.holes.iter())
        .collect();
    let mut owned_cycles = Vec::with_capacity(rings.len());
    for ring in &rings {
        let (_, vertices) = ring
            .vertices
            .split_last()
            .expect("assembled rings are closed");
        let mut points = Vec::with_capacity(vertices.len());
        points.extend(
            vertices
                .iter()
                .map(|&vertex| subdivision.vertex(vertex).point.clone()),
        );
        owned_cycles.push(points);
    }
    let point_cycles: Vec<_> = owned_cycles.iter().map(Vec::as_slice).collect();
    let noded_cycles: Vec<_> = owned_cycles
        .iter()
        .zip(&rings)
        .map(|(points, ring)| NodedExactCycle { points, ring })
        .collect();
    visit(&point_cycles, &noded_cycles)
}

#[derive(Clone, Copy, Debug, Default)]
struct AssembledContactWork {
    edges: usize,
    endpoint_pairs: usize,
    exact_contacts: usize,
    touch_fans: usize,
    incidence_records: usize,
    aabb_pairs: usize,
}

fn actual_producer_contact_work(sites: &[Site], clip: &Polygon) -> Result<AssembledContactWork> {
    let subdivision = grafted_clip_subdivision(sites, clip);
    let parts = assemble_cells(&subdivision)?;
    let mut work = AssembledContactWork::default();
    for part in &parts {
        with_part_cycles(&subdivision, part, |cycles, noded_cycles| {
            let index = NodedSegmentIndex::new(noded_cycles)?;
            work.edges = work
                .edges
                .checked_add(index.endpoints.len())
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            work.incidence_records = work
                .incidence_records
                .checked_add(index.incidences.len())
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            work.touch_fans = work
                .touch_fans
                .checked_add(for_each_touch_fan(&index, |_, _, _, _, _| Ok(()))?)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            for_each_assembled_pair(cycles, &index, |_, contact| {
                work.endpoint_pairs = work
                    .endpoint_pairs
                    .checked_add(1)
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
                if contact.is_some() {
                    work.exact_contacts = work
                        .exact_contacts
                        .checked_add(1)
                        .ok_or(crate::error::ResourceError::SizeOverflow)?;
                }
                Ok(())
            })?;

            let mut boxes = Vec::with_capacity(index.endpoints.len());
            for cycle in cycles {
                for (start, end) in cycle.iter().zip(cycle.iter().cycle().skip(1)) {
                    let endpoints: [&exact::ExactPoint; 2] = (start, end).into();
                    boxes.push(rounded_box(endpoints)?);
                }
            }
            let mut aabb_pairs = Vec::new();
            let _completion = for_each_indexed_overlapping_bounds_pair(&boxes, |left, right| {
                aabb_pairs.push([left, right]);
                std::ops::ControlFlow::<()>::Continue(())
            })?;
            work.aabb_pairs = work
                .aabb_pairs
                .checked_add(aabb_pairs.len())
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            Ok(())
        })?;
    }
    Ok(work)
}

#[test]
fn actual_voronoi_ring_contacts_match_the_exhaustive_oracle() {
    let holes = (0..8)
        .map(|index| {
            let x = 2.0 + f64::from(index) * 2.0;
            closed_ring(&[(x, 10.0), (x + 0.25, 9.5), (x - 0.25, 9.5)])
        })
        .collect();
    let clip = Polygon::new(rectangle(0.0, 0.0, 20.0, 10.0), holes);
    assert!(polygon_is_valid(&clip).unwrap());
    let sites = sites_xy(&[(5.0, 5.0), (15.0, 5.0), (10.0, 7.0)]);
    let subdivision = grafted_clip_subdivision(&sites, &clip);
    let parts = assemble_cells(&subdivision).unwrap();
    let mut contact_count = 0;

    for part in &parts {
        with_part_cycles(&subdivision, part, |point_cycles, noded_cycles| {
            validate_assembled_exact_cycles(&subdivision, noded_cycles, 0, || {
                validate_exact_cycle_nesting(point_cycles, 0)
            })?;
            let expected = validate_exact_cycles(point_cycles, 0)?.contacts;
            let index = NodedSegmentIndex::new(noded_cycles)?;
            let mut actual = Vec::new();
            for_each_assembled_pair(point_cycles, &index, |segments, contact| {
                if let Some(contact) = contact {
                    assert_eq!(contact.segments, segments);
                    actual.push(contact);
                }
                Ok(())
            })?;
            assert_exact_contacts_equal(&actual, &expected);
            assert!(
                actual
                    .array_windows::<2>()
                    .all(|[left, right]| left.segments < right.segments)
            );
            contact_count += actual.len();
            Ok(())
        })
        .unwrap();
    }
    assert!(contact_count > 0);
}

#[test]
fn actual_producer_pair_work_scales_with_ring_contacts() {
    let tangent_sites = sites_xy(&[(5.0, 5.0), (15.0, 5.0), (10.0, 7.0)]);
    let tangent_clip = |count: usize| {
        let holes = (0..count)
            .map(|index| {
                let ordinal =
                    f64::from(u32::try_from(index).expect("the fixture has fewer than u32 holes"));
                let x = 2.0 + ordinal;
                closed_ring(&[(x, 10.0), (x + 0.25, 9.5), (x - 0.25, 9.5)])
            })
            .collect();
        Polygon::new(rectangle(0.0, 0.0, 20.0, 10.0), holes)
    };
    let tangent_small = actual_producer_contact_work(&tangent_sites, &tangent_clip(8)).unwrap();
    let tangent_large = actual_producer_contact_work(&tangent_sites, &tangent_clip(16)).unwrap();
    for work in [tangent_small, tangent_large] {
        assert_eq!(
            work.incidence_records,
            work.edges.checked_mul(2).unwrap(),
            "{work:?}"
        );
        assert_eq!(
            work.endpoint_pairs,
            work.edges.checked_add(work.exact_contacts).unwrap(),
            "{work:?}"
        );
        assert!(work.exact_contacts > 0);
        assert!(work.touch_fans > 0);
    }
    assert!(tangent_large.endpoint_pairs > tangent_small.endpoint_pairs);

    let thin_sites = sites_xy(&[(16.0, 1.0), (16.0, 1_000_000.0), (1_000_000.0, 1.0)]);
    let thin_clip = |count: usize| {
        let holes = (0..count)
            .map(|index| {
                let ordinal =
                    f64::from(u32::try_from(index).expect("the fixture has fewer than u32 holes"));
                let base = 0.5 + ordinal / 128.0;
                let slope = 1.0 / 32.0 + ordinal / 32_768.0;
                let thickness = 1.0 / 1_024.0;
                closed_ring(&[
                    (1.0, base),
                    (31.0, base + slope * 30.0),
                    (31.0, base + slope * 30.0 + thickness),
                    (1.0, base + thickness),
                ])
            })
            .collect();
        Polygon::new(rectangle(0.0, 0.0, 32.0, 2.0), holes)
    };
    let thin_small = actual_producer_contact_work(&thin_sites, &thin_clip(8)).unwrap();
    let thin_large = actual_producer_contact_work(&thin_sites, &thin_clip(16)).unwrap();
    for work in [thin_small, thin_large] {
        assert_eq!(
            work.incidence_records,
            work.edges.checked_mul(2).unwrap(),
            "{work:?}"
        );
        assert_eq!(work.endpoint_pairs, work.edges, "{work:?}");
        assert_eq!(work.exact_contacts, 0, "{work:?}");
        assert_eq!(work.touch_fans, 0, "{work:?}");
        assert!(work.aabb_pairs > work.endpoint_pairs, "{work:?}");
    }
    assert!(
        thin_large.aabb_pairs > thin_small.aabb_pairs.checked_mul(2).unwrap(),
        "{thin_small:?} -> {thin_large:?}"
    );
    assert!(
        thin_large.endpoint_pairs < thin_small.endpoint_pairs.checked_mul(3).unwrap(),
        "{thin_small:?} -> {thin_large:?}"
    );
}
