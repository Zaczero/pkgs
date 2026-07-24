use super::*;

pub(crate) fn probe_interior_faces(
    pool: &OperandPool,
    computer: &mut TopologyComputer<'_>,
    testers: AreaTesters<'_>,
) -> bool {
    for ring in pool
        .rings
        .iter()
        .filter(|ring| ring.operand == Operand::Left && ring.ring == 0)
    {
        let Some(point) =
            polygon_interior_probe(pool, Operand::Left, ring.polygon, Operand::Right, testers)
        else {
            return false;
        };
        match classify_area_point(pool, Operand::Right, point, testers) {
            Loc::Interior => computer.add_cell(0, 0, 2),
            Loc::Exterior => computer.add_cell(0, 2, 2),
            Loc::Boundary => return false,
        }
        if computer.decided() {
            return true;
        }
    }
    for ring in pool
        .rings
        .iter()
        .filter(|ring| ring.operand == Operand::Right && ring.ring == 0)
    {
        let Some(point) =
            polygon_interior_probe(pool, Operand::Right, ring.polygon, Operand::Left, testers)
        else {
            return false;
        };
        match classify_area_point(pool, Operand::Left, point, testers) {
            Loc::Interior => computer.add_cell(0, 0, 2),
            Loc::Exterior => computer.add_cell(2, 0, 2),
            Loc::Boundary => return false,
        }
        if computer.decided() {
            return true;
        }
    }
    true
}

pub(crate) fn classify_area_point(
    pool: &OperandPool,
    operand: Operand,
    point: XY,
    testers: AreaTesters<'_>,
) -> Loc {
    if let Some(tester) = testers.for_operand(operand)
        && let Some(class) = tester.classify_area_point(point.point())
    {
        return match class {
            RingClass::Interior => Loc::Interior,
            RingClass::Boundary => Loc::Boundary,
            RingClass::Exterior => Loc::Exterior,
        };
    }
    if other_contains(pool, operand, point) {
        Loc::Interior
    } else if operand_covers_boundary(pool, operand, point) {
        Loc::Boundary
    } else {
        Loc::Exterior
    }
}

pub(crate) fn polygon_interior_probe(
    pool: &OperandPool,
    operand: Operand,
    polygon: u32,
    avoid_boundary: Operand,
    testers: AreaTesters<'_>,
) -> Option<XY> {
    let off_other_boundary = |point: XY| {
        testers.for_operand(avoid_boundary).map_or_else(
            || !operand_covers_boundary(pool, avoid_boundary, point),
            |tester| {
                !matches!(
                    tester.classify_area_point(point.point()),
                    Some(RingClass::Boundary)
                )
            },
        )
    };
    // Fast path: the polygon's cached representative interior point (computed
    // once at staging) is the SAME point the scanline below would pick first,
    // so when it is off the other operand's boundary — the common case — return
    // it directly with no per-call scanline.
    if let Some(cached) = pool
        .rings
        .iter()
        .find(|ring| ring.operand == operand && ring.polygon == polygon && ring.ring == 0)
        .and_then(|ring| ring.probe)
        && off_other_boundary(cached)
    {
        return Some(cached);
    }
    let rings: Vec<&[XY]> = pool
        .rings
        .iter()
        .filter(|ring| ring.operand == operand && ring.polygon == polygon)
        .map(|ring| ring.points.as_ref())
        .collect();
    let shell = *rings.first()?;
    let mut miny = f64::INFINITY;
    let mut maxy = f64::NEG_INFINITY;
    for point in shell {
        miny = miny.min(point.y);
        maxy = maxy.max(point.y);
    }
    let y = f64::midpoint(miny, maxy);
    let mut crossings = Vec::with_capacity(rings.iter().map(|ring| ring.len()).sum());
    for ring in &rings {
        let n = ring.len();
        for index in 0..n {
            let next = wrap_index(index + 1, n);
            let (y0, y1) = (ring[index].y, ring[next].y);
            if (y0 <= y) != (y1 <= y) {
                let t = (y - y0) / (y1 - y0);
                crossings.push(ring[index].x + t * (ring[next].x - ring[index].x));
            }
        }
    }
    if crossings.len() < 2 || !crossings.len().is_multiple_of(2) {
        return None;
    }
    crossings.sort_unstable_by(f64::total_cmp);
    crossings.as_chunks::<2>().0.iter().find_map(|span| {
        if span[1] <= span[0] {
            return None;
        }
        [0.5, 0.25, 0.75].into_iter().find_map(|weight| {
            let point = XY::new(span[0] * (1.0 - weight) + span[1] * weight, y);
            // Confirm the candidate is interior to this operand via its cached
            // banded raycaster (O(band)); only fall back to the O(n) ring scan
            // when no tester is prepared.
            let inside = testers.for_operand(operand).map_or_else(
                || topology::polygon_rings_contain_interior(rings.iter().copied(), point),
                |tester| {
                    matches!(
                        tester.classify_area_point(point.point()),
                        Some(RingClass::Interior)
                    )
                },
            );
            (inside && off_other_boundary(point)).then_some(point)
        })
    })
}
