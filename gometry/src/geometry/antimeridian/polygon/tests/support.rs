fn line(xs: &[f64], ys: &[f64]) -> Shape {
    Shape::LineString(
        LineSeq::try_new(CoordSeq::try_from_vecs(xs.to_vec(), ys.to_vec(), None, None).unwrap())
            .unwrap(),
    )
}

fn closed_ring(xs: &[f64], ys: &[f64]) -> Ring {
    Ring::closed_coordseq(CoordSeq::try_from_vecs(xs.to_vec(), ys.to_vec(), None, None).unwrap())
        .unwrap()
}

fn xyzm_ring(coordinates: &[(f64, f64, f64, f64)]) -> Ring {
    let points = coordinates
        .iter()
        .map(|&(x, y, z, m)| Point::new_axes(x, y, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap())
        .collect::<Vec<_>>();
    Ring::closed_coordseq(CoordSeq::try_from_points(&points).unwrap()).unwrap()
}

fn independent_meridian_fraction(
    parameter: f64,
    start_degrees: f64,
    end_degrees: f64,
    flattening: f64,
) -> f64 {
    fn integrate(limit: f64, start_degrees: f64, delta_radians: f64, flattening: f64) -> f64 {
        let intervals = 4096;
        let step = limit / f64::from(intervals);
        let eccentricity_squared = flattening * (2.0 - flattening);
        // Keep accumulation error below the Simpson refinement error.
        let mut weighted = 0.0;
        let mut correction = 0.0;
        for index in 0..=intervals {
            let parameter = f64::from(index) * step;
            let latitude = (start_degrees + parameter * delta_radians.to_degrees()).to_radians();
            let denominator = 1.0 - eccentricity_squared * latitude.sin() * latitude.sin();
            let radius = (1.0 - eccentricity_squared) / denominator.powf(1.5);
            let weight = if index == 0 || index == intervals {
                1.0
            } else if index % 2 == 0 {
                2.0
            } else {
                4.0
            };
            let term = weight * radius * delta_radians - correction;
            let next = weighted + term;
            correction = (next - weighted) - term;
            weighted = next;
        }
        step * weighted / 3.0
    }
    let delta_radians = (end_degrees - start_degrees).to_radians();
    integrate(parameter, start_degrees, delta_radians, flattening)
        / integrate(1.0, start_degrees, delta_radians, flattening)
}

fn ulp_error_limit(value: f64, ulps: f64) -> f64 {
    let magnitude = value.abs();
    let spacing = if magnitude == 0.0 {
        f64::from_bits(1)
    } else {
        magnitude.next_up() - magnitude
    };
    ulps * spacing
}

fn split_crs84(shape: &Shape) -> Shape {
    shape
        .split_antimeridian_in_frame(GeographicFrame::crs84())
        .unwrap()
}

fn polygon(shell: &[(f64, f64)], holes: &[&[(f64, f64)]]) -> Shape {
    let ring = |points: &[(f64, f64)]| {
        let mut xs: Vec<f64> = points.iter().map(|&(x, _)| x).collect();
        let mut ys: Vec<f64> = points.iter().map(|&(_, y)| y).collect();
        xs.push(xs[0]);
        ys.push(ys[0]);
        closed_ring(&xs, &ys)
    };
    Shape::Polygon(Polygon::new(
        ring(shell),
        holes.iter().map(|hole| ring(hole)).collect(),
    ))
}

/// Every spelling of the seam vertices names one polygon, so each must
/// split into valid parts covering the same chart area.
fn assert_seam_spellings_split_alike(
    shell: &[(f64, f64)],
    holes: &[&[(f64, f64)]],
    parts: usize,
    area: f64,
) {
    let seam: Vec<(usize, usize)> = std::iter::once(shell)
        .chain(holes.iter().copied())
        .enumerate()
        .flat_map(|(ring, points)| {
            points
                .iter()
                .enumerate()
                .filter(|(_, (x, _))| x.abs() == 180.0)
                .map(move |(vertex, _)| (ring, vertex))
        })
        .collect();
    for spelling in 0..1_u32 << seam.len() {
        let mut rings: Vec<Vec<(f64, f64)>> = std::iter::once(shell)
            .chain(holes.iter().copied())
            .map(<[_]>::to_vec)
            .collect();
        for (bit, &(ring, vertex)) in seam.iter().enumerate() {
            rings[ring][vertex].0 = if (spelling >> bit) & 1 == 1 {
                180.0
            } else {
                -180.0
            };
        }
        let hole_rings: Vec<&[(f64, f64)]> = rings[1..].iter().map(Vec::as_slice).collect();
        let split = split_crs84(&polygon(&rings[0], &hole_rings));
        assert_eq!(
            split.validate().unwrap(),
            None,
            "spelling {spelling:b}: {split:?}"
        );
        assert_eq!(
            split.part_count(),
            parts,
            "spelling {spelling:b}: {split:?}"
        );
        assert!(
            (split.area() - area).abs() < 1e-9,
            "spelling {spelling:b}: {split:?}"
        );
    }
}
